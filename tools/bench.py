"""Latency and load harness for the lab.

latency    run one query N times (after warm-up) and report percentiles.
           %(tenant)s in the SQL is replaced per run by a seeded random tenant.
load       K threads for T seconds running a read/write mix against Postgres
           or Citus; every operation is logged with its wall-clock time.
summarize  cut a load log into windows (before / during / after a pair of
           marks, e.g. a rebalance) and report percentiles and errors.

  python tools/bench.py latency --dsn $DSN --sql "SELECT ..." --runs 30
  python tools/bench.py latency --clickhouse http://localhost:8123 --sql "SELECT ..."
  python tools/bench.py load --dsn $DSN --threads 8 --seconds 300 --out load.csv
  python tools/bench.py summarize --load load.csv --marks marks.json
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np


def percentiles(ms: list[float]) -> dict[str, float]:
    if not ms:
        return {"n": 0}
    a = np.asarray(ms)
    return {
        "n": len(ms),
        "p50": round(float(np.percentile(a, 50)), 3),
        "p95": round(float(np.percentile(a, 95)), 3),
        "p99": round(float(np.percentile(a, 99)), 3),
        "mean": round(float(a.mean()), 3),
        "min": round(float(a.min()), 3),
        "max": round(float(a.max()), 3),
    }


def parse_range(spec: str) -> tuple[int, int]:
    lo, hi = spec.split("..")
    return int(lo), int(hi)


# ── latency ───────────────────────────────────────────────────────────────────
def latency_pg(
    dsn: str,
    sql: str,
    runs: int,
    warmup: int,
    tenants: tuple[int, int],
    seed: int,
    settings: list[str],
) -> dict[str, Any]:
    import psycopg

    rng = random.Random(seed)
    ms: list[float] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        for s in settings:
            conn.execute(s)
        for i in range(warmup + runs):
            params = {"tenant": rng.randint(*tenants)}
            t0 = time.perf_counter()
            with conn.cursor() as cur:
                cur.execute(sql, params if "%(" in sql else None)
                cur.fetchall()
            if i >= warmup:
                ms.append((time.perf_counter() - t0) * 1000)
    return percentiles(ms)


def latency_ch(
    url: str, sql: str, runs: int, warmup: int, tenants: tuple[int, int], seed: int
) -> dict[str, Any]:
    import httpx

    rng = random.Random(seed)
    ms: list[float] = []
    with httpx.Client(base_url=url, timeout=600) as client:
        for i in range(warmup + runs):
            q = sql.replace("%(tenant)s", str(rng.randint(*tenants)))
            t0 = time.perf_counter()
            r = client.post("/", content=q)
            r.raise_for_status()
            if i >= warmup:
                ms.append((time.perf_counter() - t0) * 1000)
    return percentiles(ms)


# ── load ──────────────────────────────────────────────────────────────────────
READ_SQL = (
    "SELECT count(*), coalesce(sum(value), 0) FROM events "
    "WHERE tenant_id = %s AND ts >= '2026-08-01'"
)
WRITE_SQL = (
    "INSERT INTO events (id, tenant_id, event_type, ts, value) "
    "VALUES (%s, %s, 'page_view', now(), 1.0)"
)


def load(
    dsn: str,
    threads: int,
    seconds: float,
    read_share: float,
    tenants: tuple[int, int],
    out: Path,
    seed: int,
) -> dict[str, Any]:
    import psycopg

    rows: list[tuple[float, str, float, int, str]] = []
    lock = threading.Lock()
    stop_at = time.time() + seconds

    def worker(n: int) -> None:
        rng = random.Random(seed + n)
        # ids unique across runs and threads: microsecond start time x 64 + thread.
        # A seed-derived range collided with an earlier, aborted run's inserts.
        next_id = time.time_ns() // 1_000 * 64 + n
        local: list[tuple[float, str, float, int, str]] = []
        conn = None
        while time.time() < stop_at:
            if conn is None or conn.closed:
                try:
                    conn = psycopg.connect(dsn, autocommit=True, connect_timeout=5)
                except Exception as exc:  # noqa: BLE001 — logged, the load keeps going
                    local.append((time.time(), "connect", 0.0, 0, type(exc).__name__))
                    time.sleep(0.2)
                    continue
            is_read = rng.random() < read_share
            tenant = rng.randint(*tenants)
            t0 = time.perf_counter()
            wall = time.time()
            try:
                with conn.cursor() as cur:
                    if is_read:
                        cur.execute(READ_SQL, (tenant,))
                        cur.fetchall()
                    else:
                        next_id += 64
                        cur.execute(WRITE_SQL, (next_id, tenant))
                ok, err = 1, ""
            except Exception as exc:  # noqa: BLE001
                ok, err = 0, f"{type(exc).__name__}: {str(exc).splitlines()[0][:120]}"
                # reconnect only when the connection itself is gone; a data error
                # in autocommit mode leaves it usable, and reconnecting would
                # charge every failed write a new session
                if conn.closed or isinstance(exc, psycopg.OperationalError):
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
            local.append(
                (wall, "read" if is_read else "write", (time.perf_counter() - t0) * 1000, ok, err)
            )
        with lock:
            rows.extend(local)

    pool = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join()
    rows.sort()
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["wall", "op", "ms", "ok", "error"])
        w.writerows(rows)
    return {"ops": len(rows), "errors": sum(1 for r in rows if not r[3]), "out": str(out)}


def summarize(load_csv: Path, marks: dict[str, float]) -> dict[str, Any]:
    with load_csv.open() as fh:
        rows = list(csv.DictReader(fh))
    start, end = marks.get("start"), marks.get("end")
    windows: dict[str, list[dict[str, str]]] = {"before": [], "during": [], "after": []}
    for r in rows:
        w = float(r["wall"])
        key = (
            "before"
            if start is None or w < start
            else "during"
            if end is None or w <= end
            else "after"
        )
        windows[key].append(r)
    out: dict[str, Any] = {"marks": marks}
    for name, rs in windows.items():
        if not rs:
            continue
        span = float(rs[-1]["wall"]) - float(rs[0]["wall"]) or 1.0
        errors: dict[str, int] = {}
        for r in rs:
            if r["ok"] == "0":
                errors[r["error"]] = errors.get(r["error"], 0) + 1
        out[name] = {
            "seconds": round(span, 1),
            "ops_per_s": round(len(rs) / span, 1),
            "read_ms": percentiles(
                [float(r["ms"]) for r in rs if r["op"] == "read" and r["ok"] == "1"]
            ),
            "write_ms": percentiles(
                [float(r["ms"]) for r in rs if r["op"] == "write" and r["ok"] == "1"]
            ),
            "errors": errors,
        }
    return out


def explain_times(
    dsn: str, sql: str, runs: int, tenants: tuple[int, int], seed: int, settings: list[str]
) -> dict[str, Any]:
    """Median server-side planning and execution time from EXPLAIN (ANALYZE)."""
    import re

    import psycopg

    rng = random.Random(seed)
    plan_ms: list[float] = []
    exec_ms: list[float] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        for s in settings:
            conn.execute(s)
        for _ in range(runs):
            params = {"tenant": rng.randint(*tenants)}
            with conn.cursor() as cur:
                cur.execute("EXPLAIN (ANALYZE, SUMMARY) " + sql, params if "%(" in sql else None)
                text = "\n".join(r[0] for r in cur.fetchall())
            plan_ms.append(float(re.search(r"Planning Time: ([\d.]+)", text).group(1)))
            exec_ms.append(float(re.search(r"Execution Time: ([\d.]+)", text).group(1)))
    return {"planning_ms": percentiles(plan_ms), "execution_ms": percentiles(exec_ms)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    lat = sub.add_parser("latency")
    lat.add_argument("--dsn")
    lat.add_argument("--clickhouse")
    lat.add_argument("--sql", required=True)
    lat.add_argument("--runs", type=int, default=30)
    lat.add_argument("--warmup", type=int, default=3)
    lat.add_argument("--tenants", default="2..2000")
    lat.add_argument("--seed", type=int, default=1)
    lat.add_argument("--set", action="append", default=[], help="SQL run once per connection")
    ld = sub.add_parser("load")
    ld.add_argument("--dsn", required=True)
    ld.add_argument("--threads", type=int, default=8)
    ld.add_argument("--seconds", type=float, default=120)
    ld.add_argument("--read-share", type=float, default=0.8)
    ld.add_argument("--tenants", default="1..2000")
    ld.add_argument("--out", type=Path, required=True)
    ld.add_argument("--seed", type=int, default=1)
    ex = sub.add_parser("explain")
    ex.add_argument("--dsn", required=True)
    ex.add_argument("--sql", required=True)
    ex.add_argument("--runs", type=int, default=15)
    ex.add_argument("--tenants", default="1..100")
    ex.add_argument("--seed", type=int, default=1)
    ex.add_argument("--set", action="append", default=[])
    sm = sub.add_parser("summarize")
    sm.add_argument("--load", type=Path, required=True)
    sm.add_argument("--marks", type=Path)
    a = ap.parse_args(argv)
    if a.cmd == "latency":
        rng = parse_range(a.tenants)
        res = (
            latency_ch(a.clickhouse, a.sql, a.runs, a.warmup, rng, a.seed)
            if a.clickhouse
            else latency_pg(a.dsn, a.sql, a.runs, a.warmup, rng, a.seed, a.set)
        )
    elif a.cmd == "load":
        res = load(a.dsn, a.threads, a.seconds, a.read_share, parse_range(a.tenants), a.out, a.seed)
    elif a.cmd == "explain":
        res = explain_times(a.dsn, a.sql, a.runs, parse_range(a.tenants), a.seed, a.set)
    else:
        marks = json.loads(a.marks.read_text()) if a.marks else {}
        res = summarize(a.load, marks)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
