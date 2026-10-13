"""Seeded synthetic datasets for the lab.

Three datasets, all deterministic for a given --seed and --scale:

  saas    multi-tenant SaaS: plans, countries, tenants, users, projects, events,
          orders, webhooks, webhook_deliveries and a mixed-case "AuditLog".
          Tenant sizes follow a Zipf law and one "whale" tenant is forced to own
          a fixed share of events, which is what makes shard *sizes* diverge
          while shard *counts* stay equal.
  ticks   time-series: entities x metrics sampled every --step-s seconds.
  events  event analytics for ClickHouse and the cross-engine comparison.

Output is CSV with a header row, one file per table, written to --out.

    python tools/datagen.py saas   --scale lab   --out data/saas
    python tools/datagen.py ticks  --scale lab   --out data/ticks
    python tools/datagen.py events --scale lab   --out data/events

Nothing here is real data; every name is generated.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np

EPOCH = datetime(2026, 7, 1, tzinfo=UTC)  # fixed so reruns are byte-identical
EVENT_TYPES = [
    "page_view",
    "click",
    "signup",
    "login",
    "search",
    "add_to_cart",
    "checkout",
    "purchase",
    "share",
    "logout",
]
EVENT_WEIGHTS = np.array([40, 25, 1, 8, 10, 6, 3, 2, 2, 3], dtype=float)
COUNTRIES = [
    "NP",
    "IN",
    "US",
    "GB",
    "DE",
    "FR",
    "NL",
    "SE",
    "ES",
    "IT",
    "PL",
    "BR",
    "MX",
    "CA",
    "AU",
    "JP",
    "KR",
    "SG",
    "AE",
    "ZA",
    "NG",
    "KE",
    "EG",
    "TR",
    "AR",
    "CL",
    "CO",
    "ID",
    "PH",
    "VN",
]
PLANS = [(1, "free", 0), (2, "team", 49), (3, "business", 199), (4, "enterprise", 999)]


@dataclass(frozen=True)
class SaasScale:
    tenants: int
    users: int
    projects: int
    events: int
    orders: int
    audit: int
    webhooks: int
    deliveries: int
    whale_share: float = 0.15  # share of events owned by tenant 1
    null_tenant_share: float = 0.005  # "system" events with no tenant


SAAS_SCALES = {
    "tiny": SaasScale(20, 400, 60, 5_000, 600, 300, 40, 500),
    "small": SaasScale(200, 10_000, 1_000, 200_000, 20_000, 10_000, 400, 20_000),
    "lab": SaasScale(2_000, 200_000, 20_000, 6_000_000, 600_000, 300_000, 5_000, 500_000),
}


@dataclass(frozen=True)
class TickScale:
    entities: int
    metrics: int
    days: int
    step_s: int


TICK_SCALES = {
    "tiny": TickScale(4, 2, 1, 600),
    "small": TickScale(20, 2, 2, 60),
    "lab": TickScale(100, 2, 12, 10),
}

EVENT_SCALES = {"tiny": 5_000, "small": 200_000, "lab": 10_000_000}


def zipf_weights(n: int, s: float) -> np.ndarray:
    ranks = np.arange(1, n + 1, dtype=float)
    w = 1.0 / ranks**s
    return w / w.sum()


def tenant_event_weights(n_tenants: int, whale_share: float) -> np.ndarray:
    """Tenant 1 pinned to `whale_share`; tenants 2..n follow Zipf from rank 2,
    so the whale stays the largest tenant at every scale."""
    ranks = np.arange(2, n_tenants + 1, dtype=float)
    rest = 1.0 / ranks**1.1
    rest = rest / rest.sum() * (1.0 - whale_share)
    return np.concatenate([[whale_share], rest])


def iso(ts: np.ndarray) -> list[str]:
    """Epoch-offset seconds -> ISO-8601 UTC strings."""
    return [(EPOCH + timedelta(seconds=int(s))).strftime("%Y-%m-%d %H:%M:%S+00") for s in ts]


def write_csv(path: Path, header: list[str], rows: Iterator[list]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for row in rows:
            w.writerow(row)
            n += 1
    return n


def chunks(total: int, size: int = 250_000) -> Iterator[tuple[int, int]]:
    start = 0
    while start < total:
        end = min(total, start + size)
        yield start, end
        start = end


# ── saas ──────────────────────────────────────────────────────────────────────
def gen_saas(scale: SaasScale, out: Path, seed: int) -> dict[str, int]:
    rng = np.random.default_rng(seed)
    counts: dict[str, int] = {}
    span_s = 90 * 86_400

    counts["plans"] = write_csv(
        out / "plans.csv", ["id", "name", "monthly_price"], iter([list(p) for p in PLANS])
    )
    counts["countries"] = write_csv(
        out / "countries.csv", ["code", "name"], iter([[c, f"Country {c}"] for c in COUNTRIES])
    )

    t_plan = rng.choice([1, 2, 3, 4], size=scale.tenants, p=[0.55, 0.3, 0.12, 0.03])
    t_plan[0] = 4  # the whale is an enterprise customer
    t_created = rng.integers(0, span_s, size=scale.tenants)
    counts["tenants"] = write_csv(
        out / "tenants.csv",
        ["id", "name", "plan_id", "created_at"],
        ([i + 1, f"tenant-{i + 1:05d}", int(t_plan[i]), c] for i, c in enumerate(iso(t_created))),
    )

    # Users and projects follow the same skew as events, so the whale is big
    # everywhere, the way a real large customer is.
    tw = tenant_event_weights(scale.tenants, scale.whale_share)
    u_tenant = rng.choice(scale.tenants, size=scale.users, p=tw) + 1
    u_country = rng.choice(len(COUNTRIES), size=scale.users)
    u_created = rng.integers(0, span_s, size=scale.users)
    counts["users"] = write_csv(
        out / "users.csv",
        ["id", "tenant_id", "email", "country_code", "created_at"],
        (
            [i + 1, int(u_tenant[i]), f"user{i + 1}@example.test", COUNTRIES[u_country[i]], c]
            for i, c in enumerate(iso(u_created))
        ),
    )

    p_tenant = rng.choice(scale.tenants, size=scale.projects, p=tw) + 1
    counts["projects"] = write_csv(
        out / "projects.csv",
        ["id", "tenant_id", "name"],
        ([i + 1, int(p_tenant[i]), f"project-{i + 1}"] for i in range(scale.projects)),
    )

    # index users and projects by tenant so events reference a user and a
    # project that belong to the same tenant (what a real FK guarantees)
    users_by_tenant = _group(u_tenant, scale.tenants)
    projects_by_tenant = _group(p_tenant, scale.tenants)
    ev_p = EVENT_WEIGHTS / EVENT_WEIGHTS.sum()

    def event_rows() -> Iterator[list]:
        eid = 0
        for a, b in chunks(scale.events):
            n = b - a
            tenant = rng.choice(scale.tenants, size=n, p=tw) + 1
            etype = rng.choice(len(EVENT_TYPES), size=n, p=ev_p)
            ts = np.sort(rng.integers(0, span_s, size=n))
            value = np.round(rng.gamma(2.0, 3.0, size=n), 3)
            null_mask = rng.random(n) < scale.null_tenant_share
            pick_u = rng.random(n)
            pick_p = rng.random(n)
            stamps = iso(ts)
            for i in range(n):
                eid += 1
                t = int(tenant[i])
                us, ps = users_by_tenant[t], projects_by_tenant[t]
                uid = int(us[int(pick_u[i] * len(us))]) if len(us) else ""
                pid = int(ps[int(pick_p[i] * len(ps))]) if len(ps) else ""
                yield [
                    eid,
                    "" if null_mask[i] else t,
                    uid,
                    pid,
                    EVENT_TYPES[etype[i]],
                    stamps[i],
                    value[i],
                ]

    counts["events"] = write_csv(
        out / "events.csv",
        ["id", "tenant_id", "user_id", "project_id", "event_type", "ts", "value"],
        event_rows(),
    )

    o_tenant = rng.choice(scale.tenants, size=scale.orders, p=tw) + 1
    o_amount = np.round(rng.lognormal(3.5, 0.8, size=scale.orders), 2)
    o_status = rng.choice(["paid", "refunded", "pending"], size=scale.orders, p=[0.9, 0.04, 0.06])
    o_ts = rng.integers(0, span_s, size=scale.orders)
    pick = rng.random(scale.orders)

    def order_rows() -> Iterator[list]:
        for i, c in enumerate(iso(o_ts)):
            t = int(o_tenant[i])
            us = users_by_tenant[t]
            uid = int(us[int(pick[i] * len(us))]) if len(us) else ""
            yield [i + 1, t, uid, o_amount[i], o_status[i], c]

    counts["orders"] = write_csv(
        out / "orders.csv",
        ["id", "tenant_id", "user_id", "amount", "status", "created_at"],
        order_rows(),
    )

    a_tenant = rng.choice(scale.tenants, size=scale.audit, p=tw) + 1
    a_ts = rng.integers(0, span_s, size=scale.audit)
    actions = ["login", "invite", "role_change", "export", "api_key_rotate"]
    a_act = rng.choice(len(actions), size=scale.audit)
    counts["AuditLog"] = write_csv(
        out / "AuditLog.csv",
        ["Id", "TenantId", "Action", "CreatedAt"],
        ([i + 1, int(a_tenant[i]), actions[a_act[i]], c] for i, c in enumerate(iso(a_ts))),
    )

    w_tenant = rng.choice(scale.tenants, size=scale.webhooks, p=tw) + 1
    counts["webhooks"] = write_csv(
        out / "webhooks.csv",
        ["id", "tenant_id", "url"],
        (
            [i + 1, int(w_tenant[i]), f"https://hooks.example.test/{i + 1}"]
            for i in range(scale.webhooks)
        ),
    )
    d_hook = rng.integers(1, scale.webhooks + 1, size=scale.deliveries)
    d_code = rng.choice(
        [200, 202, 400, 500, 503], size=scale.deliveries, p=[0.85, 0.05, 0.03, 0.04, 0.03]
    )
    d_ts = rng.integers(0, span_s, size=scale.deliveries)
    counts["webhook_deliveries"] = write_csv(
        out / "webhook_deliveries.csv",
        ["id", "webhook_id", "status_code", "delivered_at"],
        ([i + 1, int(d_hook[i]), int(d_code[i]), c] for i, c in enumerate(iso(d_ts))),
    )
    return counts


def _group(owner: np.ndarray, n_tenants: int) -> list[np.ndarray]:
    """owner[i] is the 1-based tenant of row i+1; return row ids per tenant."""
    order = np.argsort(owner, kind="stable")
    sorted_owner = owner[order]
    bounds = np.searchsorted(sorted_owner, np.arange(1, n_tenants + 2))
    ids = order + 1
    return [np.array([], dtype=np.int64)] + [
        ids[bounds[t - 1] : bounds[t]] for t in range(1, n_tenants + 1)
    ]


# ── ticks ─────────────────────────────────────────────────────────────────────
def gen_ticks(scale: TickScale, out: Path, seed: int) -> dict[str, int]:
    rng = np.random.default_rng(seed)
    steps = scale.days * 86_400 // scale.step_s
    series = [
        (e + 1, m) for e in range(scale.entities) for m in ("price", "volume")[: scale.metrics]
    ]
    level = rng.uniform(10, 500, size=len(series))
    path = out / "ticks.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["ts", "entity_id", "metric", "value"])
        # time-major, the order a real feed arrives in
        block = 2_000
        for t0 in range(0, steps, block):
            t1 = min(steps, t0 + block)
            shocks = rng.normal(0, 0.002, size=(t1 - t0, len(series)))
            walk = level * np.exp(np.cumsum(shocks, axis=0))
            level = walk[-1]
            for k, t in enumerate(range(t0, t1)):
                stamp = (EPOCH + timedelta(seconds=t * scale.step_s)).strftime(
                    "%Y-%m-%d %H:%M:%S+00"
                )
                for j, (ent, met) in enumerate(series):
                    w.writerow([stamp, ent, met, f"{walk[k, j]:.4f}"])
                    n += 1
    return {"ticks": n}


# ── events (ClickHouse + compare) ─────────────────────────────────────────────
def gen_events(rows: int, out: Path, seed: int, tenants: int = 2_000) -> dict[str, int]:
    rng = np.random.default_rng(seed)
    tw = tenant_event_weights(tenants, 0.15)
    ev_p = EVENT_WEIGHTS / EVENT_WEIGHTS.sum()
    span_s = 90 * 86_400

    def rows_iter() -> Iterator[list]:
        for a, b in chunks(rows):
            n = b - a
            tenant = rng.choice(tenants, size=n, p=tw) + 1
            user = tenant * 1_000 + rng.integers(0, 1_000, size=n)
            etype = rng.choice(len(EVENT_TYPES), size=n, p=ev_p)
            country = rng.choice(len(COUNTRIES), size=n)
            ts = rng.integers(0, span_s, size=n)
            dur = rng.integers(5, 30_000, size=n)
            revenue = np.where(
                etype == EVENT_TYPES.index("purchase"),
                np.round(rng.lognormal(3.5, 0.8, size=n), 2),
                0.0,
            )
            stamps = iso(ts)
            for i in range(n):
                yield [
                    int(tenant[i]),
                    int(user[i]),
                    EVENT_TYPES[etype[i]],
                    COUNTRIES[country[i]],
                    stamps[i],
                    int(dur[i]),
                    revenue[i],
                ]

    n = write_csv(
        out / "events.csv",
        ["tenant_id", "user_id", "event_type", "country", "ts", "duration_ms", "revenue"],
        rows_iter(),
    )
    return {"events": n}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("dataset", choices=["saas", "ticks", "events"])
    ap.add_argument("--scale", default="small", choices=["tiny", "small", "lab"])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=20261013)
    args = ap.parse_args(argv)
    if args.dataset == "saas":
        counts = gen_saas(SAAS_SCALES[args.scale], args.out, args.seed)
    elif args.dataset == "ticks":
        counts = gen_ticks(TICK_SCALES[args.scale], args.out, args.seed)
    else:
        counts = gen_events(EVENT_SCALES[args.scale], args.out, args.seed)
    (args.out / "manifest.json").write_text(
        json.dumps(
            {"dataset": args.dataset, "scale": args.scale, "seed": args.seed, "rows": counts},
            indent=2,
        )
        + "\n"
    )
    print(json.dumps(counts), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
