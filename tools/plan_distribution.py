"""Plan a Citus distribution from a single-node schema's catalog.

Reads tables, columns, primary/unique keys and foreign keys, then decides:

  * the root table and its distribution key (the tenant): the referenced table
    whose direct referrers hold the most rows, unless --root/--key are given;
  * each table's distribution column — a foreign key to the root, or a column
    whose name matches the key once case and underscores are ignored
    ("TenantId" matches tenant_id);
  * reference tables: small tables with no key that distributed tables point at;
  * one co-location group for everything distributed by the key.

and reports the problems that stop `create_distributed_table` from working,
each with the SQL that fixes it:

  PK_WITHOUT_KEY   a primary key or unique constraint that omits the key
  FK_WITHOUT_KEY   a foreign key between distributed tables that omits the key
  FK_TO_LOCAL      a foreign key to a table that would stay local
  NULL_KEY         rows whose distribution column is NULL
  MISSING_KEY      a large table with no key column; backfilled through its FK
  MIXED_CASE       identifiers that only work quoted — every emitted name is quoted
  NO_PATH          a large table with no route to the key; left local, flagged

The planning logic works on a plain in-memory catalog, so it is unit-tested
without a database; `introspect()` fills that catalog from a live connection.

    python tools/plan_distribution.py --dsn postgresql://postgres@localhost:5432/app \\
        --report results/plan.json --sql results/plan.sql
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

REFERENCE_MAX_ROWS = 10_000


# ── catalog model ─────────────────────────────────────────────────────────────
@dataclass
class Column:
    name: str
    type: str
    nullable: bool = True


@dataclass
class Constraint:
    name: str
    kind: str  # "p" primary, "u" unique, "f" foreign
    columns: list[str]
    ref_table: str | None = None
    ref_columns: list[str] = field(default_factory=list)


@dataclass
class Table:
    name: str
    columns: dict[str, Column]
    constraints: list[Constraint] = field(default_factory=list)
    est_rows: int = 0
    null_keys: int | None = None  # filled by introspect for the chosen key

    def fks(self) -> list[Constraint]:
        return [c for c in self.constraints if c.kind == "f"]

    def uniques(self) -> list[Constraint]:
        return [c for c in self.constraints if c.kind in ("p", "u")]


@dataclass
class Catalog:
    tables: dict[str, Table]

    def referrers(self, name: str) -> list[tuple[Table, Constraint]]:
        return [(t, c) for t in self.tables.values() for c in t.fks() if c.ref_table == name]


# ── plan model ────────────────────────────────────────────────────────────────
@dataclass
class Issue:
    code: str
    table: str
    detail: str
    fix: list[str] = field(default_factory=list)


@dataclass
class Plan:
    root: str
    key: str
    distributed: dict[str, str]  # table -> distribution column
    reference: list[str]
    local: list[str]
    issues: list[Issue]

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        d["issue_counts"] = _count(i.code for i in self.issues)
        return d


def _count(codes) -> dict[str, int]:
    out: dict[str, int] = {}
    for c in codes:
        out[c] = out.get(c, 0) + 1
    return dict(sorted(out.items()))


def norm(ident: str) -> str:
    """Case- and underscore-insensitive identifier: TenantId == tenant_id."""
    return re.sub(r"[^a-z0-9]", "", ident.lower())


def q(ident: str) -> str:
    """Always-quoted identifier. Folding "TenantId" to tenantid is the error
    this tool exists to avoid, so nothing is emitted unquoted."""
    return '"' + ident.replace('"', '""') + '"'


def is_mixed_case(ident: str) -> bool:
    return ident != ident.lower() or not re.fullmatch(r"[a-z_][a-z0-9_]*", ident)


# ── planning ──────────────────────────────────────────────────────────────────
def infer_root(cat: Catalog) -> tuple[str, str]:
    """The referenced table whose direct referrers hold the most rows."""
    best: tuple[int, str, str] | None = None
    for name in cat.tables:
        refs = cat.referrers(name)
        if not refs:
            continue
        weight = sum(t.est_rows for t, _ in refs)
        pk = next((c for c in cat.tables[name].uniques() if c.kind == "p"), None)
        if pk is None or len(pk.columns) != 1:
            continue
        cand = (weight, name, pk.columns[0])
        if best is None or cand > best:
            best = cand
    if best is None:
        raise ValueError("no table is referenced by a foreign key; pass --root and --key")
    return best[1], best[2]


def key_column(table: Table, root: str, root_pk: str, key_name: str) -> str | None:
    """The column that carries the tenant in `table`, if any."""
    if table.name == root:
        return root_pk
    for fk in table.fks():
        if fk.ref_table == root and fk.ref_columns == [root_pk] and len(fk.columns) == 1:
            return fk.columns[0]
    want = norm(key_name)
    for col in table.columns:
        if norm(col) == want:
            return col
    return None


def plan(
    cat: Catalog,
    root: str | None = None,
    key: str | None = None,
    reference_max_rows: int = REFERENCE_MAX_ROWS,
) -> Plan:
    if root is None:
        root, root_pk = infer_root(cat)
    else:
        pk = next((c for c in cat.tables[root].uniques() if c.kind == "p"), None)
        if pk is None:
            raise ValueError(f"{root} has no primary key")
        root_pk = pk.columns[0]
    # the name children use for the key: the root's FK column name if any child
    # has one, else the --key the caller asked for, else root_singular + "_id"
    key_name = key or _child_key_name(cat, root, root_pk) or f"{root.rstrip('s')}_id"

    distributed: dict[str, str] = {}
    pending_missing: list[str] = []
    for t in cat.tables.values():
        col = key_column(t, root, root_pk, key_name)
        if col is not None:
            distributed[t.name] = col
        else:
            pending_missing.append(t.name)

    issues: list[Issue] = []
    reference: list[str] = []
    local: list[str] = []
    # tables without a key: backfillable through an FK to a distributed table,
    # small enough to replicate, or left local
    for name in pending_missing:
        t = cat.tables[name]
        via = next(
            (fk for fk in t.fks() if fk.ref_table in distributed and fk.ref_table != root), None
        )
        if via is not None and t.est_rows > reference_max_rows:
            parent = cat.tables[via.ref_table]
            pkey = distributed[via.ref_table]
            issues.append(
                Issue(
                    "MISSING_KEY",
                    name,
                    (
                        f"no tenant column; it reaches {via.ref_table} through {via.name}, "
                        f"so the key can be backfilled from {via.ref_table}.{pkey}"
                    ),
                    _backfill_sql(t, via, parent, pkey, key_name),
                )
            )
            distributed[name] = key_name
        elif t.est_rows <= reference_max_rows:
            reference.append(name)
        else:
            local.append(name)
            issues.append(
                Issue(
                    "NO_PATH",
                    name,
                    (
                        f"{t.est_rows:,} rows, no tenant column and no foreign key to a "
                        "distributed table; left local — joins from distributed tables to it "
                        "will fail"
                    ),
                )
            )

    # anything a reference table points at must be a reference table too
    changed = True
    while changed:
        changed = False
        for name in list(reference):
            for fk in cat.tables[name].fks():
                if (
                    fk.ref_table
                    and fk.ref_table not in reference
                    and fk.ref_table not in distributed
                ):
                    reference.append(fk.ref_table)
                    if fk.ref_table in local:
                        local.remove(fk.ref_table)
                    changed = True

    for name, col in distributed.items():
        t = cat.tables[name]
        issues.extend(_constraint_issues(cat, t, col, distributed, reference, root))
        if t.null_keys:
            issues.append(
                Issue(
                    "NULL_KEY",
                    name,
                    (f"{t.null_keys:,} rows have NULL {col}; Citus cannot place them on a shard"),
                    _null_fix_sql(t, col, root, root_pk),
                )
            )
        mixed = [c for c in [name, *t.columns] if is_mixed_case(c)]
        if mixed:
            issues.append(
                Issue(
                    "MIXED_CASE",
                    name,
                    ("identifiers that fold to lower case unless quoted: " + ", ".join(mixed)),
                )
            )
    return Plan(
        root, root_pk, dict(sorted(distributed.items())), sorted(reference), sorted(local), issues
    )


def _child_key_name(cat: Catalog, root: str, root_pk: str) -> str | None:
    names = [
        fk.columns[0]
        for t, fk in cat.referrers(root)
        if fk.ref_columns == [root_pk] and len(fk.columns) == 1
    ]
    return max(set(names), key=names.count) if names else None


def _constraint_issues(
    cat: Catalog, t: Table, col: str, distributed: dict[str, str], reference: list[str], root: str
) -> list[Issue]:
    issues: list[Issue] = []
    for u in t.uniques():
        if col not in u.columns:
            new_cols = [col, *u.columns]
            kind = "PRIMARY KEY" if u.kind == "p" else "UNIQUE"
            dependents = [
                (rt, fk) for rt, fk in cat.referrers(t.name) if fk.ref_columns == u.columns
            ]
            fix = [
                f"ALTER TABLE {q(rt.name)} DROP CONSTRAINT IF EXISTS {q(fk.name)};"
                for rt, fk in dependents
            ]
            fix += [
                f"ALTER TABLE {q(t.name)} DROP CONSTRAINT {q(u.name)};",
                f"ALTER TABLE {q(t.name)} ADD CONSTRAINT {q(u.name)} {kind} "
                f"({', '.join(q(c) for c in new_cols)});",
            ]
            sem = "" if u.kind == "p" else " — uniqueness becomes per tenant"
            issues.append(
                Issue(
                    "PK_WITHOUT_KEY",
                    t.name,
                    (f"{kind} {u.name} ({', '.join(u.columns)}) omits {col}{sem}"),
                    fix,
                )
            )
    for fk in t.fks():
        target = fk.ref_table or ""
        if target in reference:
            continue
        if target in distributed:
            tkey = distributed[target]
            if col in fk.columns and tkey in fk.ref_columns:
                continue
            if target == root and fk.ref_columns == [tkey]:
                continue
            new_src = [col, *fk.columns]
            new_dst = [tkey, *fk.ref_columns]
            issues.append(
                Issue(
                    "FK_WITHOUT_KEY",
                    t.name,
                    (f"{fk.name} ({', '.join(fk.columns)}) -> {target} omits the tenant key"),
                    [
                        f"ALTER TABLE {q(t.name)} DROP CONSTRAINT IF EXISTS {q(fk.name)};",
                        f"-- re-added after distribution, co-located: ({', '.join(new_src)}) -> "
                        f"{target} ({', '.join(new_dst)})",
                        f"ALTER TABLE {q(t.name)} ADD CONSTRAINT {q(fk.name)} FOREIGN KEY "
                        f"({', '.join(q(c) for c in new_src)}) REFERENCES {q(target)} "
                        f"({', '.join(q(c) for c in new_dst)});",
                    ],
                )
            )
        else:
            issues.append(
                Issue(
                    "FK_TO_LOCAL",
                    t.name,
                    (f"{fk.name} points at {target}, which would stay a local table"),
                    [f"SELECT create_reference_table('{target}');"],
                )
            )
    return issues


def _backfill_sql(t: Table, via: Constraint, parent: Table, pkey: str, key_name: str) -> list[str]:
    on = " AND ".join(
        f"c.{q(a)} = p.{q(b)}" for a, b in zip(via.columns, via.ref_columns, strict=True)
    )
    return [
        f"ALTER TABLE {q(t.name)} ADD COLUMN {q(key_name)} bigint;",
        f"UPDATE {q(t.name)} c SET {q(key_name)} = p.{q(pkey)} FROM {q(parent.name)} p WHERE {on};",
        f"ALTER TABLE {q(t.name)} ALTER COLUMN {q(key_name)} SET NOT NULL;",
    ]


def _null_fix_sql(t: Table, col: str, root: str, root_pk: str) -> list[str]:
    # Rows with no tenant still point at users, projects… that belong to some
    # tenant. Moved under a 'system' tenant, those references cannot survive a
    # composite (tenant, id) foreign key — the lab hit exactly this — so they
    # are cleared in the same statement.
    others = [c for fk in t.fks() if fk.ref_table != root for c in fk.columns if c != col]
    clear = "".join(f", {q(c)} = NULL" for c in others)
    return [
        "-- option A (used here): give orphan rows a reserved 'system' tenant and drop",
        "-- their references to rows that belong to real tenants",
        f"INSERT INTO {q(root)} ({q(root_pk)}, name) VALUES (0, 'system') ON CONFLICT DO NOTHING;",
        f"UPDATE {q(t.name)} SET {q(col)} = 0{clear} WHERE {q(col)} IS NULL;",
        f"ALTER TABLE {q(t.name)} ALTER COLUMN {q(col)} SET NOT NULL;",
        "-- option B: move them to a separate local table before distributing",
    ]


def _fk_readd(cat: Catalog, p: Plan) -> tuple[list[str], list[str]]:
    """Every foreign key, dropped before distribution and re-added after.

    Citus refuses to distribute a table that local tables still reference
    ("foreign keys from reference tables and local tables to distributed tables
    are not supported"), so no table can go first while FKs are in place. FKs
    between distributed tables come back with the tenant key prepended."""
    drops: list[str] = []
    adds: list[str] = []
    for t in cat.tables.values():
        for fk in t.fks():
            drops.append(f"ALTER TABLE {q(t.name)} DROP CONSTRAINT IF EXISTS {q(fk.name)};")
            src, dst = list(fk.columns), list(fk.ref_columns)
            target = fk.ref_table or ""
            if t.name in p.distributed and target in p.distributed and target != p.root:
                key, tkey = p.distributed[t.name], p.distributed[target]
                if key not in src:
                    src, dst = [key, *src], [tkey, *dst]
            if t.name in p.local and target not in p.local:
                continue  # a local table cannot keep an FK into a distributed one
            adds.append(
                f"ALTER TABLE {q(t.name)} ADD CONSTRAINT {q(fk.name)} FOREIGN KEY "
                f"({', '.join(q(c) for c in src)}) REFERENCES {q(target)} "
                f"({', '.join(q(c) for c in dst)});"
            )
    return drops, adds


def emit_sql(cat: Catalog, p: Plan) -> str:
    """Fixes, then reference tables, then the co-located distribution, then FKs."""
    drops, adds = _fk_readd(cat, p)
    out = ["-- generated by tools/plan_distribution.py — review before running", "BEGIN;"]
    out.append("\n-- every foreign key comes off first; they return once all tables are placed")
    out += drops
    for code in ["NULL_KEY", "MISSING_KEY", "PK_WITHOUT_KEY"]:
        for i in (i for i in p.issues if i.code == code):
            out.append(f"\n-- {i.code} {i.table}: {i.detail}")
            out += [
                s for s in i.fix if "DROP CONSTRAINT IF EXISTS" not in s or code != "PK_WITHOUT_KEY"
            ]
    out.append("COMMIT;\n")
    out.append("-- reference tables (replicated to every node)")
    out += [f"SELECT create_reference_table('{q(r)}');" for r in p.reference]
    out.append("\n-- distributed tables, one co-location group led by the root")
    out.append(f"SELECT create_distributed_table('{q(p.root)}', '{p.key}');")
    for name, col in p.distributed.items():
        if name != p.root:
            out.append(
                f"SELECT create_distributed_table('{q(name)}', '{col}', "
                f"colocate_with => '{q(p.root)}');"
            )
    out.append("\n-- foreign keys, re-added; between distributed tables they carry the key")
    out += adds
    out.append("\n-- the coordinator keeps the pre-distribution rows until told to drop them")
    out += [
        f"SELECT truncate_local_data_after_distributing_table('{q(n)}');"
        for n in [*p.reference, *p.distributed]
    ]
    return "\n".join(out) + "\n"


# ── introspection ─────────────────────────────────────────────────────────────
INTROSPECT_SQL = {
    "tables": """
        SELECT c.relname, greatest(c.reltuples, 0)::bigint
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %(schema)s AND c.relkind IN ('r', 'p')
          AND NOT c.relispartition
    """,
    "columns": """
        SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull
        FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %(schema)s AND c.relkind IN ('r', 'p') AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY c.relname, a.attnum
    """,
    "constraints": """
        SELECT c.relname, con.conname, con.contype,
               (SELECT array_agg(a.attname ORDER BY k.ord) FROM unnest(con.conkey)
                  WITH ORDINALITY k(attnum, ord)
                  JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum),
               rc.relname,
               (SELECT array_agg(a.attname ORDER BY k.ord) FROM unnest(con.confkey)
                  WITH ORDINALITY k(attnum, ord)
                  JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum)
        FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_class rc ON rc.oid = con.confrelid
        WHERE n.nspname = %(schema)s AND con.contype IN ('p', 'u', 'f')
    """,
}


def introspect(conn: Any, schema: str = "public", count_nulls: bool = True) -> Catalog:
    tables: dict[str, Table] = {}
    with conn.cursor() as cur:
        cur.execute(INTROSPECT_SQL["tables"], {"schema": schema})
        for name, rows in cur.fetchall():
            tables[name] = Table(name, {}, [], int(rows))
        cur.execute(INTROSPECT_SQL["columns"], {"schema": schema})
        for tname, col, typ, nullable in cur.fetchall():
            if tname in tables:
                tables[tname].columns[col] = Column(col, typ, bool(nullable))
        cur.execute(INTROSPECT_SQL["constraints"], {"schema": schema})
        for tname, cname, kind, cols, rtable, rcols in cur.fetchall():
            if tname in tables:
                tables[tname].constraints.append(
                    Constraint(cname, kind, list(cols or []), rtable, list(rcols or []))
                )
    cat = Catalog(tables)
    if count_nulls:
        root, root_pk = infer_root(cat)
        key_name = _child_key_name(cat, root, root_pk) or root_pk
        with conn.cursor() as cur:
            for t in tables.values():
                col = key_column(t, root, root_pk, key_name)
                if col and t.columns[col].nullable and t.name != root:
                    cur.execute(
                        f"SELECT count(*) FROM {q(schema)}.{q(t.name)} WHERE {q(col)} IS NULL"
                    )
                    t.null_keys = int(cur.fetchone()[0])
    return cat


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--schema", default="public")
    ap.add_argument("--root")
    ap.add_argument("--key")
    ap.add_argument("--reference-max-rows", type=int, default=REFERENCE_MAX_ROWS)
    ap.add_argument("--report", type=Path)
    ap.add_argument("--sql", type=Path)
    args = ap.parse_args(argv)
    import psycopg  # imported here so the planner tests need no driver

    with psycopg.connect(args.dsn) as conn:
        cat = introspect(conn, args.schema)
    p = plan(cat, args.root, args.key, args.reference_max_rows)
    report = json.dumps(p.to_json(), indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report + "\n")
    if args.sql:
        args.sql.parent.mkdir(parents=True, exist_ok=True)
        args.sql.write_text(emit_sql(cat, p))
    print(
        json.dumps(
            {
                "root": p.root,
                "key": p.key,
                "distributed": len(p.distributed),
                "reference": p.reference,
                "local": p.local,
                "issues": p.to_json()["issue_counts"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
