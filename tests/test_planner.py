"""Planner tests on an in-memory catalog shaped like citus/sql/01-legacy-schema.sql."""

from tools.plan_distribution import Catalog, Column, Constraint, Table, emit_sql, norm, plan, q


def cols(*names: str, notnull: tuple[str, ...] = ()) -> dict[str, Column]:
    return {n: Column(n, "bigint", n not in notnull) for n in names}


def legacy_catalog(null_events: int = 30) -> Catalog:
    t = [
        Table("plans", cols("id", "name"), [Constraint("plans_pkey", "p", ["id"])], 4),
        Table("countries", cols("code", "name"), [Constraint("countries_pkey", "p", ["code"])], 30),
        Table(
            "tenants",
            cols("id", "name", "plan_id"),
            [
                Constraint("tenants_pkey", "p", ["id"]),
                Constraint("tenants_plan_fk", "f", ["plan_id"], "plans", ["id"]),
            ],
            2_000,
        ),
        Table(
            "users",
            cols("id", "tenant_id", "email", "country_code"),
            [
                Constraint("users_pkey", "p", ["id"]),
                Constraint("users_email_key", "u", ["email"]),
                Constraint("users_tenant_fk", "f", ["tenant_id"], "tenants", ["id"]),
                Constraint("users_country_fk", "f", ["country_code"], "countries", ["code"]),
            ],
            200_000,
        ),
        Table(
            "projects",
            cols("id", "tenant_id", "name"),
            [
                Constraint("projects_pkey", "p", ["id"]),
                Constraint("projects_tenant_fk", "f", ["tenant_id"], "tenants", ["id"]),
            ],
            20_000,
        ),
        Table(
            "events",
            cols("id", "tenant_id", "user_id", "project_id"),
            [
                Constraint("events_pkey", "p", ["id"]),
                Constraint("events_tenant_fk", "f", ["tenant_id"], "tenants", ["id"]),
                Constraint("events_user_fk", "f", ["user_id"], "users", ["id"]),
                Constraint("events_project_fk", "f", ["project_id"], "projects", ["id"]),
            ],
            6_000_000,
            null_keys=null_events,
        ),
        Table(
            "orders",
            cols("id", "tenant_id", "user_id"),
            [
                Constraint("orders_pkey", "p", ["id"]),
                Constraint("orders_tenant_fk", "f", ["tenant_id"], "tenants", ["id"]),
                Constraint("orders_user_fk", "f", ["user_id"], "users", ["id"]),
            ],
            600_000,
        ),
        Table(
            "AuditLog",
            cols("Id", "TenantId", "Action"),
            [Constraint("AuditLog_pkey", "p", ["Id"])],
            300_000,
        ),
        Table(
            "webhooks",
            cols("id", "tenant_id", "url"),
            [
                Constraint("webhooks_pkey", "p", ["id"]),
                Constraint("webhooks_tenant_fk", "f", ["tenant_id"], "tenants", ["id"]),
            ],
            5_000,
        ),
        Table(
            "webhook_deliveries",
            cols("id", "webhook_id", "status_code"),
            [
                Constraint("webhook_deliveries_pkey", "p", ["id"]),
                Constraint("deliveries_webhook_fk", "f", ["webhook_id"], "webhooks", ["id"]),
            ],
            500_000,
        ),
    ]
    return Catalog({x.name: x for x in t})


def codes(p, table):
    return sorted(i.code for i in p.issues if i.table == table)


def test_infers_tenants_as_root_and_id_as_key():
    p = plan(legacy_catalog())
    assert (p.root, p.key) == ("tenants", "id")


def test_small_unkeyed_tables_become_reference_tables():
    p = plan(legacy_catalog())
    assert p.reference == ["countries", "plans"]
    assert p.local == []


def test_mixed_case_key_is_matched_and_flagged():
    p = plan(legacy_catalog())
    assert p.distributed["AuditLog"] == "TenantId"
    assert "MIXED_CASE" in codes(p, "AuditLog")
    assert norm("TenantId") == norm("tenant_id")


def test_every_failure_class_is_reported_where_it_lives():
    p = plan(legacy_catalog())
    assert codes(p, "users") == ["PK_WITHOUT_KEY", "PK_WITHOUT_KEY"]  # pkey and email unique
    assert codes(p, "events") == ["FK_WITHOUT_KEY", "FK_WITHOUT_KEY", "NULL_KEY", "PK_WITHOUT_KEY"]
    assert "MISSING_KEY" in codes(p, "webhook_deliveries")
    assert p.distributed["webhook_deliveries"] == "tenant_id"


def test_unique_constraint_fix_says_uniqueness_becomes_per_tenant():
    p = plan(legacy_catalog())
    email = next(i for i in p.issues if i.table == "users" and "email" in i.detail)
    assert "per tenant" in email.detail
    assert any('UNIQUE ("tenant_id", "email")' in s for s in email.fix)


def test_no_null_issue_when_there_are_no_null_keys():
    p = plan(legacy_catalog(null_events=0))
    assert "NULL_KEY" not in codes(p, "events")


def test_large_table_without_any_route_stays_local():
    cat = legacy_catalog()
    cat.tables["clickstream_raw"] = Table(
        "clickstream_raw", cols("id", "payload"), [Constraint("cr_pkey", "p", ["id"])], 9_000_000
    )
    p = plan(cat)
    assert p.local == ["clickstream_raw"]
    assert codes(p, "clickstream_raw") == ["NO_PATH"]


def test_emitted_sql_orders_fixes_before_distribution_and_quotes_everything():
    cat = legacy_catalog()
    sql = emit_sql(cat, plan(cat))
    first_dist = sql.index("create_distributed_table")
    assert sql.index('ALTER TABLE "events" ALTER COLUMN "tenant_id" SET NOT NULL') < first_dist
    assert sql.index("create_reference_table") < first_dist
    assert "colocate_with => '\"tenants\"'" in sql
    assert 'ADD CONSTRAINT "AuditLog_pkey" PRIMARY KEY ("TenantId", "Id")' in sql
    # composite FKs come back only after the tables are co-located
    assert sql.rindex('ADD CONSTRAINT "events_user_fk" FOREIGN KEY') > sql.rindex(
        "create_distributed_table"
    )
    assert q('we"ird') == '"we""ird"'
