-- A single-node SaaS schema written the way most applications ship before
-- anyone plans to shard them. Every trap is deliberate and each one is a class
-- of failure I met migrating a real warehouse to Citus:
--
--   * primary keys on a surrogate id alone          (PK_WITHOUT_KEY)
--   * a global UNIQUE on email                      (PK_WITHOUT_KEY, semantic)
--   * foreign keys that never mention the tenant    (FK_WITHOUT_KEY)
--   * "system" events with no tenant at all          (NULL_KEY)
--   * a mixed-case table carried over from an ORM    (MIXED_CASE)
--   * a child table that never stored the tenant     (MISSING_KEY)

DROP TABLE IF EXISTS webhook_deliveries, webhooks, "AuditLog", orders, events, projects,
    users, tenants, countries, plans CASCADE;

CREATE TABLE plans (
    id            int PRIMARY KEY,
    name          text NOT NULL,
    monthly_price numeric(10, 2) NOT NULL
);

CREATE TABLE countries (
    code char(2) PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE tenants (
    id         bigint PRIMARY KEY,
    name       text NOT NULL,
    plan_id    int REFERENCES plans (id),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id           bigint PRIMARY KEY,
    tenant_id    bigint NOT NULL REFERENCES tenants (id),
    email        text NOT NULL UNIQUE,
    country_code char(2) REFERENCES countries (code),
    created_at   timestamptz NOT NULL
);

CREATE TABLE projects (
    id        bigint PRIMARY KEY,
    tenant_id bigint NOT NULL REFERENCES tenants (id),
    name      text NOT NULL
);

CREATE TABLE events (
    id         bigint PRIMARY KEY,
    tenant_id  bigint REFERENCES tenants (id),   -- NULL for "system" events
    user_id    bigint REFERENCES users (id),
    project_id bigint REFERENCES projects (id),
    event_type text NOT NULL,
    ts         timestamptz NOT NULL,
    value      double precision
);

CREATE TABLE orders (
    id         bigint PRIMARY KEY,
    tenant_id  bigint NOT NULL REFERENCES tenants (id),
    user_id    bigint REFERENCES users (id),
    amount     numeric(12, 2) NOT NULL,
    status     text NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE "AuditLog" (
    "Id"        bigint PRIMARY KEY,
    "TenantId"  bigint NOT NULL,
    "Action"    text NOT NULL,
    "CreatedAt" timestamptz NOT NULL
);

CREATE TABLE webhooks (
    id        bigint PRIMARY KEY,
    tenant_id bigint NOT NULL REFERENCES tenants (id),
    url       text NOT NULL
);

CREATE TABLE webhook_deliveries (
    id           bigint PRIMARY KEY,
    webhook_id   bigint NOT NULL REFERENCES webhooks (id),
    status_code  int NOT NULL,
    delivered_at timestamptz NOT NULL
);

CREATE INDEX ON events (tenant_id, ts);
CREATE INDEX ON events (user_id);
CREATE INDEX ON orders (tenant_id, created_at);
