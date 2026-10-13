-- Server-side COPY from the generated CSVs (mounted read-only at /data).
\timing on
COPY plans              FROM '/data/saas/plans.csv'              WITH (FORMAT csv, HEADER true);
COPY countries          FROM '/data/saas/countries.csv'          WITH (FORMAT csv, HEADER true);
COPY tenants            FROM '/data/saas/tenants.csv'            WITH (FORMAT csv, HEADER true);
COPY users              FROM '/data/saas/users.csv'              WITH (FORMAT csv, HEADER true);
COPY projects           FROM '/data/saas/projects.csv'           WITH (FORMAT csv, HEADER true);
COPY events             FROM '/data/saas/events.csv'             WITH (FORMAT csv, HEADER true);
COPY orders             FROM '/data/saas/orders.csv'             WITH (FORMAT csv, HEADER true);
COPY "AuditLog"         FROM '/data/saas/AuditLog.csv'           WITH (FORMAT csv, HEADER true);
COPY webhooks           FROM '/data/saas/webhooks.csv'           WITH (FORMAT csv, HEADER true);
COPY webhook_deliveries FROM '/data/saas/webhook_deliveries.csv' WITH (FORMAT csv, HEADER true);
ANALYZE;
