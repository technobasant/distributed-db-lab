-- The first thing everyone tries: distribute each table by the obvious column
-- and see what breaks. Citus reports one problem at a time, and the first one
-- hides the rest, so each block below fixes the layer above it inside a
-- transaction that is rolled back, to surface the next error. Nothing here
-- changes the schema; the planner's script does that properly afterwards.
\set ON_ERROR_STOP off
\set VERBOSITY default

\echo '== 1. foreign key into a table that is still local'
SELECT create_distributed_table('orders', 'tenant_id');

\echo '== 2. local tables still reference the table being distributed'
BEGIN;
SELECT create_reference_table('countries');
SELECT create_reference_table('plans');
SELECT create_distributed_table('tenants', 'id');
ROLLBACK;

\echo '== 3. primary key and unique constraint that omit the distribution column'
BEGIN;
DO $$DECLARE r record; BEGIN
  FOR r IN SELECT conrelid::regclass AS t, conname FROM pg_constraint
           WHERE contype = 'f' AND connamespace = 'public'::regnamespace LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.t, r.conname);
  END LOOP; END$$;
SELECT create_distributed_table('users', 'tenant_id');
ROLLBACK;

\echo '== 4. rows whose distribution column is NULL'
BEGIN;
DO $$DECLARE r record; BEGIN
  FOR r IN SELECT conrelid::regclass AS t, conname FROM pg_constraint
           WHERE contype = 'f' AND connamespace = 'public'::regnamespace LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.t, r.conname);
  END LOOP; END$$;
ALTER TABLE events DROP CONSTRAINT events_pkey;
SELECT create_distributed_table('events', 'tenant_id');
ROLLBACK;

\echo '== 5. mixed-case identifiers fold to lower case unless quoted'
SELECT create_distributed_table('AuditLog', 'TenantId');
SELECT create_distributed_table('"AuditLog"', 'tenantid');

\echo '== 6. the tenant was never stored on this table'
SELECT create_distributed_table('webhook_deliveries', 'tenant_id');

\echo '== nothing above was kept'
SELECT count(*) AS distributed_tables FROM citus_tables;
