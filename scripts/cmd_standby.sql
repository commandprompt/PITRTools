-- These are the helper scripts for cmd_standby. You apply them to the master "postgres" database;
-- ;
CREATE OR REPLACE FUNCTION cmd_get_data_dirs() RETURNS SETOF TEXT AS $$
   SELECT DISTINCT spclocation FROM pg_catalog.pg_tablespace
   WHERE spclocation <> '';
$$ LANGUAGE 'SQL';

COMMENT ON FUNCTION cmd_get_data_dirs() IS 'Returns tablespace paths. The text input is for later when we have to determine between > 8.3';

CREATE OR REPLACE FUNCTION cmd_get_pgdata() RETURNS TEXT AS $$
	SELECT setting FROM pg_catalog.pg_settings WHERE name='data_directory';
$$ LANGUAGE 'SQL' IMMUTABLE;

CREATE OR REPLACE FUNCTION cmd_get_tablespaces() RETURNS SETOF TEXT AS $$
	SELECT DISTINCT spclocation FROM pg_catalog.pg_tablespace WHERE spclocation IS NOT NULL;
$$ LANGUAGE 'SQL' STABLE;

CREATE OR REPLACE FUNCTION cmd_pg_start_backup() RETURNS INT AS $$
   SELECT pg_start_backup('base_backup');
   SELECT 1;
$$ LANGUAGE 'SQL';

COMMENT ON FUNCTION cmd_pg_start_backup() IS 'Slim wrapper around pg_start_backup for flexibility';

CREATE OR REPLACE FUNCTION cmd_pg_stop_backup() RETURNS INT AS $$
   SELECT pg_stop_backup();
   SELECT 1;
$$ LANGUAGE 'SQL';

COMMENT ON FUNCTION cmd_pg_stop_backup() IS 'Slim wrapper around pg_stop_backup for flexibility';




