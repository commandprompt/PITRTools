CREATE OR REPLACE FUNCTION cmd_get_data_dirs() RETURNS SETOF TEXT AS $$
   SELECT DISTINCT CASE WHEN t.spclocation <> '' THEN t.spclocation ELSE s.setting END 
      FROM pg_catalog.pg_settings s, pg_catalog.pg_tablespace t 
   WHERE s.name = 'data_directory';
$$ LANGUAGE 'SQL';
COMMENT ON FUNCTION cmd_get_data_dirs() IS 'Returns data paths. The text input is for later when we have to determine between > 8.3';

CREATE OR REPLACE FUNCTION cmd_pg_start_backup() RETURNS INT AS $$
   SELECT pg_start_backup('base_backup');
   SELECT 1;
$$ LANGUAGE 'SQL';

COMMENT ON FUNCTION cmd_pg_start_backup() IS 'Slim wrapper around pg_start_backup for flexibility';

CREATE OR REPLACE FUNCTION cmd_pg_stop_backup() RETURNS INT AS $$
   SELECT pg_stop_backup();
   SELECT 1;
$$ LANGUAGE 'SQL';

COMMENT ON FUNCTION cmd_pg_start_backup() IS 'Slim wrapper around pg_stop_backup for flexibility';




