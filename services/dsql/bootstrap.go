package dsql

// bootstrapSQL installs the DSQL system objects in the cluster's database as the engine superuser.
// sys.jobs, sys.wait_for_job and sys.iam_pg_role_mappings follow the Aurora DSQL User Guide
// (working-with-create-index-async, using-database-and-iam-roles); the gs_* objects are engine-private.
const bootstrapSQL = `
CREATE SCHEMA IF NOT EXISTS sys;

CREATE TABLE IF NOT EXISTS sys.gs_jobs (
	seq bigserial PRIMARY KEY,
	job_id text NOT NULL UNIQUE,
	status text NOT NULL,
	details text,
	job_type text NOT NULL,
	class_id oid,
	object_id oid,
	object_name text,
	start_time timestamptz,
	update_time timestamptz,
	ddl text NOT NULL,
	owner name NOT NULL
);

CREATE OR REPLACE VIEW sys.jobs AS
	SELECT job_id, status, details, job_type, class_id, object_id, object_name, start_time, update_time
	FROM sys.gs_jobs;

CREATE TABLE IF NOT EXISTS sys.gs_iam_grants (
	iam_oid bigserial PRIMARY KEY,
	arn text NOT NULL,
	role_name name NOT NULL,
	grantor name NOT NULL,
	UNIQUE (arn, role_name)
);

CREATE TABLE IF NOT EXISTS sys.gs_role_secrets (role_name name PRIMARY KEY, secret text NOT NULL);

CREATE OR REPLACE VIEW sys.iam_pg_role_mappings AS
	SELECT g.iam_oid, g.arn, r.oid AS pg_role_oid, r.rolname AS pg_role_name,
		gr.oid AS grantor_pg_role_oid, gr.rolname AS grantor_pg_role_name
	FROM sys.gs_iam_grants g
	JOIN pg_roles r ON r.rolname = g.role_name
	JOIN pg_roles gr ON gr.rolname = g.grantor;

CREATE OR REPLACE FUNCTION sys.gs_create_index_async(ddl text) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
	alphabet constant text := 'abcdefghijklmnopqrstuvwxyz234567';
	id text := '';
	obj text;
BEGIN
	IF ddl !~* '^\s*create\s+(unique\s+)?index\s' OR ddl ~ '[\\$]' OR ddl LIKE '%--%' OR ddl LIKE '%/*%'
		OR regexp_replace(ddl, '''[^'']*''|"[^"]*"', '', 'g') LIKE '%;%' THEN
		RAISE EXCEPTION 'only a single CREATE INDEX statement is allowed' USING ERRCODE = 'feature_not_supported';
	END IF;
	FOR i IN 1..26 LOOP
		id := id || substr(alphabet, 1 + floor(random() * 32)::int, 1);
	END LOOP;
	obj := substring(ddl from '(?i)index\s+(?:concurrently\s+)?(?:if\s+not\s+exists\s+)?("[^"]+"|[A-Za-z0-9_$.]+)\s+on\s');
	IF obj IS NOT NULL AND position('.' in obj) = 0 THEN
		obj := 'public.' || obj;
	END IF;
	DELETE FROM sys.gs_jobs WHERE status IN ('completed', 'failed') AND update_time < now() - interval '30 minutes';
	INSERT INTO sys.gs_jobs (job_id, status, job_type, class_id, object_name, update_time, ddl, owner)
		VALUES (id, 'submitted', 'INDEX_BUILD', 1259, obj, now(), ddl, session_user);
	PERFORM pg_notify('gs_dsql_jobs', id);
	RETURN id;
END $fn$;

CREATE EXTENSION IF NOT EXISTS dblink SCHEMA sys;

CREATE OR REPLACE FUNCTION sys.wait_for_job(job_id text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, sys, pg_temp AS $fn$
DECLARE
	st text;
	conn constant text := 'gs_wait_' || pg_backend_pid();
BEGIN
	PERFORM sys.dblink_connect(conn, 'dbname=postgres user=gsroot');
	BEGIN
		LOOP
			SELECT r.status INTO st FROM sys.dblink(conn,
				format('SELECT status FROM sys.gs_jobs WHERE job_id = %L', wait_for_job.job_id)) AS r(status text);
			IF NOT FOUND THEN
				RAISE EXCEPTION 'job "%" does not exist', wait_for_job.job_id USING ERRCODE = 'no_data_found';
			END IF;
			IF st IN ('completed', 'failed') THEN
				PERFORM sys.dblink_disconnect(conn);
				RETURN st = 'completed';
			END IF;
			PERFORM pg_sleep(0.05);
		END LOOP;
	EXCEPTION WHEN OTHERS THEN
		IF conn = ANY (sys.dblink_get_connections()) THEN
			PERFORM sys.dblink_disconnect(conn);
		END IF;
		RAISE;
	END;
END $fn$;

CREATE TABLE IF NOT EXISTS sys.gs_limits (name text PRIMARY KEY, value bigint NOT NULL);
INSERT INTO sys.gs_limits (name, value) VALUES
	('columns', 255), ('indexes', 24), ('index_keys', 8), ('ext_stats', 5), ('schemas', 10), ('tables', 1000),
	('views', 5000), ('sequences', 5000), ('txn_rows', 3000), ('txn_bytes', 10485760), ('row_bytes', 2097152),
	('col_bytes', 1048576)
	ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION sys.gs_limit(n text) RETURNS bigint
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $fn$ SELECT value FROM sys.gs_limits WHERE name = n $fn$;

CREATE OR REPLACE FUNCTION sys.gs_txn_guard(
	kind text, n bigint, bytes bigint, maxrow bigint, maxcol bigint) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
	total bigint;
	size bigint;
BEGIN
	IF session_user = 'gsroot' OR current_setting('gs.internal', true) = '1' THEN
		RETURN;
	END IF;
	IF kind = 'ddl' THEN
		IF current_setting('gs.dml', true) = '1' THEN
			RAISE EXCEPTION 'ddl and dml are not supported in the same transaction' USING ERRCODE = '0A000';
		END IF;
		IF current_setting('gs.ddl', true) = '1' THEN
			RAISE EXCEPTION 'only one DDL statement is allowed per transaction' USING ERRCODE = '0A000';
		END IF;
		PERFORM set_config('gs.ddl', '1', true);
		RETURN;
	END IF;
	IF current_setting('gs.ddl', true) = '1' THEN
		RAISE EXCEPTION 'ddl and dml are not supported in the same transaction' USING ERRCODE = '0A000';
	END IF;
	IF maxrow > sys.gs_limit('row_bytes') THEN
		RAISE EXCEPTION 'maximum row size exceeded' USING ERRCODE = '54000';
	END IF;
	IF maxcol > sys.gs_limit('col_bytes') THEN
		RAISE EXCEPTION 'maximum column size exceeded' USING ERRCODE = '54000';
	END IF;
	total := coalesce(nullif(current_setting('gs.rows', true), ''), '0')::bigint + n;
	IF total > sys.gs_limit('txn_rows') THEN
		RAISE EXCEPTION 'transaction row limit exceeded' USING ERRCODE = '54000';
	END IF;
	size := coalesce(nullif(current_setting('gs.bytes', true), ''), '0')::bigint + bytes;
	IF size > sys.gs_limit('txn_bytes') THEN
		RAISE EXCEPTION 'transaction size limit %mb exceeded', sys.gs_limit('txn_bytes') / 1048576
			USING ERRCODE = '54000', DETAIL = format('Current transaction size %smb', round(size / 1048576.0, 2));
	END IF;
	PERFORM set_config('gs.dml', '1', true);
	PERFORM set_config('gs.rows', total::text, true);
	PERFORM set_config('gs.bytes', size::text, true);
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_dml_ins() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
	PERFORM sys.gs_txn_guard('dml', s.n, s.total, s.widest, s.col)
		FROM (SELECT count(*) AS n, coalesce(sum(octet_length(t::text)), 0) AS total,
			coalesce(max(octet_length(t::text)), 0) AS widest,
			coalesce(max((SELECT max(octet_length(e.value)) FROM jsonb_each_text(to_jsonb(t)) e)), 0) AS col
			FROM gs_new t) s;
	RETURN NULL;
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_dml_upd() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
	PERFORM sys.gs_txn_guard('dml', s.n, s.total, s.widest, s.col)
		FROM (SELECT count(*) AS n, coalesce(sum(octet_length(t::text)), 0) AS total,
			coalesce(max(octet_length(t::text)), 0) AS widest,
			coalesce(max((SELECT max(octet_length(e.value)) FROM jsonb_each_text(to_jsonb(t)) e)), 0) AS col
			FROM gs_new t) s;
	RETURN NULL;
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_dml_del() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
	PERFORM sys.gs_txn_guard('dml', s.n, s.total, 0, 0)
		FROM (SELECT count(*) AS n, coalesce(sum(octet_length(t::text)), 0) AS total FROM gs_old t) s;
	RETURN NULL;
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_ddl_start() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
	IF TG_TAG = 'DROP TRIGGER' AND session_user <> 'gsroot' THEN
		RAISE EXCEPTION 'DROP TRIGGER is not supported in Aurora DSQL' USING ERRCODE = '0A000';
	END IF;
	PERFORM sys.gs_txn_guard('ddl', 0, 0, 0, 0);
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_ddl_end() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
	r record;
BEGIN
	FOR r IN SELECT c.objid, c.schema_name FROM pg_event_trigger_ddl_commands() c
		WHERE c.object_type = 'table' AND c.schema_name NOT IN ('sys', 'pg_catalog')
	LOOP
		IF EXISTS (SELECT 1 FROM pg_class k WHERE k.oid = r.objid AND k.relkind = 'r' AND NOT k.relispartition) THEN
			PERFORM set_config('gs.internal', '1', true);
			EXECUTE format('CREATE TRIGGER gs_dml_ins AFTER INSERT ON %s REFERENCING NEW TABLE AS gs_new '
				'FOR EACH STATEMENT EXECUTE FUNCTION sys.gs_dml_ins()', r.objid::regclass);
			EXECUTE format('CREATE TRIGGER gs_dml_upd AFTER UPDATE ON %s REFERENCING NEW TABLE AS gs_new '
				'FOR EACH STATEMENT EXECUTE FUNCTION sys.gs_dml_upd()', r.objid::regclass);
			EXECUTE format('CREATE TRIGGER gs_dml_del AFTER DELETE ON %s REFERENCING OLD TABLE AS gs_old '
				'FOR EACH STATEMENT EXECUTE FUNCTION sys.gs_dml_del()', r.objid::regclass);
			PERFORM set_config('gs.internal', '', true);
		END IF;
	END LOOP;
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_count_user(kind "char") RETURNS bigint
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
	SELECT count(*) FROM pg_class k JOIN pg_namespace n ON n.oid = k.relnamespace
	WHERE k.relkind = kind AND n.nspname NOT LIKE 'pg\_%' AND n.nspname NOT IN ('information_schema', 'sys')
$fn$;

CREATE OR REPLACE FUNCTION sys.gs_check_relation(rel oid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
	IF (SELECT count(*) FROM pg_attribute a WHERE a.attrelid = rel AND a.attnum > 0 AND NOT a.attisdropped)
		> sys.gs_limit('columns') THEN
		RAISE EXCEPTION 'tables can have at most % columns', sys.gs_limit('columns') USING ERRCODE = '54011';
	END IF;
	IF (SELECT coalesce(max(i.indnkeyatts), 0) FROM pg_index i WHERE i.indrelid = rel) > sys.gs_limit('index_keys') THEN
		RAISE EXCEPTION 'more than % column keys in an index are not supported', sys.gs_limit('index_keys')
			USING ERRCODE = '54011';
	END IF;
	IF (SELECT count(*) FROM pg_index i WHERE i.indrelid = rel AND NOT i.indisprimary) > sys.gs_limit('indexes') THEN
		RAISE EXCEPTION 'more than % indexes per table are not allowed', sys.gs_limit('indexes') USING ERRCODE = '54000';
	END IF;
	IF (SELECT count(*) FROM pg_statistic_ext e WHERE e.stxrelid = rel) > sys.gs_limit('ext_stats') THEN
		RAISE EXCEPTION 'more than % extended statistics per table are not allowed', sys.gs_limit('ext_stats')
			USING ERRCODE = '54000';
	END IF;
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_ddl_limits() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
	r record;
	rel oid;
BEGIN
	FOR r IN SELECT c.objid, c.object_type, c.command_tag FROM pg_event_trigger_ddl_commands() c LOOP
		rel := NULL;
		IF r.object_type = 'table' THEN
			rel := r.objid;
			IF r.command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
				AND sys.gs_count_user('r') + sys.gs_count_user('p') > sys.gs_limit('tables') THEN
				RAISE EXCEPTION 'creating more than % tables not allowed', sys.gs_limit('tables') USING ERRCODE = '54000';
			END IF;
		ELSIF r.object_type = 'index' THEN
			SELECT i.indrelid INTO rel FROM pg_index i WHERE i.indexrelid = r.objid;
		ELSIF r.object_type = 'statistics object' THEN
			SELECT e.stxrelid INTO rel FROM pg_statistic_ext e WHERE e.oid = r.objid;
		ELSIF r.object_type = 'schema' THEN
			IF (SELECT count(*) FROM pg_namespace n WHERE n.nspname NOT LIKE 'pg\_%'
				AND n.nspname NOT IN ('information_schema', 'sys')) > sys.gs_limit('schemas') THEN
				RAISE EXCEPTION 'more than % schemas not allowed', sys.gs_limit('schemas') USING ERRCODE = '54000';
			END IF;
		ELSIF r.object_type = 'view' AND sys.gs_count_user('v') > sys.gs_limit('views') THEN
			RAISE EXCEPTION 'creating more than % views not allowed', sys.gs_limit('views') USING ERRCODE = '54000';
		ELSIF r.object_type = 'sequence' AND sys.gs_count_user('S') > sys.gs_limit('sequences') THEN
			RAISE EXCEPTION 'creating more than % sequences is not allowed', sys.gs_limit('sequences')
				USING ERRCODE = '54000';
		END IF;
		IF rel IS NOT NULL THEN
			PERFORM sys.gs_check_relation(rel);
		END IF;
	END LOOP;
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_iam_grant(role_name text, iam_arn text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
DECLARE
	secret text;
BEGIN
	IF session_user <> 'admin' THEN
		RAISE EXCEPTION 'permission denied to grant IAM access' USING ERRCODE = 'insufficient_privilege';
	END IF;
	IF role_name = 'admin' OR NOT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname = role_name AND r.rolcanlogin) THEN
		RAISE EXCEPTION 'role "%" does not exist or cannot log in', role_name USING ERRCODE = 'undefined_object';
	END IF;
	INSERT INTO sys.gs_role_secrets (role_name, secret)
		VALUES (role_name, md5(random()::text || clock_timestamp()::text) || md5(random()::text))
		ON CONFLICT DO NOTHING;
	SELECT s.secret INTO secret FROM sys.gs_role_secrets s WHERE s.role_name = gs_iam_grant.role_name;
	EXECUTE format('ALTER ROLE %I WITH PASSWORD %L', role_name, secret);
	INSERT INTO sys.gs_iam_grants (arn, role_name, grantor) VALUES (iam_arn, role_name, session_user);
EXCEPTION WHEN unique_violation THEN
	RAISE EXCEPTION 'role "%" is already associated with "%"', role_name, iam_arn USING ERRCODE = 'duplicate_object';
END $fn$;

CREATE OR REPLACE FUNCTION sys.gs_iam_revoke(role_name text, iam_arn text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $fn$
BEGIN
	IF session_user <> 'admin' THEN
		RAISE EXCEPTION 'permission denied to revoke IAM access' USING ERRCODE = 'insufficient_privilege';
	END IF;
	DELETE FROM sys.gs_iam_grants g WHERE g.arn = iam_arn AND g.role_name = gs_iam_revoke.role_name;
	IF NOT FOUND THEN
		RAISE EXCEPTION 'role "%" is not associated with "%"', role_name, iam_arn USING ERRCODE = 'undefined_object';
	END IF;
END $fn$;

REVOKE ALL ON ALL TABLES IN SCHEMA sys FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA sys FROM PUBLIC;
GRANT USAGE ON SCHEMA sys TO PUBLIC;
GRANT SELECT ON sys.jobs, sys.iam_pg_role_mappings TO PUBLIC;
REVOKE ALL ON FUNCTION sys.gs_create_index_async(text), sys.wait_for_job(text), sys.gs_iam_grant(text, text),
	sys.gs_iam_revoke(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sys.gs_create_index_async(text), sys.wait_for_job(text), sys.gs_iam_grant(text, text),
	sys.gs_iam_revoke(text, text) TO PUBLIC;
ALTER DATABASE postgres SET timezone TO 'UTC';
ALTER DATABASE postgres SET default_transaction_isolation TO 'repeatable read';

CREATE EVENT TRIGGER gs_ddl_start ON ddl_command_start EXECUTE FUNCTION sys.gs_ddl_start();
CREATE EVENT TRIGGER gs_ddl_end ON ddl_command_end
	WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO') EXECUTE FUNCTION sys.gs_ddl_end();
CREATE EVENT TRIGGER gs_ddl_limits ON ddl_command_end
	WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO', 'ALTER TABLE', 'CREATE INDEX', 'CREATE STATISTICS',
		'CREATE SCHEMA', 'CREATE VIEW', 'CREATE SEQUENCE') EXECUTE FUNCTION sys.gs_ddl_limits();
ALTER SCHEMA public OWNER TO admin;
GRANT CREATE ON DATABASE postgres TO admin;
`
