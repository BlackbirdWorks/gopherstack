CREATE TYPE @extschema@._s3_uri_1 AS (bucket text, file_path text, region text);
CREATE TYPE @extschema@._aws_credentials_1 AS (access_key text, secret_key text, session_token text);
CREATE TYPE @extschema@._lambda_function_arn_1 AS (function_name text, region text);

CREATE FUNCTION @extschema@.create_s3_uri(bucket text, file_path text, region text DEFAULT NULL)
RETURNS @extschema@._s3_uri_1 LANGUAGE sql IMMUTABLE AS
$$ SELECT ROW(bucket, file_path, region)::@extschema@._s3_uri_1 $$;

CREATE FUNCTION @extschema@.create_aws_credentials(access_key text, secret_key text, session_token text DEFAULT '')
RETURNS @extschema@._aws_credentials_1 LANGUAGE sql IMMUTABLE AS
$$ SELECT ROW(access_key, secret_key, session_token)::@extschema@._aws_credentials_1 $$;

CREATE FUNCTION @extschema@.create_lambda_function_arn(function_name text, region text DEFAULT NULL)
RETURNS @extschema@._lambda_function_arn_1 LANGUAGE sql IMMUTABLE AS
$$ SELECT ROW(function_name, region)::@extschema@._lambda_function_arn_1 $$;

CREATE FUNCTION @extschema@._urlenc(v text, keep_slash boolean DEFAULT false) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce(string_agg(CASE
    WHEN b BETWEEN 48 AND 57 OR b BETWEEN 65 AND 90 OR b BETWEEN 97 AND 122 OR b IN (45, 46, 95, 126)
      OR (keep_slash AND b = 47) THEN chr(b)
    ELSE '%' || lpad(to_hex(b), 2, '0') END, '' ORDER BY i), '')
  FROM (SELECT i, get_byte(convert_to(v, 'UTF8'), i) AS b
        FROM generate_series(0, octet_length(convert_to(v, 'UTF8')) - 1) AS i) s
$$;

CREATE FUNCTION @extschema@._b64(v text) RETURNS text LANGUAGE sql IMMUTABLE AS
$$ SELECT replace(encode(convert_to(v, 'UTF8'), 'base64'), E'\n', '') $$;

CREATE FUNCTION @extschema@._unb64(v text) RETURNS text LANGUAGE sql IMMUTABLE AS
$$ SELECT convert_from(decode(v, 'base64'), 'UTF8') $$;

CREATE FUNCTION @extschema@._call(p_method text, p_path text, p_headers text DEFAULT '', p_body text DEFAULT NULL)
RETURNS TABLE (status int, executed_version text, log_result text, function_error text, content_length text, body text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $fn$
DECLARE
  cmd text;
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS pg_temp.gs_call_out (status int, v text, l text, e text, n text, b text);
  TRUNCATE pg_temp.gs_call_out;
  cmd := 'perl -e "$GS_AWS_HELPER" json ' || p_method || ' ' || quote_literal(p_path) || ' ' ||
         quote_literal(p_headers) || ' ' || quote_literal(coalesce(p_body, ''));
  EXECUTE format('COPY pg_temp.gs_call_out FROM PROGRAM %L', cmd);
  RETURN QUERY SELECT o.status, @extschema@._unb64(o.v), @extschema@._unb64(o.l), @extschema@._unb64(o.e),
                      @extschema@._unb64(o.n), o.b FROM pg_temp.gs_call_out o;
END
$fn$;

CREATE FUNCTION @extschema@._check(feature text, has_credentials boolean) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $fn$
DECLARE
  r record;
BEGIN
  SELECT * INTO r FROM @extschema@._call('POST', '/auth/' || feature || '?cred=' || has_credentials::int);
  IF r.status = 403 THEN
    RAISE EXCEPTION '%', @extschema@._unb64(r.body) USING ERRCODE = '42501';
  ELSIF r.status <> 200 THEN
    RAISE EXCEPTION 'aws callout failed with HTTP %: %', r.status, @extschema@._unb64(r.body);
  END IF;
END
$fn$;
