CREATE FUNCTION @extschema@._invoke(p_function text, p_payload text, p_invocation_type text, p_log_type text,
                                    p_context text, p_qualifier text)
RETURNS TABLE (status_code int, payload json, executed_version text, log_result text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $fn$
DECLARE
  r record;
  hdrs text;
  q text;
  body text;
BEGIN
  IF p_invocation_type NOT IN ('RequestResponse', 'Event', 'DryRun') THEN
    RAISE EXCEPTION 'invalid invocation_type: %', p_invocation_type USING ERRCODE = '22023';
  END IF;
  IF p_log_type NOT IN ('None', 'Tail') THEN
    RAISE EXCEPTION 'invalid log_type: %', p_log_type USING ERRCODE = '22023';
  END IF;
  PERFORM aws_commons._check('Lambda', false);
  hdrs := 'X-Amz-Invocation-Type: ' || p_invocation_type || E'\n' || 'X-Amz-Log-Type: ' || p_log_type;
  IF p_context IS NOT NULL THEN
    hdrs := hdrs || E'\nX-Amz-Client-Context: ' || aws_commons._b64(p_context);
  END IF;
  q := CASE WHEN p_qualifier IS NULL OR p_qualifier = '' THEN '' ELSE '?Qualifier=' || aws_commons._urlenc(p_qualifier) END;
  SELECT * INTO r FROM aws_commons._call('POST',
    '/lambda/' || aws_commons._urlenc(p_function) || q, hdrs, aws_commons._b64(coalesce(p_payload, 'null')));
  IF r.status >= 400 THEN
    RAISE EXCEPTION 'Lambda invoke failed with HTTP %: %', r.status, aws_commons._unb64(r.body)
      USING ERRCODE = 'XX000';
  END IF;
  body := aws_commons._unb64(r.body);
  RETURN QUERY SELECT r.status, CASE WHEN body = '' THEN NULL ELSE body::json END,
                      nullif(r.executed_version, ''), nullif(r.log_result, '');
END
$fn$;

CREATE FUNCTION @extschema@.invoke(function_name text, payload json, region text DEFAULT NULL,
                                   invocation_type text DEFAULT 'RequestResponse', log_type text DEFAULT 'None',
                                   context json DEFAULT NULL, qualifier varchar(128) DEFAULT NULL)
RETURNS TABLE (status_code int, payload json, executed_version text, log_result text)
LANGUAGE sql AS
$$ SELECT * FROM @extschema@._invoke(function_name, payload::text, invocation_type, log_type, context::text, qualifier) $$;

CREATE FUNCTION @extschema@.invoke(function_name text, payload jsonb, region text DEFAULT NULL,
                                   invocation_type text DEFAULT 'RequestResponse', log_type text DEFAULT 'None',
                                   context jsonb DEFAULT NULL, qualifier varchar(128) DEFAULT NULL)
RETURNS TABLE (status_code int, payload json, executed_version text, log_result text)
LANGUAGE sql AS
$$ SELECT * FROM @extschema@._invoke(function_name, payload::text, invocation_type, log_type, context::text, qualifier) $$;

CREATE FUNCTION @extschema@.invoke(function_name aws_commons._lambda_function_arn_1, payload json,
                                   invocation_type text DEFAULT 'RequestResponse', log_type text DEFAULT 'None',
                                   context json DEFAULT NULL, qualifier varchar(128) DEFAULT NULL)
RETURNS TABLE (status_code int, payload json, executed_version text, log_result text)
LANGUAGE sql AS
$$ SELECT * FROM @extschema@._invoke((function_name).function_name, payload::text, invocation_type, log_type,
                                     context::text, qualifier) $$;

CREATE FUNCTION @extschema@.invoke(function_name aws_commons._lambda_function_arn_1, payload jsonb,
                                   invocation_type text DEFAULT 'RequestResponse', log_type text DEFAULT 'None',
                                   context jsonb DEFAULT NULL, qualifier varchar(128) DEFAULT NULL)
RETURNS TABLE (status_code int, payload json, executed_version text, log_result text)
LANGUAGE sql AS
$$ SELECT * FROM @extschema@._invoke((function_name).function_name, payload::text, invocation_type, log_type,
                                     context::text, qualifier) $$;
