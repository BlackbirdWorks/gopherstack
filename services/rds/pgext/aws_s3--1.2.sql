CREATE FUNCTION @extschema@._import(table_name text, column_list text, options text,
                                    bucket text, file_path text, has_credentials boolean)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER AS $fn$
DECLARE
  path text := '/s3/' || aws_commons._urlenc(bucket) || '/' || aws_commons._urlenc(file_path, true);
  opts text := coalesce(nullif(btrim(options), ''), '(format text)');
  cols text := coalesce(nullif(btrim(column_list), ''), '');
  head record;
  n bigint;
BEGIN
  PERFORM aws_commons._check('s3Import', has_credentials);
  SELECT * INTO head FROM aws_commons._call('HEAD', path);
  IF head.status = 404 THEN
    RAISE EXCEPTION 'The specified key does not exist: s3://%/%', bucket, file_path USING ERRCODE = '58030';
  ELSIF head.status <> 200 THEN
    RAISE EXCEPTION 'HTTP %. Permission denied. Check bucket or provided credentials as they may no longer be valid.',
      head.status USING ERRCODE = '42501';
  END IF;
  IF left(opts, 1) <> '(' THEN opts := '(' || opts || ')'; END IF;
  EXECUTE format('COPY %s %s FROM PROGRAM %L WITH %s', table_name,
                 CASE WHEN cols = '' THEN '' ELSE '(' || cols || ')' END,
                 'perl -e "$GS_AWS_HELPER" raw GET ' || quote_literal(path), opts);
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN format('%s rows imported into relation "%s" from file %s of %s bytes', n, table_name, file_path,
                head.content_length);
END
$fn$;

CREATE FUNCTION @extschema@.table_import_from_s3(table_name text, column_list text, options text,
                                                 bucket text, file_path text, region text)
RETURNS text LANGUAGE sql AS
$$ SELECT @extschema@._import(table_name, column_list, options, bucket, file_path, false) $$;

CREATE FUNCTION @extschema@.table_import_from_s3(table_name text, column_list text, options text,
                                                 bucket text, file_path text, region text,
                                                 access_key text, secret_key text, session_token text)
RETURNS text LANGUAGE sql AS
$$ SELECT @extschema@._import(table_name, column_list, options, bucket, file_path, true) $$;

CREATE FUNCTION @extschema@.table_import_from_s3(table_name text, column_list text, options text,
                                                 s3_info aws_commons._s3_uri_1)
RETURNS text LANGUAGE sql AS
$$ SELECT @extschema@._import(table_name, column_list, options, s3_info.bucket, s3_info.file_path, false) $$;

CREATE FUNCTION @extschema@.table_import_from_s3(table_name text, column_list text, options text,
                                                 s3_info aws_commons._s3_uri_1,
                                                 credentials aws_commons._aws_credentials_1)
RETURNS text LANGUAGE sql AS
$$ SELECT @extschema@._import(table_name, column_list, options, s3_info.bucket, s3_info.file_path, true) $$;

CREATE FUNCTION @extschema@._export(query text, bucket text, file_path text, options text, has_credentials boolean)
RETURNS TABLE (rows_uploaded bigint, files_uploaded bigint, bytes_uploaded bigint)
LANGUAGE plpgsql SECURITY DEFINER AS $fn$
DECLARE
  path text := '/s3/' || aws_commons._urlenc(bucket) || '/' || aws_commons._urlenc(file_path, true);
  n bigint;
  head record;
BEGIN
  PERFORM aws_commons._check('s3Export', has_credentials);
  EXECUTE format('COPY (%s) TO PROGRAM %L WITH (%s)', query,
                 'perl -e "$GS_AWS_HELPER" raw PUT ' || quote_literal(path),
                 coalesce(nullif(btrim(options), ''), 'format text'));
  GET DIAGNOSTICS n = ROW_COUNT;
  SELECT * INTO head FROM aws_commons._call('HEAD', path);
  IF head.status <> 200 THEN
    RAISE EXCEPTION 'export to s3://%/% failed: HTTP %', bucket, file_path, head.status;
  END IF;
  RETURN QUERY SELECT n, 1::bigint, head.content_length::bigint;
END
$fn$;

CREATE FUNCTION @extschema@.query_export_to_s3(query text, bucket text, file_path text,
                                               region text DEFAULT NULL, options text DEFAULT NULL,
                                               kms_key text DEFAULT NULL)
RETURNS TABLE (rows_uploaded bigint, files_uploaded bigint, bytes_uploaded bigint)
LANGUAGE sql AS
$$ SELECT * FROM @extschema@._export(query, bucket, file_path, options, false) $$;

CREATE FUNCTION @extschema@.query_export_to_s3(query text, s3_info aws_commons._s3_uri_1,
                                               options text DEFAULT NULL, kms_key text DEFAULT NULL)
RETURNS TABLE (rows_uploaded bigint, files_uploaded bigint, bytes_uploaded bigint)
LANGUAGE sql AS
$$ SELECT * FROM @extschema@._export(query, s3_info.bucket, s3_info.file_path, options, false) $$;
