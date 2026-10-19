package redshiftdata

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

func TestParseCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, s *copySpec)
		name    string
		sql     string
		errPart string
		handled bool
	}{
		{
			name: "csv_iam_role",
			sql: "COPY public.sales (id, \"Name\") FROM 's3://b/in/' IAM_ROLE 'arn:aws:iam::1:role/r' " +
				"FORMAT AS CSV IGNOREHEADER 1 GZIP DELIMITER ';' REGION 'us-east-1';",
			handled: true,
			check: func(t *testing.T, s *copySpec) {
				t.Helper()
				assert.Equal(t, fmtCSV, s.format)
				assert.Equal(t, "public.sales", s.table)
				assert.Equal(t, []string{"id", "Name"}, s.columns)
				assert.Equal(t, `(id, "Name")`, s.columnsRaw)
				assert.Equal(t, 1, s.ignoreHeader)
				assert.True(t, s.gzip)
				assert.Equal(t, ";", s.delimiter)
			},
		},
		{
			name: "json_paths", sql: "copy t from 's3://b/p' iam_role default format json 's3://b/paths.json' manifest",
			handled: true,
			check: func(t *testing.T, s *copySpec) {
				t.Helper()
				assert.Equal(t, fmtLines, s.format)
				assert.Equal(t, "s3://b/paths.json", s.jsonPaths)
				assert.True(t, s.manifest)
			},
		},
		{
			name: "parquet", sql: "COPY t FROM 's3://b/p' CREDENTIALS 'aws_iam_role=x' PARQUET",
			handled: true,
			check: func(t *testing.T, s *copySpec) {
				t.Helper()
				assert.Equal(t, fmtParquet, s.format)
			},
		},
		{name: "local_source_passes_through", sql: "COPY t FROM '/tmp/x.csv' CSV", handled: false},
		{name: "plain_select", sql: "SELECT 1", handled: false},
		{
			name: "unknown_option", sql: "COPY t FROM 's3://b/p' BOGUS", handled: true,
			errPart: `syntax error at or near "BOGUS"`,
		},
		{
			name: "avro_unsupported", sql: "COPY t FROM 's3://b/p' FORMAT AS AVRO 'auto'", handled: true,
			errPart: "AVRO is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, handled, err := parseCopy(tt.sql)
			assert.Equal(t, tt.handled, handled)

			if tt.errPart != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errPart)

				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, s)
			}
		})
	}
}

func TestParseUnload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, s *unloadSpec)
		name    string
		sql     string
		errPart string
	}{
		{
			name: "parquet_partitioned",
			sql: "UNLOAD ('select * from t where n = ''x''') TO 's3://bkt/out/p_' IAM_ROLE 'arn' " +
				"FORMAT AS PARQUET PARTITION BY (region, yr) INCLUDE ALLOWOVERWRITE MANIFEST VERBOSE",
			check: func(t *testing.T, s *unloadSpec) {
				t.Helper()
				assert.Equal(t, "select * from t where n = 'x'", s.query)
				assert.Equal(t, "bkt", s.bucket)
				assert.Equal(t, "out/p_", s.prefix)
				assert.Equal(t, fmtParquet, s.format)
				assert.Equal(t, []string{"region", "yr"}, s.partitionBy)
				assert.True(t, s.includePart)
				assert.True(t, s.allowOverride)
				assert.True(t, s.verbose)
			},
		},
		{
			name: "delimited_options",
			sql: "UNLOAD ($$select 1$$) TO 's3://bkt/p' DELIMITER AS ',' HEADER GZIP NULL AS 'nil' ADDQUOTES " +
				"PARALLEL OFF MAXFILESIZE 100 MB EXTENSION 'txt'",
			check: func(t *testing.T, s *unloadSpec) {
				t.Helper()
				assert.Equal(t, "select 1", s.query)
				assert.Equal(t, ",", s.delimiter)
				assert.True(t, s.header)
				assert.True(t, s.gzip)
				assert.Equal(t, "nil", s.nullAs)
				assert.Equal(t, "txt", s.extension)
				assert.Equal(t, ".txt.gz", s.fileExtension())
			},
		},
		{name: "not_s3", sql: "UNLOAD ('select 1') TO '/tmp/x'", errPart: "must be an S3 URL"},
		{name: "bad_option", sql: "UNLOAD ('select 1') TO 's3://b/p' FROB", errPart: `at or near "FROB"`},
		{name: "bzip2", sql: "UNLOAD ('select 1') TO 's3://b/p' BZIP2", errPart: "BZIP2 is not supported"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := parseUnload(tt.sql)
			if tt.errPart != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errPart)

				return
			}

			require.NoError(t, err)
			tt.check(t, s)
		})
	}
}

func TestCopyStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "default_pipe_text",
			sql:  "COPY t FROM 's3://b/p'",
			want: `COPY t FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '\N')`,
		},
		{
			name: "csv_columns_quote",
			sql:  "COPY s.t (a,b) FROM 's3://b/p' CSV QUOTE AS '\"'",
			want: `COPY s.t (a, b) FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '', QUOTE '"')`,
		},
		{
			name: "tab_delimiter",
			sql:  `COPY t FROM 's3://b/p' DELIMITER '\t' NULL AS 'NA'`,
			want: "COPY t FROM STDIN WITH (FORMAT text, DELIMITER '\t', NULL 'NA')",
		},
		{
			name: "json_uses_tab_text",
			sql:  "COPY t FROM 's3://b/p' JSON 'auto'",
			want: "COPY t FROM STDIN WITH (FORMAT text, DELIMITER '\t', NULL '\\N')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, handled, err := parseCopy(tt.sql)
			require.NoError(t, err)
			require.True(t, handled)
			assert.Equal(t, tt.want, copyStatement(s))
		})
	}
}

func TestAppendCopyObject(t *testing.T) {
	t.Parallel()

	gz := func(s string) []byte {
		var b bytes.Buffer

		zw := gzip.NewWriter(&b)
		_, _ = io.WriteString(zw, s)
		_ = zw.Close()

		return b.Bytes()
	}

	paths := `{"jsonpaths":["$['id']","$.user.name","$.tags[1]"]}`

	tests := []struct {
		name    string
		sql     string
		want    string
		errPart string
		cols    []string
		data    []byte
	}{
		{
			name: "ignoreheader_and_blank_lines",
			sql:  "COPY t FROM 's3://b/p' IGNOREHEADER 1 IGNOREBLANKLINES",
			data: []byte("id|name\n1|a\n\n2|b"),
			want: "1|a\n2|b\n",
		},
		{
			name: "gzip",
			sql:  "COPY t FROM 's3://b/p' GZIP",
			data: gz("1|a\n"),
			want: "1|a\n",
		},
		{
			name: "json_auto_case_insensitive",
			sql:  "COPY t FROM 's3://b/p' JSON 'auto'",
			cols: []string{"id", "name", "meta", "gone"},
			data: []byte(`{"ID":1,"name":"a\tb","meta":{"k":[1,2]}}` + "\n" + `{"id":2,"name":null}`),
			want: "1\ta\\tb\t{\"k\":[1,2]}\t\\N\n2\t\\N\t\\N\t\\N\n",
		},
		{
			name: "jsonpaths_file",
			sql:  "COPY t FROM 's3://b/p' JSON 's3://b/paths'",
			cols: []string{"a", "b", "c"},
			data: []byte(`{"id":7,"user":{"name":"zed"},"tags":["x","y"]}`),
			want: "7\tzed\ty\n",
		},
		{
			name:    "jsonpaths_count_mismatch",
			sql:     "COPY t FROM 's3://b/p' JSON 's3://b/paths'",
			cols:    []string{"a"},
			data:    []byte(`{}`),
			errPart: "jsonpaths and the number of columns",
		},
		{
			name:    "json_record_not_object",
			sql:     "COPY t FROM 's3://b/p' JSON 'auto'",
			cols:    []string{"a"},
			data:    []byte(`[1]`),
			errPart: "must be a JSON object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, handled, err := parseCopy(tt.sql)
			require.NoError(t, err)
			require.True(t, handled)

			var out bytes.Buffer

			fetch := func(string) ([]byte, error) { return []byte(paths), nil }

			err = appendCopyObject(&out, s, tt.cols, tt.data, fetch)
			if tt.errPart != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errPart)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, out.String())
		})
	}
}

func str(s string) Field { return Field{StringValue: &s} }

func lng(v int64) Field { return Field{LongValue: &v} }

func TestUnloadEncoders(t *testing.T) {
	t.Parallel()

	cols := []ColumnMetadata{
		{Name: "id", TypeName: "int8"},
		{Name: "note", TypeName: "text"},
		{Name: "region", TypeName: "text"},
	}
	rows := [][]Field{
		{lng(1), str(`a,"b"`), str("east")},
		{lng(2), {IsNull: true}, str("west")},
	}

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "delimited_default",
			sql:  "UNLOAD ('q') TO 's3://b/p'",
			want: "1|a,\"b\"|east\n2||west\n",
		},
		{
			name: "delimited_options",
			sql:  "UNLOAD ('q') TO 's3://b/p' DELIMITER ';' HEADER NULL AS 'NULL' ADDQUOTES",
			want: "id;note;region\n\"1\";\"a,\"b\"\";\"east\"\n\"2\";NULL;\"west\"\n",
		},
		{
			name: "csv",
			sql:  "UNLOAD ('q') TO 's3://b/p' CSV HEADER",
			want: "id,note,region\n1,\"a,\"\"b\"\"\",east\n2,,west\n",
		},
		{
			name: "json_lines",
			sql:  "UNLOAD ('q') TO 's3://b/p' FORMAT JSON",
			want: `{"id":1,"note":"a,\"b\"","region":"east"}` + "\n" + `{"id":2,"note":null,"region":"west"}` + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := parseUnload(tt.sql)
			require.NoError(t, err)

			got, err := encodeUnload(s, cols, rows)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func TestUnloadFilesPartitionAndParquet(t *testing.T) {
	t.Parallel()

	rs := &ResultSet{
		Columns: []ColumnMetadata{
			{Name: "id", TypeName: "int4"}, {Name: "region", TypeName: "text"},
		},
		Rows: [][]Field{
			{lng(1), str("us east")}, {lng(2), str("west")}, {lng(3), str("us east")},
		},
	}

	s, err := parseUnload("UNLOAD ('q') TO 's3://b/out/' PARQUET PARTITION BY (region) GZIP MANIFEST")
	require.Error(t, err)
	assert.Nil(t, s)

	s, err = parseUnload("UNLOAD ('q') TO 's3://b/out/' PARQUET PARTITION BY (region)")
	require.NoError(t, err)

	files, err := buildUnloadFiles(s, rs)
	require.NoError(t, err)
	require.Len(t, files, 2)
	assert.Equal(t, "out/region=us%20east/0000_part_00.parquet", files[0].key)
	assert.Equal(t, "out/region=west/0000_part_00.parquet", files[1].key)
	assert.Equal(t, 2, files[0].records)

	f, err := parquetio.Open(files[0].body)
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, f.Columns())
	assert.Equal(t, int64(2), f.NumRows())

	var ids []any

	require.NoError(t, f.ForEach([]string{"id"}, func(v []any) error {
		ids = append(ids, v[0])

		return nil
	}))
	assert.Equal(t, []any{int64(1), int64(3)}, ids)
}

func TestParquetCopyRoundTrip(t *testing.T) {
	t.Parallel()

	rs := &ResultSet{
		Columns: []ColumnMetadata{{Name: "id", TypeName: "int8"}, {Name: "name", TypeName: "text"}},
		Rows:    [][]Field{{lng(1), str("a")}, {lng(2), {IsNull: true}}},
	}

	body, err := encodeParquet(rs.Columns, rs.Rows)
	require.NoError(t, err)

	s, handled, err := parseCopy("COPY t FROM 's3://b/p' PARQUET")
	require.NoError(t, err)
	require.True(t, handled)

	var out bytes.Buffer

	require.NoError(t, appendCopyObject(&out, s, []string{"id", "name"}, body, nil))
	assert.Equal(t, "1\ta\n2\t\\N\n", out.String())

	err = appendCopyObject(&out, s, []string{"only"}, body, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "columns")
}
