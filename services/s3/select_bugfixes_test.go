package s3_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// selectBugfixesDrain reads a SelectObjectContent event stream to completion
// and returns the concatenated Records payload.
func selectBugfixesDrain(t *testing.T, out *sdk_s3.SelectObjectContentOutput) []byte {
	t.Helper()
	defer out.GetStream().Close()

	var records []byte

	for event := range out.GetStream().Events() {
		if v, ok := event.(*types.SelectObjectContentEventStreamMemberRecords); ok {
			records = append(records, v.Value.Payload...)
		}
	}

	require.NoError(t, out.GetStream().Err())

	return records
}

// selectBugfixesDrainExpectErr reads a SelectObjectContent event stream to
// completion and returns the terminal stream error. Query-evaluation errors
// (as opposed to request-parse errors) surface as a mid-stream exception
// event, not as a synchronous error from the initial SelectObjectContent
// call: the 200 response and headers are already committed by the time
// evaluateQuery runs, so the SDK's event-stream decoder reports the failure
// via GetStream().Err() once it reaches the exception frame.
func selectBugfixesDrainExpectErr(t *testing.T, out *sdk_s3.SelectObjectContentOutput) error {
	t.Helper()
	defer out.GetStream().Close()

	for range out.GetStream().Events() { //nolint:revive // draining the channel is the point
	}

	return out.GetStream().Err()
}

func selectBugfixesPutObject(t *testing.T, client *sdk_s3.Client, key string, data []byte) string {
	t.Helper()

	ctx := t.Context()
	bucket := "select-bugfix-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err)

	return bucket
}

func selectBugfixesErrCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "expected a real smithy.APIError from the SDK deserializer")

	return apiErr.ErrorCode()
}

// TestSelectObjectContent_TrailingClauseRejected is a regression test: the
// parser never checked for leftover tokens after WHERE/ORDER BY/LIMIT, so an
// unsupported trailing construct was silently dropped and the query ran on
// whatever prefix did parse -- e.g. "... JOIN other o ON ..." executed as a
// plain unfiltered SELECT, returning every row instead of erroring. Verified
// by hand-reverting the expectEOF() call in select_sql_parser.go: all cases
// below then return 200 with real row data instead of ParseException.
func TestSelectObjectContent_TrailingClauseRejected(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)

	tests := []struct {
		name string
		expr string
	}{
		{name: "join", expr: "SELECT s.name FROM s3object s JOIN other o ON s.dept = o.dept"},
		{name: "group by", expr: "SELECT s.name, COUNT(*) FROM s3object s GROUP BY s.name"},
		{name: "having", expr: "SELECT COUNT(*) FROM s3object s HAVING COUNT(*) > 1"},
		{name: "union", expr: "SELECT s.name FROM s3object s UNION SELECT s.name FROM s3object s"},
		{name: "trailing garbage", expr: "SELECT s.name FROM s3object s WHERE s.age > 1 bogus"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bucket := selectBugfixesPutObject(
				t, client, "data.csv", []byte("name,age,dept\nAlice,30,eng\nBob,25,sales\n"),
			)

			_, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
				Bucket:         aws.String(bucket),
				Key:            aws.String("data.csv"),
				Expression:     aws.String(tt.expr),
				ExpressionType: "SQL",
				InputSerialization: &types.InputSerialization{
					CSV: &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse},
				},
				OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
			})
			require.Error(t, err)
			require.Equal(t, "ParseException", selectBugfixesErrCode(t, err))
		})
	}
}

// TestSelectObjectContent_AggregateColumnMismatch is a regression test: this
// engine has no GROUP BY, so a plain per-row column selected alongside an
// aggregate function was silently dropped from the output rather than
// erroring -- "SELECT s.name, COUNT(*) FROM s3object s" returned only the
// count, discarding the name column with no indication anything was wrong.
// Verified by hand-reverting validateAggregateColumns() in
// select_sql_parser.go: the "plain column with aggregate" case then returns
// 200 with the name column missing instead of ParseException.
func TestSelectObjectContent_AggregateColumnMismatch(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := selectBugfixesPutObject(t, client, "data.csv", []byte("name,age\nAlice,30\nBob,25\n"))

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{name: "plain column with aggregate", expr: "SELECT s.name, COUNT(*) FROM s3object s", wantErr: true},
		{name: "literal with aggregate", expr: "SELECT 'x', COUNT(*) FROM s3object s", wantErr: false},
		{name: "only aggregates", expr: "SELECT COUNT(*), MAX(s.age) FROM s3object s", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
				Bucket:         aws.String(bucket),
				Key:            aws.String("data.csv"),
				Expression:     aws.String(tt.expr),
				ExpressionType: "SQL",
				InputSerialization: &types.InputSerialization{
					CSV: &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse},
				},
				OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
			})

			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, "ParseException", selectBugfixesErrCode(t, err))

				return
			}

			require.NoError(t, err)
			selectBugfixesDrain(t, out)
		})
	}
}

// TestSelectObjectContent_ParquetRejected is a regression test: Parquet
// input was not modeled on the wire struct at all, so a client requesting it
// fell through to the CSV default and got either a confusing CSV parse error
// or -- for byte layouts that happen to tokenize as valid-looking CSV --
// wrong data silently returned as if it were a real query result. Verified
// by hand-reverting the Parquet field/check in select.go: the request then
// returns InternalError ("reading CSV: record on line ...") instead of a
// clear UnsupportedFileType error.
func TestSelectObjectContent_ParquetRejected(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := selectBugfixesPutObject(t, client, "data.parquet", []byte("PAR1\x00\x01,junk\nnot,real,parquet\nPAR1"))

	out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String("data.parquet"),
		Expression:     aws.String("SELECT * FROM s3object"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			Parquet: &types.ParquetInput{},
		},
		OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
	})
	require.NoError(t, err)

	streamErr := selectBugfixesDrainExpectErr(t, out)
	require.Error(t, streamErr)
	require.Equal(t, "UnsupportedFileType", selectBugfixesErrCode(t, streamErr))
}

// TestSelectObjectContent_Compression is a regression test: CompressionType
// was parsed off the request XML and then never read anywhere -- compressed
// bytes went straight into the CSV reader. For an object that really was
// GZIP-compressed, the raw bytes didn't error out of the (lenient) CSV
// reader; they just produced zero rows, so the client got a silent empty
// result instead of its actual data or an error. Verified by hand-reverting
// decompressSelectInput() in select.go: the "gzip real data" case then
// returns 200 with zero records instead of the two real rows.
func TestSelectObjectContent_Compression(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)

	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	_, err := gw.Write([]byte("name,age\nAlice,30\nBob,25\n"))
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	// Precomputed with the system `bzip2` binary (stdlib compress/bzip2 is
	// decode-only) over "name,age\nAlice,30\n".
	const bzip2B64 = "QlpoOTFBWSZTWVeZ6LEAAAjdAAAQAARIACAAKqcgACKZPRM00IBoA/5uABkSViT3ou5IpwoSCvM9FiA="
	bz2, err := base64.StdEncoding.DecodeString(bzip2B64)
	require.NoError(t, err)

	tests := []struct {
		name        string
		want        string
		compression types.CompressionType
		data        []byte
	}{
		{name: "gzip", compression: types.CompressionTypeGzip, data: gz.Bytes(), want: "Alice"},
		{name: "bzip2", compression: types.CompressionTypeBzip2, data: bz2, want: "Alice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bucket := selectBugfixesPutObject(t, client, "data."+tt.name, tt.data)

			out, selectErr := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
				Bucket:         aws.String(bucket),
				Key:            aws.String("data." + tt.name),
				Expression:     aws.String("SELECT * FROM s3object"),
				ExpressionType: "SQL",
				InputSerialization: &types.InputSerialization{
					CSV:             &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse},
					CompressionType: tt.compression,
				},
				OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
			})
			require.NoError(t, selectErr)

			records := string(selectBugfixesDrain(t, out))
			require.Contains(t, records, tt.want)
		})
	}
}

// TestSelectObjectContent_CSVColumnOrder is a regression test: CSV result
// rows are stored as map[string]string, which has no iteration order, so
// writing them out with encoding/csv needs an explicit column order or CSV
// output (positional, no field names) silently transposes columns.
// "SELECT * FROM s3object" over a "name,age" header came back as "age,name"
// (alphabetical, from the old sortedKeys fallback) instead of preserving the
// file's real column order; an explicit "SELECT s.name, s.age" list came
// back in the same wrong alphabetical order instead of the order the client
// asked for. Verified by hand-reverting csvOutputColumnOrder()'s use in
// select_csv.go (passing nil instead): both cases below then return
// "30,Alice" instead of "Alice,30".
func TestSelectObjectContent_CSVColumnOrder(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := selectBugfixesPutObject(t, client, "data.csv", []byte("name,age\nAlice,30\n"))

	tests := []struct {
		name string
		expr string
	}{
		{name: "select star", expr: "SELECT * FROM s3object"},
		{name: "explicit column list", expr: "SELECT s.name, s.age FROM s3object s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
				Bucket:         aws.String(bucket),
				Key:            aws.String("data.csv"),
				Expression:     aws.String(tt.expr),
				ExpressionType: "SQL",
				InputSerialization: &types.InputSerialization{
					CSV: &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse},
				},
				OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
			})
			require.NoError(t, err)

			records := string(selectBugfixesDrain(t, out))
			require.Equal(t, "Alice,30\n", records)
		})
	}
}

// TestSelectObjectContent_CompressionMismatchErrors covers the case a client
// claims a compression type that the object's bytes don't actually have --
// this should surface as a clear decompression error, not a silent empty or
// wrong result.
func TestSelectObjectContent_CompressionMismatchErrors(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := selectBugfixesPutObject(t, client, "data.csv", []byte("name,age\nAlice,30\n"))

	out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String("data.csv"),
		Expression:     aws.String("SELECT * FROM s3object"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			CSV:             &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse},
			CompressionType: types.CompressionTypeGzip,
		},
		OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
	})
	require.NoError(t, err)

	streamErr := selectBugfixesDrainExpectErr(t, out)
	require.Error(t, streamErr)
	require.Equal(t, "GZIPDecompression", selectBugfixesErrCode(t, streamErr))
}

// TestSelectObjectContent_CSVInputQuoteCharacter is a regression test for
// gopherstack-3nud: CSVInput.QuoteCharacter was parsed off the request XML
// and never read -- encoding/csv.Reader hardcodes '"' as the only quote
// character it recognises, so a client quoting fields with a custom
// character (here, a single quote) got its embedded field-delimiter treated
// as a real delimiter instead of literal content. The output is re-quoted
// with the default '"' (output serialization here is left at its default)
// because the parsed value "x,y" itself contains the output field
// delimiter. Verified by hand-reverting evaluateCSVQuery's use of
// resolveCSVInputOptions/parseCSVInput (restoring the old
// newCSVReader(csvIn, data) call): the source's unrecognised leading quote
// then splits on its embedded comma, corrupting the field.
func TestSelectObjectContent_CSVInputQuoteCharacter(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := selectBugfixesPutObject(t, client, "data.csv", []byte("name,note\nAlice,'x,y'\n"))

	out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String("data.csv"),
		Expression:     aws.String("SELECT s.note FROM s3object s"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			CSV: &types.CSVInput{
				FileHeaderInfo: types.FileHeaderInfoUse,
				QuoteCharacter: aws.String("'"),
			},
		},
		OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
	})
	require.NoError(t, err)

	records := string(selectBugfixesDrain(t, out))
	require.Equal(t, "\"x,y\"\n", records)
}

// TestSelectObjectContent_CSVInputQuoteEscapeCharacter is a regression test
// for gopherstack-3nud: CSVInput.QuoteEscapeCharacter was parsed and never
// read -- encoding/csv.Reader only understands a doubled quote ("") as an
// escaped quote inside a quoted field, so a client using a backslash to
// escape an embedded quote got the raw backslash-quote sequence back
// instead of an unescaped literal quote. The parsed value, `She said "hi"`,
// contains the output quote character, so default output serialization
// re-quotes and doubles it. Verified by hand-reverting the same
// resolveCSVInputOptions/parseCSVInput wiring: the un-recognised backslash
// escape then leaves the field truncated at the first embedded quote instead
// of the real unescaped value.
func TestSelectObjectContent_CSVInputQuoteEscapeCharacter(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := selectBugfixesPutObject(
		t, client, "data.csv", []byte(`name,quote`+"\n"+`Alice,"She said \"hi\""`+"\n"),
	)

	out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String("data.csv"),
		Expression:     aws.String("SELECT s.quote FROM s3object s"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			CSV: &types.CSVInput{
				FileHeaderInfo:       types.FileHeaderInfoUse,
				QuoteEscapeCharacter: aws.String(`\`),
			},
		},
		OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
	})
	require.NoError(t, err)

	records := string(selectBugfixesDrain(t, out))
	require.Equal(t, `"She said ""hi"""`+"\n", records)
}

// TestSelectObjectContent_CSVInputRecordDelimiter is a regression test for
// gopherstack-3nud: CSVInput's own RecordDelimiter was parsed and never
// read -- encoding/csv.Reader only ever splits records on "\n", so a client
// using a custom record delimiter got its whole object treated as a single
// header line with zero data rows. Verified by hand-reverting
// resolveCSVInputOptions/parseCSVInput: the unrecognised ";" delimiter then
// makes the entire object one CSV row (the header), so SELECT * returns no
// records instead of the two real rows.
func TestSelectObjectContent_CSVInputRecordDelimiter(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := selectBugfixesPutObject(t, client, "data.csv", []byte("name,age;Alice,30;Bob,25;"))

	out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String("data.csv"),
		Expression:     aws.String("SELECT * FROM s3object"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			CSV: &types.CSVInput{
				FileHeaderInfo:  types.FileHeaderInfoUse,
				RecordDelimiter: aws.String(";"),
			},
		},
		OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
	})
	require.NoError(t, err)

	records := string(selectBugfixesDrain(t, out))
	require.Contains(t, records, "Alice")
	require.Contains(t, records, "Bob")
}

// TestSelectObjectContent_CSVInputAllowQuotedRecordDelimiter is a
// regression test for gopherstack-3nud: CSVInput.AllowQuotedRecordDelimiter
// was parsed and never read. Its documented default is FALSE: a record
// delimiter inside a quoted field still ends the record, splitting the
// field. Setting it TRUE instead treats the delimiter inside quotes as
// literal content. Both use a custom ";" RecordDelimiter (the default "\n"
// path is handled by encoding/csv and does not go through this flag at
// all), so the flag's effect is isolated to the quote-aware-vs-blind split
// in splitCSVRecords. Verified by hand-reverting
// resolveCSVInputOptions/parseCSVInput: the "true" case then also blind
// splits (same as "false"), so both produce the broken field instead of
// only the false case doing so.
func TestSelectObjectContent_CSVInputAllowQuotedRecordDelimiter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		want        string
		allowQuoted bool
	}{
		{name: "true preserves quoted delimiter", allowQuoted: true, want: "a;b"},
		{name: "false splits inside quotes", allowQuoted: false, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := selectBugfixesPutObject(
				t, client, "data.csv", []byte(`name,note;Alice,"a;b";Bob,ok;`),
			)

			out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
				Bucket:         aws.String(bucket),
				Key:            aws.String("data.csv"),
				Expression:     aws.String("SELECT s.note FROM s3object s WHERE s.name = 'Alice'"),
				ExpressionType: "SQL",
				InputSerialization: &types.InputSerialization{
					CSV: &types.CSVInput{
						FileHeaderInfo:             types.FileHeaderInfoUse,
						RecordDelimiter:            aws.String(";"),
						AllowQuotedRecordDelimiter: aws.Bool(tt.allowQuoted),
					},
				},
				OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
			})
			require.NoError(t, err)

			records := string(selectBugfixesDrain(t, out))

			if tt.want != "" {
				require.Contains(t, records, tt.want)
			} else {
				require.NotContains(t, records, "a;b")
			}
		})
	}
}

// TestSelectObjectContent_CSVOutputQuoteOptions is a regression test for
// gopherstack-3nud: CSVOutput's QuoteCharacter, QuoteEscapeCharacter, and
// QuoteFields were all parsed and never read -- output was always written
// through encoding/csv.Writer, which hardcodes '"'/'"'/ASNEEDED. Verified by
// hand-reverting serializeCSVRows's use of resolveCSVOutputOptions/
// encodeCSVField (restoring the old csv.Writer-based loop): each case below
// then returns the RFC4180-default rendering instead of respecting the
// requested option.
func TestSelectObjectContent_CSVOutputQuoteOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		csvOut *types.CSVOutput
		name   string
		srcRow string
		want   string
	}{
		{
			name:   "custom quote character",
			csvOut: &types.CSVOutput{QuoteCharacter: aws.String("'")},
			// Quoted at the source so it parses as the single field "a,b";
			// output must re-quote it (it contains the field delimiter), and
			// with a custom quote character the wrapping quotes are "'", not '"'.
			srcRow: `"a,b"`,
			want:   "'a,b'\n",
		},
		{
			name: "custom quote escape character",
			csvOut: &types.CSVOutput{
				QuoteCharacter:       aws.String(`"`),
				QuoteEscapeCharacter: aws.String(`\`),
			},
			// The parsed value itself contains a literal '"', which forces
			// quoting; the embedded quote must be escaped with '\' rather
			// than doubled.
			srcRow: `a"b`,
			want:   `"a\"b"` + "\n",
		},
		{
			name:   "QuoteFields ALWAYS quotes every field",
			csvOut: &types.CSVOutput{QuoteFields: types.QuoteFieldsAlways},
			srcRow: "plain",
			want:   `"plain"` + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := selectBugfixesPutObject(t, client, "data.csv", []byte("val\n"+tt.srcRow+"\n"))

			out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
				Bucket:         aws.String(bucket),
				Key:            aws.String("data.csv"),
				Expression:     aws.String("SELECT s.val FROM s3object s"),
				ExpressionType: "SQL",
				InputSerialization: &types.InputSerialization{
					CSV: &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse},
				},
				OutputSerialization: &types.OutputSerialization{CSV: tt.csvOut},
			})
			require.NoError(t, err)

			records := string(selectBugfixesDrain(t, out))
			require.Equal(t, tt.want, records)
		})
	}
}

// TestSelectObjectContent_NegativeLimitRejected is a regression test for
// gopherstack-3nud: a negative LIMIT parsed to a negative int, and the
// executor's "q.limit > 0" cutoff check silently treated any non-positive
// value as no-limit-at-all instead of the invalid value it is. Verified by
// hand-reverting the "n < 0" check in parseLimit(): "LIMIT -1" then returns
// 200 with every row instead of a ParseException.
func TestSelectObjectContent_NegativeLimitRejected(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := selectBugfixesPutObject(t, client, "data.csv", []byte("name,age\nAlice,30\nBob,25\n"))

	_, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String("data.csv"),
		Expression:     aws.String("SELECT * FROM s3object LIMIT -1"),
		ExpressionType: "SQL",
		InputSerialization: &types.InputSerialization{
			CSV: &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse},
		},
		OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
	})
	require.Error(t, err)
	require.Equal(t, "ParseException", selectBugfixesErrCode(t, err))
}

// TestSelectObjectContent_WhereOrderByColumnValidation is a regression test
// for gopherstack-3nud: validateCSVQueryColumns only checked the SELECT
// list against the CSV header row, so a typo'd column in WHERE or ORDER BY
// failed closed (an empty result set) instead of raising MissingSQLColumn
// the way the same typo in the SELECT list already did. Verified by
// hand-reverting validateCSVQueryColumns to its SELECT-list-only form: both
// cases below then return 200 with zero records instead of a
// MissingSQLColumn exception.
func TestSelectObjectContent_WhereOrderByColumnValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
	}{
		{name: "where", expr: "SELECT s.name FROM s3object s WHERE s.naem = 'Alice'"},
		{name: "order by", expr: "SELECT s.name FROM s3object s ORDER BY s.naem"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			bucket := selectBugfixesPutObject(t, client, "data.csv", []byte("name,age\nAlice,30\n"))

			out, err := client.SelectObjectContent(context.Background(), &sdk_s3.SelectObjectContentInput{
				Bucket:         aws.String(bucket),
				Key:            aws.String("data.csv"),
				Expression:     aws.String(tt.expr),
				ExpressionType: "SQL",
				InputSerialization: &types.InputSerialization{
					CSV: &types.CSVInput{FileHeaderInfo: types.FileHeaderInfoUse},
				},
				OutputSerialization: &types.OutputSerialization{CSV: &types.CSVOutput{}},
			})
			require.NoError(t, err)

			streamErr := selectBugfixesDrainExpectErr(t, out)
			require.Error(t, streamErr)
			require.Equal(t, "MissingSQLColumn", selectBugfixesErrCode(t, streamErr))
		})
	}
}
