package s3_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_SelectObjectContent_CSV(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantResult string
		wantAbsent string
		csvData    []byte
		wantStatus int
	}{
		{
			name:    "select all with no filter",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "select with WHERE clause filters rows",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age > 26</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "select with LIMIT",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,35\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object LIMIT 1</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "select with LIKE operator",
			csvData: []byte("name,age\nAlice,30\nAlex,28\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name LIKE 'Al%'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "JSON output from CSV input",
			csvData: []byte("name,age\nAlice,30\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "empty result (no matching rows)",
			csvData: []byte("name,age\nAlice,30\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object WHERE age > 100</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
		},
		{
			name:    "positional columns (NONE header info)",
			csvData: []byte("Alice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s._1 FROM s3object s WHERE s._2 > 26</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>NONE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "IGNORE header info",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>IGNORE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "AND operator",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age &gt; 20 AND s.age &lt; 30</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Bob",
			wantAbsent: "Alice",
		},
		{
			name:    "OR operator",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age = 30 OR s.age = 20</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "NOT operator",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE NOT s.name = 'Bob'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "IS NOT NULL",
			csvData: []byte("name,age\nAlice,30\n,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name IS NOT NULL</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "CAST expression",
			csvData: []byte("name,age\nAlice,30\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE CAST(s.age AS INTEGER) >= 30</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "BETWEEN operator",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age BETWEEN 25 AND 30</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Charlie",
		},
		{
			name:    "IN operator",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name IN ('Alice', 'Charlie')</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "NOT LIKE",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name NOT LIKE 'Bo%'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "invalid ExpressionType returns 400",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object</Expression>
				<ExpressionType>XPATH</ExpressionType>
				<InputSerialization><CSV/></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:    "empty expression returns 400",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
				<Expression></Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV/></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:    "invalid SQL returns 400",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
				<Expression>INVALID SQL HERE</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV/></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "malformed XML returns 400",
			csvData:    []byte("name\nAlice\n"),
			body:       `not-valid-xml`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:    "BETWEEN missing AND keyword returns 400",
			csvData: []byte("name,age\nAlice,30\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age BETWEEN 25 30</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:    "IN list with multiple items",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name IN ('Alice', 'Charlie', 'Dave')</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "NOT LIKE followed by pattern",
			csvData: []byte("name,age\nAlice,30\nBob,25\nAlexander,35\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name NOT LIKE 'Al%'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Bob",
			wantAbsent: "Alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			const bucket = "test-bucket"
			const key = "data.csv"

			mustCreateBucket(t, backend, bucket)
			mustPutObject(t, backend, bucket, key, tt.csvData)

			req := httptest.NewRequest(
				http.MethodPost,
				"/"+bucket+"/"+key+"?select&select-type=2",
				strings.NewReader(tt.body),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResult != "" {
				assert.Contains(t, rec.Body.String(), tt.wantResult)
			}

			if tt.wantAbsent != "" {
				assert.NotContains(t, rec.Body.String(), tt.wantAbsent)
			}
		})
	}
}

func TestHandler_SelectObjectContent_JSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       string
		name       string
		wantResult string
		wantAbsent string
		jsonData   []byte
		wantStatus int
	}{
		{
			name:     "JSON lines - select all",
			jsonData: []byte(`{"name":"Alice","age":30}` + "\n" + `{"name":"Bob","age":25}` + "\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:     "JSON lines - select with WHERE",
			jsonData: []byte(`{"name":"Alice","age":30}` + "\n" + `{"name":"Bob","age":25}` + "\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age > 26</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:     "JSON DOCUMENT - array input",
			jsonData: []byte(`[{"name":"Alice","age":30},{"name":"Bob","age":25}]`),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>DOCUMENT</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:     "JSON input - CSV output",
			jsonData: []byte(`{"name":"Alice","age":30}` + "\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:     "JSON lines - empty result",
			jsonData: []byte(`{"name":"Alice","age":30}` + "\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT * FROM s3object WHERE age > 100</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
		},
		{
			name: "JSON lines - isTruthy float64 path",
			jsonData: []byte(
				`{"name":"Alice","score":9.5}` + "\n" + `{"name":"Bob","score":0}` + "\n",
			),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.score</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name: "JSON lines - isTruthy bool path",
			jsonData: []byte(
				`{"name":"Alice","active":true}` + "\n" + `{"name":"Bob","active":false}` + "\n",
			),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.active</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			const bucket = "json-bucket"
			const key = "data.json"

			mustCreateBucket(t, backend, bucket)
			mustPutObject(t, backend, bucket, key, tt.jsonData)

			req := httptest.NewRequest(
				http.MethodPost,
				"/"+bucket+"/"+key+"?select&select-type=2",
				strings.NewReader(tt.body),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResult != "" {
				assert.Contains(t, rec.Body.String(), tt.wantResult)
			}

			if tt.wantAbsent != "" {
				assert.NotContains(t, rec.Body.String(), tt.wantAbsent)
			}
		})
	}
}

func TestHandler_SelectObjectContent_MissingObject(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)

	mustCreateBucket(t, backend, "test-bucket")

	body := `<SelectObjectContentRequest>
		<Expression>SELECT name FROM s3object</Expression>
		<ExpressionType>SQL</ExpressionType>
		<InputSerialization><CSV/></InputSerialization>
		<OutputSerialization><CSV/></OutputSerialization>
	</SelectObjectContentRequest>`

	req := httptest.NewRequest(
		http.MethodPost,
		"/test-bucket/missing.csv?select&select-type=2",
		strings.NewReader(body),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_SelectObjectContent_SSEC is a table test verifying that
// SelectObjectContent against an SSE-C encrypted object requires the caller
// to supply the same X-Amz-Server-Side-Encryption-Customer-* headers
// GetObject requires (SelectObjectContentInput.SSECustomerAlgorithm/-Key/
// -KeyMD5 are HTTP-header bound per the real SDK's serializer, not part of
// the request XML body): missing headers must fail with 400, and the correct
// key must let the query run against the decrypted plaintext. The two cases
// share one SSE-C source object (read-only across both, safe under
// t.Parallel) and differ only in request headers in / status+body out —
// this doesn't fit the existing CSV/JSON tables (TestHandler_SelectObjectContent_CSV/
// _JSON), whose rows vary the SQL query and input data against a
// non-encrypted fixture, not the request's SSE-C headers or object
// encryption state, so it's kept as its own small table rather than forced
// into an unrelated one.
func TestHandler_SelectObjectContent_SSEC(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ssec-select-bucket")

	keyB64, keyMD5 := mustPutSSECObject(t, handler, "ssec-select-bucket", "data.csv", "name,age\nAlice,30\nBob,25\n")

	body := `<SelectObjectContentRequest>
		<Expression>SELECT name FROM s3object</Expression>
		<ExpressionType>SQL</ExpressionType>
		<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
		<OutputSerialization><CSV/></OutputSerialization>
	</SelectObjectContentRequest>`

	tests := []struct {
		name             string
		headers          map[string]string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:       "missing SSE-C headers returns 400",
			headers:    nil,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:             "correct SSE-C headers decrypts and runs query",
			headers:          ssecHeaders(keyB64, keyMD5),
			wantStatus:       http.StatusOK,
			wantBodyContains: "Alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(
				http.MethodPost, "/ssec-select-bucket/data.csv?select&select-type=2", strings.NewReader(body))
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, tt.wantStatus, rec.Code, "body=%s", rec.Body.String())

			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestHandler_SelectObjectContent_EventStreamFormat(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)

	mustCreateBucket(t, backend, "event-bucket")
	mustPutObject(t, backend, "event-bucket", "data.csv", []byte("name,age\nAlice,30\n"))

	body := `<SelectObjectContentRequest>
		<Expression>SELECT name, age FROM s3object</Expression>
		<ExpressionType>SQL</ExpressionType>
		<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
		<OutputSerialization><CSV/></OutputSerialization>
	</SelectObjectContentRequest>`

	req := httptest.NewRequest(
		http.MethodPost,
		"/event-bucket/data.csv?select&select-type=2",
		strings.NewReader(body),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	assert.Positive(t, rec.Body.Len(), "response body should be non-empty event stream")
}

func TestHandler_SelectObjectContent_DefaultCSVInput(t *testing.T) {
	t.Parallel()

	// Test that when no InputSerialization type is specified, CSV is used by default.
	handler, backend := newTestHandler(t)

	mustCreateBucket(t, backend, "default-bucket")
	mustPutObject(t, backend, "default-bucket", "data.csv", []byte("Alice,30\n"))

	body := `<SelectObjectContentRequest>
		<Expression>SELECT _1 FROM s3object</Expression>
		<ExpressionType>SQL</ExpressionType>
		<InputSerialization></InputSerialization>
		<OutputSerialization><CSV/></OutputSerialization>
	</SelectObjectContentRequest>`

	req := httptest.NewRequest(
		http.MethodPost,
		"/default-bucket/data.csv?select&select-type=2",
		strings.NewReader(body),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Alice")
}

func TestSQLParser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		rows    []map[string]string
		want    []string // names expected in result
		wantErr bool
	}{
		{
			name: "SELECT star",
			sql:  "SELECT * FROM s3object",
			rows: []map[string]string{{"name": "Alice", "age": "30"}},
			want: []string{"Alice"},
		},
		{
			name: "WHERE equals",
			sql:  "SELECT * FROM s3object WHERE name = 'Alice'",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
			},
			want: []string{"Alice"},
		},
		{
			name: "WHERE greater-than numeric",
			sql:  "SELECT * FROM s3object WHERE age > 26",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
			},
			want: []string{"Alice"},
		},
		{
			name: "WHERE LIKE",
			sql:  "SELECT * FROM s3object WHERE name LIKE 'Al%'",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
			},
			want: []string{"Alice"},
		},
		{
			name: "LIMIT",
			sql:  "SELECT * FROM s3object LIMIT 1",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
				{"name": "Charlie", "age": "20"},
			},
			want: []string{"Alice"},
		},
		{
			name: "AND",
			sql:  "SELECT * FROM s3object WHERE age > 20 AND age < 30",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
				{"name": "Charlie", "age": "20"},
			},
			want: []string{"Bob"},
		},
		{
			name: "OR",
			sql:  "SELECT * FROM s3object WHERE age = 30 OR age = 20",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
				{"name": "Charlie", "age": "20"},
			},
			want: []string{"Alice", "Charlie"},
		},
		{
			name: "NOT",
			sql:  "SELECT * FROM s3object WHERE NOT name = 'Bob'",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
			},
			want: []string{"Alice"},
		},
		{
			name: "IS NULL",
			sql:  "SELECT * FROM s3object WHERE name IS NULL",
			rows: []map[string]string{
				{"name": "", "age": "30"},
				{"name": "Bob", "age": "25"},
			},
			want: []string{"30"},
		},
		{
			name: "IS NOT NULL",
			sql:  "SELECT * FROM s3object WHERE name IS NOT NULL",
			rows: []map[string]string{
				{"name": "", "age": "30"},
				{"name": "Bob", "age": "25"},
			},
			want: []string{"Bob"},
		},
		{
			name: "BETWEEN",
			sql:  "SELECT * FROM s3object WHERE age BETWEEN 25 AND 30",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
				{"name": "Charlie", "age": "20"},
			},
			want: []string{"Alice", "Bob"},
		},
		{
			name: "IN list",
			sql:  "SELECT * FROM s3object WHERE name IN ('Alice', 'Charlie')",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
				{"name": "Charlie", "age": "20"},
			},
			want: []string{"Alice", "Charlie"},
		},
		{
			name: "NOT LIKE",
			sql:  "SELECT * FROM s3object WHERE name NOT LIKE 'Bo%'",
			rows: []map[string]string{
				{"name": "Alice", "age": "30"},
				{"name": "Bob", "age": "25"},
			},
			want: []string{"Alice"},
		},
		{
			name: "column alias (AS)",
			sql:  "SELECT name AS person_name FROM s3object",
			rows: []map[string]string{{"name": "Alice"}},
			want: []string{"Alice"},
		},
		{
			name:    "invalid SQL",
			sql:     "INVALID",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			const bucket = "parser-test"
			const key = "data.csv"

			mustCreateBucket(t, backend, bucket)

			// Build CSV from rows
			var csvLines []string

			if len(tt.rows) > 0 {
				// Header line
				headers := make([]string, 0)
				for k := range tt.rows[0] {
					headers = append(headers, k)
				}
				csvLines = append(csvLines, strings.Join(headers, ","))

				for _, row := range tt.rows {
					vals := make([]string, len(headers))
					for i, h := range headers {
						vals[i] = row[h]
					}

					csvLines = append(csvLines, strings.Join(vals, ","))
				}
			}

			mustPutObject(t, backend, bucket, key, []byte(strings.Join(csvLines, "\n")+"\n"))

			// XML-escape the SQL expression to handle special characters (<, >, &, etc.)
			var escapedSQL bytes.Buffer
			_ = xml.EscapeText(&escapedSQL, []byte(tt.sql))

			body := fmt.Sprintf(`<SelectObjectContentRequest>
				<Expression>%s</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`, escapedSQL.String())

			req := httptest.NewRequest(
				http.MethodPost,
				"/"+bucket+"/"+key+"?select&select-type=2",
				strings.NewReader(body),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			for _, want := range tt.want {
				assert.Contains(t, rec.Body.String(), want, "expected %q in response", want)
			}
		})
	}
}
