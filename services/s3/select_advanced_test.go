package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_SelectObjectContent_ExtraOperators(t *testing.T) {
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
			name:    "not-equal operator !=",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name != 'Bob'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "not-equal operator <>",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name &lt;&gt; 'Bob'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "less-than-or-equal operator <=",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age &lt;= 25</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Bob",
			wantAbsent: "Alice",
		},
		{
			name:    "greater-than-or-equal operator >=",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age &gt;= 30</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "CAST as DECIMAL",
			csvData: []byte("name,score\nAlice,9.5\nBob,8.0\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE CAST(s.score AS DECIMAL) &gt;= 9.0</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "CAST as BOOLEAN",
			csvData: []byte("name,active\nAlice,true\nBob,false\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE CAST(s.active AS BOOLEAN) = 'true'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "quoted identifier in SQL",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s."name" FROM s3object s WHERE s.age = 30</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "LIKE with underscore wildcard",
			csvData: []byte("name,age\nAlice,30\nAlex,28\nAl,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.name LIKE 'Al__e'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Alex",
		},
		{
			name:    "comparison less-than string",
			csvData: []byte("name,city\nAlice,NYC\nBob,LA\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.city &lt; 'NYC'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Bob",
			wantAbsent: "Alice",
		},
		{
			name:    "JSON document with single object",
			csvData: []byte(`{"name":"Alice","age":30}`),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>DOCUMENT</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "IS NULL for missing field",
			csvData: []byte("name,age\nAlice,30\nBob,\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age IS NULL</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Bob",
			wantAbsent: "Alice",
		},
		{
			name:    "string less-than-or-equal operator (compareString <=)",
			csvData: []byte("name,city\nAlice,NYC\nBob,LA\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.city &lt;= 'LA'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Bob",
			wantAbsent: "Alice",
		},
		{
			name:    "string greater-than-or-equal operator (compareString >=)",
			csvData: []byte("name,city\nAlice,NYC\nBob,LA\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.city &gt;= 'NYC'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "string greater-than operator (compareString >)",
			csvData: []byte("name,city\nAlice,NYC\nBob,LA\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.city &gt; 'LA'</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name: "CSV output with JSON input positional",
			csvData: []byte(
				`{"name":"Alice","score":9.5}` + "\n" + `{"name":"Bob","score":5.0}` + "\n",
			),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.score &gt; 6</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "IS NOT NULL for present field",
			csvData: []byte("name,age\nAlice,30\nBob,\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age IS NOT NULL</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
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

			const bucket = "extra-ops-bucket"
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

func TestSQLParser_AdvancedExpressions(t *testing.T) {
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
			name:    "parenthesized expression",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE (s.age &gt; 26)</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "complex nested parens with AND",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,35\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE (s.age &gt; 26 AND s.age &lt; 33)</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Charlie",
		},
		{
			name:    "number literal comparison",
			csvData: []byte("age\n30\n25\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.age FROM s3object s WHERE s.age = 30</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "30",
		},
		{
			name:    "select with AS alias",
			csvData: []byte("first_name,last_name\nAlice,Smith\nBob,Jones\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.first_name AS name FROM s3object s WHERE s.last_name = 'Smith'</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "negative number literal",
			csvData: []byte("name,balance\nAlice,-50\nBob,100\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE s.balance &lt; 0</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "multiple comma-separated SELECT columns",
			csvData: []byte("name,city,age\nAlice,NYC,30\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name, s.city FROM s3object s</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "empty CSV file (header only)",
			csvData: []byte("name,age\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
		},
		{
			name:    "CAST as STRING type",
			csvData: []byte("name,age\nAlice,30\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT CAST(s.age AS STRING) FROM s3object s</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "30",
		},
		{
			name:    "column reference as truthy condition (isTruthy string path)",
			csvData: []byte("name,active\nAlice,true\nBob,\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE s.active</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
			wantAbsent: "Bob",
		},
		{
			name:    "isTruthy false string (empty value)",
			csvData: []byte("name,active\nAlice,false\nBob,true\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE s.active</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Bob",
			wantAbsent: "Alice",
		},
		{
			name:    "SELECT star from CSV with NONE header",
			csvData: []byte("Alice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s._1, s._2 FROM s3object s WHERE s._2 &gt; 26</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>NONE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><JSON/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "invalid SQL - missing FROM keyword",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT name WHERE age &gt; 0</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV/></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:    "LIKE case-insensitive match",
			csvData: []byte("name,age\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE s.name LIKE 'al%'</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
		},
		{
			name:    "invalid paren expression",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE (</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:    "unterminated string literal returns 400",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE s.name = 'Alice</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:    "WHERE NULL literal (isTruthy sqlNullType path)",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE NULL</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			// WHERE NULL is always false, so no rows returned
		},
		{
			name:    "NOT NULL literal (isTruthy NULL negation)",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE NOT NULL</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			// NOT NULL is always false, so no rows
		},
		{
			name:    "FALSE literal in WHERE",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE FALSE</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			// WHERE FALSE never returns rows
		},
		{
			name:    "TRUE literal in WHERE returns all rows",
			csvData: []byte("name\nAlice\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name FROM s3object s WHERE TRUE</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
		{
			name:    "JSON output with number fields",
			csvData: []byte("name,score\nAlice,9.5\n"),
			body: `<SelectObjectContentRequest>
<Expression>SELECT s.name, s.score FROM s3object s</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><JSON/></OutputSerialization>
</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "Alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			const bucket = "advanced-sql-bucket"
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

func TestHandler_SelectObjectContent_Aggregates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantResult string
		csvData    []byte
		wantStatus int
	}{
		{
			name:    "COUNT(*) all rows",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT COUNT(*) FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "3",
		},
		{
			name:    "COUNT(*) with WHERE filter",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT COUNT(*) FROM s3object WHERE age &gt; 22</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "2",
		},
		{
			name:    "SUM of numeric column",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT SUM(age) FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "75",
		},
		{
			name:    "AVG of numeric column",
			csvData: []byte("name,score\nAlice,90\nBob,80\nCharlie,70\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT AVG(score) FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "80",
		},
		{
			name:    "MIN of numeric column",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT MIN(age) FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "20",
		},
		{
			name:    "MAX of numeric column",
			csvData: []byte("name,age\nAlice,30\nBob,25\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT MAX(age) FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "30",
		},
		{
			name:    "multiple aggregates in one SELECT",
			csvData: []byte("name,score\nAlice,90\nBob,60\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT MIN(score), MAX(score) FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "60",
		},
		{
			name:    "COUNT(*) on JSON LINES",
			csvData: []byte("{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT COUNT(*) FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "2",
		},
		{
			name: "SUM with WHERE on JSON LINES",
			csvData: []byte(
				"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n{\"name\":\"Charlie\",\"age\":20}\n",
			),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT SUM(age) FROM s3object WHERE age &gt; 22</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "55",
		},
		{
			name:    "COUNT with column arg (non-null only)",
			csvData: []byte("name,age\nAlice,30\nBob,\nCharlie,20\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT COUNT(age) FROM s3object</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantResult: "2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			const bucket = "agg-bucket"
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
		})
	}
}

func TestHandler_SelectObjectContent_OrderBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantOrder  []string
		wantAbsent string
		csvData    []byte
		wantStatus int
	}{
		{
			name:    "ORDER BY numeric ASC",
			csvData: []byte("name,age\nCharlie,20\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s ORDER BY s.age ASC</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantOrder:  []string{"Charlie", "Bob", "Alice"},
		},
		{
			name:    "ORDER BY numeric DESC",
			csvData: []byte("name,age\nCharlie,20\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s ORDER BY s.age DESC</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantOrder:  []string{"Alice", "Bob", "Charlie"},
		},
		{
			name:    "ORDER BY string ASC",
			csvData: []byte("name,age\nCharlie,20\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s ORDER BY s.name ASC</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantOrder:  []string{"Alice", "Bob", "Charlie"},
		},
		{
			name:    "ORDER BY with LIMIT",
			csvData: []byte("name,age\nCharlie,20\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s ORDER BY s.age DESC LIMIT 2</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantOrder:  []string{"Alice", "Bob"},
			wantAbsent: "Charlie",
		},
		{
			name:    "ORDER BY with WHERE filter",
			csvData: []byte("name,age\nCharlie,20\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s WHERE s.age &gt; 20 ORDER BY s.age ASC</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantOrder:  []string{"Bob", "Alice"},
			wantAbsent: "Charlie",
		},
		{
			name:    "ORDER BY default (no ASC/DESC) is ASC",
			csvData: []byte("name,age\nCharlie,20\nAlice,30\nBob,25\n"),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s ORDER BY s.age</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
				<OutputSerialization><CSV/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantOrder:  []string{"Charlie", "Bob", "Alice"},
		},
		{
			name: "ORDER BY on JSON LINES",
			csvData: []byte(
				"{\"name\":\"Charlie\",\"age\":20}\n{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n",
			),
			body: `<SelectObjectContentRequest>
				<Expression>SELECT s.name FROM s3object s ORDER BY s.age ASC</Expression>
				<ExpressionType>SQL</ExpressionType>
				<InputSerialization><JSON><Type>LINES</Type></JSON></InputSerialization>
				<OutputSerialization><JSON/></OutputSerialization>
			</SelectObjectContentRequest>`,
			wantStatus: http.StatusOK,
			wantOrder:  []string{"Charlie", "Bob", "Alice"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			const bucket = "orderby-bucket"
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

			body := rec.Body.String()

			lastPos := 0
			for _, want := range tt.wantOrder {
				pos := strings.Index(body[lastPos:], want)
				require.GreaterOrEqual(
					t,
					pos,
					0,
					"expected %q after position %d in response",
					want,
					lastPos,
				)
				lastPos += pos + len(want)
			}

			if tt.wantAbsent != "" {
				assert.NotContains(t, body, tt.wantAbsent)
			}
		})
	}
}

func TestHandler_SelectObjectContent_RequestProgress(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)

	mustCreateBucket(t, backend, "progress-bucket")
	mustPutObject(t, backend, "progress-bucket", "data.csv", []byte("name,age\nAlice,30\nBob,25\n"))

	body := `<SelectObjectContentRequest>
		<Expression>SELECT * FROM s3object</Expression>
		<ExpressionType>SQL</ExpressionType>
		<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
		<OutputSerialization><CSV/></OutputSerialization>
		<RequestProgress><Enabled>true</Enabled></RequestProgress>
	</SelectObjectContentRequest>`

	req := httptest.NewRequest(
		http.MethodPost,
		"/progress-bucket/data.csv?select&select-type=2",
		strings.NewReader(body),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Progress")
	assert.Contains(t, rec.Body.String(), "Stats")
	assert.Contains(t, rec.Body.String(), "Alice")
}

func TestHandler_SelectObjectContent_RequestProgress_Disabled(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)

	mustCreateBucket(t, backend, "noprogress-bucket")
	mustPutObject(t, backend, "noprogress-bucket", "data.csv", []byte("name,age\nAlice,30\n"))

	body := `<SelectObjectContentRequest>
		<Expression>SELECT * FROM s3object</Expression>
		<ExpressionType>SQL</ExpressionType>
		<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
		<OutputSerialization><CSV/></OutputSerialization>
		<RequestProgress><Enabled>false</Enabled></RequestProgress>
	</SelectObjectContentRequest>`

	req := httptest.NewRequest(
		http.MethodPost,
		"/noprogress-bucket/data.csv?select&select-type=2",
		strings.NewReader(body),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "Progress")
	assert.Contains(t, rec.Body.String(), "Stats")
}
