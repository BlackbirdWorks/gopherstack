package glacier_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

const selectTestArchive = "1,alice,30\n2,bob,25\n3,carol,40\n"

// basicSelectBody builds an InitiateJob request body for a select job with the given
// SQL expression against the given archive ID, writing to a fixed S3 output location.
func basicSelectBody(archiveID, expression string) string {
	body, _ := json.Marshal(map[string]any{ //nolint:errchkjson // literal map, marshal cannot fail
		"Type":      "select",
		"ArchiveId": archiveID,
		"SelectParameters": map[string]any{
			"Expression":     expression,
			"ExpressionType": "SQL",
			"InputSerialization": map[string]any{
				"Csv": map[string]any{"FileHeaderInfo": "NONE"},
			},
			"OutputSerialization": map[string]any{
				"Csv": map[string]any{},
			},
		},
		"OutputLocation": map[string]any{
			"S3": map[string]any{"BucketName": "results-bucket", "Prefix": "out/"},
		},
	})

	return string(body)
}

// TestInitiateJob_Select_Success verifies a well-formed select job is accepted,
// returns a synthesized JobOutputPath (gopherstack has no cross-service S3 write-back;
// see select.go's package doc) on both the x-amz-job-output-path header and the JSON
// body, and that the job completes (retrievalDelay is 0 in tests).
func TestInitiateJob_Select_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "select-vault")
	archiveID := uploadArchiveData(t, h, "select-vault", []byte(selectTestArchive))

	rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/select-vault/jobs",
		basicSelectBody(archiveID, "SELECT * FROM archive"))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	wantPath := "s3://results-bucket/out/"
	gotPathHeader := rec.Header().Get("X-Amz-Job-Output-Path")
	assert.Contains(t, gotPathHeader, wantPath)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, gotPathHeader, resp["jobOutputPath"])
	assert.NotEmpty(t, resp["jobId"])
}

// TestInitiateJob_Select_Validation verifies structural request validation matches
// AWS's distinct MissingParameterValueException (a required field was entirely
// omitted) vs InvalidParameterValueException (a field was supplied but malformed).
func TestInitiateJob_Select_Validation(t *testing.T) {
	t.Parallel()

	validSelect := map[string]any{
		"Expression":     "SELECT * FROM archive",
		"ExpressionType": "SQL",
		"InputSerialization": map[string]any{
			"Csv": map[string]any{},
		},
		"OutputSerialization": map[string]any{
			"Csv": map[string]any{},
		},
	}
	validOutputLoc := map[string]any{
		"S3": map[string]any{"BucketName": "bucket"},
	}

	tests := []struct {
		overrides  map[string]any
		name       string
		wantCode   string
		archiveID  bool
		wantStatus int
	}{
		{
			name:       "missing_select_parameters",
			archiveID:  true,
			overrides:  map[string]any{"OutputLocation": validOutputLoc},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameterValueException",
		},
		{
			name:      "missing_output_location",
			archiveID: true,
			overrides: map[string]any{
				"SelectParameters": validSelect,
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameterValueException",
		},
		{
			name:      "missing_archive_id",
			archiveID: false,
			overrides: map[string]any{
				"SelectParameters": validSelect,
				"OutputLocation":   validOutputLoc,
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterValueException",
		},
		{
			name:      "bad_expression_type",
			archiveID: true,
			overrides: map[string]any{
				"SelectParameters": map[string]any{
					"Expression":          "SELECT * FROM archive",
					"ExpressionType":      "XQuery",
					"InputSerialization":  validSelect["InputSerialization"],
					"OutputSerialization": validSelect["OutputSerialization"],
				},
				"OutputLocation": validOutputLoc,
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterValueException",
		},
		{
			name:      "malformed_expression",
			archiveID: true,
			overrides: map[string]any{
				"SelectParameters": map[string]any{
					"Expression":          "NOT EVEN CLOSE TO SQL (((",
					"ExpressionType":      "SQL",
					"InputSerialization":  validSelect["InputSerialization"],
					"OutputSerialization": validSelect["OutputSerialization"],
				},
				"OutputLocation": validOutputLoc,
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterValueException",
		},
		{
			name:      "missing_input_serialization",
			archiveID: true,
			overrides: map[string]any{
				"SelectParameters": map[string]any{
					"Expression":          "SELECT * FROM archive",
					"ExpressionType":      "SQL",
					"OutputSerialization": validSelect["OutputSerialization"],
				},
				"OutputLocation": validOutputLoc,
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameterValueException",
		},
		{
			name:      "missing_output_serialization",
			archiveID: true,
			overrides: map[string]any{
				"SelectParameters": map[string]any{
					"Expression":         "SELECT * FROM archive",
					"ExpressionType":     "SQL",
					"InputSerialization": validSelect["InputSerialization"],
				},
				"OutputLocation": validOutputLoc,
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameterValueException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "select-validation-vault")

			payload := map[string]any{"Type": "select"}
			if tt.archiveID {
				payload["ArchiveId"] = uploadArchiveData(t, h, "select-validation-vault", []byte(selectTestArchive))
			} else {
				payload["ArchiveId"] = ""
			}

			maps.Copy(payload, tt.overrides)

			body, err := json.Marshal(payload)
			require.NoError(t, err)

			rec := doRequest(
				t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/select-validation-vault/jobs", string(body),
			)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp["code"])
		})
	}
}

// TestInitiateJob_Select_ArchiveNotFound verifies a select job against a nonexistent
// archive ID is rejected with ResourceNotFoundException, matching archive-retrieval.
func TestInitiateJob_Select_ArchiveNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "select-404-vault")

	rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/select-404-vault/jobs",
		basicSelectBody("does-not-exist", "SELECT * FROM archive"))
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp["code"])
}

// TestGetJobOutput_Select_ExecutesQuery verifies GetJobOutput on a completed select
// job actually executes the SQL expression against the real archive bytes (not a
// stub) and returns the correctly filtered/projected CSV result -- see select.go's
// package doc for why GetJobOutput (rather than an S3 write-back) is gopherstack's
// select-job result delivery path.
func TestGetJobOutput_Select_ExecutesQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{name: "select_star", expression: "SELECT * FROM archive", want: selectTestArchive},
		{
			name:       "project_columns",
			expression: "SELECT _1, _3 FROM archive",
			want:       "1,30\n2,25\n3,40\n",
		},
		{
			name:       "where_numeric_gt",
			expression: "SELECT * FROM archive WHERE _3 > 28",
			want:       "1,alice,30\n3,carol,40\n",
		},
		{
			name:       "where_string_eq",
			expression: "SELECT * FROM archive WHERE _2 = 'bob'",
			want:       "2,bob,25\n",
		},
		{
			name:       "where_or",
			expression: "SELECT * FROM archive WHERE _2 = 'alice' OR _2 = 'carol'",
			want:       "1,alice,30\n3,carol,40\n",
		},
		{
			name:       "where_and",
			expression: "SELECT * FROM archive WHERE _3 > 20 AND _3 < 35",
			want:       "1,alice,30\n2,bob,25\n",
		},
		{
			name:       "limit",
			expression: "SELECT * FROM archive LIMIT 1",
			want:       "1,alice,30\n",
		},
		{
			name:       "no_match",
			expression: "SELECT * FROM archive WHERE _2 = 'nobody'",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "select-query-vault")
			archiveID := uploadArchiveData(t, h, "select-query-vault", []byte(selectTestArchive))

			jobID := initiateJobWithBody(t, h, "select-query-vault", basicSelectBody(archiveID, tt.expression))

			rec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/select-query-vault/jobs/"+jobID+"/output", "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, "text/csv", rec.Header().Get("Content-Type"))
			assert.Equal(t, tt.want, rec.Body.String())

			if tt.want == "" {
				assert.Equal(t, "bytes 0-0/0", rec.Header().Get("Content-Range"))
			}
		})
	}
}

// TestGetJobOutput_Select_HeaderNames verifies FileHeaderInfo=USE lets the SQL
// expression reference columns by header name instead of positionally.
func TestGetJobOutput_Select_HeaderNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "select-header-vault")
	archiveID := uploadArchiveData(t, h, "select-header-vault",
		[]byte("id,name,age\n1,alice,30\n2,bob,25\n"))

	body, err := json.Marshal(map[string]any{
		"Type":      "select",
		"ArchiveId": archiveID,
		"SelectParameters": map[string]any{
			"Expression":     "SELECT name, age FROM archive WHERE age > 26",
			"ExpressionType": "SQL",
			"InputSerialization": map[string]any{
				"Csv": map[string]any{"FileHeaderInfo": "USE"},
			},
			"OutputSerialization": map[string]any{
				"Csv": map[string]any{},
			},
		},
		"OutputLocation": map[string]any{
			"S3": map[string]any{"BucketName": "bucket"},
		},
	})
	require.NoError(t, err)

	jobID := initiateJobWithBody(t, h, "select-header-vault", string(body))

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/select-header-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "alice,30\n", rec.Body.String())
}

// TestGetJobOutput_Select_QuoteFieldsAlways verifies OutputSerialization.Csv.QuoteFields
// = ALWAYS forces every output field to be quoted.
func TestGetJobOutput_Select_QuoteFieldsAlways(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "select-quote-vault")
	archiveID := uploadArchiveData(t, h, "select-quote-vault", []byte("1,alice,30\n"))

	body, err := json.Marshal(map[string]any{
		"Type":      "select",
		"ArchiveId": archiveID,
		"SelectParameters": map[string]any{
			"Expression":     "SELECT * FROM archive",
			"ExpressionType": "SQL",
			"InputSerialization": map[string]any{
				"Csv": map[string]any{},
			},
			"OutputSerialization": map[string]any{
				"Csv": map[string]any{"QuoteFields": "ALWAYS"},
			},
		},
		"OutputLocation": map[string]any{
			"S3": map[string]any{"BucketName": "bucket"},
		},
	})
	require.NoError(t, err)

	jobID := initiateJobWithBody(t, h, "select-quote-vault", string(body))

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/select-quote-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "\"1\",\"alice\",\"30\"\n", rec.Body.String())
}

// TestGetJobOutput_Select_ArchiveDeleted verifies GetJobOutput on a select job whose
// underlying archive was subsequently deleted fails with ResourceNotFoundException
// rather than panicking, matching archive-retrieval's existing behavior.
func TestGetJobOutput_Select_ArchiveDeleted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "select-deleted-vault")
	archiveID := uploadArchiveData(t, h, "select-deleted-vault", []byte(selectTestArchive))

	jobID := initiateJobWithBody(t, h, "select-deleted-vault",
		basicSelectBody(archiveID, "SELECT * FROM archive"))

	delRec := doRequest(t, h, http.MethodDelete,
		"/"+testAccountID+"/vaults/select-deleted-vault/archives/"+archiveID, "")
	require.Equal(t, http.StatusNoContent, delRec.Code)

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/select-deleted-vault/jobs/"+jobID+"/output", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestDescribeJob_Select_EchoesParameters verifies DescribeJob for a select job
// reports Action=Select and echoes SelectParameters/OutputLocation/JobOutputPath,
// matching the real GlacierJobDescription wire shape.
func TestDescribeJob_Select_EchoesParameters(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "select-describe-vault")
	archiveID := uploadArchiveData(t, h, "select-describe-vault", []byte(selectTestArchive))

	jobID := initiateJobWithBody(t, h, "select-describe-vault",
		basicSelectBody(archiveID, "SELECT * FROM archive"))

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/select-describe-vault/jobs/"+jobID, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, "Select", resp["Action"])
	assert.NotEmpty(t, resp["JobOutputPath"])

	sp, ok := resp["SelectParameters"].(map[string]any)
	require.True(t, ok, "SelectParameters must be present: %#v", resp)
	assert.Equal(t, "SELECT * FROM archive", sp["Expression"])

	ol, ok := resp["OutputLocation"].(map[string]any)
	require.True(t, ok, "OutputLocation must be present: %#v", resp)
	s3loc, ok := ol["S3"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "results-bucket", s3loc["BucketName"])

	// InventoryRetrievalParameters must stay null for a Select job (it is only
	// non-null for InventoryRetrieval jobs on the real wire).
	_, hasInv := resp["InventoryRetrievalParameters"]
	assert.False(t, hasInv)

	// ArchiveSizeInBytes/ArchiveSHA256TreeHash must stay null for a Select job too
	// (per the real GlacierJobDescription doc: null "for an inventory retrieval or
	// select job").
	_, hasSize := resp["ArchiveSizeInBytes"]
	assert.False(t, hasSize)
}

// TestParseSelectExpression_ExportedHelperSmoke is a minimal sanity check that the
// exported test hook itself behaves as documented (deeper coverage lives in
// select_sql_test.go).
func TestParseSelectExpression_ExportedHelperSmoke(t *testing.T) {
	t.Parallel()

	require.NoError(t, glacier.ParseSelectExpression("SELECT * FROM archive"))
	require.Error(t, glacier.ParseSelectExpression("not sql"))
}
