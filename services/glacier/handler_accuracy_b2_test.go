package glacier_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// -------------------------------------------------------------------------
// Issue 21: GetJobOutput requires a completed job
// -------------------------------------------------------------------------

func TestAccuracyB2_GetJobOutput_RequiresCompletedJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		completed  bool
		wantStatus int
	}{
		{
			name:       "completed_job_returns_output",
			completed:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "incomplete_job_rejected",
			completed:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "job-output-vault")

			// Seed a job with the desired completion state.
			bk := glacier.NewInMemoryBackend()
			h2 := glacier.NewHandler(bk)
			h2.AccountID = testAccountID
			h2.DefaultRegion = testRegion

			createVault(t, h2, "job-output-vault2")

			bk.AddJobInternal(testAccountID, testRegion, "job-output-vault2", &glacier.Job{
				JobID:      "test-job-id-output",
				VaultARN:   "arn:aws:glacier:us-east-1:123456789012:vaults/job-output-vault2",
				VaultName:  "job-output-vault2",
				Action:     "InventoryRetrieval",
				StatusCode: func() string {
					if tt.completed {
						return "Succeeded"
					}
					return "InProgress"
				}(),
				Completed: tt.completed,
			})

			rec := doRequest(t, h2, http.MethodGet,
				"/"+testAccountID+"/vaults/job-output-vault2/jobs/test-job-id-output/output", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -------------------------------------------------------------------------
// Issue 22: CSV inventory uses RFC 4180 quoting (not Go %q)
// -------------------------------------------------------------------------

func TestAccuracyB2_CsvField_RFC4180(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "plain_string_no_quotes",
			input: "hello",
			want:  "hello",
		},
		{
			name:  "empty_no_quotes",
			input: "",
			want:  "",
		},
		{
			name:  "contains_comma_quoted",
			input: "hello, world",
			want:  `"hello, world"`,
		},
		{
			name:  "contains_double_quote_doubled",
			input: `say "hi"`,
			want:  `"say ""hi"""`,
		},
		{
			name:  "contains_newline_quoted",
			input: "line1\nline2",
			want:  "\"line1\nline2\"",
		},
		{
			name:  "go_style_backslash_not_used",
			input: `path\to\file`,
			want:  `path\to\file`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := glacier.CsvField(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAccuracyB2_InventoryCSV_RFC4180QuotedDescriptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "csv-vault")

	// Upload an archive with a description containing a comma.
	e := echo.New()
	body := []byte("archive content")
	req := httptest.NewRequest(http.MethodPost,
		"/"+testAccountID+"/vaults/csv-vault/archives", strings.NewReader(string(body)))
	req.Header.Set("X-Amz-Archive-Description", "my archive, v2")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Initiate an inventory job and get its output as CSV.
	jobBody := `{"Type":"inventory-retrieval","Format":"CSV"}`
	jobRec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/csv-vault/jobs", jobBody)
	require.Equal(t, http.StatusAccepted, jobRec.Code)

	var jobResp map[string]string
	require.NoError(t, json.Unmarshal(jobRec.Body.Bytes(), &jobResp))
	jobID := jobResp["jobId"]
	require.NotEmpty(t, jobID)

	outRec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/csv-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusOK, outRec.Code)
	assert.Equal(t, "text/csv", outRec.Header().Get("Content-Type"))

	body2 := outRec.Body.String()
	// RFC 4180: description with comma must be surrounded by double-quotes.
	assert.Contains(t, body2, `"my archive, v2"`)
	// Must NOT use Go-style backslash escaping.
	assert.NotContains(t, body2, `\`)
}

func TestAccuracyB2_InventoryCSV_DescriptionWithDoubleQuote(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "csv-dq-vault")

	e := echo.New()
	body := []byte("x")
	req := httptest.NewRequest(http.MethodPost,
		"/"+testAccountID+"/vaults/csv-dq-vault/archives", strings.NewReader(string(body)))
	req.Header.Set("X-Amz-Archive-Description", `say "hi"`)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, rec.Code)

	jobBody := `{"Type":"inventory-retrieval","Format":"CSV"}`
	jobRec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/csv-dq-vault/jobs", jobBody)
	require.Equal(t, http.StatusAccepted, jobRec.Code)

	var jobResp map[string]string
	require.NoError(t, json.Unmarshal(jobRec.Body.Bytes(), &jobResp))
	jobID := jobResp["jobId"]

	outRec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/csv-dq-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusOK, outRec.Code)

	body2 := outRec.Body.String()
	// RFC 4180: internal double-quotes are doubled, not backslash-escaped.
	assert.Contains(t, body2, `"say ""hi"""`)
	assert.NotContains(t, body2, `\"`)
}

// -------------------------------------------------------------------------
// Issue 23: Vault name validation
// -------------------------------------------------------------------------

func TestAccuracyB2_VaultName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{
			name:       "alphanumeric_ok",
			vaultName:  "MyVault123",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_dash_ok",
			vaultName:  "my-vault",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_underscore_ok",
			vaultName:  "my_vault",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_dot_ok",
			vaultName:  "my.vault",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "space_rejected",
			vaultName:  "my vault",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "slash_rejected",
			vaultName:  "my/vault",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "exclamation_rejected",
			vaultName:  "my!vault",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "too_long_rejected",
			vaultName:  strings.Repeat("a", 256),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "max_length_ok",
			vaultName:  strings.Repeat("a", 255),
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, http.MethodPut,
				"/"+testAccountID+"/vaults/"+tt.vaultName, "")
			assert.Equal(t, tt.wantStatus, rec.Code, "vault name: %q", tt.vaultName)
		})
	}
}

func TestAccuracyB2_ValidateVaultName_Unit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		wantErr   bool
	}{
		{name: "empty_invalid", vaultName: "", wantErr: true},
		{name: "space_invalid", vaultName: "a b", wantErr: true},
		{name: "colon_invalid", vaultName: "a:b", wantErr: true},
		{name: "hash_invalid", vaultName: "a#b", wantErr: true},
		{name: "255_chars_valid", vaultName: strings.Repeat("x", 255), wantErr: false},
		{name: "256_chars_invalid", vaultName: strings.Repeat("x", 256), wantErr: true},
		{name: "alnum_valid", vaultName: "abc123", wantErr: false},
		{name: "dash_valid", vaultName: "abc-123", wantErr: false},
		{name: "underscore_valid", vaultName: "abc_123", wantErr: false},
		{name: "dot_valid", vaultName: "abc.123", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := glacier.ValidateVaultName(tt.vaultName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// -------------------------------------------------------------------------
// Issue 24: InitiateMultipartUpload requires X-Amz-Part-Size header
// -------------------------------------------------------------------------

func TestAccuracyB2_InitiateMultipartUpload_RequiresPartSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		partSize   string
		wantStatus int
	}{
		{
			name:       "missing_part_size_rejected",
			partSize:   "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_1mb_accepted",
			partSize:   "1048576",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "valid_4mb_accepted",
			partSize:   "4194304",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "not_power_of_two_rejected",
			partSize:   "3000000",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "below_1mb_rejected",
			partSize:   "524288",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "mpu-vault")

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost,
				"/"+testAccountID+"/vaults/mpu-vault/multipart-uploads", http.NoBody)
			if tt.partSize != "" {
				req.Header.Set("X-Amz-Part-Size", tt.partSize)
			}
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -------------------------------------------------------------------------
// Issue 25: UploadMultipartPart requires valid Content-Range header
// -------------------------------------------------------------------------

func TestAccuracyB2_IsValidMultipartRange_Unit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "valid_first_part", input: "bytes 0-1048575/*", want: true},
		{name: "valid_second_part", input: "bytes 1048576-2097151/*", want: true},
		{name: "single_byte", input: "bytes 0-0/*", want: true},
		{name: "missing_header", input: "", want: false},
		{name: "missing_bytes_prefix", input: "0-1048575/*", want: false},
		{name: "missing_wildcard", input: "bytes 0-1048575/1048576", want: false},
		{name: "end_before_start", input: "bytes 100-50/*", want: false},
		{name: "negative_start", input: "bytes -1-100/*", want: false},
		{name: "not_numeric", input: "bytes a-b/*", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := glacier.IsValidMultipartRange(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAccuracyB2_UploadMultipartPart_RequiresContentRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		contentRange string
		wantStatus   int
	}{
		{
			name:         "missing_content_range_rejected",
			contentRange: "",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "invalid_format_rejected",
			contentRange: "0-1048575",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "valid_content_range_accepted",
			contentRange: "bytes 0-1048575/*",
			wantStatus:   http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "mpu-part-vault")

			// Initiate a multipart upload first.
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost,
				"/"+testAccountID+"/vaults/mpu-part-vault/multipart-uploads", http.NoBody)
			req.Header.Set("X-Amz-Part-Size", "1048576")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, rec.Code)

			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]
			require.NotEmpty(t, uploadID)

			// Now upload a part.
			req2 := httptest.NewRequest(http.MethodPut,
				"/"+testAccountID+"/vaults/mpu-part-vault/multipart-uploads/"+uploadID,
				strings.NewReader(strings.Repeat("a", 1<<20)))
			if tt.contentRange != "" {
				req2.Header.Set("Content-Range", tt.contentRange)
			}
			rec2 := httptest.NewRecorder()
			c2 := e.NewContext(req2, rec2)
			err = h.Handler()(c2)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

// -------------------------------------------------------------------------
// Issue 26: CompleteMultipartUpload requires X-Amz-Archive-Size and
//
//	X-Amz-Sha256-Tree-Hash headers
//
// -------------------------------------------------------------------------

func TestAccuracyB2_CompleteMultipartUpload_RequiresHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		archiveSize string
		treeHash    string
		wantStatus  int
	}{
		{
			name:        "both_headers_present_ok",
			archiveSize: "1048576",
			treeHash:    strings.Repeat("a", 64),
			wantStatus:  http.StatusCreated,
		},
		{
			name:        "missing_archive_size_rejected",
			archiveSize: "",
			treeHash:    strings.Repeat("a", 64),
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing_tree_hash_rejected",
			archiveSize: "1048576",
			treeHash:    "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "mpu-complete-vault")

			// Initiate upload.
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost,
				"/"+testAccountID+"/vaults/mpu-complete-vault/multipart-uploads", http.NoBody)
			req.Header.Set("X-Amz-Part-Size", "1048576")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			require.Equal(t, http.StatusCreated, rec.Code)

			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			uploadID := initResp["uploadId"]
			require.NotEmpty(t, uploadID)

			// Complete the upload.
			req2 := httptest.NewRequest(http.MethodPost,
				"/"+testAccountID+"/vaults/mpu-complete-vault/multipart-uploads/"+uploadID,
				http.NoBody)
			if tt.archiveSize != "" {
				req2.Header.Set("X-Amz-Archive-Size", tt.archiveSize)
			}
			if tt.treeHash != "" {
				req2.Header.Set("X-Amz-Sha256-Tree-Hash", tt.treeHash)
			}
			rec2 := httptest.NewRecorder()
			c2 := e.NewContext(req2, rec2)
			require.NoError(t, h.Handler()(c2))
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

// -------------------------------------------------------------------------
// Issue 27: SetVaultNotifications requires non-empty SNSTopic and Events
// -------------------------------------------------------------------------

func TestAccuracyB2_SetVaultNotifications_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "valid_config_ok",
			body:       `{"SNSTopic":"arn:aws:sns:us-east-1:123:topic","Events":["ArchiveRetrievalCompleted"]}`,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "missing_sns_topic_rejected",
			body:       `{"SNSTopic":"","Events":["ArchiveRetrievalCompleted"]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_events_rejected",
			body:       `{"SNSTopic":"arn:aws:sns:us-east-1:123:topic","Events":[]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no_topic_field_rejected",
			body:       `{"Events":["ArchiveRetrievalCompleted"]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "no_events_field_rejected",
			body:       `{"SNSTopic":"arn:aws:sns:us-east-1:123:topic"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "notif-req-vault")

			rec := doRequest(t, h, http.MethodPut,
				"/"+testAccountID+"/vaults/notif-req-vault/notification-configuration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -------------------------------------------------------------------------
// Issue 28: ListJobs completed param must be "true" or "false"
// -------------------------------------------------------------------------

func TestAccuracyB2_ListJobs_CompletedParamValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		completed  string
		wantStatus int
	}{
		{
			name:       "true_accepted",
			completed:  "true",
			wantStatus: http.StatusOK,
		},
		{
			name:       "false_accepted",
			completed:  "false",
			wantStatus: http.StatusOK,
		},
		{
			name:       "omitted_accepted",
			completed:  "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_value_rejected",
			completed:  "yes",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "numeric_rejected",
			completed:  "1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "mixed_case_rejected",
			completed:  "True",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "listjobs-completed-vault")

			path := "/"+testAccountID+"/vaults/listjobs-completed-vault/jobs"
			if tt.completed != "" {
				path += "?completed=" + tt.completed
			}

			rec := doRequest(t, h, http.MethodGet, path, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -------------------------------------------------------------------------
// Issue 29: GetJobOutput returns error for still-running job (filter combo)
// -------------------------------------------------------------------------

func TestAccuracyB2_GetJobOutput_CompletedFilter_Integration(t *testing.T) {
	t.Parallel()

	// Initiate a job (which is immediately Succeeded in our emulator) then verify
	// that GetJobOutput works. Then seed an InProgress job and verify rejection.
	h := newTestHandler()
	createVault(t, h, "job-filter-vault")

	jobRec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/job-filter-vault/jobs",
		`{"Type":"inventory-retrieval"}`)
	require.Equal(t, http.StatusAccepted, jobRec.Code)

	var jr map[string]string
	require.NoError(t, json.Unmarshal(jobRec.Body.Bytes(), &jr))
	jobID := jr["jobId"]
	require.NotEmpty(t, jobID)

	// Completed job: output should succeed.
	outRec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/job-filter-vault/jobs/"+jobID+"/output", "")
	assert.Equal(t, http.StatusOK, outRec.Code)

	// Seed an InProgress job and verify rejection.
	bk2 := glacier.NewInMemoryBackend()
	h2 := glacier.NewHandler(bk2)
	h2.AccountID = testAccountID
	h2.DefaultRegion = testRegion

	createVault(t, h2, "job-filter-vault2")
	bk2.AddJobInternal(testAccountID, testRegion, "job-filter-vault2", &glacier.Job{
		JobID:      "inprog-job",
		VaultARN:   "arn:aws:glacier:us-east-1:123456789012:vaults/job-filter-vault2",
		VaultName:  "job-filter-vault2",
		Action:     "InventoryRetrieval",
		StatusCode: "InProgress",
		Completed:  false,
	})

	outRec2 := doRequest(t, h2, http.MethodGet,
		"/"+testAccountID+"/vaults/job-filter-vault2/jobs/inprog-job/output", "")
	assert.Equal(t, http.StatusBadRequest, outRec2.Code)
}
