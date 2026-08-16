package glacier_test

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func TestGetJobOutput_ArchiveDescriptionHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
		wantHeader  bool
	}{
		{name: "description_present", description: "quarterly backup", wantHeader: true},
		{name: "description_empty", description: "", wantHeader: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			glacier.SetRetrievalDelay(bk, 0)
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "desc-vault"})
			bk.AddArchiveInternal(testAccountID, testRegion, "desc-vault", &glacier.Archive{
				ArchiveID:   "desc-archive",
				Size:        16,
				Description: tt.description,
			})

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/desc-vault/jobs",
				`{"Type":"ArchiveRetrieval","ArchiveId":"desc-archive"}`)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var initResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"].(string)

			rec = doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/desc-vault/jobs/"+jobID+"/output", "")
			require.Equal(t, http.StatusOK, rec.Code)

			got := rec.Header().Get("X-Amz-Archive-Description")
			if tt.wantHeader {
				assert.Equal(t, tt.description, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

func TestGetJobOutput_RequiresCompletedJob(t *testing.T) {
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
				JobID:     "test-job-id-output",
				VaultARN:  "arn:aws:glacier:us-east-1:123456789012:vaults/job-output-vault2",
				VaultName: "job-output-vault2",
				Action:    "InventoryRetrieval",
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

func TestCsvField_RFC4180(t *testing.T) {
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

func TestInventoryCSV_RFC4180QuotedDescriptions(t *testing.T) {
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

func TestInventoryCSV_DescriptionWithDoubleQuote(t *testing.T) {
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

func TestGetJobOutput_CompletedFilter_Integration(t *testing.T) {
	t.Parallel()

	// Initiate a job and verify that, with the simulated retrieval window disabled,
	// GetJobOutput works immediately. A zero retrieval delay keeps the assertion
	// deterministic. Then seed an InProgress job and verify rejection.
	bk := glacier.NewInMemoryBackend()
	glacier.SetRetrievalDelay(bk, 0)
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion
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

func TestGetJobOutput_SHA256Header(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantHeader string
	}{
		{name: "sha256_header_set", wantHeader: "deadbeef"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			glacier.SetRetrievalDelay(bk, 0)
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "sha-vault"})
			bk.AddArchiveInternal(testAccountID, testRegion, "sha-vault", &glacier.Archive{
				ArchiveID:      "arch-sha",
				Size:           512,
				SHA256TreeHash: tt.wantHeader,
			})

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/sha-vault/jobs",
				`{"Type":"ArchiveRetrieval","ArchiveId":"arch-sha"}`)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var initResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"].(string)

			rec = doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/sha-vault/jobs/"+jobID+"/output", "")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantHeader, rec.Header().Get("X-Amz-Sha256-Tree-Hash"))
		})
	}
}

func uploadArchiveData(t *testing.T, h *glacier.Handler, vaultName string, data []byte) string {
	t.Helper()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/"+testAccountID+"/vaults/"+vaultName+"/archives",
		strings.NewReader(string(data)))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id := resp["archiveId"]
	require.NotEmpty(t, id)

	return id
}

// initiateJobWithBody initiates a job and returns the jobId.

func initiateJobWithBody(t *testing.T, h *glacier.Handler, vaultName, body string) string {
	t.Helper()
	rec := doRequestWithHeaders(t, h, http.MethodPost, "/"+testAccountID+"/vaults/"+vaultName+"/jobs", body, nil)
	require.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id := resp["jobId"]
	require.NotEmpty(t, id)

	return id
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Full archive-retrieval lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestArchiveRetrieval_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{name: "small_archive", content: "hello glacier retrieval"},
		{name: "binary_like_content", content: "\x00\x01\x02\x03binary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "arch-lifecycle")
			archiveID := uploadArchiveData(t, h, "arch-lifecycle", []byte(tt.content))

			jobID := initiateJobWithBody(t, h, "arch-lifecycle",
				`{"Type":"archive-retrieval","ArchiveId":"`+archiveID+`"}`)

			// DescribeJob should show Succeeded (zero delay).
			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/arch-lifecycle/jobs/"+jobID, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var desc map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
			assert.Equal(t, "Succeeded", desc["StatusCode"])
			assert.Equal(t, true, desc["Completed"])

			// GetJobOutput should return the archive bytes.
			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/arch-lifecycle/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.content, rec.Body.String())
		})
	}
}

func TestArchiveRetrieval_RetrievalByteRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		content    string
		byteRange  string
		wantOutput string
	}{
		{
			name:       "first_5_bytes",
			content:    "hello glacier",
			byteRange:  "0-4",
			wantOutput: "hello",
		},
		{
			name:       "middle_bytes",
			content:    "abcdefghijklmnop",
			byteRange:  "3-7",
			wantOutput: "defgh",
		},
		{
			name:       "last_byte",
			content:    "xyz",
			byteRange:  "2-2",
			wantOutput: "z",
		},
		{
			name:       "full_range",
			content:    "fullcontent",
			byteRange:  "0-10",
			wantOutput: "fullcontent",
		},
		{
			name:       "range_beyond_end_clamped",
			content:    "short",
			byteRange:  "0-9999",
			wantOutput: "short",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "range-vault-"+tt.name)
			archiveID := uploadArchiveData(t, h, "range-vault-"+tt.name, []byte(tt.content))

			jobID := initiateJobWithBody(t, h, "range-vault-"+tt.name,
				`{"Type":"archive-retrieval","ArchiveId":"`+archiveID+`","RetrievalByteRange":"`+tt.byteRange+`"}`)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/range-vault-"+tt.name+"/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantOutput, rec.Body.String())
		})
	}
}

func TestArchiveRetrieval_SHA256TreeHashHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{name: "header_set_from_archive", content: "checksum test content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "checksum-vault")
			data := []byte(tt.content)
			archiveID := uploadArchiveData(t, h, "checksum-vault", data)
			expectedHash := glacier.ComputeTreeHash(data)

			jobID := initiateJobWithBody(t, h, "checksum-vault",
				`{"Type":"archive-retrieval","ArchiveId":"`+archiveID+`"}`)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/checksum-vault/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, expectedHash, rec.Header().Get("X-Amz-Sha256-Tree-Hash"))
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Inventory retrieval lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestInventoryRetrieval_JSONLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		archiveCount int
	}{
		{name: "empty_vault", archiveCount: 0},
		{name: "three_archives", archiveCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "inv-json-"+tt.name)
			for i := range tt.archiveCount {
				uploadArchiveData(t, h, "inv-json-"+tt.name, fmt.Appendf(nil, "archive-%d", i))
			}

			jobID := initiateJobWithBody(t, h, "inv-json-"+tt.name, `{"Type":"inventory-retrieval"}`)

			// GetJobOutput returns JSON inventory.
			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/inv-json-"+tt.name+"/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

			var inv map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &inv))
			archList, ok := inv["ArchiveList"].([]any)
			require.True(t, ok)
			assert.Len(t, archList, tt.archiveCount)
			assert.NotEmpty(t, inv["VaultARN"])
		})
	}
}

func TestInventoryRetrieval_CSVLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		archiveCount int
	}{
		{name: "empty_vault_csv", archiveCount: 0},
		{name: "two_archives_csv", archiveCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "inv-csv-"+tt.name)
			for i := range tt.archiveCount {
				uploadArchiveData(t, h, "inv-csv-"+tt.name, fmt.Appendf(nil, "data-%d", i))
			}

			jobID := initiateJobWithBody(t, h, "inv-csv-"+tt.name,
				`{"Type":"inventory-retrieval","Format":"CSV"}`)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/inv-csv-"+tt.name+"/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "text/csv", rec.Header().Get("Content-Type"))

			r := csv.NewReader(strings.NewReader(rec.Body.String()))
			rows, err := r.ReadAll()
			require.NoError(t, err)
			// Header row + one row per archive.
			assert.Len(t, rows, 1+tt.archiveCount)
			assert.Equal(t, "ArchiveId", rows[0][0])
		})
	}
}

func TestInventoryRetrieval_InventorySizeInBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		archiveCount int
	}{
		{name: "populated_after_get_output", archiveCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "invsize-vault")
			for i := range tt.archiveCount {
				uploadArchiveData(t, h, "invsize-vault", fmt.Appendf(nil, "data-%d", i))
			}

			jobID := initiateJobWithBody(t, h, "invsize-vault", `{"Type":"inventory-retrieval"}`)

			// Before GetJobOutput: InventorySizeInBytes may be 0.
			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/invsize-vault/jobs/"+jobID+"/output", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			payloadSize := rec.Body.Len()
			require.Positive(t, payloadSize)

			// After GetJobOutput: DescribeJob should have InventorySizeInBytes set.
			rec2 := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/invsize-vault/jobs/"+jobID, "", nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var desc map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &desc))
			raw, present := desc["InventorySizeInBytes"]
			assert.True(t, present, "InventorySizeInBytes must be present after GetJobOutput")
			got := int64(raw.(float64))
			assert.Equal(t, int64(payloadSize), got, "InventorySizeInBytes must match actual payload size")
		})
	}
}

func TestInventoryRetrieval_LastInventoryDateUpdated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "last_inventory_date_set_on_job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "lastinv-vault")

			// Before any job: no LastInventoryDate.
			rec := doRequestWithHeaders(t, h, http.MethodGet, "/"+testAccountID+"/vaults/lastinv-vault", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var vaultDesc map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vaultDesc))
			assert.Empty(t, vaultDesc["LastInventoryDate"])

			initiateJobWithBody(t, h, "lastinv-vault", `{"Type":"inventory-retrieval"}`)

			// After job: LastInventoryDate set.
			rec = doRequestWithHeaders(t, h, http.MethodGet, "/"+testAccountID+"/vaults/lastinv-vault", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vaultDesc))
			assert.NotEmpty(t, vaultDesc["LastInventoryDate"], tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Vault isolation
// ─────────────────────────────────────────────────────────────────────────────

func TestGetJobOutput_IncompleteJobReturns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "in_progress_job_output_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			// Long delay keeps job InProgress.
			glacier.SetRetrievalDelay(bk, 30_000_000_000) // 30 seconds
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			createVault(t, h, "incomplete-job-vault")
			jobID := initiateJobWithBody(t, h, "incomplete-job-vault", `{"Type":"inventory-retrieval"}`)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/incomplete-job-vault/jobs/"+jobID+"/output", "", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 22. DeleteVault errors
// ─────────────────────────────────────────────────────────────────────────────

func TestGetJobOutput_RangeOnInventory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rangeHeader string
		wantStatus  int
	}{
		{
			name:        "first_10_bytes",
			rangeHeader: "bytes=0-9",
			wantStatus:  http.StatusPartialContent,
		},
		{
			name:        "invalid_range",
			rangeHeader: "bytes=9999-10000",
			wantStatus:  http.StatusRequestedRangeNotSatisfiable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "inv-range-vault-"+tt.name)
			uploadArchiveData(t, h, "inv-range-vault-"+tt.name, []byte("some archive data"))

			jobID := initiateJobWithBody(t, h, "inv-range-vault-"+tt.name, `{"Type":"inventory-retrieval"}`)

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet,
				"/"+testAccountID+"/vaults/inv-range-vault-"+tt.name+"/jobs/"+jobID+"/output",
				http.NoBody)
			req.Header.Set("Range", tt.rangeHeader)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 16. Vault notifications
// ─────────────────────────────────────────────────────────────────────────────
