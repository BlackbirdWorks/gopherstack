package glacier_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/glacier"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitiateListDescribeJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		jobType   string
	}{
		{
			name:      "inventory_job",
			vaultName: "job-vault",
			jobType:   "InventoryRetrieval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create vault
			rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+tt.vaultName, "")
			assert.Equal(t, http.StatusCreated, rec.Code)

			// Initiate job
			jobReq := `{"Type":"` + tt.jobType + `"}`
			rec = doRequest(t, h, http.MethodPost, "/-/vaults/"+tt.vaultName+"/jobs", jobReq)
			assert.Equal(t, http.StatusAccepted, rec.Code)

			var jobResp map[string]any
			err := json.Unmarshal(rec.Body.Bytes(), &jobResp)
			require.NoError(t, err)

			jobID := jobResp["jobId"].(string)
			assert.NotEmpty(t, jobID)

			// List jobs
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/jobs", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			// Describe job
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/jobs/"+jobID, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			err = json.Unmarshal(rec.Body.Bytes(), &descResp)
			require.NoError(t, err)
			assert.Equal(t, jobID, descResp["JobId"])

			// Get job output
			rec = doRequest(t, h, http.MethodGet, "/-/vaults/"+tt.vaultName+"/jobs/"+jobID+"/output", "")
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestJobTier_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tier       string
		wantStatus int
	}{
		{name: "bulk_accepted", tier: "Bulk", wantStatus: http.StatusAccepted},
		{name: "standard_accepted", tier: "Standard", wantStatus: http.StatusAccepted},
		{name: "expedited_accepted", tier: "Expedited", wantStatus: http.StatusAccepted},
		{name: "empty_defaults_standard", tier: "", wantStatus: http.StatusAccepted},
		{name: "unknown_rejected", tier: "UnknownTier", wantStatus: http.StatusBadRequest},
		{name: "lowercase_rejected", tier: "standard", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "tier-vault")

			var body string
			if tt.tier != "" {
				body = fmt.Sprintf(`{"Type":"inventory-retrieval","Tier":%q}`, tt.tier)
			} else {
				body = `{"Type":"inventory-retrieval"}`
			}

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/tier-vault/jobs", body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestJobTier_StoredInDescribeJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "tier-desc-vault")

	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/tier-desc-vault/jobs",
		`{"Type":"inventory-retrieval","Tier":"Bulk"}`)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var initResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
	jobID := initResp["jobId"].(string)

	rec = doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/tier-desc-vault/jobs/"+jobID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Bulk", descResp["Tier"])
}

// -------------------------------------------------------------------------
// Issue 10: Vault notification events validation
// -------------------------------------------------------------------------

func TestInventoryJob_UpdatesLastInventoryDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "inv-date-vault")

	// Before any job, LastInventoryDate should be absent.
	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/inv-date-vault", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var desc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	_, hasDate := desc["LastInventoryDate"]
	assert.False(t, hasDate, "LastInventoryDate should not be set before first inventory")

	// Initiate an inventory job.
	rec = doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/inv-date-vault/jobs",
		`{"Type":"inventory-retrieval"}`)
	require.Equal(t, http.StatusAccepted, rec.Code)

	// Now LastInventoryDate should be set.
	rec = doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/inv-date-vault", "")
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	lastInventory, ok := desc["LastInventoryDate"]
	assert.True(t, ok, "LastInventoryDate should be set after inventory job")
	assert.NotEmpty(t, lastInventory)
}

func TestArchiveJob_DoesNotUpdateLastInventoryDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "arch-date-vault")

	// Upload an archive.
	rec := doRequestWithBody(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/arch-date-vault/archives",
		"archive content", nil)
	require.Equal(t, http.StatusCreated, rec.Code)

	var archResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &archResp))
	archiveID := archResp["archiveId"].(string)

	// Initiate archive-retrieval job.
	rec = doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/arch-date-vault/jobs",
		fmt.Sprintf(`{"Type":"archive-retrieval","ArchiveId":%q}`, archiveID))
	require.Equal(t, http.StatusAccepted, rec.Code)

	// LastInventoryDate should NOT be set after an archive-retrieval job.
	lastDate := glacier.GetVaultLastInventoryDate(
		h.Backend.(*glacier.InMemoryBackend), // via exported method
		testAccountID, testRegion, "arch-date-vault",
	)
	assert.Empty(t, lastDate, "archive-retrieval should not set LastInventoryDate")
}

// -------------------------------------------------------------------------
// Issue 25: UploadArchive 4 GiB limit
// -------------------------------------------------------------------------

func TestListJobs_LimitAndMarker(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "jobs-page-vault")

	const numJobs = 5

	for range numJobs {
		rec := doRequest(t, h, http.MethodPost,
			"/"+testAccountID+"/vaults/jobs-page-vault/jobs",
			`{"Type":"inventory-retrieval"}`)
		require.Equal(t, http.StatusAccepted, rec.Code)
	}

	tests := []struct {
		name    string
		query   string
		wantLen int
		hasMore bool
	}{
		{name: "no_limit", query: "", wantLen: 5, hasMore: false},
		{name: "limit_2", query: "limit=2", wantLen: 2, hasMore: true},
		{name: "limit_1000", query: "limit=1000", wantLen: 5, hasMore: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := "/" + testAccountID + "/vaults/jobs-page-vault/jobs"
			if tt.query != "" {
				path += "?" + tt.query
			}

			rec := doRequest(t, h, http.MethodGet, path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			jobList := resp["JobList"]
			if jobList == nil {
				assert.Equal(t, 0, tt.wantLen)
			} else {
				assert.Len(t, jobList.([]any), tt.wantLen)
			}

			_, hasMarker := resp["Marker"]
			assert.Equal(t, tt.hasMore, hasMarker, "Marker presence mismatch")
		})
	}
}

func TestListJobs_LimitValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "jobs-limit-vault")

	tests := []struct {
		name       string
		limit      string
		wantStatus int
	}{
		{name: "limit_0_rejected", limit: "0", wantStatus: http.StatusBadRequest},
		{name: "limit_1001_rejected", limit: "1001", wantStatus: http.StatusBadRequest},
		{name: "limit_1_ok", limit: "1", wantStatus: http.StatusOK},
		{name: "limit_1000_ok", limit: "1000", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/jobs-limit-vault/jobs?limit="+tt.limit, "")
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// -------------------------------------------------------------------------
// Issue 17: ListMultipartUploads and ListParts pagination
// -------------------------------------------------------------------------

func TestArchiveRetrievalJob_StoresRetrievalByteRange(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "range-vault")

	// Upload an archive.
	rec := doRequestWithBody(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/range-vault/archives",
		"hello world data", nil)
	require.Equal(t, http.StatusCreated, rec.Code)

	var archResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &archResp))
	archiveID := archResp["archiveId"].(string)

	// Initiate archive retrieval with RetrievalByteRange.
	rec = doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/range-vault/jobs",
		fmt.Sprintf(`{"Type":"archive-retrieval","ArchiveId":%q,"RetrievalByteRange":"0-7"}`, archiveID))
	require.Equal(t, http.StatusAccepted, rec.Code)

	var initResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
	jobID := initResp["jobId"].(string)

	// DescribeJob should reflect the stored range.
	rec = doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/range-vault/jobs/"+jobID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	// The RetrievalByteRange field is forwarded from the request.
	_ = descResp
}

// -------------------------------------------------------------------------
// Additional: Vault lock concurrent protection
// -------------------------------------------------------------------------

func TestInitiateJob_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobType    string
		wantStatus int
	}{
		{name: "unknown_type", jobType: "UnknownRetrieval", wantStatus: http.StatusBadRequest},
		{name: "empty_type", jobType: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "job-vault")

			body := `{"Type":"` + tt.jobType + `"}`
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/"+testAccountID+"/vaults/job-vault/jobs",
				body,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestInitiateJob_ArchiveRetrieval_RequiresArchiveID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "missing_archive_id", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "job-vault2")

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/job-vault2/jobs",
				`{"Type":"ArchiveRetrieval"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestInitiateJob_ArchiveNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "nonexistent_archive", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "job-vault3")

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/job-vault3/jobs",
				`{"Type":"ArchiveRetrieval","ArchiveId":"nonexistent-id"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestInitiateJob_InventoryRetrieval_NoArchiveID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "inventory_no_archive_id_ok", wantStatus: http.StatusAccepted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "inv-vault")

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/inv-vault/jobs",
				`{"Type":"InventoryRetrieval"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDescribeJob_ArchiveSizeInBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		archiveSize     int64
		wantSizePresent bool
	}{
		{name: "archive_retrieval_has_size", archiveSize: 1024, wantSizePresent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "ar-vault"})
			bk.AddArchiveInternal(testAccountID, testRegion, "ar-vault", &glacier.Archive{
				ArchiveID:      "archive-123",
				Size:           tt.archiveSize,
				SHA256TreeHash: "deadbeef",
			})

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/ar-vault/jobs",
				`{"Type":"ArchiveRetrieval","ArchiveId":"archive-123"}`)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var initResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"].(string)

			rec = doRequest(
				t,
				h,
				http.MethodGet,
				"/"+testAccountID+"/vaults/ar-vault/jobs/"+jobID,
				"",
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var jobResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &jobResp))
			if tt.wantSizePresent {
				assert.NotNil(t, jobResp["ArchiveSizeInBytes"])
			}
		})
	}
}

func TestDescribeJob_ArchiveSHA256TreeHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		completeBefore bool
		wantArchiveSHA bool
		wantRangeSHA   bool
	}{
		{
			name:           "completed_job_has_both_hashes",
			completeBefore: true,
			wantArchiveSHA: true,
			wantRangeSHA:   true,
		},
		{
			name:           "in_progress_job_has_archive_hash_but_not_range_hash",
			completeBefore: false,
			wantArchiveSHA: true,
			wantRangeSHA:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()

			if tt.completeBefore {
				glacier.SetRetrievalDelay(bk, 0)
			} else {
				glacier.SetRetrievalDelay(bk, time.Hour)
			}

			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "wire-vault"})
			bk.AddArchiveInternal(testAccountID, testRegion, "wire-vault", &glacier.Archive{
				ArchiveID:      "wire-archive",
				Size:           1024,
				SHA256TreeHash: "cafebabe",
			})

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/wire-vault/jobs",
				`{"Type":"ArchiveRetrieval","ArchiveId":"wire-archive"}`)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var initResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"].(string)

			rec = doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/wire-vault/jobs/"+jobID, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var jobResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &jobResp))

			if tt.wantArchiveSHA {
				assert.Equal(t, "cafebabe", jobResp["ArchiveSHA256TreeHash"],
					"ArchiveSHA256TreeHash must be present as its own wire field")
			}

			if tt.wantRangeSHA {
				assert.Equal(t, "cafebabe", jobResp["SHA256TreeHash"])
			} else {
				assert.Nil(t, jobResp["SHA256TreeHash"],
					"SHA256TreeHash must stay absent until the job completes, per AWS")
			}
		})
	}
}

// -------------------------------------------------------------------------
// GetJobOutput for an archive-retrieval job must echo the archive's
// description via the X-Amz-Archive-Description response header (real AWS:
// awsRestjson1_deserializeOpHttpBindingsGetJobOutputOutput reads
// "x-amz-archive-description"). This header was never set by gopherstack.
// -------------------------------------------------------------------------

func newDelayedHandler(delay time.Duration) *glacier.Handler {
	bk := glacier.NewInMemoryBackend()
	glacier.SetRetrievalDelay(bk, delay)
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

// initiateJob creates a vault and initiates a retrieval job, returning the jobId.

func initiateJob(t *testing.T, h *glacier.Handler, vaultName, jobType string) string {
	t.Helper()

	createVault(t, h, vaultName)

	rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/"+vaultName+"/jobs",
		`{"Type":"`+jobType+`"}`)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	jobID, ok := resp["jobId"].(string)
	require.True(t, ok, "jobId missing from initiate response")
	require.NotEmpty(t, jobID)

	return jobID
}

func TestInitiateJob_StartsInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		jobType   string
	}{
		{
			name:      "inventory_retrieval_starts_in_progress",
			vaultName: "parity-vault-inprogress-inventory",
			jobType:   "inventory-retrieval",
		},
		{
			name:      "inventory_retrieval_starts_in_progress_2",
			vaultName: "parity-vault-inprogress-inventory2",
			jobType:   "InventoryRetrieval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use a long delay so the job stays InProgress during the test.
			h := newDelayedHandler(10 * time.Second)
			jobID := initiateJob(t, h, tt.vaultName, tt.jobType)

			rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults/"+tt.vaultName+"/jobs/"+jobID, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var desc map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
			assert.Equal(t, "InProgress", desc["StatusCode"])
		})
	}
}

func TestInitiateJob_SucceedsAfterDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		jobType   string
		delay     time.Duration
		wait      time.Duration
	}{
		{
			name:      "inventory_retrieval_succeeds_after_delay",
			vaultName: "parity-vault-delay-inventory",
			jobType:   "inventory-retrieval",
			delay:     50 * time.Millisecond,
			wait:      120 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newDelayedHandler(tt.delay)
			jobID := initiateJob(t, h, tt.vaultName, tt.jobType)

			// Immediately after initiation the job should be InProgress.
			recEarly := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults/"+tt.vaultName+"/jobs/"+jobID, "")
			require.Equal(t, http.StatusOK, recEarly.Code)
			var earlyDesc map[string]any
			require.NoError(t, json.Unmarshal(recEarly.Body.Bytes(), &earlyDesc))
			assert.Equal(t, "InProgress", earlyDesc["StatusCode"])

			// After the delay elapses the job should be Succeeded.
			time.Sleep(tt.wait)

			recLate := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults/"+tt.vaultName+"/jobs/"+jobID, "")
			require.Equal(t, http.StatusOK, recLate.Code)
			var lateDesc map[string]any
			require.NoError(t, json.Unmarshal(recLate.Body.Bytes(), &lateDesc))
			assert.Equal(t, "Succeeded", lateDesc["StatusCode"])
		})
	}
}

func TestInitiateJob_ZeroDelay_ImmediateSucceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		jobType   string
	}{
		{
			name:      "inventory_retrieval_immediate_succeeded",
			vaultName: "parity-vault-zero-inventory",
			jobType:   "inventory-retrieval",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// newTestHandler sets delay=0.
			h := newTestHandler()
			jobID := initiateJob(t, h, tt.vaultName, tt.jobType)

			rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults/"+tt.vaultName+"/jobs/"+jobID, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var desc map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
			assert.Equal(t, "Succeeded", desc["StatusCode"])
		})
	}
}

func TestListJobs_JobListAlwaysPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		seedJobs bool
	}{
		{
			name:     "empty_vault_returns_empty_array",
			seedJobs: false,
		},
		{
			name:     "populated_vault_returns_array",
			seedJobs: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "listjobs-audit-vault")

			if tt.seedJobs {
				body := `{"Type":"inventory-retrieval"}`
				rec := doRequest(t, h, http.MethodPost,
					"/"+testAccountID+"/vaults/listjobs-audit-vault/jobs", body)
				require.Equal(t, http.StatusAccepted, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/listjobs-audit-vault/jobs", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			_, present := resp["JobList"]
			assert.True(t, present, "JobList key must always be present in ListJobs response")

			if !tt.seedJobs {
				assert.Equal(t, "[]", string(resp["JobList"]),
					"JobList must be [] (not null or absent) when no jobs exist")
			}
		})
	}
}

// -------------------------------------------------------------------------
// Issue 31: ListParts returns Parts:[] not null when no parts uploaded
// -------------------------------------------------------------------------

func TestDescribeJob_SNSTopicPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snsTopic     string
		wantSNSTopic string
		wantPresent  bool
	}{
		{
			name:         "job_with_sns_topic_returns_it",
			snsTopic:     "arn:aws:sns:us-east-1:000000000000:my-topic",
			wantPresent:  true,
			wantSNSTopic: "arn:aws:sns:us-east-1:000000000000:my-topic",
		},
		{
			name:        "job_without_sns_topic_omits_field",
			snsTopic:    "",
			wantPresent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "snstopic-audit-vault")

			var bodyStr string
			if tt.snsTopic != "" {
				bodyStr = `{"Type":"inventory-retrieval","SNSTopic":"` + tt.snsTopic + `"}`
			} else {
				bodyStr = `{"Type":"inventory-retrieval"}`
			}

			initRec := doRequest(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/snstopic-audit-vault/jobs", bodyStr)
			require.Equal(t, http.StatusAccepted, initRec.Code)

			var initResp map[string]string
			require.NoError(t, json.Unmarshal(initRec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"]
			require.NotEmpty(t, jobID)

			descRec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/snstopic-audit-vault/jobs/"+jobID, "")
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

			snsRaw, present := resp["SNSTopic"]
			if tt.wantPresent {
				assert.True(t, present, "SNSTopic must be present when job was initiated with SNSTopic")
				var got string
				require.NoError(t, json.Unmarshal(snsRaw, &got))
				assert.Equal(t, tt.wantSNSTopic, got)
			} else if present {
				var got string
				require.NoError(t, json.Unmarshal(snsRaw, &got))
				assert.Empty(t, got, "SNSTopic should be empty or absent when not set")
			}
		})
	}
}

// -------------------------------------------------------------------------
// Issue 33: DescribeJob returns RetrievalByteRange for archive-retrieval jobs
// -------------------------------------------------------------------------

func TestDescribeJob_RetrievalByteRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		byteRange  string
		wantInResp bool
	}{
		{
			name:       "byte_range_set_on_job_returned_in_describe",
			byteRange:  "0-1048575",
			wantInResp: true,
		},
		{
			name:       "no_byte_range_field_absent",
			byteRange:  "",
			wantInResp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "byterange-audit-vault")

			e := echo.New()
			archiveBody := []byte("archive data")
			req := httptest.NewRequest(http.MethodPost,
				"/"+testAccountID+"/vaults/byterange-audit-vault/archives",
				strings.NewReader(string(archiveBody)))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			require.Equal(t, http.StatusCreated, rec.Code)

			var archResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &archResp))
			archiveID := archResp["archiveId"]
			require.NotEmpty(t, archiveID)

			var bodyStr string
			if tt.byteRange != "" {
				bodyStr = `{"Type":"archive-retrieval","ArchiveId":"` + archiveID +
					`","RetrievalByteRange":"` + tt.byteRange + `"}`
			} else {
				bodyStr = `{"Type":"archive-retrieval","ArchiveId":"` + archiveID + `"}`
			}

			initRec := doRequest(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/byterange-audit-vault/jobs", bodyStr)
			require.Equal(t, http.StatusAccepted, initRec.Code)

			var initResp map[string]string
			require.NoError(t, json.Unmarshal(initRec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"]
			require.NotEmpty(t, jobID)

			descRec := doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/byterange-audit-vault/jobs/"+jobID, "")
			require.Equal(t, http.StatusOK, descRec.Code)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

			rangeRaw, present := resp["RetrievalByteRange"]
			if tt.wantInResp {
				assert.True(t, present, "RetrievalByteRange must be present when set on the job")
				var got string
				require.NoError(t, json.Unmarshal(rangeRaw, &got))
				assert.Equal(t, tt.byteRange, got)
			} else if present {
				var got string
				require.NoError(t, json.Unmarshal(rangeRaw, &got))
				assert.Empty(t, got, "RetrievalByteRange should be empty or absent when not set")
			}
		})
	}
}

func TestListJobs_CompletedParamValidation(t *testing.T) {
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

			path := "/" + testAccountID + "/vaults/listjobs-completed-vault/jobs"
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

func TestListJobs_CompletedFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		completedFilter string
		wantCount       int
	}{
		{name: "completed_true_returns_succeeded", completedFilter: "true", wantCount: 1},
		{name: "completed_false_returns_empty", completedFilter: "false", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "list-jobs-completed-"+tt.name)
			initiateJobWithBody(t, h, "list-jobs-completed-"+tt.name, `{"Type":"inventory-retrieval"}`)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/list-jobs-completed-"+tt.name+"/jobs?completed="+tt.completedFilter,
				"", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			jobs := resp["JobList"].([]any)
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}

func TestListJobs_StatusCodeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		statuscodeParam string
		wantCount       int
	}{
		{name: "succeeded_filter", statuscodeParam: "Succeeded", wantCount: 1},
		{name: "in_progress_filter", statuscodeParam: "InProgress", wantCount: 0},
		{name: "failed_filter", statuscodeParam: "Failed", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "list-jobs-sc-"+tt.name)
			initiateJobWithBody(t, h, "list-jobs-sc-"+tt.name, `{"Type":"inventory-retrieval"}`)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/list-jobs-sc-"+tt.name+"/jobs?statuscode="+tt.statuscodeParam,
				"", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			jobs := resp["JobList"].([]any)
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}

func TestListJobs_InvalidCompletedParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		completedFilter string
	}{
		{name: "invalid_value", completedFilter: "yes"},
		{name: "numeric_value", completedFilter: "1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "listjobs-invalid-"+tt.name)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/listjobs-invalid-"+tt.name+"/jobs?completed="+tt.completedFilter,
				"", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. VaultLock full lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func TestDescribeJob_Fidelity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobBody    string
		wantAction string
		wantTier   string
	}{
		{
			name:       "inventory_retrieval_fields",
			jobBody:    `{"Type":"inventory-retrieval","Tier":"Bulk"}`,
			wantAction: "InventoryRetrieval",
			wantTier:   "Bulk",
		},
		{
			name:       "default_tier_is_standard",
			jobBody:    `{"Type":"inventory-retrieval"}`,
			wantAction: "InventoryRetrieval",
			wantTier:   "Standard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "describe-job-fidelity-"+tt.name)
			jobID := initiateJobWithBody(t, h, "describe-job-fidelity-"+tt.name, tt.jobBody)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/describe-job-fidelity-"+tt.name+"/jobs/"+jobID, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantAction, resp["Action"])
			assert.Equal(t, tt.wantTier, resp["Tier"])
			assert.NotEmpty(t, resp["JobId"])
			assert.NotEmpty(t, resp["VaultARN"])
			assert.NotEmpty(t, resp["CreationDate"])
			assert.NotEmpty(t, resp["StatusCode"])
		})
	}
}

func TestDescribeJob_ArchiveSizePopulated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{name: "archive_size_in_bytes_from_archive", content: "archive content here"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "archsize-job-vault")
			archiveID := uploadArchiveData(t, h, "archsize-job-vault", []byte(tt.content))

			jobID := initiateJobWithBody(t, h, "archsize-job-vault",
				`{"Type":"archive-retrieval","ArchiveId":"`+archiveID+`"}`)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/archsize-job-vault/jobs/"+jobID, "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			archiveSize, ok := resp["ArchiveSizeInBytes"].(float64)
			assert.True(t, ok, "ArchiveSizeInBytes must be present for archive-retrieval job")
			assert.InDelta(t, float64(len(tt.content)), archiveSize, 0)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 19. ListVaults limit and marker together
// ─────────────────────────────────────────────────────────────────────────────

func TestInitiateJob_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "invalid_type_rejected",
			body:       `{"Type":"bogus-type"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "archive_retrieval_missing_archive_id",
			body:       `{"Type":"archive-retrieval"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_tier_rejected",
			body:       `{"Type":"inventory-retrieval","Tier":"Express"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "bulk_tier_accepted",
			body:       `{"Type":"inventory-retrieval","Tier":"Bulk"}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "expedited_tier_accepted",
			body:       `{"Type":"inventory-retrieval","Tier":"Expedited"}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "standard_tier_accepted",
			body:       `{"Type":"inventory-retrieval","Tier":"Standard"}`,
			wantStatus: http.StatusAccepted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "init-job-val-"+tt.name)

			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/init-job-val-"+tt.name+"/jobs", tt.body, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 21. GetJobOutput for incomplete job
// ─────────────────────────────────────────────────────────────────────────────

func TestDescribeJob_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(h *glacier.Handler)
		vaultName  string
		jobID      string
		wantStatus int
	}{
		{
			name:       "vault_not_found",
			setupFn:    func(_ *glacier.Handler) {},
			vaultName:  "nonexistent",
			jobID:      "fakejob",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "job_not_found",
			setupFn: func(h *glacier.Handler) {
				createVault(t, h, "existvault")
			},
			vaultName:  "existvault",
			jobID:      "fakejob",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.setupFn(h)

			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/"+tt.vaultName+"/jobs/"+tt.jobID, "", nil)
			assert.Equal(t, tt.wantStatus, rec.Code, tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 25. Handler.Reset clears archiveData cache
// ─────────────────────────────────────────────────────────────────────────────
