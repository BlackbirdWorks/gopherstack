package glacier_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// newDelayedHandler returns a Glacier handler with a non-zero retrieval delay.
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

	setupVault(t, h, vaultName)

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

func TestParity_InitiateJob_StartsInProgress(t *testing.T) {
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

func TestParity_InitiateJob_SucceedsAfterDelay(t *testing.T) {
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

func TestParity_InitiateJob_ZeroDelay_ImmediateSucceeded(t *testing.T) {
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
