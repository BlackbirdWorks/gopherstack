package glacier_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// newInventoryTestHandler creates a handler backed by a fresh in-memory backend with
// the simulated retrieval delay disabled, exposing the backend directly so tests can
// seed archives with controlled CreationDate values via AddArchiveInternal (real
// UploadArchive always stamps CreationDate = time.Now(), which can't be controlled
// precisely enough for date-range filter tests).
func newInventoryTestHandler(t *testing.T, vaultName string) (*glacier.Handler, *glacier.InMemoryBackend) {
	t.Helper()

	bk := glacier.NewInMemoryBackend()
	glacier.SetRetrievalDelay(bk, 0)
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: vaultName})

	return h, bk
}

// seedDatedArchives adds archives with the given IDs and CreationDate values (in the
// same order) to vaultName.
func seedDatedArchives(bk *glacier.InMemoryBackend, vaultName string, ids, dates []string) {
	for i, id := range ids {
		bk.AddArchiveInternal(testAccountID, testRegion, vaultName, &glacier.Archive{
			ArchiveID:    id,
			CreationDate: dates[i],
			Size:         10,
		})
	}
}

// inventoryArchiveIDs extracts the ArchiveId list from a GetJobOutput inventory JSON body.
func inventoryArchiveIDs(t *testing.T, body []byte) []string {
	t.Helper()

	var resp struct {
		ArchiveList []struct {
			ArchiveID string `json:"ArchiveId"`
		} `json:"ArchiveList"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))

	ids := make([]string, len(resp.ArchiveList))
	for i, a := range resp.ArchiveList {
		ids[i] = a.ArchiveID
	}

	return ids
}

// TestGetJobOutput_InventoryRetrieval_DateRangeFilters verifies that
// InventoryRetrievalParameters.StartDate/EndDate on InitiateJob filters the returned
// inventory to archives with CreationDate in [StartDate, EndDate), matching AWS's
// range inventory retrieval semantics.
func TestGetJobOutput_InventoryRetrieval_DateRangeFilters(t *testing.T) {
	t.Parallel()

	h, bk := newInventoryTestHandler(t, "inv-range-vault")
	seedDatedArchives(bk, "inv-range-vault",
		[]string{"a1", "a2", "a3"},
		[]string{"2020-01-15T00:00:00.000Z", "2020-02-15T00:00:00.000Z", "2020-03-15T00:00:00.000Z"},
	)

	body := `{"Type":"inventory-retrieval","InventoryRetrievalParameters":` +
		`{"StartDate":"2020-02-01T00:00:00Z","EndDate":"2020-03-01T00:00:00Z"}}`
	jobID := initiateJobWithBody(t, h, "inv-range-vault", body)

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/inv-range-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	assert.Equal(t, []string{"a2"}, inventoryArchiveIDs(t, rec.Body.Bytes()))
}

// TestGetJobOutput_InventoryRetrieval_Limit verifies the Limit parameter caps the
// number of archives returned.
func TestGetJobOutput_InventoryRetrieval_Limit(t *testing.T) {
	t.Parallel()

	h, bk := newInventoryTestHandler(t, "inv-limit-vault")
	seedDatedArchives(bk, "inv-limit-vault",
		[]string{"a1", "a2", "a3", "a4", "a5"},
		[]string{
			"2020-01-01T00:00:00.000Z", "2020-01-02T00:00:00.000Z", "2020-01-03T00:00:00.000Z",
			"2020-01-04T00:00:00.000Z", "2020-01-05T00:00:00.000Z",
		},
	)

	jobID := initiateJobWithBody(t, h, "inv-limit-vault",
		`{"Type":"inventory-retrieval","InventoryRetrievalParameters":{"Limit":"2"}}`)

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/inv-limit-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	assert.Equal(t, []string{"a1", "a2"}, inventoryArchiveIDs(t, rec.Body.Bytes()))
}

// TestGetJobOutput_InventoryRetrieval_Marker verifies the Marker parameter resumes
// pagination strictly after the named archive, matching AWS's continuation pattern
// for range inventory retrieval.
func TestGetJobOutput_InventoryRetrieval_Marker(t *testing.T) {
	t.Parallel()

	h, bk := newInventoryTestHandler(t, "inv-marker-vault")
	seedDatedArchives(bk, "inv-marker-vault",
		[]string{"a1", "a2", "a3"},
		[]string{"2020-01-01T00:00:00.000Z", "2020-01-02T00:00:00.000Z", "2020-01-03T00:00:00.000Z"},
	)

	jobID := initiateJobWithBody(t, h, "inv-marker-vault",
		`{"Type":"inventory-retrieval","InventoryRetrievalParameters":{"Marker":"a1"}}`)

	rec := doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/inv-marker-vault/jobs/"+jobID+"/output", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	assert.Equal(t, []string{"a2", "a3"}, inventoryArchiveIDs(t, rec.Body.Bytes()))
}

// TestInitiateJob_InventoryRetrieval_InvalidParameters verifies malformed
// InventoryRetrievalParameters are rejected with InvalidParameterValueException
// rather than silently ignored.
func TestInitiateJob_InventoryRetrieval_InvalidParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "bad_start_date",
			body: `{"Type":"inventory-retrieval","InventoryRetrievalParameters":{"StartDate":"not-a-date"}}`,
		},
		{
			name: "bad_end_date",
			body: `{"Type":"inventory-retrieval","InventoryRetrievalParameters":{"EndDate":"not-a-date"}}`,
		},
		{
			name: "non_numeric_limit",
			body: `{"Type":"inventory-retrieval","InventoryRetrievalParameters":{"Limit":"abc"}}`,
		},
		{
			name: "zero_limit",
			body: `{"Type":"inventory-retrieval","InventoryRetrievalParameters":{"Limit":"0"}}`,
		},
		{
			name: "negative_limit",
			body: `{"Type":"inventory-retrieval","InventoryRetrievalParameters":{"Limit":"-5"}}`,
		},
		{
			name: "bad_format",
			body: `{"Type":"inventory-retrieval","Format":"XML"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInventoryTestHandler(t, "inv-invalid-vault")

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/inv-invalid-vault/jobs", tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidParameterValueException", errResp["code"])
		})
	}
}

// TestDescribeJob_InventoryRetrieval_EchoesParameters verifies DescribeJob nests
// InventoryRetrievalParameters (StartDate/EndDate/Format/Limit/Marker) under its own
// object, matching the real GlacierJobDescription wire shape -- and that the invented
// top-level "Format" field this response DTO used to carry is gone (see PARITY.md).
func TestDescribeJob_InventoryRetrieval_EchoesParameters(t *testing.T) {
	t.Parallel()

	h, _ := newInventoryTestHandler(t, "inv-describe-vault")

	body := `{"Type":"inventory-retrieval","Format":"CSV","InventoryRetrievalParameters":` +
		`{"StartDate":"2020-01-01T00:00:00Z","EndDate":"2020-02-01T00:00:00Z","Limit":"100","Marker":"a1"}}`
	jobID := initiateJobWithBody(t, h, "inv-describe-vault", body)

	rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults/inv-describe-vault/jobs/"+jobID, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasTopLevelFormat := resp["Format"]
	assert.False(t, hasTopLevelFormat, "top-level Format is not a real GlacierJobDescription field")

	invParams, ok := resp["InventoryRetrievalParameters"].(map[string]any)
	require.True(t, ok, "InventoryRetrievalParameters must be present: %#v", resp)
	assert.Equal(t, "2020-01-01T00:00:00Z", invParams["StartDate"])
	assert.Equal(t, "2020-02-01T00:00:00Z", invParams["EndDate"])
	assert.Equal(t, "CSV", invParams["Format"])
	assert.Equal(t, "100", invParams["Limit"])
	assert.Equal(t, "a1", invParams["Marker"])
}

// TestDescribeJob_ArchiveRetrieval_NoInventoryRetrievalParameters verifies
// InventoryRetrievalParameters stays null (omitted) for a non-InventoryRetrieval job,
// matching AWS ("For an archive retrieval job... this field is null").
func TestDescribeJob_ArchiveRetrieval_NoInventoryRetrievalParameters(t *testing.T) {
	t.Parallel()

	h, bk := newInventoryTestHandler(t, "inv-ar-vault")
	bk.AddArchiveInternal(testAccountID, testRegion, "inv-ar-vault", &glacier.Archive{
		ArchiveID: "a1", Size: 10,
	})

	jobID := initiateJobWithBody(t, h, "inv-ar-vault", `{"Type":"ArchiveRetrieval","ArchiveId":"a1"}`)

	rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults/inv-ar-vault/jobs/"+jobID, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, has := resp["InventoryRetrievalParameters"]
	assert.False(t, has)
}
