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

// -------------------------------------------------------------------------
// DescribeJob/ListJobs must return the ArchiveSHA256TreeHash field distinct
// from SHA256TreeHash on the wire, matching real aws-sdk-go-v2/service/glacier
// (deserializers.go decodes both "ArchiveSHA256TreeHash" and "SHA256TreeHash"
// as separate GlacierJobDescription fields). Before this fix,
// ArchiveSHA256TreeHash was never sent, so real SDK clients calling
// DescribeJob for a completed ArchiveRetrieval job always got a nil archive
// checksum -- even though gopherstack had the value all along under the
// wrong (single, conflated) field name.
// -------------------------------------------------------------------------

func TestWireParity_DescribeJob_ArchiveSHA256TreeHash(t *testing.T) {
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

func TestWireParity_GetJobOutput_ArchiveDescriptionHeader(t *testing.T) {
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
