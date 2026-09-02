package glacier_test

// pagination_arithmetic_test.go verifies the three marker+limit paginators
// this service hand-rolls (paginateJobList, paginateUploadList,
// paginatePartList -- all in handler_jobs.go/handler_multipart_uploads.go,
// explicitly marked //nolint:dupl as sharing identical structure). Unlike
// the offset-token helpers elsewhere in this campaign, all three already
// default a marker miss to an EMPTY result (items[:0]), not to index 0 --
// the safe default this campaign's Class B/C fix recommends -- so a stale
// or tampered marker terminates instead of looping. They take *echo.Context
// directly rather than a testable value, so this is proven through the real
// SDK client (ListJobs, representative of all three) rather than a unit
// call directly against the helper.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func seedJobs(bk *glacier.InMemoryBackend, vaultName string, n int) []string {
	ids := make([]string, 0, n)

	for i := range n {
		id := "job-" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		bk.AddJobInternal(testAccountID, testRegion, vaultName, &glacier.Job{
			JobID:      id,
			VaultName:  vaultName,
			Action:     "InventoryRetrieval",
			StatusCode: "Succeeded",
			Completed:  true,
		})
		ids = append(ids, id)
	}

	return ids
}

// TestListJobs_SDKRoundTrip_BoundaryWalkNoDropNoDuplicate walks the full job
// list one item at a time via the real SDK client and requires the
// concatenation of every page to reproduce the seeded set exactly.
func TestListJobs_SDKRoundTrip_BoundaryWalkNoDropNoDuplicate(t *testing.T) {
	t.Parallel()

	bk, client := newWireBackendAndClient(t)

	const vaultName = "pagination-walk-vault"

	_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
	})
	require.NoError(t, err)

	want := seedJobs(bk, vaultName, 7)

	var seen []string

	marker := ""
	for {
		in := &glaciersdk.ListJobsInput{
			AccountId: aws.String("-"), VaultName: aws.String(vaultName), Limit: aws.Int32(1),
		}
		if marker != "" {
			in.Marker = aws.String(marker)
		}

		out, listErr := client.ListJobs(t.Context(), in)
		require.NoError(t, listErr)
		require.Len(t, out.JobList, 1)

		seen = append(seen, aws.ToString(out.JobList[0].JobId))

		if out.Marker == nil {
			break
		}

		marker = aws.ToString(out.Marker)
	}

	assert.Equal(t, want, seen,
		"walking one job at a time must reproduce every seeded job, in order, no drops or dupes")
}

// TestListJobs_SDKRoundTrip_TamperedMarkerTerminates proves a marker naming
// no known job returns an empty page (paginateJobList's documented
// default), not the full list restarting at page one.
func TestListJobs_SDKRoundTrip_TamperedMarkerTerminates(t *testing.T) {
	t.Parallel()

	bk, client := newWireBackendAndClient(t)

	const vaultName = "pagination-tampered-vault"

	_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
	})
	require.NoError(t, err)

	seedJobs(bk, vaultName, 5)

	out, err := client.ListJobs(t.Context(), &glaciersdk.ListJobsInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
		Marker:    aws.String("job-does-not-exist"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.JobList, "an unmatched marker must terminate with an empty page, not restart at page one")
	assert.Nil(t, out.Marker)
}
