package glacier_test

// list_filter_params_test.go ratifies the gopherstack-6flj wrapper-key sweep's
// constrained-parameter fixes for glacier: ListJobs, ListMultipartUploads,
// ListParts, and ListVaults all left their `limit` page cap unapplied when
// the client sent none, returning every item in one page instead of the
// documented default (glacier@v1.35.4 api_op_List*.go: "The default limit is
// 50" for Jobs/Uploads/Parts, "The default limit is 10" for Vaults).

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// newWireBackendAndClient is newWireTestClient's sibling: it also returns the
// backend, needed by tests that seed state directly via the internal
// AddXInternal helpers rather than through the SDK.
func newWireBackendAndClient(t *testing.T) (*glacier.InMemoryBackend, *glaciersdk.Client) {
	t.Helper()

	bk := glacier.NewInMemoryBackend()
	glacier.SetRetrievalDelay(bk, 0)
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := glaciersdk.NewFromConfig(cfg, func(o *glaciersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return bk, client
}

func TestListVaults_DefaultLimit(t *testing.T) {
	t.Parallel()

	_, client := newWireBackendAndClient(t)

	const seeded = 11

	for i := range seeded {
		_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
			AccountId: aws.String("-"),
			VaultName: aws.String("vault-" + string(rune('a'+i))),
		})
		require.NoError(t, err)
	}

	out, err := client.ListVaults(t.Context(), &glaciersdk.ListVaultsInput{AccountId: aws.String("-")})
	require.NoError(t, err)
	assert.Len(t, out.VaultList, 10, "no limit given: must default to the documented 10")
	assert.NotNil(t, out.Marker, "11 vaults > default limit of 10: a marker must be returned")
}

func TestListJobs_DefaultLimit(t *testing.T) {
	t.Parallel()

	bk, client := newWireBackendAndClient(t)

	const vaultName = "list-jobs-default-vault"

	_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
	})
	require.NoError(t, err)

	const seeded = 51

	for i := range seeded {
		id := "job-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		bk.AddJobInternal(testAccountID, testRegion, vaultName, &glacier.Job{
			JobID:      id,
			VaultName:  vaultName,
			Action:     "InventoryRetrieval",
			StatusCode: "Succeeded",
			Completed:  true,
		})
	}

	out, err := client.ListJobs(t.Context(), &glaciersdk.ListJobsInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
	})
	require.NoError(t, err)
	assert.Len(t, out.JobList, 50, "no limit given: must default to the documented 50")
	assert.NotNil(t, out.Marker, "51 jobs > default limit of 50: a marker must be returned")
}

func TestListMultipartUploads_DefaultLimit(t *testing.T) {
	t.Parallel()

	bk, client := newWireBackendAndClient(t)

	const vaultName = "list-uploads-default-vault"

	_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
	})
	require.NoError(t, err)

	const seeded = 51

	for i := range seeded {
		id := "upload-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		bk.AddMultipartUploadInternal(testAccountID, testRegion, vaultName, &glacier.MultipartUpload{
			MultipartUploadID: id,
			PartSizeInBytes:   1 << 20,
		})
	}

	out, err := client.ListMultipartUploads(t.Context(), &glaciersdk.ListMultipartUploadsInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
	})
	require.NoError(t, err)
	assert.Len(t, out.UploadsList, 50, "no limit given: must default to the documented 50")
	assert.NotNil(t, out.Marker, "51 uploads > default limit of 50: a marker must be returned")
}

func TestListParts_DefaultLimit(t *testing.T) {
	t.Parallel()

	bk, client := newWireBackendAndClient(t)

	const vaultName = "list-parts-default-vault"

	_, err := client.CreateVault(t.Context(), &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
	})
	require.NoError(t, err)

	initOut, err := client.InitiateMultipartUpload(t.Context(), &glaciersdk.InitiateMultipartUploadInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName),
		PartSize: aws.String("1048576"),
	})
	require.NoError(t, err)

	uploadID := aws.ToString(initOut.UploadId)

	const seeded = 51

	for i := range seeded {
		start := int64(i) * (1 << 20)
		end := start + (1 << 20) - 1
		bk.AddMultipartPartInternal(testAccountID, testRegion, vaultName, uploadID, glacier.MultipartPart{
			RangeInBytes: fmt.Sprintf("%d-%d", start, end),
		})
	}

	out, err := client.ListParts(t.Context(), &glaciersdk.ListPartsInput{
		AccountId: aws.String("-"), VaultName: aws.String(vaultName), UploadId: aws.String(uploadID),
	})
	require.NoError(t, err)
	assert.Len(t, out.Parts, 50, "no limit given: must default to the documented 50")
	assert.NotNil(t, out.Marker, "51 parts > default limit of 50: a marker must be returned")
}
