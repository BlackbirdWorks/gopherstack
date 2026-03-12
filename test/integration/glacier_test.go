package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	glaciersdk "github.com/aws/aws-sdk-go-v2/service/glacier"
	glaciertypes "github.com/aws/aws-sdk-go-v2/service/glacier/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createGlacierClient returns a Glacier client pointed at the shared test container.
func createGlacierClient(t *testing.T) *glaciersdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		require.NoError(t, err, "unable to load SDK config")
	}

	return glaciersdk.NewFromConfig(cfg, func(o *glaciersdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Glacier_VaultLifecycle tests the full vault CRUD lifecycle via the SDK.
func TestIntegration_Glacier_VaultLifecycle(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := createGlacierClient(t)
	vaultName := "integration-test-vault-" + t.Name()

	// --- Create ---
	_, err := client.CreateVault(ctx, &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
	})
	require.NoError(t, err, "CreateVault should succeed")

	// --- Describe ---
	descOut, err := client.DescribeVault(ctx, &glaciersdk.DescribeVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
	})
	require.NoError(t, err, "DescribeVault should succeed")
	assert.Equal(t, vaultName, aws.ToString(descOut.VaultName))
	assert.NotEmpty(t, descOut.VaultARN)

	// --- List ---
	listOut, err := client.ListVaults(ctx, &glaciersdk.ListVaultsInput{
		AccountId: aws.String("-"),
	})
	require.NoError(t, err, "ListVaults should succeed")

	found := false

	for _, v := range listOut.VaultList {
		if aws.ToString(v.VaultName) == vaultName {
			found = true

			break
		}
	}

	assert.True(t, found, "created vault should appear in list")

	// --- Tags ---
	err = glacierAddTags(t, vaultName, map[string]string{"env": "integration", "team": "platform"})
	require.NoError(t, err, "AddTagsToVault should succeed")

	tagsOut, err := client.ListTagsForVault(ctx, &glaciersdk.ListTagsForVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
	})
	require.NoError(t, err, "ListTagsForVault should succeed")
	assert.Equal(t, "integration", tagsOut.Tags["env"])
	assert.Equal(t, "platform", tagsOut.Tags["team"])

	_, err = client.RemoveTagsFromVault(ctx, &glaciersdk.RemoveTagsFromVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
		TagKeys:   []string{"team"},
	})
	require.NoError(t, err, "RemoveTagsFromVault should succeed")

	// --- Delete ---
	_, err = client.DeleteVault(ctx, &glaciersdk.DeleteVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
	})
	require.NoError(t, err, "DeleteVault should succeed")

	// Verify deleted.
	_, err = client.DescribeVault(ctx, &glaciersdk.DescribeVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
	})
	require.Error(t, err, "DescribeVault after delete should fail")
}

// TestIntegration_Glacier_JobLifecycle tests initiating and describing jobs.
func TestIntegration_Glacier_JobLifecycle(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := createGlacierClient(t)
	vaultName := "job-test-vault-" + t.Name()

	_, err := client.CreateVault(ctx, &glaciersdk.CreateVaultInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
	})
	require.NoError(t, err, "CreateVault should succeed")

	defer func() {
		_, _ = client.DeleteVault(ctx, &glaciersdk.DeleteVaultInput{
			AccountId: aws.String("-"),
			VaultName: aws.String(vaultName),
		})
	}()

	// Initiate an inventory retrieval job.
	initiateOut, err := client.InitiateJob(ctx, &glaciersdk.InitiateJobInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
		JobParameters: &glaciertypes.JobParameters{
			Type: aws.String("inventory-retrieval"),
		},
	})
	require.NoError(t, err, "InitiateJob should succeed")
	require.NotEmpty(t, initiateOut.JobId)

	// Describe the job.
	descJobOut, err := client.DescribeJob(ctx, &glaciersdk.DescribeJobInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
		JobId:     initiateOut.JobId,
	})
	require.NoError(t, err, "DescribeJob should succeed")
	assert.Equal(t, aws.ToString(initiateOut.JobId), aws.ToString(descJobOut.JobId))
	assert.True(t, descJobOut.Completed, "job should complete synchronously in the stub")

	// List jobs.
	listJobsOut, err := client.ListJobs(ctx, &glaciersdk.ListJobsInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
	})
	require.NoError(t, err, "ListJobs should succeed")
	assert.Len(t, listJobsOut.JobList, 1)

	// Get job output.
	getOutputResp, err := client.GetJobOutput(ctx, &glaciersdk.GetJobOutputInput{
		AccountId: aws.String("-"),
		VaultName: aws.String(vaultName),
		JobId:     initiateOut.JobId,
	})
	require.NoError(t, err, "GetJobOutput should succeed")

	_ = getOutputResp.Body.Close()
}

// TestIntegration_Glacier_VaultNotFound verifies that 404 is returned for a non-existent vault.
func TestIntegration_Glacier_VaultNotFound(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/-/vaults/does-not-exist", http.NoBody)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// glacierAddTags is a helper to add tags to a vault using the HTTP API directly.
func glacierAddTags(t *testing.T, vaultName string, tags map[string]string) error {
	t.Helper()

	type addTagsReq struct {
		Tags map[string]string `json:"Tags"`
	}

	payload, err := json.Marshal(addTagsReq{Tags: tags})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		endpoint+"/-/vaults/"+vaultName+"/tags?operation=add", bytes.NewReader(payload))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		return assert.AnError
	}

	return nil
}
