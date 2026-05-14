package mwaa_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func newLifecycleBackend(t *testing.T) *mwaa.InMemoryBackend {
	t.Helper()

	return mwaa.NewInMemoryBackend("us-east-1", "123456789012")
}

func newLifecycleCreateReq() *mwaa.ExportedCreateEnvironmentRequest {
	return &mwaa.ExportedCreateEnvironmentRequest{
		DagS3Path:        "dags/",
		ExecutionRoleArn: "arn:aws:iam::123456789012:role/mwaa-role",
		SourceBucketArn:  "arn:aws:s3:::my-bucket",
		NetworkConfiguration: &mwaa.NetworkConfig{
			SubnetIDs:        []string{"subnet-a", "subnet-b"},
			SecurityGroupIDs: []string{"sg-1"},
		},
	}
}

func TestUpdateEnvironment_StatusTransitionsToUpdatingThenAvailable(t *testing.T) {
	t.Parallel()

	b := newLifecycleBackend(t)

	_, err := b.CreateEnvironment("us-east-1", "123456789012", "lc-env", newLifecycleCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment("lc-env", &mwaa.ExportedUpdateEnvironmentRequest{
		EnvironmentClass: "mw1.medium",
	})
	require.NoError(t, err)

	first, err := b.GetEnvironment("lc-env")
	require.NoError(t, err)
	assert.Equal(t, "UPDATING", first.Status)

	second, err := b.GetEnvironment("lc-env")
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", second.Status)
}

func TestUpdateEnvironment_RejectsEmptyNetworkConfig(t *testing.T) {
	t.Parallel()

	b := newLifecycleBackend(t)

	_, err := b.CreateEnvironment("us-east-1", "123456789012", "nc-env", newLifecycleCreateReq())
	require.NoError(t, err)

	_, err = b.UpdateEnvironment("nc-env", &mwaa.ExportedUpdateEnvironmentRequest{
		NetworkConfiguration: &mwaa.NetworkConfig{},
	})
	require.Error(t, err)
}
