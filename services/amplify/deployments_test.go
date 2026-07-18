package amplify_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_Deployment_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("DeployApp", "", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateBranch(app.AppID, "main", "", "", false, nil)
	require.NoError(t, err)

	// CreateDeployment for nonexistent app
	_, _, err = b.CreateDeployment("nonexistent", "main")
	require.Error(t, err)

	// CreateDeployment for nonexistent branch
	_, _, err = b.CreateDeployment(app.AppID, "nonexistent-branch")
	require.Error(t, err)

	// CreateDeployment success
	jobID, uploadURL, err := b.CreateDeployment(app.AppID, "main")
	require.NoError(t, err)
	assert.NotEmpty(t, jobID)
	assert.NotEmpty(t, uploadURL)

	// StartDeployment for nonexistent app
	_, err = b.StartDeployment("nonexistent", "main", jobID, "")
	require.Error(t, err)

	// StartDeployment for nonexistent branch
	_, err = b.StartDeployment(app.AppID, "nonexistent", jobID, "")
	require.Error(t, err)

	// StartDeployment success with explicit jobID
	job, err := b.StartDeployment(app.AppID, "main", jobID, "https://example.com/artifact.zip")
	require.NoError(t, err)
	assert.Equal(t, jobID, job.JobID)

	// StartDeployment success with auto-generated jobID
	job2, err := b.StartDeployment(app.AppID, "main", "", "")
	require.NoError(t, err)
	assert.NotEmpty(t, job2.JobID)
}
