package amplify_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_BackendEnvironment_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("EnvApp", "", "", "", nil)
	require.NoError(t, err)

	// Create for nonexistent app
	_, err = b.CreateBackendEnvironment("nonexistent", "prod", "stack1", "deploy1")
	require.Error(t, err)

	// Create
	env, err := b.CreateBackendEnvironment(app.AppID, "prod", "MyStack", "MyDeploy")
	require.NoError(t, err)
	assert.Equal(t, "prod", env.EnvironmentName)
	assert.NotEmpty(t, env.BackendEnvironmentARN)

	// Duplicate create
	_, err = b.CreateBackendEnvironment(app.AppID, "prod", "AnotherStack", "AnotherDeploy")
	require.Error(t, err)

	// Get
	got, err := b.GetBackendEnvironment(app.AppID, "prod")
	require.NoError(t, err)
	assert.Equal(t, "prod", got.EnvironmentName)

	// Get nonexistent
	_, err = b.GetBackendEnvironment(app.AppID, "nothere")
	require.Error(t, err)

	// List
	list, _, err := b.ListBackendEnvironments(app.AppID, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	// List for nonexistent app
	_, _, err = b.ListBackendEnvironments("nonexistent", "", "", 0)
	require.Error(t, err)

	// Delete
	deleted, err := b.DeleteBackendEnvironment(app.AppID, "prod")
	require.NoError(t, err)
	assert.Equal(t, "prod", deleted.EnvironmentName)

	// Delete again
	_, err = b.DeleteBackendEnvironment(app.AppID, "prod")
	require.Error(t, err)
}
