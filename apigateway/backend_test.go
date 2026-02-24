package apigateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/apigateway"
)

func TestBackend_CreateAndGetRestApi(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()

	api, err := b.CreateRestAPI("my-api", "desc", map[string]string{"env": "test"})
	require.NoError(t, err)
	assert.NotEmpty(t, api.ID)
	assert.Equal(t, "my-api", api.Name)

	got, err := b.GetRestAPI(api.ID)
	require.NoError(t, err)
	assert.Equal(t, api.ID, got.ID)

	_, err = b.GetRestAPI("nonexistent")
	require.Error(t, err)
}

func TestBackend_GetRestApis(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	_, _ = b.CreateRestAPI("a", "", nil)
	_, _ = b.CreateRestAPI("b", "", nil)

	apis, pos, err := b.GetRestAPIs(0, "")
	require.NoError(t, err)
	assert.Len(t, apis, 2)
	assert.Empty(t, pos)
}

func TestBackend_DeleteRestApi(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI("to-del", "", nil)

	err := b.DeleteRestAPI(api.ID)
	require.NoError(t, err)

	_, err = b.GetRestAPI(api.ID)
	require.Error(t, err)

	err = b.DeleteRestAPI("nonexistent")
	require.Error(t, err)
}

func TestBackend_RootResourceCreatedOnApiCreate(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI("api", "", nil)

	resources, _, err := b.GetResources(api.ID, "", 0)
	require.NoError(t, err)
	assert.Len(t, resources, 1)
	assert.Equal(t, "/", resources[0].Path)
	assert.Empty(t, resources[0].PathPart)
}

func TestBackend_CreateAndGetResource(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI("api", "", nil)

	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	child, err := b.CreateResource(api.ID, rootID, "users")
	require.NoError(t, err)
	assert.Equal(t, "/users", child.Path)
	assert.Equal(t, rootID, child.ParentID)

	got, err := b.GetResource(api.ID, child.ID)
	require.NoError(t, err)
	assert.Equal(t, child.ID, got.ID)
}

func TestBackend_DeleteResource(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI("api", "", nil)
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	child, _ := b.CreateResource(api.ID, rootID, "items")

	err := b.DeleteResource(api.ID, child.ID)
	require.NoError(t, err)

	_, err = b.GetResource(api.ID, child.ID)
	require.Error(t, err)
}

func TestBackend_PutGetDeleteMethod(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI("api", "", nil)
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	m, err := b.PutMethod(api.ID, rootID, "GET", "NONE", false)
	require.NoError(t, err)
	assert.Equal(t, "GET", m.HTTPMethod)

	got, err := b.GetMethod(api.ID, rootID, "GET")
	require.NoError(t, err)
	assert.Equal(t, "NONE", got.AuthorizationType)

	err = b.DeleteMethod(api.ID, rootID, "GET")
	require.NoError(t, err)

	_, err = b.GetMethod(api.ID, rootID, "GET")
	require.Error(t, err)
}

func TestBackend_PutGetDeleteIntegration(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI("api", "", nil)
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	b.PutMethod(api.ID, rootID, "POST", "NONE", false)

	input := apigateway.PutIntegrationInput{Type: "MOCK"}
	integ, err := b.PutIntegration(api.ID, rootID, "POST", input)
	require.NoError(t, err)
	assert.Equal(t, "MOCK", integ.Type)

	got, err := b.GetIntegration(api.ID, rootID, "POST")
	require.NoError(t, err)
	assert.Equal(t, "MOCK", got.Type)

	err = b.DeleteIntegration(api.ID, rootID, "POST")
	require.NoError(t, err)

	_, err = b.GetIntegration(api.ID, rootID, "POST")
	require.Error(t, err)
}

func TestBackend_CreateDeploymentAndStage(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI("api", "", nil)

	depl, err := b.CreateDeployment(api.ID, "prod", "initial")
	require.NoError(t, err)
	assert.NotEmpty(t, depl.ID)

	depls, err := b.GetDeployments(api.ID)
	require.NoError(t, err)
	assert.Len(t, depls, 1)

	stages, err := b.GetStages(api.ID)
	require.NoError(t, err)
	assert.Len(t, stages, 1)
	assert.Equal(t, "prod", stages[0].StageName)
}

func TestBackend_GetAndDeleteStage(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI("api", "", nil)
	b.CreateDeployment(api.ID, "v1", "")

	stage, err := b.GetStage(api.ID, "v1")
	require.NoError(t, err)
	assert.Equal(t, "v1", stage.StageName)

	err = b.DeleteStage(api.ID, "v1")
	require.NoError(t, err)

	_, err = b.GetStage(api.ID, "v1")
	require.Error(t, err)
}

func TestBackend_InvalidParams(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()

	_, err := b.CreateRestAPI("", "", nil)
	require.Error(t, err)

	api, _ := b.CreateRestAPI("api", "", nil)
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, err = b.CreateResource(api.ID, rootID, "")
	require.Error(t, err)
}
