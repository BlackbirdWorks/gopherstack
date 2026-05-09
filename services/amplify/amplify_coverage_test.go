package amplify_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Provider tests ----

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &amplify.Provider{}
	assert.Equal(t, "Amplify", p.Name())
}

func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &amplify.Provider{}
	reg, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &amplify.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

// ---- Handler chaos/region tests ----

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	ops := h.ChaosOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateApp")
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	regions := h.ChaosRegions()
	assert.NotEmpty(t, regions)
}

// ---- DomainAssociation backend tests ----

func TestInMemoryBackend_DomainAssociation_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("TestApp", "", "", "", nil)
	require.NoError(t, err)

	subs := []amplify.SubDomainSetting{
		{Prefix: "www", BranchName: "main"},
	}

	// Create
	da, err := b.CreateDomainAssociation(app.AppID, "example.com", subs, true)
	require.NoError(t, err)
	assert.Equal(t, "example.com", da.DomainName)
	assert.Equal(t, app.AppID, da.AppID)
	assert.Len(t, da.SubDomains, 1)
	assert.NotEmpty(t, da.ARN)

	// Duplicate create
	_, err = b.CreateDomainAssociation(app.AppID, "example.com", subs, false)
	require.Error(t, err)

	// Create for nonexistent app
	_, err = b.CreateDomainAssociation("nonexistent", "example.com", subs, false)
	require.Error(t, err)

	// Get
	got, err := b.GetDomainAssociation(app.AppID, "example.com")
	require.NoError(t, err)
	assert.Equal(t, "example.com", got.DomainName)

	// Get nonexistent
	_, err = b.GetDomainAssociation(app.AppID, "nothere.com")
	require.Error(t, err)

	// List
	list, _, err := b.ListDomainAssociations(app.AppID, "", 0)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	// List for nonexistent app
	_, _, err = b.ListDomainAssociations("nonexistent", "", 0)
	require.Error(t, err)

	// Update
	newSubs := []amplify.SubDomainSetting{
		{Prefix: "api", BranchName: "main"},
	}
	updated, err := b.UpdateDomainAssociation(app.AppID, "example.com", newSubs, false)
	require.NoError(t, err)
	assert.Len(t, updated.SubDomains, 1)
	assert.Equal(t, "api", updated.SubDomains[0].SubDomainSetting.Prefix)

	// Update nonexistent
	_, err = b.UpdateDomainAssociation(app.AppID, "nothere.com", newSubs, false)
	require.Error(t, err)

	// Delete
	deleted, err := b.DeleteDomainAssociation(app.AppID, "example.com")
	require.NoError(t, err)
	assert.Equal(t, "example.com", deleted.DomainName)

	// Delete again
	_, err = b.DeleteDomainAssociation(app.AppID, "example.com")
	require.Error(t, err)
}

// ---- Webhook backend tests ----

func TestInMemoryBackend_Webhook_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("WebhookApp", "", "", "", nil)
	require.NoError(t, err)
	_, err = b.CreateBranch(app.AppID, "main", "", "", false, nil)
	require.NoError(t, err)

	// Create webhook for nonexistent app
	_, err = b.CreateWebhook("nonexistent", "main", "test")
	require.Error(t, err)

	// Create webhook
	wh, err := b.CreateWebhook(app.AppID, "main", "my webhook")
	require.NoError(t, err)
	assert.NotEmpty(t, wh.WebhookID)
	assert.Equal(t, "main", wh.BranchName)
	assert.Equal(t, "my webhook", wh.Description)

	// Get webhook
	got, err := b.GetWebhook(wh.WebhookID)
	require.NoError(t, err)
	assert.Equal(t, wh.WebhookID, got.WebhookID)

	// Get nonexistent
	_, err = b.GetWebhook("doesnotexist")
	require.Error(t, err)

	// List webhooks
	list, _, err := b.ListWebhooks(app.AppID, "", 0)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	// List for nonexistent app
	_, _, err = b.ListWebhooks("nonexistent", "", 0)
	require.Error(t, err)

	// Update webhook
	updated, err := b.UpdateWebhook(wh.WebhookID, "main", "updated desc")
	require.NoError(t, err)
	assert.Equal(t, "updated desc", updated.Description)

	// Update nonexistent
	_, err = b.UpdateWebhook("nonexistent", "main", "desc")
	require.Error(t, err)

	// Delete webhook
	deleted, err := b.DeleteWebhook(wh.WebhookID)
	require.NoError(t, err)
	assert.Equal(t, wh.WebhookID, deleted.WebhookID)

	// Delete again
	_, err = b.DeleteWebhook(wh.WebhookID)
	require.Error(t, err)
}

// ---- BackendEnvironment tests ----

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
	list, _, err := b.ListBackendEnvironments(app.AppID, "", 0)
	require.NoError(t, err)
	assert.Len(t, list, 1)

	// List for nonexistent app
	_, _, err = b.ListBackendEnvironments("nonexistent", "", 0)
	require.Error(t, err)

	// Delete
	deleted, err := b.DeleteBackendEnvironment(app.AppID, "prod")
	require.NoError(t, err)
	assert.Equal(t, "prod", deleted.EnvironmentName)

	// Delete again
	_, err = b.DeleteBackendEnvironment(app.AppID, "prod")
	require.Error(t, err)
}

// ---- Job/Deployment tests ----

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

// ---- GenerateAccessLogs / GetArtifactURL / ListArtifacts tests ----

func TestInMemoryBackend_AccessLogsAndArtifacts(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("ArtifactApp", "", "", "", nil)
	require.NoError(t, err)

	// GenerateAccessLogs for nonexistent app
	_, err = b.GenerateAccessLogs("nonexistent", "", "", "")
	require.Error(t, err)

	// GenerateAccessLogs success
	url, err := b.GenerateAccessLogs(app.AppID, "", "", "")
	require.NoError(t, err)
	assert.NotEmpty(t, url)

	// GetArtifactURL for nonexistent artifact
	_, _, err = b.GetArtifactURL("nonexistent-artifact")
	require.Error(t, err)

	// ListArtifacts
	artifacts, _, err := b.ListArtifacts(app.AppID, "main", "job1", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, artifacts)
}

// ---- Handler additional routing tests ----

func TestHandler_GetAndDeleteApp(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// Get nonexistent app
	rec := doRequest(t, h, http.MethodGet, "/amplify/v1/apps/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Delete nonexistent app
	rec = doRequest(t, h, http.MethodDelete, "/amplify/v1/apps/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_Branch_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// Get branch on nonexistent app
	rec := doRequest(t, h, http.MethodGet, "/amplify/v1/apps/nonexistent/branches/main", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// List branches for nonexistent app
	rec = doRequest(t, h, http.MethodGet, "/amplify/v1/apps/nonexistent/branches", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
