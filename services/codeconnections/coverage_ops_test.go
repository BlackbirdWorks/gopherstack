package codeconnections_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

func newCCHandler() *codeconnections.Handler {
	be := codeconnections.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return codeconnections.NewHandler(be)
}

func createCCConnection(t *testing.T, h *codeconnections.Handler) string {
	t.Helper()

	rec := doJSON(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "test-conn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	arn, _ := resp["ConnectionArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

func TestCC_ListHosts(t *testing.T) {
	t.Parallel()

	h := newCCHandler()
	rec := doJSON(t, h, "ListHosts", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	assert.NotNil(t, resp["Hosts"])
}

func TestCC_RepositoryLink_ListUpdate(t *testing.T) {
	t.Parallel()

	h := newCCHandler()
	connArn := createCCConnection(t, h)
	linkID := createRepositoryLink(t, h, connArn, "my-owner", "my-repo")

	// ListRepositoryLinks
	rec := doJSON(t, h, "ListRepositoryLinks", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	links, _ := resp["RepositoryLinks"].([]any)
	assert.NotEmpty(t, links)

	// UpdateRepositoryLink
	rec = doJSON(t, h, "UpdateRepositoryLink", map[string]any{
		"RepositoryLinkId": linkID,
		"ConnectionArn":    connArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCC_SyncConfiguration_CRUD(t *testing.T) {
	t.Parallel()

	h := newCCHandler()
	connArn := createCCConnection(t, h)
	linkID := createRepositoryLink(t, h, connArn, "owner", "repo")

	// CreateSyncConfiguration
	rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":              "main",
		"ConfigFile":          ".aws/sync.yaml",
		"RepositoryLinkId":    linkID,
		"ResourceName":        "my-resource",
		"RoleArn":             "arn:aws:iam::123456789012:role/SyncRole",
		"SyncType":            "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetSyncConfiguration
	rec = doJSON(t, h, "GetSyncConfiguration", map[string]any{
		"ResourceName": "my-resource",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListSyncConfigurations
	rec = doJSON(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
		"SyncType":         "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateSyncConfiguration
	rec = doJSON(t, h, "UpdateSyncConfiguration", map[string]any{
		"ResourceName": "my-resource",
		"SyncType":     "CFN_STACK_SYNC",
		"Branch":       "dev",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetSyncBlockerSummary
	rec = doJSON(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "my-resource",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListRepositorySyncDefinitions
	rec = doJSON(t, h, "ListRepositorySyncDefinitions", map[string]any{
		"RepositoryLinkId": linkID,
		"SyncType":         "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteSyncConfiguration
	rec = doJSON(t, h, "DeleteSyncConfiguration", map[string]any{
		"ResourceName": "my-resource",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
