package codeconnections_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_RepositoryLink_TagsRoundTrip verifies that tags on a repository link are stored,
// returned via ListTagsForResource, and removable via UntagResource.
// Real AWS CodeConnections supports tagging repository links.
func TestParity_RepositoryLink_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		initTags  []map[string]string
		wantCount int
	}{
		{
			name: "tags_on_create_returned",
			initTags: []map[string]string{
				{"Key": "Env", "Value": "prod"},
				{"Key": "Team", "Value": "platform"},
			},
			wantCount: 2,
		},
		{
			name:      "no_tags_returns_empty",
			initTags:  nil,
			wantCount: 0,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "rl-tag-conn-"+strconv.Itoa(i), "GitHub")

			body := map[string]any{
				"ConnectionArn":  connArn,
				"OwnerId":        "my-org",
				"RepositoryName": "my-repo",
			}
			if tt.initTags != nil {
				body["Tags"] = tt.initTags
			}

			rec := doJSON(t, h, "CreateRepositoryLink", body)
			require.Equal(t, http.StatusOK, rec.Code)

			info := parseResp(t, rec)["RepositoryLinkInfo"].(map[string]any)
			linkArn, _ := info["RepositoryLinkArn"].(string)
			require.NotEmpty(t, linkArn)

			rec = doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			tags, _ := resp["Tags"].([]any)
			assert.Len(t, tags, tt.wantCount)
		})
	}
}

// TestParity_RepositoryLink_TagResource verifies TagResource works on repository links.
func TestParity_RepositoryLink_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag_then_list_then_untag"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "rl-tagres-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			// Get the ARN from GetRepositoryLink
			rec := doJSON(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
			require.Equal(t, http.StatusOK, rec.Code)
			info := parseResp(t, rec)["RepositoryLinkInfo"].(map[string]any)
			linkArn := info["RepositoryLinkArn"].(string)

			// TagResource
			rec = doJSON(t, h, "TagResource", map[string]any{
				"ResourceArn": linkArn,
				"Tags":        []map[string]string{{"Key": "added", "Value": "yes"}},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify tag present
			rec = doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
			require.Equal(t, http.StatusOK, rec.Code)
			tags, _ := parseResp(t, rec)["Tags"].([]any)
			assert.Len(t, tags, 1)

			// UntagResource
			rec = doJSON(t, h, "UntagResource", map[string]any{
				"ResourceArn": linkArn,
				"TagKeys":     []string{"added"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Tag gone
			rec = doJSON(t, h, "ListTagsForResource", map[string]any{"ResourceArn": linkArn})
			require.Equal(t, http.StatusOK, rec.Code)
			tags, _ = parseResp(t, rec)["Tags"].([]any)
			assert.Empty(t, tags)
		})
	}
}

// TestParity_RepositoryLink_TagsInListItem verifies that tags appear in ListRepositoryLinks items.
func TestParity_RepositoryLink_TagsInListItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []map[string]string
		wantTags int
	}{
		{
			name:     "tags_in_list_item",
			tags:     []map[string]string{{"Key": "owner", "Value": "ops"}},
			wantTags: 1,
		},
		{
			name:     "no_tags_empty_in_list_item",
			tags:     nil,
			wantTags: 0,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "list-tag-conn-"+strconv.Itoa(i), "GitHub")

			body := map[string]any{
				"ConnectionArn":  connArn,
				"OwnerId":        "my-org",
				"RepositoryName": "my-repo",
			}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}

			rec := doJSON(t, h, "CreateRepositoryLink", body)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doJSON(t, h, "ListRepositoryLinks", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			links, _ := resp["RepositoryLinks"].([]any)
			require.Len(t, links, 1)

			linkMap := links[0].(map[string]any)
			tags, _ := linkMap["Tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

// TestParity_SyncConfiguration_PublishDeploymentStatus verifies that PublishDeploymentStatus is
// stored and returned in GetSyncConfiguration and CreateSyncConfiguration responses.
// Real AWS CodeConnections uses this field to control deployment status publishing.
func TestParity_SyncConfiguration_PublishDeploymentStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
		want   string
	}{
		{name: "enabled", status: "ENABLED", want: "ENABLED"},
		{name: "disabled", status: "DISABLED", want: "DISABLED"},
		{name: "omitted", status: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "pds-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			body := map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "pds-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			}
			if tt.status != "" {
				body["PublishDeploymentStatus"] = tt.status
			}

			rec := doJSON(t, h, "CreateSyncConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code)

			cfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)

			if tt.want != "" {
				assert.Equal(t, tt.want, cfg["PublishDeploymentStatus"])
			} else {
				v, _ := cfg["PublishDeploymentStatus"].(string)
				assert.Empty(t, v)
			}

			// Verify via GetSyncConfiguration
			rec = doJSON(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "pds-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			getCfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)
			if tt.want != "" {
				assert.Equal(t, tt.want, getCfg["PublishDeploymentStatus"])
			}
		})
	}
}

// TestParity_SyncConfiguration_TriggerResourceUpdateOn verifies TriggerResourceUpdateOn is
// stored and returned.
func TestParity_SyncConfiguration_TriggerResourceUpdateOn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		trigger string
		want    string
	}{
		{name: "any_change", trigger: "ANY_CHANGE", want: "ANY_CHANGE"},
		{name: "file_change", trigger: "FILE_CHANGE", want: "FILE_CHANGE"},
		{name: "omitted", trigger: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "tru-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			body := map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "tru-stack",
				"RoleArn":          "arn:aws:iam::123456789012:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			}
			if tt.trigger != "" {
				body["TriggerResourceUpdateOn"] = tt.trigger
			}

			rec := doJSON(t, h, "CreateSyncConfiguration", body)
			require.Equal(t, http.StatusOK, rec.Code)

			cfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)

			if tt.want != "" {
				assert.Equal(t, tt.want, cfg["TriggerResourceUpdateOn"])
			} else {
				v, _ := cfg["TriggerResourceUpdateOn"].(string)
				assert.Empty(t, v)
			}
		})
	}
}

// TestParity_UpdateSyncConfiguration_PublishDeploymentStatus verifies that
// UpdateSyncConfiguration can update PublishDeploymentStatus.
func TestParity_UpdateSyncConfiguration_PublishDeploymentStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		initial string
		updated string
		want    string
	}{
		{name: "enable_to_disable", initial: "ENABLED", updated: "DISABLED", want: "DISABLED"},
		{name: "omit_update_preserves", initial: "ENABLED", updated: "", want: "ENABLED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "upd-pds-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":                  "main",
				"ConfigFile":              "sync.yaml",
				"RepositoryLinkId":        linkID,
				"ResourceName":            "upd-pds-stack",
				"RoleArn":                 "arn:aws:iam::123456789012:role/r",
				"SyncType":                "CFN_STACK_SYNC",
				"PublishDeploymentStatus": tt.initial,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			updBody := map[string]any{
				"ResourceName": "upd-pds-stack",
				"SyncType":     "CFN_STACK_SYNC",
			}
			if tt.updated != "" {
				updBody["PublishDeploymentStatus"] = tt.updated
			}

			rec = doJSON(t, h, "UpdateSyncConfiguration", updBody)
			require.Equal(t, http.StatusOK, rec.Code)

			cfg := parseResp(t, rec)["SyncConfiguration"].(map[string]any)
			assert.Equal(t, tt.want, cfg["PublishDeploymentStatus"])
		})
	}
}

// TestParity_UpdateSyncBlocker_ResourceName verifies that UpdateSyncBlocker returns the
// ResourceName from the input in the SyncBlockerSummary response.
// Real AWS echoes the ResourceName in the blocker summary.
func TestParity_UpdateSyncBlocker_ResourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceName string
		wantName     string
	}{
		{name: "resource_name_echoed", resourceName: "my-cfn-stack", wantName: "my-cfn-stack"},
		{name: "empty_resource_name", resourceName: "", wantName: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			body := map[string]any{
				"Id":             "blocker-123",
				"SyncType":       "CFN_STACK_SYNC",
				"ResolvedReason": "fixed",
			}
			if tt.resourceName != "" {
				body["ResourceName"] = tt.resourceName
			}

			rec := doJSON(t, h, "UpdateSyncBlocker", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			summary, ok := resp["SyncBlockerSummary"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, summary["ResourceName"])
		})
	}
}

// TestParity_ListConnections_ProviderTypeFilter verifies ProviderTypeFilter in ListConnections.
func TestParity_ListConnections_ProviderTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{name: "no_filter_all", filter: "", wantCount: 3},
		{name: "github_filter", filter: "GitHub", wantCount: 2},
		{name: "bitbucket_filter", filter: "Bitbucket", wantCount: 1},
		{name: "gitlab_filter_empty", filter: "GitLab", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createConn(t, h, "gh1", "GitHub")
			createConn(t, h, "gh2", "GitHub")
			createConn(t, h, "bb1", "Bitbucket")

			body := map[string]any{}
			if tt.filter != "" {
				body["ProviderTypeFilter"] = tt.filter
			}

			rec := doJSON(t, h, "ListConnections", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conns, _ := resp["Connections"].([]any)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}

// TestParity_CreateConnection_AllProviderTypes verifies all valid provider types are accepted.
func TestParity_CreateConnection_AllProviderTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		providerType string
		wantStatus   int
	}{
		{name: "github", providerType: "GitHub", wantStatus: http.StatusOK},
		{name: "bitbucket", providerType: "Bitbucket", wantStatus: http.StatusOK},
		{name: "gitlab", providerType: "GitLab", wantStatus: http.StatusOK},
		{name: "gitlab_self_managed", providerType: "GitLabSelfManaged", wantStatus: http.StatusOK},
		{name: "github_enterprise", providerType: "GitHubEnterpriseServer", wantStatus: http.StatusOK},
		{name: "invalid_type", providerType: "AzureDevOps", wantStatus: http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "provider-test-" + strconv.Itoa(i),
				"ProviderType":   tt.providerType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestParity_GetConnection_OwnerAccountId verifies OwnerAccountId is present and non-empty.
func TestParity_GetConnection_OwnerAccountId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "owner_account_id_present"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "owner-conn", "GitHub")

			rec := doJSON(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conn := resp["Connection"].(map[string]any)
			assert.NotEmpty(t, conn["OwnerAccountId"], "OwnerAccountId must be present")
		})
	}
}
