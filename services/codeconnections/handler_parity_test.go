package codeconnections_test

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// --- CreateConnection parity ---

// TestParity_CreateConnection_HostArn verifies HostArn is accepted and round-tripped.
func TestParity_CreateConnection_HostArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantArn    bool
	}{
		{
			name: "with_host_arn",
			body: map[string]any{
				"ConnectionName": "ghe-conn",
				"ProviderType":   "GitHubEnterpriseServer",
				"HostArn":        "arn:aws:codeconnections:us-east-1:123456789012:host/abc",
			},
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
		{
			name: "without_host_arn",
			body: map[string]any{
				"ConnectionName": "gh-conn",
				"ProviderType":   "GitHub",
			},
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doJSON(t, h, "CreateConnection", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantArn {
				resp := parseResp(t, rec)
				assert.NotEmpty(t, resp["ConnectionArn"])
			}
		})
	}
}

// TestParity_CreateConnection_ReturnsTags verifies Tags are returned in CreateConnection response.
func TestParity_CreateConnection_ReturnsTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inputTags []map[string]string
		wantCount int
	}{
		{
			name: "tags_returned",
			inputTags: []map[string]string{
				{"Key": "Env", "Value": "prod"},
				{"Key": "Owner", "Value": "platform"},
			},
			wantCount: 2,
		},
		{
			name:      "no_tags_omitted",
			inputTags: nil,
			wantCount: 0,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{
				"ConnectionName": "tag-conn-" + strconv.Itoa(i),
				"ProviderType":   "GitHub",
			}

			if tt.inputTags != nil {
				body["Tags"] = tt.inputTags
			}

			rec := doJSON(t, h, "CreateConnection", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			if tt.wantCount > 0 {
				tags, ok := resp["Tags"].([]any)
				require.True(t, ok, "Tags field should be present")
				assert.Len(t, tags, tt.wantCount)
			} else {
				// nil or missing Tags is acceptable for zero-tag case
				if tags, ok := resp["Tags"].([]any); ok {
					assert.Empty(t, tags)
				}
			}
		})
	}
}

// TestParity_CreateConnection_TagsSortedInResponse verifies tags are alphabetically sorted.
func TestParity_CreateConnection_TagsSortedInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inputTags []map[string]string
		wantKeys  []string
	}{
		{
			name: "sorted_alphabetically",
			inputTags: []map[string]string{
				{"Key": "Zebra", "Value": "z"},
				{"Key": "Alpha", "Value": "a"},
				{"Key": "Mango", "Value": "m"},
			},
			wantKeys: []string{"Alpha", "Mango", "Zebra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "sorted-conn",
				"ProviderType":   "GitHub",
				"Tags":           tt.inputTags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			require.Len(t, tags, len(tt.wantKeys))

			for i, wantKey := range tt.wantKeys {
				tagMap, isMap := tags[i].(map[string]any)
				require.True(t, isMap)
				assert.Equal(t, wantKey, tagMap["Key"])
			}
		})
	}
}

// --- GetConnection parity ---

// TestParity_GetConnection_HostArnPreserved verifies HostArn is stored and returned.
func TestParity_GetConnection_HostArnPreserved(t *testing.T) {
	t.Parallel()

	const hostArn = "arn:aws:codeconnections:us-east-1:123456789012:host/myhost"

	tests := []struct {
		name        string
		wantHostArn string
	}{
		{name: "host_arn_preserved", wantHostArn: hostArn},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "ghe-conn",
				"ProviderType":   "GitHubEnterpriseServer",
				"HostArn":        hostArn,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			connArn := parseResp(t, rec)["ConnectionArn"].(string)

			getRec := doJSON(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
			require.Equal(t, http.StatusOK, getRec.Code)

			resp := parseResp(t, getRec)
			conn, ok := resp["Connection"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantHostArn, conn["HostArn"])
		})
	}
}

// --- GetHost parity ---

// TestParity_GetHost_IncludesHostArn verifies HostArn is returned in GetHost response.
func TestParity_GetHost_IncludesHostArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		hostName      string
		providerType  string
		endpoint      string
		wantFieldsSet bool
	}{
		{
			name:          "host_arn_in_response",
			hostName:      "my-ghe-host",
			providerType:  "GitHubEnterpriseServer",
			endpoint:      "https://ghe.example.com",
			wantFieldsSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn := createHost(t, h, tt.hostName, tt.providerType, tt.endpoint)

			rec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			if tt.wantFieldsSet {
				assert.Equal(t, hostArn, resp["HostArn"])
				assert.Equal(t, tt.hostName, resp["Name"])
				assert.Equal(t, tt.endpoint, resp["ProviderEndpoint"])
				assert.Equal(t, tt.providerType, resp["ProviderType"])
				assert.Equal(t, "AVAILABLE", resp["Status"])
			}
		})
	}
}

// --- Host uniqueness parity ---

// TestParity_CreateHost_NameUniqueness verifies duplicate host names are rejected.
func TestParity_CreateHost_NameUniqueness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		hostName      string
		wantDuplicate bool
		wantStatus    int
	}{
		{
			name:       "unique_name_succeeds",
			hostName:   "unique-host",
			wantStatus: http.StatusOK,
		},
		{
			name:          "duplicate_name_rejected",
			hostName:      "dup-host",
			wantDuplicate: true,
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.wantDuplicate {
				createHost(t, h, tt.hostName, "GitHubEnterpriseServer", "https://a.example.com")
			}

			rec := doJSON(t, h, "CreateHost", map[string]any{
				"Name":             tt.hostName,
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://b.example.com",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_DeleteHost_CleansNameIndex verifies host name can be reused after delete.
func TestParity_DeleteHost_CleansNameIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "can_recreate_after_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn := createHost(
				t,
				h,
				"recycled-host",
				"GitHubEnterpriseServer",
				"https://a.example.com",
			)

			delRec := doJSON(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
			require.Equal(t, http.StatusOK, delRec.Code)

			rec := doJSON(t, h, "CreateHost", map[string]any{
				"Name":             "recycled-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://b.example.com",
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// --- ListHosts parity ---

// TestParity_ListHosts_IncludesTags verifies Tags are included in ListHosts output items.
func TestParity_ListHosts_IncludesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []map[string]string
		wantTags int
	}{
		{
			name: "tags_in_list_item",
			tags: []map[string]string{
				{"Key": "Env", "Value": "staging"},
				{"Key": "Owner", "Value": "ops"},
			},
			wantTags: 2,
		},
		{
			name:     "no_tags_list_item",
			tags:     nil,
			wantTags: 0,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{
				"Name":             "tagged-host-" + strconv.Itoa(i),
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://ghe.example.com",
			}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}

			rec := doJSON(t, h, "CreateHost", body)
			require.Equal(t, http.StatusOK, rec.Code)

			listRec := doJSON(t, h, "ListHosts", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			resp := parseResp(t, listRec)
			hosts, ok := resp["Hosts"].([]any)
			require.True(t, ok)
			require.Len(t, hosts, 1)

			hostMap, isMap := hosts[0].(map[string]any)
			require.True(t, isMap)

			tags, _ := hostMap["Tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

// TestParity_ListHosts_Pagination verifies MaxResults and NextToken work for ListHosts.
func TestParity_ListHosts_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		hostCount  int
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:       "first_page",
			hostCount:  3,
			maxResults: 2,
			wantCount:  2,
			wantToken:  true,
		},
		{
			name:       "all_results",
			hostCount:  2,
			maxResults: 10,
			wantCount:  2,
			wantToken:  false,
		},
		{
			name:       "zero_max_defaults",
			hostCount:  2,
			maxResults: 0,
			wantCount:  2,
			wantToken:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			for i := range tt.hostCount {
				rec := doJSON(t, h, "CreateHost", map[string]any{
					"Name":             "phost-" + strconv.Itoa(i),
					"ProviderType":     "GitHubEnterpriseServer",
					"ProviderEndpoint": "https://ghe" + strconv.Itoa(i) + ".example.com",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.maxResults > 0 {
				body["MaxResults"] = tt.maxResults
			}

			rec := doJSON(t, h, "ListHosts", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			hosts, ok := resp["Hosts"].([]any)
			require.True(t, ok)
			assert.Len(t, hosts, tt.wantCount)

			_, hasToken := resp["NextToken"]
			assert.Equal(t, tt.wantToken, hasToken)
		})
	}
}

// TestParity_ListHosts_Continuation verifies two-page traversal for ListHosts.
func TestParity_ListHosts_Continuation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for i := range 3 {
		rec := doJSON(t, h, "CreateHost", map[string]any{
			"Name":             "cont-host-" + strconv.Itoa(i),
			"ProviderType":     "GitHubEnterpriseServer",
			"ProviderEndpoint": "https://ghe" + strconv.Itoa(i) + ".example.com",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doJSON(t, h, "ListHosts", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResp(t, rec1)
	page1, ok := resp1["Hosts"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	nextToken, hasToken := resp1["NextToken"].(string)
	require.True(t, hasToken)
	require.NotEmpty(t, nextToken)

	rec2 := doJSON(t, h, "ListHosts", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	page2, ok := resp2["Hosts"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)

	_, stillHasToken := resp2["NextToken"]
	assert.False(t, stillHasToken)

	names := make([]string, 0, 3)
	for _, item := range append(page1, page2...) {
		hMap := item.(map[string]any)
		names = append(names, hMap["Name"].(string))
	}
	assert.ElementsMatch(t, []string{"cont-host-0", "cont-host-1", "cont-host-2"}, names)
}

// --- ListRepositoryLinks parity ---

// TestParity_ListRepositoryLinks_Pagination verifies MaxResults/NextToken for ListRepositoryLinks.
func TestParity_ListRepositoryLinks_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		linkCount  int
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:       "first_page",
			linkCount:  3,
			maxResults: 2,
			wantCount:  2,
			wantToken:  true,
		},
		{
			name:       "all_results",
			linkCount:  2,
			maxResults: 10,
			wantCount:  2,
			wantToken:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "link-conn", "GitHub")

			for i := range tt.linkCount {
				rec := doJSON(t, h, "CreateRepositoryLink", map[string]any{
					"ConnectionArn":  connArn,
					"OwnerId":        "my-org",
					"RepositoryName": "repo-" + strconv.Itoa(i),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.maxResults > 0 {
				body["MaxResults"] = tt.maxResults
			}

			rec := doJSON(t, h, "ListRepositoryLinks", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			links, ok := resp["RepositoryLinks"].([]any)
			require.True(t, ok)
			assert.Len(t, links, tt.wantCount)

			_, hasToken := resp["NextToken"]
			assert.Equal(t, tt.wantToken, hasToken)
		})
	}
}

// --- ListSyncConfigurations parity ---

// TestParity_ListSyncConfigurations_Pagination verifies MaxResults/NextToken pagination.
func TestParity_ListSyncConfigurations_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfgCount   int
		maxResults int
		wantCount  int
		wantToken  bool
	}{
		{
			name:       "first_page",
			cfgCount:   3,
			maxResults: 2,
			wantCount:  2,
			wantToken:  true,
		},
		{
			name:       "all_results",
			cfgCount:   2,
			maxResults: 10,
			wantCount:  2,
			wantToken:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			connArn := createConn(t, h, "sync-conn", "GitHub")
			linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			for i := range tt.cfgCount {
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "config.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "resource-" + strconv.Itoa(i),
					"RoleArn":          "arn:aws:iam::123456789012:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{
				"RepositoryLinkId": linkID,
			}
			if tt.maxResults > 0 {
				body["MaxResults"] = tt.maxResults
			}

			rec := doJSON(t, h, "ListSyncConfigurations", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			cfgs, ok := resp["SyncConfigurations"].([]any)
			require.True(t, ok)
			assert.Len(t, cfgs, tt.wantCount)

			_, hasToken := resp["NextToken"]
			assert.Equal(t, tt.wantToken, hasToken)
		})
	}
}

// --- UpdateHost parity ---

// TestParity_UpdateHost exercises UpdateHost happy path and error cases.
func TestParity_UpdateHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupHostArn func(t *testing.T, h *codeconnections.Handler) string
		name         string
		newEndpoint  string
		wantStatus   int
	}{
		{
			name: "success_updates_endpoint",
			setupHostArn: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createHost(
					t,
					h,
					"updateable-host",
					"GitHubEnterpriseServer",
					"https://old.example.com",
				)
			},
			newEndpoint: "https://new.example.com",
			wantStatus:  http.StatusOK,
		},
		{
			name: "not_found",
			setupHostArn: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:host/nonexistent"
			},
			newEndpoint: "https://new.example.com",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name: "missing_arn",
			setupHostArn: func(_ *testing.T, _ *codeconnections.Handler) string {
				return ""
			},
			newEndpoint: "https://new.example.com",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn := tt.setupHostArn(t, h)

			rec := doJSON(t, h, "UpdateHost", map[string]any{
				"HostArn":          hostArn,
				"ProviderEndpoint": tt.newEndpoint,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				getRec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
				require.Equal(t, http.StatusOK, getRec.Code)
				resp := parseResp(t, getRec)
				assert.Equal(t, tt.newEndpoint, resp["ProviderEndpoint"])
			}
		})
	}
}

// --- UpdateRepositoryLink parity ---

// TestParity_UpdateRepositoryLink exercises UpdateRepositoryLink operations.
func TestParity_UpdateRepositoryLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupLinkID func(t *testing.T, h *codeconnections.Handler) string
		name        string
		newConnArn  string
		wantStatus  int
		wantNewConn bool
	}{
		{
			name: "success_updates_connection",
			setupLinkID: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()
				connArn := createConn(t, h, "original-conn", "GitHub")

				return createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			},
			newConnArn:  "arn:aws:codeconnections:us-east-1:123:connection/new-conn",
			wantStatus:  http.StatusOK,
			wantNewConn: true,
		},
		{
			name: "not_found",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "nonexistent-link-id"
			},
			newConnArn: "arn:aws:codeconnections:us-east-1:123:connection/new",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_repository_link_id",
			setupLinkID: func(_ *testing.T, _ *codeconnections.Handler) string {
				return ""
			},
			newConnArn: "arn:aws:codeconnections:us-east-1:123:connection/new",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := tt.setupLinkID(t, h)

			rec := doJSON(t, h, "UpdateRepositoryLink", map[string]any{
				"RepositoryLinkId": linkID,
				"ConnectionArn":    tt.newConnArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantNewConn {
				resp := parseResp(t, rec)
				info, ok := resp["RepositoryLinkInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.newConnArn, info["ConnectionArn"])
			}
		})
	}
}

// --- GetSyncConfiguration parity ---

// TestParity_GetSyncConfiguration exercises the GetSyncConfiguration handler.
func TestParity_GetSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantBranch string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
			wantBranch: "feature-branch",
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "cfg-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "feature-branch",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "get-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doJSON(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "get-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				cfg, ok := resp["SyncConfiguration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantBranch, cfg["Branch"])
				assert.Equal(t, "my-org", cfg["OwnerId"])
				assert.Equal(t, "my-repo", cfg["RepositoryName"])
				assert.Equal(t, "GitHub", cfg["ProviderType"])
			}
		})
	}
}

// --- UpdateSyncConfiguration parity ---

// TestParity_UpdateSyncConfiguration exercises UpdateSyncConfiguration.
func TestParity_UpdateSyncConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		newBranch  string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success_updates_branch",
			preCreate:  true,
			newBranch:  "release",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			newBranch:  "release",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "upd-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "upd-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doJSON(t, h, "UpdateSyncConfiguration", map[string]any{
				"ResourceName": "upd-stack",
				"SyncType":     "CFN_STACK_SYNC",
				"Branch":       tt.newBranch,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				cfg, ok := resp["SyncConfiguration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.newBranch, cfg["Branch"])
			}
		})
	}
}

// --- GetSyncBlockerSummary parity ---

// TestParity_GetSyncBlockerSummary exercises the GetSyncBlockerSummary handler.
func TestParity_GetSyncBlockerSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success_returns_summary",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_resource_name",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.preCreate {
				connArn := createConn(t, h, "blocker-conn", "GitHub")
				linkID := createRepositoryLink(t, h, connArn, "my-org", "my-repo")
				rec := doJSON(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "blocker-stack",
					"RoleArn":          "arn:aws:iam::123456789012:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{
				"SyncType": "CFN_STACK_SYNC",
			}
			if tt.name != "missing_resource_name" {
				body["ResourceName"] = "blocker-stack"
			}

			rec := doJSON(t, h, "GetSyncBlockerSummary", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				summary, ok := resp["SyncBlockerSummary"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "blocker-stack", summary["ResourceName"])
				_, hasBlockers := summary["LatestBlockers"]
				assert.True(t, hasBlockers)
			}
		})
	}
}

// --- UpdateSyncBlocker parity ---

// TestParity_UpdateSyncBlocker verifies UpdateSyncBlocker validates Id is required.
func TestParity_UpdateSyncBlocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		wantStatus int
	}{
		{
			name:       "success_with_id",
			id:         "some-blocker-id",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_id",
			id:         "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{
				"ResolvedReason": "issue fixed",
				"ResourceName":   "my-stack",
				"SyncType":       "CFN_STACK_SYNC",
			}
			if tt.id != "" {
				body["Id"] = tt.id
			}

			rec := doJSON(t, h, "UpdateSyncBlocker", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- ListRepositorySyncDefinitions parity ---

// TestParity_ListRepositorySyncDefinitions exercises the handler.
func TestParity_ListRepositorySyncDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success_returns_empty",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repository_link_id",
			preCreate:  false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			linkID := "nonexistent-link"

			if tt.preCreate {
				connArn := createConn(t, h, "def-conn", "GitHub")
				linkID = createRepositoryLink(t, h, connArn, "my-org", "my-repo")
			}

			body := map[string]any{
				"SyncType": "CFN_STACK_SYNC",
			}
			if tt.name != "missing_repository_link_id" {
				body["RepositoryLinkId"] = linkID
			}

			rec := doJSON(t, h, "ListRepositorySyncDefinitions", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				defs, ok := resp["RepositorySyncDefinitions"].([]any)
				require.True(t, ok)
				assert.NotNil(t, defs)
			}
		})
	}
}

// --- Host name uniqueness after snapshot/restore ---

// TestParity_SnapshotRestore_HostsByName verifies hostsByName index is preserved in snapshot.
func TestParity_SnapshotRestore_HostsByName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "hosts_by_name_restored"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createHost(t, h, "snap-host", "GitHubEnterpriseServer", "https://ghe.example.com")

			snap := h.Backend.Snapshot(t.Context())
			require.NotNil(t, snap)

			newBackend := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, newBackend.Restore(t.Context(), snap))

			// Attempting to create a host with same name should fail (name index restored).
			_, err := newBackend.CreateHost(
				context.Background(),
				"snap-host",
				"GitHubEnterpriseServer",
				"https://new.example.com",
				nil,
			)
			require.Error(t, err, "duplicate host name should fail after restore")
		})
	}
}

// --- ListConnections with HostArn filter after CreateConnection with HostArn ---

// TestParity_ListConnections_HostArnFilterAfterCreate verifies that connections created with
// HostArn can be retrieved using the HostArnFilter.
func TestParity_ListConnections_HostArnFilterAfterCreate(t *testing.T) {
	t.Parallel()

	const hostArn = "arn:aws:codeconnections:us-east-1:123456789012:host/myhost"

	tests := []struct {
		name        string
		applyFilter bool
		wantCount   int
	}{
		{name: "no_filter_returns_both", applyFilter: false, wantCount: 2},
		{name: "host_filter_returns_one", applyFilter: true, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// conn1 with HostArn
			rec1 := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "ghe-conn",
				"ProviderType":   "GitHubEnterpriseServer",
				"HostArn":        hostArn,
			})
			require.Equal(t, http.StatusOK, rec1.Code)

			// conn2 without HostArn
			createConn(t, h, "gh-conn", "GitHub")

			body := map[string]any{}
			if tt.applyFilter {
				body["HostArnFilter"] = hostArn
			}

			rec := doJSON(t, h, "ListConnections", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conns, ok := resp["Connections"].([]any)
			require.True(t, ok)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}

// --- Provider init parity (codeconnections allows nil ctx) ---

// TestParity_ProviderInit_NilCtx verifies that codeconnections Provider.Init tolerates nil ctx.
func TestParity_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_ctx_no_panic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &codeconnections.Provider{}
			reg, err := p.Init(nil)
			require.NoError(t, err)
			assert.NotNil(t, reg)
		})
	}
}

// --- Backend direct parity tests ---

// TestParity_Backend_CreateConnection_HostArn verifies hostArn is stored in backend.
func TestParity_Backend_CreateConnection_HostArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		hostArn     string
		wantHostArn string
	}{
		{
			name:        "with_host_arn",
			hostArn:     "arn:aws:codeconnections:us-east-1:123:host/h1",
			wantHostArn: "arn:aws:codeconnections:us-east-1:123:host/h1",
		},
		{
			name:        "without_host_arn",
			hostArn:     "",
			wantHostArn: "",
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			conn, err := b.CreateConnection(
				context.Background(),
				"conn-"+strconv.Itoa(i),
				"GitHub",
				tt.hostArn,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantHostArn, conn.HostArn)

			got, err := b.GetConnection(context.Background(), conn.ConnectionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantHostArn, got.HostArn)
		})
	}
}

// TestParity_Backend_CreateHost_NameUniqueness verifies duplicate host names fail.
func TestParity_Backend_CreateHost_NameUniqueness(t *testing.T) {
	t.Parallel()

	b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateHost(
		context.Background(),
		"unique-host-x",
		"GitHubEnterpriseServer",
		"https://a.example.com",
		nil,
	)
	require.NoError(t, err, "first create should succeed")

	_, err = b.CreateHost(
		context.Background(),
		"unique-host-x",
		"GitHubEnterpriseServer",
		"https://b.example.com",
		nil,
	)
	require.Error(t, err, "duplicate host name must fail")
}

// TestParity_Backend_HostsByName_DeleteRestores verifies delete releases the name for reuse.
func TestParity_Backend_HostsByName_DeleteRestores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "name_reusable_after_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			host, err := b.CreateHost(
				context.Background(),
				"recycled-host",
				"GitHubEnterpriseServer",
				"https://a.example.com",
				nil,
			)
			require.NoError(t, err)

			err = b.DeleteHost(context.Background(), host.HostArn)
			require.NoError(t, err)

			_, err = b.CreateHost(
				context.Background(),
				"recycled-host",
				"GitHubEnterpriseServer",
				"https://b.example.com",
				nil,
			)
			require.NoError(t, err, "name should be reusable after delete")
		})
	}
}

// TestParity_Backend_AddHostInternal_UpdatesNameIndex verifies the name index is populated.
func TestParity_Backend_AddHostInternal_UpdatesNameIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "internal_add_blocks_duplicate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			b.AddHostInternal(context.Background(), &codeconnections.Host{
				Name:             "seeded-host",
				HostArn:          "arn:aws:codeconnections:us-east-1:123:host/seeded",
				ProviderType:     "GitHubEnterpriseServer",
				ProviderEndpoint: "https://ghe.example.com",
				Status:           "AVAILABLE",
				Tags:             map[string]string{},
			})

			_, err := b.CreateHost(
				context.Background(),
				"seeded-host",
				"GitHubEnterpriseServer",
				"https://other.example.com",
				nil,
			)
			require.Error(t, err, "AddHostInternal must populate name index")
		})
	}
}

// TestParity_Backend_ErrAlreadyExists_HostDuplicate verifies the correct error type.
func TestParity_Backend_ErrAlreadyExists_HostDuplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "duplicate_host_returns_already_exists_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")
			_, err := b.CreateHost(
				context.Background(),
				"dup-h",
				"GitHubEnterpriseServer",
				"https://a.example.com",
				nil,
			)
			require.NoError(t, err)

			_, err = b.CreateHost(
				context.Background(),
				"dup-h",
				"GitHubEnterpriseServer",
				"https://b.example.com",
				nil,
			)
			require.Error(t, err)
			// The error should wrap ErrAlreadyExists.
			assert.ErrorIs(t, err, codeconnections.ErrAlreadyExists)
		})
	}
}
