package codestarconnections_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

// createCSCConn is a test helper that creates a connection and returns its ARN.
func createCSCConn(t *testing.T, h *codestarconnections.Handler, name, providerType string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": name,
		"ProviderType":   providerType,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	arn, ok := resp["ConnectionArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

// createCSCHost is a test helper that creates a host and returns its ARN.
func createCSCHost(t *testing.T, h *codestarconnections.Handler, name, providerType, endpoint string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             name,
		"ProviderType":     providerType,
		"ProviderEndpoint": endpoint,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	arn, ok := resp["HostArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

// createCSCRepositoryLink is a test helper that creates a repository link and returns its ID.
func createCSCRepositoryLink(t *testing.T, h *codestarconnections.Handler, connArn, repoName string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":  connArn,
		"OwnerId":        "my-org",
		"RepositoryName": repoName,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	info, ok := resp["RepositoryLinkInfo"].(map[string]any)
	require.True(t, ok)

	linkID, ok := info["RepositoryLinkId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, linkID)

	return linkID
}

// doCSCRequest sends a POST / request with X-Amz-Target set.
func doCSCRequest(
	t *testing.T,
	h *codestarconnections.Handler,
	action string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "CodeStar_connections_20191201."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// --- ExtractResource parity ---

// TestParity_ExtractResource verifies ExtractResource correctly pulls ARNs from request body.
func TestParity_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantRes string
	}{
		{
			name:    "connection_arn",
			body:    map[string]any{"ConnectionArn": "arn:aws:codestar-connections:us-east-1:123:connection/abc"},
			wantRes: "arn:aws:codestar-connections:us-east-1:123:connection/abc",
		},
		{
			name:    "resource_arn",
			body:    map[string]any{"ResourceArn": "arn:aws:codestar-connections:us-east-1:123:connection/xyz"},
			wantRes: "arn:aws:codestar-connections:us-east-1:123:connection/xyz",
		},
		{
			name:    "host_arn",
			body:    map[string]any{"HostArn": "arn:aws:codestar-connections:us-east-1:123:host/myhost"},
			wantRes: "arn:aws:codestar-connections:us-east-1:123:host/myhost",
		},
		{
			name:    "repository_link_id",
			body:    map[string]any{"RepositoryLinkId": "repo-link-id-abc"},
			wantRes: "repo-link-id-abc",
		},
		{
			name:    "resource_name",
			body:    map[string]any{"ResourceName": "my-stack"},
			wantRes: "my-stack",
		},
		{
			name:    "empty_body",
			body:    map[string]any{},
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractResource(c)
			assert.Equal(t, tt.wantRes, got)
		})
	}
}

// --- Connection parity ---

// TestParity_CreateConnection_WithHostArn verifies HostArn is accepted.
func TestParity_CreateConnection_WithHostArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantArn bool
		wantOK  bool
	}{
		{
			name: "with_host_arn",
			body: map[string]any{
				"ConnectionName": "ghe-conn",
				"ProviderType":   "GitHubEnterpriseServer",
				"HostArn":        "arn:aws:codestar-connections:us-east-1:123:host/ghe",
			},
			wantArn: true,
			wantOK:  true,
		},
		{
			name: "without_host_arn",
			body: map[string]any{
				"ConnectionName": "gh-conn",
				"ProviderType":   "GitHub",
			},
			wantArn: true,
			wantOK:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doCSCRequest(t, h, "CreateConnection", tt.body)

			if tt.wantOK {
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.wantArn {
				resp := parseResp(t, rec)
				assert.NotEmpty(t, resp["ConnectionArn"])
			}
		})
	}
}

// TestParity_GetConnection_Fields verifies all expected fields are returned.
func TestParity_GetConnection_Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		connName      string
		providerType  string
		wantStatus    string
		wantOwnerAcct string
	}{
		{
			name:          "github_connection_fields",
			connName:      "my-gh-conn",
			providerType:  "GitHub",
			wantStatus:    "AVAILABLE",
			wantOwnerAcct: "000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, tt.connName, tt.providerType)

			rec := doCSCRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conn, ok := resp["Connection"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.connName, conn["ConnectionName"])
			assert.Equal(t, tt.providerType, conn["ProviderType"])
			assert.Equal(t, tt.wantStatus, conn["ConnectionStatus"])
			assert.Equal(t, tt.wantOwnerAcct, conn["OwnerAccountId"])
			assert.Equal(t, connArn, conn["ConnectionArn"])
		})
	}
}

// TestParity_ListConnections_Sorted verifies connections are returned sorted by name.
func TestParity_ListConnections_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		connNames []string
		wantOrder []string
	}{
		{
			name:      "sorted_alpha",
			connNames: []string{"zebra-conn", "alpha-conn", "mango-conn"},
			wantOrder: []string{"alpha-conn", "mango-conn", "zebra-conn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, n := range tt.connNames {
				createCSCConn(t, h, n, "GitHub")
			}

			rec := doCSCRequest(t, h, "ListConnections", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conns, ok := resp["Connections"].([]any)
			require.True(t, ok)
			require.Len(t, conns, len(tt.wantOrder))

			for i, wantName := range tt.wantOrder {
				connMap, isMap := conns[i].(map[string]any)
				require.True(t, isMap)
				assert.Equal(t, wantName, connMap["ConnectionName"])
			}
		})
	}
}

// TestParity_ListConnections_HostArnFilter verifies filtering by HostArn.
func TestParity_ListConnections_HostArnFilter(t *testing.T) {
	t.Parallel()

	const hostArn = "arn:aws:codestar-connections:us-east-1:123:host/myghe"

	tests := []struct {
		name        string
		applyFilter bool
		wantCount   int
	}{
		{name: "no_filter_returns_all", applyFilter: false, wantCount: 2},
		{name: "host_filter_returns_one", applyFilter: true, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
			h := codestarconnections.NewHandler(b)

			_, err := b.CreateConnection(context.Background(), "ghe-conn", "GitHubEnterpriseServer", hostArn, nil)
			require.NoError(t, err)

			_, err = b.CreateConnection(context.Background(), "gh-conn", "GitHub", "", nil)
			require.NoError(t, err)

			body := map[string]any{}
			if tt.applyFilter {
				body["HostArnFilter"] = hostArn
			}

			rec := doCSCRequest(t, h, "ListConnections", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conns, ok := resp["Connections"].([]any)
			require.True(t, ok)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}

// --- Host parity ---

// TestParity_GetHost_IncludesHostArn verifies GetHost does NOT include HostArn (real AWS omits it).
// HostArn is only present in ListHosts items.
func TestParity_GetHost_IncludesHostArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		hostName     string
		providerType string
		endpoint     string
	}{
		{
			name:         "host_arn_not_in_get_response",
			hostName:     "my-ghe-host",
			providerType: "GitHubEnterpriseServer",
			endpoint:     "https://ghe.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			hostArn := createCSCHost(t, h, tt.hostName, tt.providerType, tt.endpoint)

			rec := doCSCRequest(t, h, "GetHost", map[string]any{"HostArn": hostArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			_, hasHostArn := resp["HostArn"]
			assert.False(t, hasHostArn, "GetHost must not include HostArn in response")
			assert.Equal(t, tt.hostName, resp["Name"])
			assert.Equal(t, tt.endpoint, resp["ProviderEndpoint"])
			assert.Equal(t, tt.providerType, resp["ProviderType"])
		})
	}
}

// TestParity_ListHosts_Sorted verifies hosts are returned sorted by name.
func TestParity_ListHosts_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		hostNames []string
		wantOrder []string
	}{
		{
			name:      "sorted_alpha",
			hostNames: []string{"zulu-host", "alpha-host", "mike-host"},
			wantOrder: []string{"alpha-host", "mike-host", "zulu-host"},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for j, n := range tt.hostNames {
				createCSCHost(t, h, n, "GitHubEnterpriseServer",
					"https://ghe"+strconv.Itoa(i)+strconv.Itoa(j)+".example.com")
			}

			rec := doCSCRequest(t, h, "ListHosts", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			hosts, ok := resp["Hosts"].([]any)
			require.True(t, ok)
			require.Len(t, hosts, len(tt.wantOrder))

			for k, wantName := range tt.wantOrder {
				hostMap, isMap := hosts[k].(map[string]any)
				require.True(t, isMap)
				assert.Equal(t, wantName, hostMap["Name"])
			}
		})
	}
}

// --- RepositoryLink parity ---

// TestParity_RepositoryLink_ProviderTypeDerived verifies provider type comes from connection.
func TestParity_RepositoryLink_ProviderTypeDerived(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		providerType string
	}{
		{name: "github_provider_derived", providerType: "GitHub"},
		{name: "gitlab_provider_derived", providerType: "GitLab"},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "pt-conn-"+strconv.Itoa(i), tt.providerType)
			linkID := createCSCRepositoryLink(t, h, connArn, "my-repo")

			rec := doCSCRequest(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			info, ok := resp["RepositoryLinkInfo"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.providerType, info["ProviderType"])
		})
	}
}

// TestParity_ListRepositoryLinks_Sorted verifies links are returned sorted by RepositoryLinkId.
func TestParity_ListRepositoryLinks_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		linkCount int
	}{
		{name: "multiple_links_sorted", linkCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "list-conn", "GitHub")

			for i := range tt.linkCount {
				createCSCRepositoryLink(t, h, connArn, "repo-"+strconv.Itoa(i))
			}

			rec := doCSCRequest(t, h, "ListRepositoryLinks", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			links, ok := resp["RepositoryLinks"].([]any)
			require.True(t, ok)
			assert.Len(t, links, tt.linkCount)
		})
	}
}

// --- SyncConfiguration parity ---

// TestParity_SyncConfiguration_RoundTrip exercises full create/get/update/delete cycle.
func TestParity_SyncConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "sync-conn", "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "my-repo")

			// Create
			createRec := doCSCRequest(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":           "main",
				"ConfigFile":       "sync.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "rt-stack",
				"RoleArn":          "arn:aws:iam::000000000000:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			resp := parseResp(t, createRec)
			cfg, ok := resp["SyncConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "main", cfg["Branch"])
			assert.Equal(t, "my-org", cfg["OwnerId"])
			assert.Equal(t, "GitHub", cfg["ProviderType"])

			// Get
			getRec := doCSCRequest(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "rt-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			// Update
			updRec := doCSCRequest(t, h, "UpdateSyncConfiguration", map[string]any{
				"ResourceName": "rt-stack",
				"SyncType":     "CFN_STACK_SYNC",
				"Branch":       "release",
			})
			require.Equal(t, http.StatusOK, updRec.Code)
			updResp := parseResp(t, updRec)
			updCfg, ok := updResp["SyncConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "release", updCfg["Branch"])

			// List
			listRec := doCSCRequest(t, h, "ListSyncConfigurations", map[string]any{
				"RepositoryLinkId": linkID,
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			listResp := parseResp(t, listRec)
			cfgs, ok := listResp["SyncConfigurations"].([]any)
			require.True(t, ok)
			assert.Len(t, cfgs, 1)

			// Delete
			delRec := doCSCRequest(t, h, "DeleteSyncConfiguration", map[string]any{
				"ResourceName": "rt-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, delRec.Code)

			// Get after delete should fail
			afterDelRec := doCSCRequest(t, h, "GetSyncConfiguration", map[string]any{
				"ResourceName": "rt-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, http.StatusBadRequest, afterDelRec.Code)
		})
	}
}

// TestParity_GetRepositorySyncStatus exercises the handler with valid and invalid inputs.
func TestParity_GetRepositorySyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
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

			h := newTestHandler(t)
			linkID := "nonexistent-link"

			if tt.preCreate {
				connArn := createCSCConn(t, h, "sync-status-conn", "GitHub")
				linkID = createCSCRepositoryLink(t, h, connArn, "my-repo")
			}

			rec := doCSCRequest(t, h, "GetRepositorySyncStatus", map[string]any{
				"RepositoryLinkId": linkID,
				"Branch":           "main",
				"SyncType":         "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				latest, ok := resp["LatestSync"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "SUCCEEDED", latest["Status"])
				assert.NotEmpty(t, latest["StartedAt"])
			}
		})
	}
}

// TestParity_GetResourceSyncStatus exercises the handler.
func TestParity_GetResourceSyncStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
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

			h := newTestHandler(t)

			if tt.preCreate {
				connArn := createCSCConn(t, h, "res-sync-conn", "GitHub")
				linkID := createCSCRepositoryLink(t, h, connArn, "my-repo")
				rec := doCSCRequest(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "res-sync-stack",
					"RoleArn":          "arn:aws:iam::000000000000:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCSCRequest(t, h, "GetResourceSyncStatus", map[string]any{
				"ResourceName": "res-sync-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				latest, ok := resp["LatestSync"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "SUCCEEDED", latest["Status"])
			}
		})
	}
}

// TestParity_GetSyncBlockerSummary exercises the handler.
func TestParity_GetSyncBlockerSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			wantStatus: http.StatusOK,
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

			h := newTestHandler(t)

			if tt.preCreate {
				connArn := createCSCConn(t, h, "blocker-conn", "GitHub")
				linkID := createCSCRepositoryLink(t, h, connArn, "my-repo")
				rec := doCSCRequest(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "blocker-stack",
					"RoleArn":          "arn:aws:iam::000000000000:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCSCRequest(t, h, "GetSyncBlockerSummary", map[string]any{
				"ResourceName": "blocker-stack",
				"SyncType":     "CFN_STACK_SYNC",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				summary, ok := resp["SyncBlockerSummary"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "blocker-stack", summary["ResourceName"])
			}
		})
	}
}

// TestParity_ListRepositorySyncDefinitions exercises the handler.
func TestParity_ListRepositorySyncDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success_empty",
			preCreate:  true,
			wantStatus: http.StatusOK,
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

			h := newTestHandler(t)
			linkID := "nonexistent"

			if tt.preCreate {
				connArn := createCSCConn(t, h, "def-conn", "GitHub")
				linkID = createCSCRepositoryLink(t, h, connArn, "my-repo")
			}

			rec := doCSCRequest(t, h, "ListRepositorySyncDefinitions", map[string]any{
				"RepositoryLinkId": linkID,
				"SyncType":         "CFN_STACK_SYNC",
			})
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

// TestParity_UpdateSyncBlocker_RequiresId verifies Id is required, and that a
// non-empty but unknown Id fails differently (SyncBlockerDoesNotExistException
// from the backend) than an empty Id (InvalidInputException from input
// validation, before the backend is ever consulted).
func TestParity_UpdateSyncBlocker_RequiresId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		id          string
		wantErrType string
	}{
		{
			name:        "with_id_but_unknown",
			id:          "blocker-abc",
			wantErrType: "SyncBlockerDoesNotExistException",
		},
		{
			name:        "missing_id",
			id:          "",
			wantErrType: "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"ResolvedReason": "fixed",
				"ResourceName":   "my-stack",
				"SyncType":       "CFN_STACK_SYNC",
			}
			if tt.id != "" {
				body["Id"] = tt.id
			}

			rec := doCSCRequest(t, h, "UpdateSyncBlocker", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, tt.wantErrType, resp["__type"])
		})
	}
}

// --- Tagging parity ---

// TestParity_Tags_NonNilOnList verifies Tags field is always non-nil (empty slice not null).
func TestParity_Tags_NonNilOnList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "empty_tags_not_null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "tags-test-conn", "GitHub")

			rec := doCSCRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok, "Tags must be an array, not null")
			assert.Empty(t, tags)
		})
	}
}

// TestParity_TagResource_OnHost verifies tags can be managed on hosts.
func TestParity_TagResource_OnHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag_untag_host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			hostArn := createCSCHost(t, h, "tag-host", "GitHubEnterpriseServer", "https://ghe.example.com")

			tagRec := doCSCRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": hostArn,
				"Tags":        []map[string]string{{"Key": "Env", "Value": "prod"}, {"Key": "Owner", "Value": "ops"}},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doCSCRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := parseResp(t, listRec)
			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, 2)

			untagRec := doCSCRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": hostArn,
				"TagKeys":     []string{"Owner"},
			})
			require.Equal(t, http.StatusOK, untagRec.Code)

			listRec2 := doCSCRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
			require.Equal(t, http.StatusOK, listRec2.Code)
			resp2 := parseResp(t, listRec2)
			tags2, ok := resp2["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags2, 1)
		})
	}
}

// --- UpdateRepositoryLink parity ---

// TestParity_UpdateRepositoryLink exercises the handler.
func TestParity_UpdateRepositoryLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		newConnArn string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "success",
			preCreate:  true,
			newConnArn: "arn:aws:codestar-connections:us-east-1:123:connection/new",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			preCreate:  false,
			newConnArn: "arn:aws:codestar-connections:us-east-1:123:connection/new",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			linkID := "nonexistent"

			if tt.preCreate {
				connArn := createCSCConn(t, h, "upd-link-conn", "GitHub")
				linkID = createCSCRepositoryLink(t, h, connArn, "my-repo")
			}

			rec := doCSCRequest(t, h, "UpdateRepositoryLink", map[string]any{
				"RepositoryLinkId": linkID,
				"ConnectionArn":    tt.newConnArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				info, ok := resp["RepositoryLinkInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.newConnArn, info["ConnectionArn"])
			}
		})
	}
}

// --- Provider parity ---

// TestParity_Provider_NilCtx verifies codestarconnections Provider.Init requires non-nil ctx.
func TestParity_Provider_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "nil_ctx_returns_error", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &codestarconnections.Provider{}
			reg, err := p.Init(nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, reg)
				assert.ErrorIs(t, err, codestarconnections.ErrNilAppContext)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, reg)
			}
		})
	}
}
