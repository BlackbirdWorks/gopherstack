package codestarconnections_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

// --- Connection name validation ---

func TestAudit2_ConnectionName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connName   string
		wantStatus int
	}{
		{
			name:       "valid simple name",
			connName:   "my-conn",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid name with dots",
			connName:   "my.conn.1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid name with underscores",
			connName:   "my_conn_1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid max length name",
			connName:   "abcdefghijklmnopqrstuvwxyz123456",
			wantStatus: http.StatusOK,
		},
		{
			name:       "name too long (33 chars)",
			connName:   "abcdefghijklmnopqrstuvwxyz1234567",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name with invalid chars (space)",
			connName:   "my conn",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name with invalid chars (slash)",
			connName:   "my/conn",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty name from body missing ConnectionName",
			connName:   "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"ProviderType": "GitHub"}
			if tt.connName != "" {
				body["ConnectionName"] = tt.connName
			}

			rec := doRequest(t, h, "CreateConnection", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body=%v", body)
		})
	}
}

// --- Provider type validation ---

func TestAudit2_ProviderType_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		providerType string
		wantStatus   int
	}{
		{name: "GitHub", providerType: "GitHub", wantStatus: http.StatusOK},
		{name: "Bitbucket", providerType: "Bitbucket", wantStatus: http.StatusOK},
		{name: "GitLab", providerType: "GitLab", wantStatus: http.StatusOK},
		{name: "GitHubEnterpriseServer", providerType: "GitHubEnterpriseServer", wantStatus: http.StatusOK},
		{name: "GitLabSelfManaged", providerType: "GitLabSelfManaged", wantStatus: http.StatusOK},
		{name: "empty provider type is allowed", providerType: "", wantStatus: http.StatusOK},
		{name: "invalid provider type", providerType: "Subversion", wantStatus: http.StatusBadRequest},
		{name: "case sensitive invalid", providerType: "github", wantStatus: http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"ConnectionName": "conn-" + tt.providerType + "-" + string(rune('0'+i)),
				"ProviderType":   tt.providerType,
			}
			if tt.providerType == "" {
				body["ConnectionName"] = "conn-empty-pt"
			}

			rec := doRequest(t, h, "CreateConnection", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Tag validation ---

func TestAudit2_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name:       "valid single tag",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty value is valid",
			tags:       []map[string]string{{"Key": "flag", "Value": ""}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag key too long (129 chars)",
			tags:       []map[string]string{{"Key": repeatStr("k", 129), "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag key max length (128 chars) is valid",
			tags:       []map[string]string{{"Key": repeatStr("k", 128), "Value": "v"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag value too long (257 chars)",
			tags:       []map[string]string{{"Key": "k", "Value": repeatStr("v", 257)}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "tag value max length (256 chars) is valid",
			tags:       []map[string]string{{"Key": "k", "Value": repeatStr("v", 256)}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty tag key",
			tags:       []map[string]string{{"Key": "", "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"ConnectionName": "tag-conn-" + string(rune('a'+i)),
				"ProviderType":   "GitHub",
				"Tags":           tt.tags,
			}

			rec := doRequest(t, h, "CreateConnection", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "test %q", tt.name)
		})
	}
}

func repeatStr(s string, n int) string {
	result := make([]byte, n*len(s))
	for i := range n {
		copy(result[i*len(s):], s)
	}

	return string(result)
}

// --- TagResource tag count limit ---

func TestAudit2_TagResource_CountLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	conn, err := h.Backend.CreateConnection(context.Background(), "limit-conn", "GitHub", "", nil)
	require.NoError(t, err)

	// Apply 199 tags (under limit).
	tags1 := make([]map[string]string, 199)
	for i := range 199 {
		tags1[i] = map[string]string{"Key": "k" + string(rune('A'+i%26)) + string(rune('0'+i/26)), "Value": "v"}
	}

	rec1 := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"Tags":        tags1,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Add 1 more tag (total 200 - at limit, OK).
	rec2 := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"Tags":        []map[string]string{{"Key": "boundary-tag", "Value": "ok"}},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Try to add 1 more (total 201 - over limit).
	rec3 := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": conn.ConnectionArn,
		"Tags":        []map[string]string{{"Key": "over-limit", "Value": "nope"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

// --- Host validation ---

func TestAudit2_CreateHost_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid host",
			body: map[string]any{
				"Name":             "my-ghe-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://ghe.example.com",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing provider endpoint",
			body: map[string]any{
				"Name":         "no-ep-host",
				"ProviderType": "GitHubEnterpriseServer",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "name too long",
			body: map[string]any{
				"Name":             repeatStr("h", 33),
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://x.com",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "name with invalid chars",
			body: map[string]any{
				"Name":             "my host!",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://x.com",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid provider type for host",
			body: map[string]any{
				"Name":             "bad-pt-host",
				"ProviderType":     "NOTVALID",
				"ProviderEndpoint": "https://x.com",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateHost", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- DeleteHost with active connections ---

func TestAudit2_DeleteHost_ResourceInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantErrType string
		wantStatus  int
		setupConn   bool
	}{
		{
			name:       "delete host without connections succeeds",
			setupConn:  false,
			wantStatus: http.StatusOK,
		},
		{
			name:        "delete host with active connection fails",
			setupConn:   true,
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceInUseException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			hostArn := createCSCHost(t, h, "deletable-host", "GitHubEnterpriseServer", "https://ghe.example.com")

			if tt.setupConn {
				rec := doRequest(t, h, "CreateConnection", map[string]any{
					"ConnectionName": "uses-host-conn",
					"ProviderType":   "GitHubEnterpriseServer",
					"HostArn":        hostArn,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				resp := parseResp(t, rec)
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

// --- DeleteHost then connection can be deleted ---

func TestAudit2_DeleteHost_AfterDeletingConnections(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	hostArn := createCSCHost(t, h, "detach-host", "GitHubEnterpriseServer", "https://ghe.example.com")
	connArn := createCSCConn(t, h, "host-conn", "GitHubEnterpriseServer")

	// Update connection to reference the host (simulate via backend).
	h.Backend.AddConnectionInternal(&codestarconnections.Connection{
		ConnectionName:   "host-ref-conn",
		ConnectionArn:    "arn:aws:codestar-connections:us-east-1:000000000000:connection/hostref",
		ConnectionStatus: codestarconnections.ConnectionStatusAvailable,
		OwnerAccountID:   "000000000000",
		ProviderType:     "GitHubEnterpriseServer",
		HostArn:          hostArn,
	})

	// Can't delete host while connection references it.
	rec1 := doRequest(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
	assert.Equal(t, http.StatusBadRequest, rec1.Code)

	// Delete the referencing connection, then the host becomes deletable.
	rec2 := doRequest(t, h, "DeleteConnection", map[string]any{
		"ConnectionArn": "arn:aws:codestar-connections:us-east-1:000000000000:connection/hostref",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Also delete the other connection we created (it doesn't reference the host).
	rec3 := doRequest(t, h, "DeleteConnection", map[string]any{"ConnectionArn": connArn})
	require.Equal(t, http.StatusOK, rec3.Code)

	rec4 := doRequest(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
	assert.Equal(t, http.StatusOK, rec4.Code)
}

// --- DeleteRepositoryLink with active sync configs ---

func TestAudit2_DeleteRepositoryLink_ResourceInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantErrType string
		wantStatus  int
		createSync  bool
	}{
		{
			name:       "delete link without sync configs succeeds",
			createSync: false,
			wantStatus: http.StatusOK,
		},
		{
			name:        "delete link with active sync config fails",
			createSync:  true,
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceInUseException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "rl-riu-conn", "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "rl-riu-repo")

			if tt.createSync {
				rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
					"Branch":           "main",
					"ConfigFile":       "sync.yaml",
					"RepositoryLinkId": linkID,
					"ResourceName":     "rl-riu-stack",
					"RoleArn":          "arn:aws:iam::000000000000:role/r",
					"SyncType":         "CFN_STACK_SYNC",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				resp := parseResp(t, rec)
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

// --- Pagination: ListConnections ---

func TestAudit2_ListConnections_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		createCSCConn(t, h, "pag-conn-"+string(rune('a'+i)), "GitHub")
	}

	// First page: MaxResults=3.
	rec1 := doRequest(t, h, "ListConnections", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResp(t, rec1)
	conns1, ok := resp1["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns1, 3)
	nextToken, hasNext := resp1["NextToken"].(string)
	assert.True(t, hasNext && nextToken != "", "expected NextToken for first page")

	// Second page.
	rec2 := doRequest(t, h, "ListConnections", map[string]any{
		"MaxResults": 3,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	conns2, ok := resp2["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns2, 2)
	assert.Empty(t, resp2["NextToken"], "no NextToken on last page")

	// Collect all names and verify they're the same set.
	allNames := make(map[string]bool)
	for _, c := range conns1 {
		cm := c.(map[string]any)
		allNames[cm["ConnectionName"].(string)] = true
	}

	for _, c := range conns2 {
		cm := c.(map[string]any)
		allNames[cm["ConnectionName"].(string)] = true
	}

	assert.Len(t, allNames, 5)
}

// --- Pagination: ListHosts ---

func TestAudit2_ListHosts_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 4 {
		createCSCHost(t, h, "pag-host-"+string(rune('a'+i)), "GitHubEnterpriseServer",
			"https://ghe"+string(rune('a'+i))+".example.com")
	}

	rec1 := doRequest(t, h, "ListHosts", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResp(t, rec1)
	hosts1, ok := resp1["Hosts"].([]any)
	require.True(t, ok)
	assert.Len(t, hosts1, 2)

	nextToken, hasNext := resp1["NextToken"].(string)
	assert.True(t, hasNext && nextToken != "")

	rec2 := doRequest(t, h, "ListHosts", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	hosts2, ok := resp2["Hosts"].([]any)
	require.True(t, ok)
	assert.Len(t, hosts2, 2)
	assert.Empty(t, resp2["NextToken"])
}

// --- Pagination: ListRepositoryLinks ---

func TestAudit2_ListRepositoryLinks_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "pag-links-conn", "GitHub")

	for i := range 5 {
		createCSCRepositoryLink(t, h, connArn, "pag-repo-"+string(rune('a'+i)))
	}

	rec1 := doRequest(t, h, "ListRepositoryLinks", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResp(t, rec1)
	links1, ok := resp1["RepositoryLinks"].([]any)
	require.True(t, ok)
	assert.Len(t, links1, 3)

	nextToken, hasNext := resp1["NextToken"].(string)
	assert.True(t, hasNext && nextToken != "")

	rec2 := doRequest(t, h, "ListRepositoryLinks", map[string]any{
		"MaxResults": 3,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	links2, ok := resp2["RepositoryLinks"].([]any)
	require.True(t, ok)
	assert.Len(t, links2, 2)
	assert.Empty(t, resp2["NextToken"])
}

// --- Pagination: ListSyncConfigurations ---

func TestAudit2_ListSyncConfigurations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "pag-syncs-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "pag-syncs-repo")

	for i := range 5 {
		rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
			"Branch":           "main",
			"ConfigFile":       "cfg.yaml",
			"RepositoryLinkId": linkID,
			"ResourceName":     "stack-" + string(rune('a'+i)),
			"RoleArn":          "arn:aws:iam::000000000000:role/r",
			"SyncType":         "CFN_STACK_SYNC",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
		"MaxResults":       3,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResp(t, rec1)
	cfgs1, ok := resp1["SyncConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs1, 3)

	nextToken, hasNext := resp1["NextToken"].(string)
	assert.True(t, hasNext && nextToken != "")

	rec2 := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
		"MaxResults":       3,
		"NextToken":        nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	cfgs2, ok := resp2["SyncConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs2, 2)
	assert.Empty(t, resp2["NextToken"])
}

// --- SyncConfiguration: PublishDeploymentStatus and TriggerResourceUpdateOn ---

func TestAudit2_SyncConfiguration_PublishAndTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		publishDeploymentStatus string
		triggerResourceUpdateOn string
		wantPublish             string
		wantTrigger             string
		wantStatus              int
	}{
		{
			name:                    "ENABLED publish and ANY_CHANGE trigger",
			publishDeploymentStatus: "ENABLED",
			triggerResourceUpdateOn: "ANY_CHANGE",
			wantStatus:              http.StatusOK,
			wantPublish:             "ENABLED",
			wantTrigger:             "ANY_CHANGE",
		},
		{
			name:                    "DISABLED publish and FILE_CHANGE trigger",
			publishDeploymentStatus: "DISABLED",
			triggerResourceUpdateOn: "FILE_CHANGE",
			wantStatus:              http.StatusOK,
			wantPublish:             "DISABLED",
			wantTrigger:             "FILE_CHANGE",
		},
		{
			name:                    "invalid publish status",
			publishDeploymentStatus: "INVALID",
			triggerResourceUpdateOn: "ANY_CHANGE",
			wantStatus:              http.StatusBadRequest,
		},
		{
			name:                    "invalid trigger value",
			publishDeploymentStatus: "ENABLED",
			triggerResourceUpdateOn: "NEVER",
			wantStatus:              http.StatusBadRequest,
		},
		{
			name:       "no publish or trigger (omitted)",
			wantStatus: http.StatusOK,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "pt-conn-"+string(rune('a'+i)), "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "pt-repo-"+string(rune('a'+i)))

			body := map[string]any{
				"Branch":           "main",
				"ConfigFile":       "cfg.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "pt-stack-" + string(rune('a'+i)),
				"RoleArn":          "arn:aws:iam::000000000000:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			}

			if tt.publishDeploymentStatus != "" {
				body["PublishDeploymentStatus"] = tt.publishDeploymentStatus
			}

			if tt.triggerResourceUpdateOn != "" {
				body["TriggerResourceUpdateOn"] = tt.triggerResourceUpdateOn
			}

			rec := doRequest(t, h, "CreateSyncConfiguration", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantPublish != "" {
				resp := parseResp(t, rec)
				cfg := resp["SyncConfiguration"].(map[string]any)
				assert.Equal(t, tt.wantPublish, cfg["PublishDeploymentStatus"])
				assert.Equal(t, tt.wantTrigger, cfg["TriggerResourceUpdateOn"])
			}
		})
	}
}

// --- SyncConfiguration: update publish and trigger fields ---

func TestAudit2_UpdateSyncConfiguration_PublishAndTrigger(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "upd-pt-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "upd-pt-repo")

	// Create with ENABLED/ANY_CHANGE.
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":                  "main",
		"ConfigFile":              "cfg.yaml",
		"RepositoryLinkId":        linkID,
		"ResourceName":            "upd-pt-stack",
		"RoleArn":                 "arn:aws:iam::000000000000:role/r",
		"SyncType":                "CFN_STACK_SYNC",
		"PublishDeploymentStatus": "ENABLED",
		"TriggerResourceUpdateOn": "ANY_CHANGE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update to DISABLED/FILE_CHANGE.
	updRec := doRequest(t, h, "UpdateSyncConfiguration", map[string]any{
		"ResourceName":            "upd-pt-stack",
		"SyncType":                "CFN_STACK_SYNC",
		"PublishDeploymentStatus": "DISABLED",
		"TriggerResourceUpdateOn": "FILE_CHANGE",
	})
	require.Equal(t, http.StatusOK, updRec.Code)
	updResp := parseResp(t, updRec)
	cfg := updResp["SyncConfiguration"].(map[string]any)
	assert.Equal(t, "DISABLED", cfg["PublishDeploymentStatus"])
	assert.Equal(t, "FILE_CHANGE", cfg["TriggerResourceUpdateOn"])
}

// --- SyncConfiguration duplicate detection ---

func TestAudit2_CreateSyncConfiguration_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "dup-sync-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "dup-sync-repo")

	body := map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "dup-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	}

	rec1 := doRequest(t, h, "CreateSyncConfiguration", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateSyncConfiguration", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// --- SyncConfiguration: ResourceName with slash is rejected ---

func TestAudit2_CreateSyncConfiguration_ResourceNameWithSlash(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": "some-link",
		"ResourceName":     "bad/name",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- DeleteSyncConfiguration requires ResourceName and SyncType ---

func TestAudit2_DeleteSyncConfiguration_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceName string
		syncType     string
		wantStatus   int
	}{
		{
			name:         "missing resource name",
			resourceName: "",
			syncType:     "CFN_STACK_SYNC",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "missing sync type",
			resourceName: "some-stack",
			syncType:     "",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "invalid sync type",
			resourceName: "some-stack",
			syncType:     "INVALID",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DeleteSyncConfiguration", map[string]any{
				"ResourceName": tt.resourceName,
				"SyncType":     tt.syncType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Real sync status: SetRepositorySyncStatus helper ---

func TestAudit2_GetRepositorySyncStatus_RealState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setStatus  string
		wantStatus string
	}{
		{name: "SUCCEEDED", setStatus: "SUCCEEDED", wantStatus: "SUCCEEDED"},
		{name: "FAILED", setStatus: "FAILED", wantStatus: "FAILED"},
		{name: "IN_PROGRESS", setStatus: "IN_PROGRESS", wantStatus: "IN_PROGRESS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "rsync-"+tt.name, "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "rsync-repo")

			// Set a specific sync status via backend helper.
			h.Backend.SetRepositorySyncStatus(
				context.Background(),
				linkID, "main", "CFN_STACK_SYNC",
				tt.setStatus, nil,
			)

			rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
				"RepositoryLinkId": linkID,
				"Branch":           "main",
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			latest := resp["LatestSync"].(map[string]any)
			assert.Equal(t, tt.wantStatus, latest["Status"])
		})
	}
}

// --- Real sync status: SetResourceSyncStatus helper ---

func TestAudit2_GetResourceSyncStatus_RealState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setStatus  string
		wantStatus string
	}{
		{name: "SUCCEEDED", setStatus: "SUCCEEDED", wantStatus: "SUCCEEDED"},
		{name: "FAILED", setStatus: "FAILED", wantStatus: "FAILED"},
		{name: "IN_PROGRESS", setStatus: "IN_PROGRESS", wantStatus: "IN_PROGRESS"},
		{name: "QUEUED", setStatus: "QUEUED", wantStatus: "QUEUED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, "rss-conn-"+tt.name, "GitHub")
			linkID := createCSCRepositoryLink(t, h, connArn, "rss-repo-"+tt.name)

			rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
				"Branch":           "main",
				"ConfigFile":       "cfg.yaml",
				"RepositoryLinkId": linkID,
				"ResourceName":     "rss-stack-" + tt.name,
				"RoleArn":          "arn:aws:iam::000000000000:role/r",
				"SyncType":         "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			h.Backend.SetResourceSyncStatus(
				context.Background(),
				"rss-stack-"+tt.name, "CFN_STACK_SYNC",
				tt.setStatus, nil,
			)

			getRec := doRequest(t, h, "GetResourceSyncStatus", map[string]any{
				"ResourceName": "rss-stack-" + tt.name,
				"SyncType":     "CFN_STACK_SYNC",
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			resp := parseResp(t, getRec)
			latest := resp["LatestSync"].(map[string]any)
			assert.Equal(t, tt.wantStatus, latest["Status"])
		})
	}
}

// --- Sync status: auto-seeded on CreateSyncConfiguration ---

func TestAudit2_GetResourceSyncStatus_AutoSeeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "auto-seed-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "auto-seed-repo")

	// Create a sync configuration.
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "auto-seed-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Immediately get sync status - should be seeded with SUCCEEDED.
	getRec := doRequest(t, h, "GetResourceSyncStatus", map[string]any{
		"ResourceName": "auto-seed-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	resp := parseResp(t, getRec)
	latest := resp["LatestSync"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", latest["Status"])
}

// --- Sync blocker lifecycle ---

func TestAudit2_SyncBlocker_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "blocker-lifecycle-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "blocker-lifecycle-repo")

	// Create sync config.
	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "blocker-lifecycle-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create a blocker via backend.
	blocker, err := h.Backend.CreateSyncBlocker(
		context.Background(),
		"blocker-lifecycle-stack", "CFN_STACK_SYNC",
		"AUTOMATED", "Detected config drift",
	)
	require.NoError(t, err)
	assert.NotEmpty(t, blocker.ID)
	assert.Equal(t, "ACTIVE", blocker.Status)

	// GetSyncBlockerSummary should show the active blocker.
	sumRec := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "blocker-lifecycle-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, sumRec.Code)
	sumResp := parseResp(t, sumRec)
	summary := sumResp["SyncBlockerSummary"].(map[string]any)
	blockers, ok := summary["LatestBlockers"].([]any)
	require.True(t, ok)
	require.Len(t, blockers, 1)
	blockerMap := blockers[0].(map[string]any)
	assert.Equal(t, blocker.ID, blockerMap["Id"])
	assert.Equal(t, "ACTIVE", blockerMap["Status"])
	assert.Empty(t, blockerMap["ResolvedAt"])

	// Resolve the blocker.
	updRec := doRequest(t, h, "UpdateSyncBlocker", map[string]any{
		"Id":             blocker.ID,
		"ResolvedReason": "Config drift corrected",
		"ResourceName":   "blocker-lifecycle-stack",
		"SyncType":       "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	// GetSyncBlockerSummary should now show RESOLVED.
	sumRec2 := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "blocker-lifecycle-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, sumRec2.Code)
	sumResp2 := parseResp(t, sumRec2)
	summary2 := sumResp2["SyncBlockerSummary"].(map[string]any)
	blockers2, ok := summary2["LatestBlockers"].([]any)
	require.True(t, ok)
	require.Len(t, blockers2, 1)
	blockerMap2 := blockers2[0].(map[string]any)
	assert.Equal(t, "RESOLVED", blockerMap2["Status"])
	assert.NotEmpty(t, blockerMap2["ResolvedAt"])
	assert.Equal(t, "Config drift corrected", blockerMap2["ResolvedReason"])
}

// --- Sync blocker cleanup on DeleteSyncConfiguration ---

func TestAudit2_SyncBlocker_CleanedUpOnDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "blocker-cleanup-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "blocker-cleanup-repo")

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "blocker-cleanup-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	_, err := h.Backend.CreateSyncBlocker(
		context.Background(),
		"blocker-cleanup-stack", "CFN_STACK_SYNC",
		"MANUAL", "Manual block",
	)
	require.NoError(t, err)

	// Delete the sync configuration.
	delRec := doRequest(t, h, "DeleteSyncConfiguration", map[string]any{
		"ResourceName": "blocker-cleanup-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// GetSyncBlockerSummary should now fail (config deleted).
	getRec := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "blocker-cleanup-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code)
}

// --- UpdateSyncBlocker with unknown ID returns gracefully ---

func TestAudit2_UpdateSyncBlocker_UnknownID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UpdateSyncBlocker", map[string]any{
		"Id":             "unknown-blocker-id",
		"ResolvedReason": "just in case",
		"ResourceName":   "some-stack",
		"SyncType":       "CFN_STACK_SYNC",
	})
	// AWS returns 200 even for unknown blocker IDs (graceful).
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- UpdateSyncBlocker requires Id ---

func TestAudit2_UpdateSyncBlocker_MissingId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateSyncBlocker", map[string]any{
		"ResolvedReason": "no id here",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- RepositoryLink duplicate detection ---

func TestAudit2_CreateRepositoryLink_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "dup-link-conn", "GitHub")

	body := map[string]any{
		"ConnectionArn":  connArn,
		"OwnerId":        "my-org",
		"RepositoryName": "my-repo",
	}

	rec1 := doRequest(t, h, "CreateRepositoryLink", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateRepositoryLink", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// --- RepositoryLink EncryptionKeyArn roundtrip ---

func TestAudit2_RepositoryLink_EncryptionKeyArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "enc-key-conn", "GitHub")
	const encKey = "arn:aws:kms:us-east-1:000000000000:key/my-key"

	rec := doRequest(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":    connArn,
		"OwnerId":          "my-org",
		"RepositoryName":   "encrypted-repo",
		"EncryptionKeyArn": encKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	info := resp["RepositoryLinkInfo"].(map[string]any)
	assert.Equal(t, encKey, info["EncryptionKeyArn"])

	// UpdateRepositoryLink can update the encryption key.
	linkID := info["RepositoryLinkId"].(string)
	const newEncKey = "arn:aws:kms:us-east-1:000000000000:key/new-key"
	updRec := doRequest(t, h, "UpdateRepositoryLink", map[string]any{
		"RepositoryLinkId": linkID,
		"EncryptionKeyArn": newEncKey,
	})
	require.Equal(t, http.StatusOK, updRec.Code)
	updResp := parseResp(t, updRec)
	updInfo := updResp["RepositoryLinkInfo"].(map[string]any)
	assert.Equal(t, newEncKey, updInfo["EncryptionKeyArn"])
}

// --- GetRepositorySyncStatus with sync events ---

func TestAudit2_GetRepositorySyncStatus_WithEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "sync-events-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "sync-events-repo")

	events := []codestarconnections.SyncEvent{
		{Event: "start", Type: "info"},
		{Event: "apply", Type: "info", ExternalID: "ext-123"},
	}
	h.Backend.SetRepositorySyncStatus(
		context.Background(),
		linkID, "main", "CFN_STACK_SYNC",
		"SUCCEEDED", events,
	)

	rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
		"RepositoryLinkId": linkID,
		"Branch":           "main",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	latest := resp["LatestSync"].(map[string]any)
	evts, ok := latest["Events"].([]any)
	require.True(t, ok)
	require.Len(t, evts, 2)

	evt0 := evts[0].(map[string]any)
	assert.Equal(t, "start", evt0["Event"])
	assert.Equal(t, "info", evt0["Type"])
	assert.Empty(t, evt0["ExternalId"])

	evt1 := evts[1].(map[string]any)
	assert.Equal(t, "apply", evt1["Event"])
	assert.Equal(t, "ext-123", evt1["ExternalId"])
}

// --- Connection: missing HostArn in output when not set ---

func TestAudit2_Connection_HostArnOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "no-host-conn", "GitHub")

	rec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	conn := resp["Connection"].(map[string]any)
	_, hasHostArn := conn["HostArn"]
	assert.False(t, hasHostArn, "HostArn should be omitted when empty")
}

// --- Connection: HostArn appears in output when set ---

func TestAudit2_Connection_HostArnIncludedWhenSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const fakeHostArn = "arn:aws:codestar-connections:us-east-1:000000000000:host/myhost/abc12345"

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "has-host-conn",
		"ProviderType":   "GitHubEnterpriseServer",
		"HostArn":        fakeHostArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	connArn := resp["ConnectionArn"].(string)

	getRec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
	require.Equal(t, http.StatusOK, getRec.Code)

	getResp := parseResp(t, getRec)
	conn := getResp["Connection"].(map[string]any)
	assert.Equal(t, fakeHostArn, conn["HostArn"])
}

// --- Error type mapping ---

func TestAudit2_ErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      string
		body        map[string]any
		wantErrType string
	}{
		{
			name:   "not found returns ResourceNotFoundException",
			action: "GetConnection",
			body: map[string]any{
				"ConnectionArn": "arn:aws:codestar-connections:us-east-1:000000000000:connection/nonexistent",
			},
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "duplicate name returns InvalidInputException",
			action:      "CreateConnection",
			body:        map[string]any{"ConnectionName": "dup-err-conn", "ProviderType": "GitHub"},
			wantErrType: "InvalidInputException",
		},
		{
			name:        "validation error returns ValidationException",
			action:      "CreateConnection",
			body:        map[string]any{"ConnectionName": "valid-err-conn", "ProviderType": "INVALID"},
			wantErrType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// For duplicate test, create the connection first.
			if tt.name == "duplicate name returns InvalidInputException" {
				rec := doRequest(t, h, "CreateConnection", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateConnection", tt.body)
			if tt.action != "CreateConnection" {
				rec = doRequest(t, h, tt.action, tt.body)
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			resp := parseResp(t, rec)
			assert.Equal(t, tt.wantErrType, resp["__type"], "for test %q", tt.name)
		})
	}
}

// --- UpdateHost: ProviderEndpoint update ---

func TestAudit2_UpdateHost_ProviderEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		newEndpoint string
		wantOK      bool
	}{
		{
			name:        "update endpoint succeeds",
			newEndpoint: "https://new-ghe.example.com",
			wantOK:      true,
		},
		{
			name:        "empty endpoint is no-op",
			newEndpoint: "",
			wantOK:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			hostArn := createCSCHost(t, h, "upd-ep-host", "GitHubEnterpriseServer", "https://old.example.com")

			rec := doRequest(t, h, "UpdateHost", map[string]any{
				"HostArn":          hostArn,
				"ProviderEndpoint": tt.newEndpoint,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify the update by getting the host.
			if tt.newEndpoint != "" {
				getRec := doRequest(t, h, "GetHost", map[string]any{"HostArn": hostArn})
				require.Equal(t, http.StatusOK, getRec.Code)
				resp := parseResp(t, getRec)
				assert.Equal(t, tt.newEndpoint, resp["ProviderEndpoint"])
			}
		})
	}
}

// --- ListConnections: empty response has non-nil Connections ---

func TestAudit2_ListConnections_EmptyIsArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListConnections", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	conns, ok := resp["Connections"].([]any)
	require.True(t, ok, "Connections should be an array, not null")
	assert.Empty(t, conns)
}

// --- ListHosts: empty response has non-nil Hosts ---

func TestAudit2_ListHosts_EmptyIsArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListHosts", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	hosts, ok := resp["Hosts"].([]any)
	require.True(t, ok, "Hosts should be an array, not null")
	assert.Empty(t, hosts)
}

// --- ListRepositoryLinks: empty is non-nil array ---

func TestAudit2_ListRepositoryLinks_EmptyIsArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListRepositoryLinks", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	links, ok := resp["RepositoryLinks"].([]any)
	require.True(t, ok, "RepositoryLinks should be an array, not null")
	assert.Empty(t, links)
}

// --- GetSyncBlockerSummary: empty blocker list is non-nil ---

func TestAudit2_GetSyncBlockerSummary_EmptyBlockersIsArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "empty-blocker-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "empty-blocker-repo")

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "empty-blocker-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	sumRec := doRequest(t, h, "GetSyncBlockerSummary", map[string]any{
		"ResourceName": "empty-blocker-stack",
		"SyncType":     "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, sumRec.Code)

	resp := parseResp(t, sumRec)
	summary := resp["SyncBlockerSummary"].(map[string]any)
	blockers, ok := summary["LatestBlockers"].([]any)
	require.True(t, ok, "LatestBlockers should be an array, not null")
	assert.Empty(t, blockers)
}

// --- GetRepositorySyncStatus: StartedAt is RFC3339 ---

func TestAudit2_GetRepositorySyncStatus_StartedAtFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "ts-format-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "ts-format-repo")

	rec := doRequest(t, h, "GetRepositorySyncStatus", map[string]any{
		"RepositoryLinkId": linkID,
		"Branch":           "main",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	latest := resp["LatestSync"].(map[string]any)
	startedAt, ok := latest["StartedAt"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, startedAt)
	// RFC3339 contains 'T' between date and time.
	assert.Contains(t, startedAt, "T", "StartedAt must be RFC3339 formatted")
}

// --- CreateConnection: Tags NOT returned in CreateConnection response ---

func TestAudit2_CreateConnection_TagsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "tag-rt-conn",
		"ProviderType":   "GitHub",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	_, hasTags := resp["Tags"]
	assert.False(t, hasTags, "CreateConnection must not include Tags in response")

	arn := resp["ConnectionArn"].(string)
	recTags := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, recTags.Code)

	tagsResp := parseResp(t, recTags)
	tags := tagsResp["Tags"].([]any)
	require.Len(t, tags, 2)

	tag0 := tags[0].(map[string]any)
	tag1 := tags[1].(map[string]any)
	assert.Equal(t, "env", tag0["Key"])
	assert.Equal(t, "team", tag1["Key"])
}

// --- Host: Tags NOT returned in CreateHost response ---

func TestAudit2_CreateHost_TagsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "tagged-host",
		"ProviderType":     "GitHubEnterpriseServer",
		"ProviderEndpoint": "https://ghe.example.com",
		"Tags": []map[string]string{
			{"Key": "cost-center", "Value": "engineering"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	_, hasTags := resp["Tags"]
	assert.False(t, hasTags, "CreateHost must not include Tags in response")

	hostArn := resp["HostArn"].(string)
	recTags := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
	require.Equal(t, http.StatusOK, recTags.Code)

	tagsResp := parseResp(t, recTags)
	tags := tagsResp["Tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "cost-center", tag["Key"])
}

// --- ListSyncConfigurations: filter by SyncType ---

func TestAudit2_ListSyncConfigurations_SyncTypeFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "filter-sync-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "filter-sync-repo")

	for i := range 3 {
		rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
			"Branch":           "main",
			"ConfigFile":       "cfg.yaml",
			"RepositoryLinkId": linkID,
			"ResourceName":     "filter-stack-" + string(rune('a'+i)),
			"RoleArn":          "arn:aws:iam::000000000000:role/r",
			"SyncType":         "CFN_STACK_SYNC",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Filter by SyncType should return all 3 (only one type supported).
	rec := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	cfgs, ok := resp["SyncConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs, 3)

	// Empty SyncType filter returns all.
	rec2 := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	cfgs2, ok := resp2["SyncConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, cfgs2, 3)
}

// --- ListSyncConfigurations: sorted by ResourceName ---

func TestAudit2_ListSyncConfigurations_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "sorted-sync-conn", "GitHub")
	linkID := createCSCRepositoryLink(t, h, connArn, "sorted-sync-repo")

	names := []string{"zebra-stack", "alpha-stack", "mango-stack"}
	for _, name := range names {
		rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
			"Branch":           "main",
			"ConfigFile":       "cfg.yaml",
			"RepositoryLinkId": linkID,
			"ResourceName":     name,
			"RoleArn":          "arn:aws:iam::000000000000:role/r",
			"SyncType":         "CFN_STACK_SYNC",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListSyncConfigurations", map[string]any{
		"RepositoryLinkId": linkID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	cfgs, ok := resp["SyncConfigurations"].([]any)
	require.True(t, ok)
	require.Len(t, cfgs, 3)

	cfg0 := cfgs[0].(map[string]any)
	cfg1 := cfgs[1].(map[string]any)
	cfg2 := cfgs[2].(map[string]any)
	assert.Equal(t, "alpha-stack", cfg0["ResourceName"])
	assert.Equal(t, "mango-stack", cfg1["ResourceName"])
	assert.Equal(t, "zebra-stack", cfg2["ResourceName"])
}

// --- ConnectionStatus is AVAILABLE on creation ---

func TestAudit2_Connection_StatusAvailableOnCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "status-avail-conn", "GitHub")

	rec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	conn := resp["Connection"].(map[string]any)
	assert.Equal(t, "AVAILABLE", conn["ConnectionStatus"])
}

// --- HostStatus is PENDING on creation ---

func TestAudit2_Host_StatusAvailableOnCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	hostArn := createCSCHost(t, h, "status-avail-host", "GitHubEnterpriseServer", "https://ghe.example.com")

	rec := doRequest(t, h, "GetHost", map[string]any{"HostArn": hostArn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	assert.Equal(t, "PENDING", resp["Status"])
}

// --- Backend: Reset clears all state ---

func TestAudit2_Backend_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCSCConn(t, h, "reset-conn", "GitHub")
	createCSCHost(t, h, "reset-host", "GitHubEnterpriseServer", "https://x.com")

	assert.Equal(t, 1, h.Backend.ConnectionCount())
	assert.Equal(t, 1, h.Backend.HostCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.ConnectionCount())
	assert.Equal(t, 0, h.Backend.HostCount())
}

// --- GetSyncConfiguration: OwnerId and ProviderType derived from link ---

func TestAudit2_GetSyncConfiguration_DerivedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "derived-conn", "GitLab")
	linkID := createCSCRepositoryLink(t, h, connArn, "derived-repo")

	rec := doRequest(t, h, "CreateSyncConfiguration", map[string]any{
		"Branch":           "main",
		"ConfigFile":       "cfg.yaml",
		"RepositoryLinkId": linkID,
		"ResourceName":     "derived-stack",
		"RoleArn":          "arn:aws:iam::000000000000:role/r",
		"SyncType":         "CFN_STACK_SYNC",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	cfg := resp["SyncConfiguration"].(map[string]any)
	assert.Equal(t, "GitLab", cfg["ProviderType"])
	assert.Equal(t, "my-org", cfg["OwnerId"])
	assert.Equal(t, "derived-repo", cfg["RepositoryName"])
}

// --- GetRepositoryLink: all fields present ---

func TestAudit2_GetRepositoryLink_AllFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "field-conn", "GitHub")
	const encKey = "arn:aws:kms:us-east-1:000000000000:key/kk"

	rec := doRequest(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":    connArn,
		"OwnerId":          "github-org",
		"RepositoryName":   "my-service",
		"EncryptionKeyArn": encKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	info := resp["RepositoryLinkInfo"].(map[string]any)
	linkID := info["RepositoryLinkId"].(string)

	getRec := doRequest(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": linkID})
	require.Equal(t, http.StatusOK, getRec.Code)

	getResp := parseResp(t, getRec)
	getLinkInfo := getResp["RepositoryLinkInfo"].(map[string]any)
	assert.NotEmpty(t, getLinkInfo["RepositoryLinkId"])
	assert.NotEmpty(t, getLinkInfo["RepositoryLinkArn"])
	assert.Equal(t, "github-org", getLinkInfo["OwnerId"])
	assert.Equal(t, "my-service", getLinkInfo["RepositoryName"])
	assert.Equal(t, "GitHub", getLinkInfo["ProviderType"])
	assert.Equal(t, connArn, getLinkInfo["ConnectionArn"])
	assert.Equal(t, encKey, getLinkInfo["EncryptionKeyArn"])
}

// --- Pagination: no next token when all items fit on one page ---

func TestAudit2_Pagination_NoNextTokenWhenFits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		createCSCConn(t, h, "fit-conn-"+string(rune('a'+i)), "GitHub")
	}

	// MaxResults=10 should show all 3 with no NextToken.
	rec := doRequest(t, h, "ListConnections", map[string]any{"MaxResults": 10})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	conns, ok := resp["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns, 3)
	assert.Empty(t, resp["NextToken"])
}

// --- Host: all provider types accepted ---

func TestAudit2_CreateHost_AllProviderTypes(t *testing.T) {
	t.Parallel()

	// All provider types should be accepted for hosts.
	types := []string{"Bitbucket", "GitHub", "GitHubEnterpriseServer", "GitLab", "GitLabSelfManaged"}

	for i, pt := range types {
		t.Run(pt, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateHost", map[string]any{
				"Name":             "host-" + pt + string(rune('0'+i)),
				"ProviderType":     pt,
				"ProviderEndpoint": "https://x.com",
			})
			assert.Equal(t, http.StatusOK, rec.Code, "provider type %q should be accepted", pt)
		})
	}
}
