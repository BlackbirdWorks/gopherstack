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

// TestConnectionNameNotUnique verifies a duplicate ConnectionName is accepted,
// not rejected. CreateConnection's real error list is exactly
// [LimitExceededException, ResourceNotFoundException, ResourceUnavailableException]
// (aws-sdk-go-v2/service/codeconnections@v1.13.4 deserializers.go:120-134,
// awsAwsjson10_deserializeOpErrorCreateConnection's own switch) -- no
// ResourceAlreadyExistsException, unlike sibling ops CreateRepositoryLink and
// CreateSyncConfiguration in the same service, which both do list it
// (deserializers.go:349/478 case blocks). That contrast shows the omission is
// deliberate modeling, not an oversight, so a real client's second create
// call for the same name succeeds with a distinct ARN.
func TestConnectionNameNotUnique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		connName string
	}{
		{name: "unique_name_succeeds", connName: "only-created-once"},
		{name: "duplicate_name_also_succeeds", connName: "duplicate-target"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			arn1 := createConn(t, h, tt.connName, "GitHub")

			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": tt.connName,
				"ProviderType":   "GitHub",
			})
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			arn2 := parseResp(t, rec)["ConnectionArn"].(string)
			assert.NotEqual(t, arn1, arn2, "duplicate-named connections must still get distinct ARNs")
		})
	}
}

// TestProviderTypeValidation verifies invalid provider types are rejected.
func TestProviderTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		providerType string
		wantStatus   int
	}{
		{name: "valid_github", providerType: "GitHub", wantStatus: http.StatusOK},
		{name: "valid_gitlab", providerType: "GitLab", wantStatus: http.StatusOK},
		{name: "valid_bitbucket", providerType: "Bitbucket", wantStatus: http.StatusOK},
		{name: "valid_ghe", providerType: "GitHubEnterpriseServer", wantStatus: http.StatusOK},
		{
			name:         "invalid_provider",
			providerType: "InvalidProvider",
			wantStatus:   http.StatusBadRequest,
		},
		{name: "empty_provider_rejected", providerType: "", wantStatus: http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "conn-" + strconv.Itoa(i),
				"ProviderType":   tt.providerType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCreateConnectionAllProviderTypes verifies all valid provider types are accepted.
func TestCreateConnectionAllProviderTypes(t *testing.T) {
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
		// AzureDevOps is a real CodeConnections provider type (types.ProviderTypeAzureDevOps
		// in aws-sdk-go-v2/service/codeconnections/types/enums.go) -- unlike the older
		// CodeStarConnections service, which predates it and has no such enum value.
		{name: "azure_devops", providerType: "AzureDevOps", wantStatus: http.StatusOK},
		{name: "invalid_type", providerType: "NotARealProvider", wantStatus: http.StatusBadRequest},
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

// TestErrAlreadyExistsMapping verifies that ErrAlreadyExists maps to a 400
// with the ResourceAlreadyExistsException wire type, using
// CreateRepositoryLink -- unlike CreateConnection/CreateHost, its real error
// list does include ResourceAlreadyExistsException (deserializers.go:349).
func TestErrAlreadyExistsMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "duplicate_repository_link_is_400", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			connArn := createConn(t, h, "dup-link-conn", "GitHub")
			createRepositoryLink(t, h, connArn, "my-org", "my-repo")

			rec2 := doJSON(t, h, "CreateRepositoryLink", map[string]any{
				"ConnectionArn":  connArn,
				"OwnerId":        "my-org",
				"RepositoryName": "my-repo",
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
			resp := parseResp(t, rec2)
			assert.Equal(t, "ResourceAlreadyExistsException", resp["__type"])
		})
	}
}

// TestDeleteConnectionCleansIndex verifies the name index is cleared on delete.
func TestDeleteConnectionCleansIndex(t *testing.T) {
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
			arn1 := createConn(t, h, "reuse-conn", "GitHub")

			delRec := doJSON(t, h, "DeleteConnection", map[string]any{"ConnectionArn": arn1})
			require.Equal(t, http.StatusOK, delRec.Code)

			// After deletion, same name should be allowed.
			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "reuse-conn",
				"ProviderType":   "GitLab",
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// TestGetConnectionOwnerAccountId verifies OwnerAccountId is present and non-empty.
func TestGetConnectionOwnerAccountId(t *testing.T) {
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

// TestGetConnectionIncludesTags verifies Tags are included in GetConnection response.
func TestGetConnectionIncludesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantTags int
	}{
		{name: "tags_in_get_response", wantTags: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "tagged-conn",
				"ProviderType":   "GitHub",
				"Tags": []map[string]string{
					{"Key": "Env", "Value": "prod"},
					{"Key": "Owner", "Value": "team"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			connArn := parseResp(t, rec)["ConnectionArn"].(string)

			getRec := doJSON(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
			require.Equal(t, http.StatusOK, getRec.Code)
			resp := parseResp(t, getRec)
			conn, ok := resp["Connection"].(map[string]any)
			require.True(t, ok)
			tags, ok := conn["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

// TestCreateConnectionReturnsTags verifies Tags are returned in CreateConnection response.
func TestCreateConnectionReturnsTags(t *testing.T) {
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

// TestCreateConnectionTagsSortedInResponse verifies tags are alphabetically sorted.
func TestCreateConnectionTagsSortedInResponse(t *testing.T) {
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

// TestCreateConnectionHostArn verifies HostArn is accepted and round-tripped.
func TestCreateConnectionHostArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		withHost   bool
		wantArn    bool
	}{
		{
			name:       "with_host_arn",
			withHost:   true,
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
		{
			name:       "without_host_arn",
			withHost:   false,
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			body := map[string]any{
				"ConnectionName": "ghe-conn",
				"ProviderType":   "GitHubEnterpriseServer",
			}
			if tt.withHost {
				// CreateConnection now validates HostArn against a real,
				// previously-created host (ResourceNotFoundException for an
				// unknown one), so the host must exist first.
				body["HostArn"] = createHost(t, h, "ghe-host", "GitHubEnterpriseServer", "https://ghe.example.com")
			}

			rec := doJSON(t, h, "CreateConnection", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantArn {
				resp := parseResp(t, rec)
				assert.NotEmpty(t, resp["ConnectionArn"])
			}
		})
	}
}

// TestCreateConnectionHostArnNotFound verifies that CreateConnection rejects
// a HostArn that does not reference an existing host. Real CreateConnection's
// error list is [LimitExceededException, ResourceNotFoundException,
// ResourceUnavailableException] (botocore codeconnections/2023-12-01/
// service-2.json) -- a bad HostArn maps to ResourceNotFoundException, the
// same real type GetHost/DeleteHost use for a missing host. Previously this
// was accepted without any validation at all.
func TestCreateConnectionHostArnNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		hostArn string
	}{
		{name: "nonexistent_host", hostArn: "arn:aws:codeconnections:us-east-1:123456789012:host/nonexistent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "ghe-conn-badhost",
				"ProviderType":   "GitHubEnterpriseServer",
				"HostArn":        tt.hostArn,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

// TestGetConnectionHostArnPreserved verifies HostArn is stored and returned.
func TestGetConnectionHostArnPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "host_arn_preserved"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			// CreateConnection now validates HostArn against a real,
			// previously-created host, so the host must exist first.
			hostArn := createHost(t, h, "myhost", "GitHubEnterpriseServer", "https://ghe.example.com")

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
			assert.Equal(t, hostArn, conn["HostArn"])
		})
	}
}

// TestHostArnFilter verifies HostArnFilter in ListConnections.
func TestHostArnFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantCount   int
		applyFilter bool
	}{
		{name: "no_filter_returns_all", wantCount: 2, applyFilter: false},
		{name: "host_arn_filter_returns_one", wantCount: 1, applyFilter: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codeconnections.NewInMemoryBackend("123456789012", "us-east-1")

			// Seed two connections: one with HostArn, one without.
			conn1 := &codeconnections.Connection{
				ConnectionName: "with-host",
				ConnectionArn:  "arn:aws:codeconnections:us-east-1:123456789012:connection/aaa",
				ProviderType:   "GitHubEnterpriseServer",
				HostArn:        "arn:aws:codeconnections:us-east-1:123456789012:host/hst-1",
				Status:         "AVAILABLE",
				OwnerAccountID: "123456789012",
				Tags:           map[string]string{},
			}
			conn2 := &codeconnections.Connection{
				ConnectionName: "no-host",
				ConnectionArn:  "arn:aws:codeconnections:us-east-1:123456789012:connection/bbb",
				ProviderType:   "GitHub",
				Status:         "AVAILABLE",
				OwnerAccountID: "123456789012",
				Tags:           map[string]string{},
			}
			b.AddConnectionInternal(context.Background(), conn1)
			b.AddConnectionInternal(context.Background(), conn2)

			filter := ""
			if tt.applyFilter {
				filter = "arn:aws:codeconnections:us-east-1:123456789012:host/hst-1"
			}

			conns := b.ListConnections(context.Background(), "", filter)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}

// TestListConnectionsHostArnFilterAfterCreate verifies that connections created with
// HostArn can be retrieved using the HostArnFilter.
func TestListConnectionsHostArnFilterAfterCreate(t *testing.T) {
	t.Parallel()

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

			// CreateConnection now validates HostArn against a real,
			// previously-created host, so the host must exist first.
			hostArn := createHost(t, h, "myhost", "GitHubEnterpriseServer", "https://ghe.example.com")

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

// TestListConnectionsProviderTypeFilter verifies ProviderTypeFilter in ListConnections.
func TestListConnectionsProviderTypeFilter(t *testing.T) {
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

// TestBackendCreateConnectionHostArn verifies hostArn is stored in backend.
func TestBackendCreateConnectionHostArn(t *testing.T) {
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

			if tt.hostArn != "" {
				// CreateConnection now validates HostArn against a real,
				// previously-created host, so the host must exist first.
				b.AddHostInternal(context.Background(), &codeconnections.Host{
					HostArn:          tt.hostArn,
					Name:             "h1",
					ProviderType:     "GitHubEnterpriseServer",
					ProviderEndpoint: "https://ghe.example.com",
					Status:           "AVAILABLE",
					Tags:             map[string]string{},
				})
			}

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
