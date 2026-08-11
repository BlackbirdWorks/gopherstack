package codeconnections_test

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestCreateHost exercises the CreateHost handler.
func TestCreateHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantArn    bool
	}{
		{
			name: "success",
			body: map[string]any{
				"Name":             "my-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://ghe.example.com",
			},
			wantStatus: http.StatusOK,
			wantArn:    true,
		},
		{
			name: "missing_name",
			body: map[string]any{
				"ProviderEndpoint": "https://ghe.example.com",
				"ProviderType":     "GitHubEnterpriseServer",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_endpoint",
			body:       map[string]any{"Name": "my-host", "ProviderType": "GitHubEnterpriseServer"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doJSON(t, h, "CreateHost", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantArn {
				resp := parseResp(t, rec)
				assert.NotEmpty(t, resp["HostArn"])
			}
		})
	}
}

// TestGetHost exercises the GetHost handler.
func TestGetHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupHostArn   func(t *testing.T, h *codeconnections.Handler) string
		name           string
		wantName       string
		wantEndpoint   string
		wantHostStatus string
		wantStatus     int
	}{
		{
			name: "success",
			setupHostArn: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createHost(
					t,
					h,
					"my-host",
					"GitHubEnterpriseServer",
					"https://ghe.example.com",
				)
			},
			wantStatus:     http.StatusOK,
			wantName:       "my-host",
			wantEndpoint:   "https://ghe.example.com",
			wantHostStatus: "AVAILABLE",
		},
		{
			name: "not_found",
			setupHostArn: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:host/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_arn",
			setupHostArn: func(_ *testing.T, _ *codeconnections.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn := tt.setupHostArn(t, h)
			rec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				assert.Equal(t, tt.wantName, resp["Name"])
				assert.Equal(t, tt.wantEndpoint, resp["ProviderEndpoint"])
				assert.Equal(t, tt.wantHostStatus, resp["Status"])
			}
		})
	}
}

// TestDeleteHost exercises the DeleteHost handler.
func TestDeleteHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupHostArn func(t *testing.T, h *codeconnections.Handler) string
		name         string
		wantStatus   int
	}{
		{
			name: "success",
			setupHostArn: func(t *testing.T, h *codeconnections.Handler) string {
				t.Helper()

				return createHost(
					t,
					h,
					"my-host",
					"GitHubEnterpriseServer",
					"https://ghe.example.com",
				)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setupHostArn: func(_ *testing.T, _ *codeconnections.Handler) string {
				return "arn:aws:codeconnections:us-east-1:123:host/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn := tt.setupHostArn(t, h)
			rec := doJSON(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				getRec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
				assert.Equal(t, http.StatusBadRequest, getRec.Code)
			}
		})
	}
}

// TestDeleteHost_InUse verifies that a host cannot be deleted while a connection
// still references it. Real AWS documents that all connections associated to a
// host must be deleted before the host itself can be deleted.
func TestDeleteHost_InUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		attachConn bool
	}{
		{name: "host_with_active_connection_rejected", attachConn: true, wantErr: codeconnections.ErrResourceInUse},
		{name: "host_with_no_connections_deletes", attachConn: false, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestBackend()

			host, err := b.CreateHost(ctx, "my-host", "GitHubEnterpriseServer", "https://ghe.example.com", nil)
			require.NoError(t, err)

			if tt.attachConn {
				_, connErr := b.CreateConnection(ctx, "my-conn", "GitHubEnterpriseServer", host.HostArn, nil)
				require.NoError(t, connErr)
			}

			err = b.DeleteHost(ctx, host.HostArn)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestDeleteHostInUseWireErrorType verifies that DeleteHost's in-use
// rejection serializes as the real ResourceUnavailableException type over
// HTTP, not the fabricated ConflictException a previous audit pass used.
// DeleteHost's real, complete error list (botocore codeconnections/
// 2023-12-01/service-2.json) is exactly [ResourceNotFoundException,
// ResourceUnavailableException]; ConflictException is not a possible error
// for this operation at all.
func TestDeleteHostInUseWireErrorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete_host_with_active_connection"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			hostArn := createHost(t, h, "wire-host", "GitHubEnterpriseServer", "https://ghe.example.com")

			rec := doJSON(t, h, "CreateConnection", map[string]any{
				"ConnectionName": "wire-conn",
				"ProviderType":   "GitHubEnterpriseServer",
				"HostArn":        hostArn,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doJSON(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
			require.Equal(t, http.StatusBadRequest, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, "ResourceUnavailableException", resp["__type"])
		})
	}
}

// TestCreateHostWithTags verifies that tags can be passed when creating a host.
func TestCreateHostWithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []map[string]string
		wantTags int
	}{
		{
			name: "host_with_tags",
			tags: []map[string]string{
				{"Key": "Owner", "Value": "ops"},
				{"Key": "Tier", "Value": "infra"},
			},
			wantTags: 2,
		},
		{
			name:     "host_without_tags",
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
			hostArn := parseResp(t, rec)["HostArn"].(string)

			getRec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
			require.Equal(t, http.StatusOK, getRec.Code)
			resp := parseResp(t, getRec)
			tags, _ := resp["Tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

// TestCC_ListHosts exercises the ListHosts handler on an empty backend.
func TestCC_ListHosts(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doJSON(t, h, "ListHosts", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	assert.NotNil(t, resp["Hosts"])
}

// TestHostArnFormat verifies CreateHost's HostArn has the expected shape.
func TestHostArnFormat(t *testing.T) {
	t.Parallel()

	h := newHandlerFixedAccount(t)
	rec := doJSON(t, h, "CreateHost", map[string]any{
		"Name":             "my-host",
		"ProviderType":     "GitHubEnterpriseServer",
		"ProviderEndpoint": "https://github.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := parseResp(t, rec)
	hostArn, _ := m["HostArn"].(string)
	assert.True(
		t,
		strings.HasPrefix(hostArn, ccFixedArnPrefix+"host/"),
		"HostArn should start with %s, got %s",
		ccFixedArnPrefix+"host/",
		hostArn,
	)
}

// TestHostCreate exercises CreateHost with duplicate/validation error cases.
func TestHostCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, m map[string]any)
		name     string
		wantCode int
	}{
		{
			name: "creates host returns HostArn",
			body: map[string]any{
				"Name":             "ghe-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://ghe.example.com",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				assert.NotEmpty(t, m["HostArn"])
			},
		},
		{
			// CreateHost has no ResourceAlreadyExistsException in its real
			// error list (see TestCreateHostNameNotUnique), so a second
			// create with the same name must also succeed.
			name: "duplicate name also succeeds",
			body: map[string]any{
				"Name":             "dup-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://x.example.com",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				assert.NotEmpty(t, m["HostArn"])
			},
		},
		{
			name: "missing provider type returns error",
			body: map[string]any{
				"Name":             "bad-host",
				"ProviderEndpoint": "https://x.example.com",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerFixedAccount(t)

			if tt.name == "duplicate name also succeeds" {
				rec := doJSON(t, h, "CreateHost", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doJSON(t, h, "CreateHost", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, parseResp(t, rec))
			}
		})
	}
}

// TestHostGet exercises GetHost field shapes and not-found handling.
func TestHostGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, m map[string]any)
		name     string
		wantCode int
		preload  bool
	}{
		{
			name:     "returns host fields",
			wantCode: http.StatusOK,
			preload:  true,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				assert.Equal(t, "describe-host", m["Name"])
				assert.NotEmpty(t, m["HostArn"])
				assert.NotEmpty(t, m["ProviderType"])
				assert.NotEmpty(t, m["ProviderEndpoint"])
				assert.NotEmpty(t, m["Status"])
			},
		},
		{
			name:     "not found returns error",
			wantCode: http.StatusBadRequest,
			preload:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerFixedAccount(t)

			hostArn := "arn:aws:codeconnections:us-east-1:000000000000:host/nonexistent"
			if tt.preload {
				rec := doJSON(t, h, "CreateHost", map[string]any{
					"Name":             "describe-host",
					"ProviderType":     "GitHubEnterpriseServer",
					"ProviderEndpoint": "https://ghe.example.com",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				hostArn = parseResp(t, rec)["HostArn"].(string)
			}

			rec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, parseResp(t, rec))
			}
		})
	}
}

// TestHostDelete exercises the full CreateHost/DeleteHost/GetHost lifecycle.
func TestHostDelete(t *testing.T) {
	t.Parallel()

	h := newHandlerFixedAccount(t)

	rec := doJSON(t, h, "CreateHost", map[string]any{
		"Name":             "del-host",
		"ProviderType":     "GitHubEnterpriseServer",
		"ProviderEndpoint": "https://ghe.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	hostArn := parseResp(t, rec)["HostArn"].(string)

	rec = doJSON(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGetHostIncludesHostArn verifies HostArn is returned in GetHost response.
func TestGetHostIncludesHostArn(t *testing.T) {
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

// TestCreateHostNameNotUnique verifies a duplicate host Name is accepted, not
// rejected. CreateHost's real error list is exactly [LimitExceededException]
// (aws-sdk-go-v2/service/codeconnections@v1.13.4 deserializers.go:237-241,
// awsAwsjson10_deserializeOpErrorCreateHost's own switch) -- no
// ResourceAlreadyExistsException, so a real client's second create call for
// the same name succeeds with a distinct ARN.
func TestCreateHostNameNotUnique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hostName string
	}{
		{name: "unique_name_succeeds", hostName: "unique-host"},
		{name: "duplicate_name_also_succeeds", hostName: "dup-host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn1 := createHost(t, h, tt.hostName, "GitHubEnterpriseServer", "https://a.example.com")

			rec := doJSON(t, h, "CreateHost", map[string]any{
				"Name":             tt.hostName,
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://b.example.com",
			})
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			arn2 := parseResp(t, rec)["HostArn"].(string)
			assert.NotEqual(t, arn1, arn2, "duplicate-named hosts must still get distinct ARNs")
		})
	}
}

// TestDeleteHostCleansNameIndex verifies host name can be reused after delete.
func TestDeleteHostCleansNameIndex(t *testing.T) {
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
