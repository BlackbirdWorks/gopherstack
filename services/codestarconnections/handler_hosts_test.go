package codestarconnections_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

func TestHandler_CreateHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			body: map[string]any{
				"Name":             "my-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://example.com",
			},
			wantErr: false,
		},
		{
			name:    "missing name",
			body:    map[string]any{"ProviderType": "GitHubEnterpriseServer"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateHost", tt.body)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out["HostArn"])
		})
	}
}

func TestHandler_CreateHost_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"Name":             "dup-host",
		"ProviderType":     "GitHubEnterpriseServer",
		"ProviderEndpoint": "https://x.com",
	}
	rec1 := doRequest(t, h, "CreateHost", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateHost", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_GetHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost(
					context.Background(),
					"test-host",
					"GitHubEnterpriseServer",
					"https://example.com",
					nil,
					nil,
				)
				if err != nil {
					return ""
				}

				return host.HostArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:host/nonexistent/abc12345"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "GetHost", map[string]any{"HostArn": arn})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, "test-host", out["Name"])
			assert.Equal(t, "GitHubEnterpriseServer", out["ProviderType"])
			assert.Equal(t, "https://example.com", out["ProviderEndpoint"])
			assert.Equal(t, "PENDING", out["Status"])
		})
	}
}

func TestHandler_ListHosts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateHost(context.Background(), "host1", "GitHubEnterpriseServer", "https://a.com", nil, nil)
	_, _ = h.Backend.CreateHost(context.Background(), "host2", "GitHubEnterpriseServer", "https://b.com", nil, nil)

	rec := doRequest(t, h, "ListHosts", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	hosts, ok := out["Hosts"].([]any)
	require.True(t, ok)
	assert.Len(t, hosts, 2)
}

func TestHandler_DeleteHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost(
					context.Background(),
					"del-host",
					"GitHubEnterpriseServer",
					"https://x.com",
					nil,
					nil,
				)
				if err != nil {
					return ""
				}

				return host.HostArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:host/nonexistent/abc12345"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "DeleteHost", map[string]any{"HostArn": arn})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestHandler_UpdateHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost(
					context.Background(),
					"upd-host",
					"GitHubEnterpriseServer",
					"https://old.com",
					nil,
					nil,
				)
				if err != nil {
					return ""
				}

				return host.HostArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:host/nonexistent/abc12345"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "UpdateHost", map[string]any{"HostArn": arn, "ProviderEndpoint": "https://new.com"})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// TestHostNameUniqueness verifies duplicate host names are rejected.
func TestHostNameUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec1 := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "host-a",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "host-a",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://other.com",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestDeleteHostCleansIndex verifies hostsByName index is updated on delete.
func TestDeleteHostCleansIndex(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "myhost",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	hostArn := resp["HostArn"].(string)

	rec2 := doRequest(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Re-create with same name must succeed.
	rec3 := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "myhost",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestSortedListHosts verifies hosts are sorted by name.
func TestSortedListHosts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zoo-host", "apple-host", "mango-host"} {
		rec := doRequest(t, h, "CreateHost", map[string]any{
			"Name":             name,
			"ProviderType":     "GitHub",
			"ProviderEndpoint": "https://example.com",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListHosts", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	hosts := out["Hosts"].([]any)
	require.Len(t, hosts, 3)

	names := make([]string, len(hosts))
	for i, host := range hosts {
		names[i] = host.(map[string]any)["Name"].(string)
	}

	assert.Equal(t, []string{"apple-host", "mango-host", "zoo-host"}, names)
}

// TestGetHostIncludesTags verifies Tags are NOT in GetHost response
// but ARE accessible via ListTagsForResource.
func TestGetHostIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateHost", map[string]any{
		"Name":             "tagged-host",
		"ProviderType":     "GitHub",
		"ProviderEndpoint": "https://example.com",
		"Tags": []map[string]any{
			{"Key": "team", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	hostArn := parseResp(t, rec)["HostArn"].(string)

	recGet := doRequest(t, h, "GetHost", map[string]any{"HostArn": hostArn})
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	_, hasTags := out["Tags"]
	assert.False(t, hasTags, "GetHost should not include Tags in response")

	recTags := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": hostArn})
	require.Equal(t, http.StatusOK, recTags.Code)

	var tagsOut map[string]any
	require.NoError(t, json.Unmarshal(recTags.Body.Bytes(), &tagsOut))
	tags := tagsOut["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "platform", tags[0].(map[string]any)["Value"].(string))
}

// TestInMemoryBackend_HostTagsNonNil verifies CreateHost always sets a non-nil Tags map.
func TestInMemoryBackend_HostTagsNonNil(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	host, err := b.CreateHost(context.Background(), "no-tag-host", "GitHub", "https://example.com", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, host.Tags, "Tags must never be nil")
}
