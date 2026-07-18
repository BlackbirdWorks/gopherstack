package codestarconnections_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantKey string
		wantErr bool
	}{
		{
			name:    "happy path",
			body:    map[string]any{"ConnectionName": "my-conn", "ProviderType": "GitHub"},
			wantErr: false,
			wantKey: "ConnectionArn",
		},
		{
			name:    "missing name",
			body:    map[string]any{"ProviderType": "GitHub"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateConnection", tt.body)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out[tt.wantKey])
		})
	}
}

func TestHandler_CreateConnection_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{"ConnectionName": "dup-conn", "ProviderType": "GitHub"}
	rec1 := doRequest(t, h, "CreateConnection", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateConnection", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_GetConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				conn, err := h.Backend.CreateConnection(context.Background(), "test-conn", "GitHub", "", nil)
				if err != nil {
					return ""
				}

				return conn.ConnectionArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:connection/nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": arn})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			conn, ok := out["Connection"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "test-conn", conn["ConnectionName"])
			assert.Equal(t, "GitHub", conn["ProviderType"])
			assert.Equal(t, "AVAILABLE", conn["ConnectionStatus"])
			assert.Equal(t, "000000000000", conn["OwnerAccountId"])
		})
	}
}

func TestHandler_ListConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn       func(h *codestarconnections.Handler) string
		filter        map[string]any
		name          string
		wantProviders []string
		wantCount     int
	}{
		{
			name: "list all",
			setupFn: func(h *codestarconnections.Handler) string {
				_, _ = h.Backend.CreateConnection(context.Background(), "conn1", "GitHub", "", nil)
				_, _ = h.Backend.CreateConnection(context.Background(), "conn2", "Bitbucket", "", nil)

				return ""
			},
			filter:    map[string]any{},
			wantCount: 2,
		},
		{
			name: "filter by provider type",
			setupFn: func(h *codestarconnections.Handler) string {
				_, _ = h.Backend.CreateConnection(context.Background(), "conn1", "GitHub", "", nil)
				_, _ = h.Backend.CreateConnection(context.Background(), "conn2", "Bitbucket", "", nil)

				return ""
			},
			filter:    map[string]any{"ProviderTypeFilter": "GitHub"},
			wantCount: 1,
		},
		{
			name: "filter by host arn",
			setupFn: func(h *codestarconnections.Handler) string {
				host, err := h.Backend.CreateHost(
					context.Background(),
					"my-host",
					"GitHubEnterpriseServer",
					"https://example.com",
					nil,
					nil,
				)
				if err != nil {
					return ""
				}

				_, _ = h.Backend.CreateConnection(
					context.Background(),
					"conn-with-host",
					"GitHubEnterpriseServer",
					host.HostArn,
					nil,
				)
				_, _ = h.Backend.CreateConnection(context.Background(), "conn-without-host", "GitHub", "", nil)

				return host.HostArn
			},
			filter:    map[string]any{},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			hostArn := tt.setupFn(h)

			filter := tt.filter
			if tt.name == "filter by host arn" {
				filter = map[string]any{"HostArnFilter": hostArn}
			}

			rec := doRequest(t, h, "ListConnections", filter)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			conns, ok := out["Connections"].([]any)
			require.True(t, ok)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}

func TestHandler_DeleteConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn func(h *codestarconnections.Handler) string
		name    string
		wantErr bool
	}{
		{
			name: "happy path",
			setupFn: func(h *codestarconnections.Handler) string {
				conn, err := h.Backend.CreateConnection(context.Background(), "del-conn", "GitHub", "", nil)
				if err != nil {
					return ""
				}

				return conn.ConnectionArn
			},
			wantErr: false,
		},
		{
			name: "not found",
			setupFn: func(_ *codestarconnections.Handler) string {
				return "arn:aws:codestar-connections:us-east-1:000000000000:connection/nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setupFn(h)

			rec := doRequest(t, h, "DeleteConnection", map[string]any{"ConnectionArn": arn})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// TestConnectionNameUniqueness verifies duplicate connection names are rejected.
func TestConnectionNameUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec1 := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "dup",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "dup",
		"ProviderType":   "GitHub",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestDeleteConnectionCleansIndex verifies connectionsByName index is updated on delete.
func TestDeleteConnectionCleansIndex(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "myconn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	arn := resp["ConnectionArn"].(string)

	// Delete the connection.
	rec2 := doRequest(t, h, "DeleteConnection", map[string]any{"ConnectionArn": arn})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Re-create with the same name must succeed (index was cleaned).
	rec3 := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "myconn",
		"ProviderType":   "GitHub",
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// TestSortedListConnections verifies connections are sorted by name.
func TestSortedListConnections(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra", "alpha", "middle"} {
		rec := doRequest(t, h, "CreateConnection", map[string]any{
			"ConnectionName": name,
			"ProviderType":   "GitHub",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListConnections", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	conns := out["Connections"].([]any)
	require.Len(t, conns, 3)

	names := make([]string, len(conns))
	for i, c := range conns {
		names[i] = c.(map[string]any)["ConnectionName"].(string)
	}

	assert.Equal(t, []string{"alpha", "middle", "zebra"}, names)
}

// TestGetConnectionIncludesTags verifies Tags are NOT in GetConnection response
// but ARE accessible via ListTagsForResource.
func TestGetConnectionIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "conn-with-tags",
		"ProviderType":   "GitHub",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	arn := parseResp(t, rec)["ConnectionArn"].(string)

	recGet := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": arn})
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	conn := out["Connection"].(map[string]any)
	_, hasTags := conn["Tags"]
	assert.False(t, hasTags, "GetConnection should not include Tags in response")

	recTags := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, recTags.Code)

	var tagsOut map[string]any
	require.NoError(t, json.Unmarshal(recTags.Body.Bytes(), &tagsOut))
	tags := tagsOut["Tags"].([]any)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"].(string))
}

// TestInMemoryBackend_ConnectionTagsNonNil verifies CreateConnection always sets a non-nil Tags map.
func TestInMemoryBackend_ConnectionTagsNonNil(t *testing.T) {
	t.Parallel()

	b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	conn, err := b.CreateConnection(context.Background(), "no-tag-conn", "GitHub", "", nil)
	require.NoError(t, err)
	require.NotNil(t, conn.Tags, "Tags must never be nil")
}
