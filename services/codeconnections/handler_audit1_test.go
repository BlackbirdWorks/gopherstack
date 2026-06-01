package codeconnections_test

// Audit batch-1: AWS-accuracy coverage for CodeConnections (go-huop).
//
// Covers:
//   - Protocol: Content-Type application/x-amz-json-1.0 on success and error paths
//   - Error envelope: __type + message fields on all error paths
//   - Non-POST → 405, missing X-Amz-Target → 400
//   - ARN format: connection and host ARNs contain "arn:aws:codeconnections:us-east-1:000000000000:"
//   - Connection CRUD: Create/Get/List/Delete lifecycle
//   - Host CRUD: Create/Get/Delete lifecycle
//   - Tag round-trip: TagResource/ListTagsForResource/UntagResource Key+Value shape
//   - RepositoryLink basics: Create/Get/Delete field shapes
//   - Error paths: ResourceNotFoundException/ResourceAlreadyExistsException/
//     ValidationException with correct HTTP 400 + __type+message envelope

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

const (
	a1AccountID = "000000000000"
	a1Region    = "us-east-1"
	a1ArnPrefix = "arn:aws:codeconnections:us-east-1:000000000000:"
)

func a1Handler(t *testing.T) *codeconnections.Handler {
	t.Helper()

	return codeconnections.NewHandler(codeconnections.NewInMemoryBackend(a1AccountID, a1Region))
}

func a1Do(t *testing.T, h *codeconnections.Handler, action string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "CodeConnections_20231201."+action)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func a1Parse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// --- Protocol tests ---

func TestAudit1_ContentType_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "CreateConnection",
			action: "CreateConnection",
			body:   map[string]any{"ConnectionName": "ct-conn", "ProviderType": "GitHub"},
		},
		{
			name:   "ListConnections",
			action: "ListConnections",
			body:   map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			rec := a1Do(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/x-amz-json-1.0")
		})
	}
}

func TestAudit1_ContentType_Error(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	rec := a1Do(
		t,
		h,
		"GetConnection",
		map[string]any{"ConnectionArn": "arn:aws:codeconnections:us-east-1:000000000000:connection/nonexistent"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/x-amz-json-1.0")
}

func TestAudit1_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	rec := a1Do(
		t,
		h,
		"GetConnection",
		map[string]any{"ConnectionArn": "arn:aws:codeconnections:us-east-1:000000000000:connection/nonexistent"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	m := a1Parse(t, rec)
	assert.Equal(t, "ResourceNotFoundException", m["__type"])
	assert.NotEmpty(t, m["message"])
}

func TestAudit1_NonPost_Returns405(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	req := httptest.NewRequest(http.MethodPut, "/", nil)
	req.Header.Set("X-Amz-Target", "CodeConnections_20231201.ListConnections")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestAudit1_MissingTarget_Returns400(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ARN format tests ---

func TestAudit1_ConnectionArn_Format(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	rec := a1Do(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "arn-test",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1Parse(t, rec)
	connArn, _ := m["ConnectionArn"].(string)
	assert.True(
		t,
		strings.HasPrefix(connArn, a1ArnPrefix+"connection/"),
		"ConnectionArn should start with %s, got %s",
		a1ArnPrefix+"connection/",
		connArn,
	)
}

func TestAudit1_HostArn_Format(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)
	rec := a1Do(t, h, "CreateHost", map[string]any{
		"Name":             "my-host",
		"ProviderType":     "GitHubEnterpriseServer",
		"ProviderEndpoint": "https://github.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1Parse(t, rec)
	hostArn, _ := m["HostArn"].(string)
	assert.True(
		t,
		strings.HasPrefix(hostArn, a1ArnPrefix+"host/"),
		"HostArn should start with %s, got %s",
		a1ArnPrefix+"host/",
		hostArn,
	)
}

// --- Connection CRUD tests ---

func TestAudit1_Connection_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, m map[string]any)
		name     string
		wantCode int
	}{
		{
			name: "creates with GitHub provider",
			body: map[string]any{
				"ConnectionName": "gh-conn",
				"ProviderType":   "GitHub",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				assert.NotEmpty(t, m["ConnectionArn"])
			},
		},
		{
			name: "creates with tags",
			body: map[string]any{
				"ConnectionName": "tagged-conn",
				"ProviderType":   "Bitbucket",
				"Tags": []map[string]any{
					{"Key": "env", "Value": "prod"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				tags, _ := m["Tags"].([]any)
				assert.Len(t, tags, 1)
			},
		},
		{
			name:     "duplicate name returns error",
			body:     map[string]any{"ConnectionName": "dup-conn", "ProviderType": "GitHub"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing provider type returns error",
			body:     map[string]any{"ConnectionName": "no-provider"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)

			if tt.name == "duplicate name returns error" {
				rec := a1Do(t, h, "CreateConnection", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := a1Do(t, h, "CreateConnection", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				m := a1Parse(t, rec)
				tt.check(t, m)
			}
		})
	}
}

func TestAudit1_Connection_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, m map[string]any)
		name     string
		wantCode int
		preload  bool
	}{
		{
			name:     "returns connection fields",
			wantCode: http.StatusOK,
			preload:  true,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				conn, _ := m["Connection"].(map[string]any)
				require.NotNil(t, conn)
				assert.Equal(t, "get-test", conn["ConnectionName"])
				assert.NotEmpty(t, conn["ConnectionArn"])
				assert.NotEmpty(t, conn["ProviderType"])
				assert.NotEmpty(t, conn["ConnectionStatus"])
				assert.NotEmpty(t, conn["OwnerAccountId"])
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

			h := a1Handler(t)

			connArn := "arn:aws:codeconnections:us-east-1:000000000000:connection/nonexistent"
			if tt.preload {
				rec := a1Do(t, h, "CreateConnection", map[string]any{
					"ConnectionName": "get-test",
					"ProviderType":   "GitHub",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				connArn = a1Parse(t, rec)["ConnectionArn"].(string)
			}

			rec := a1Do(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, a1Parse(t, rec))
			}
		})
	}
}

func TestAudit1_Connection_List(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, m map[string]any)
		name     string
		wantCode int
		count    int
	}{
		{
			name:     "empty list returns Connections array",
			wantCode: http.StatusOK,
			count:    0,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				list, _ := m["Connections"].([]any)
				assert.Empty(t, list)
			},
		},
		{
			name:     "returns all created connections",
			wantCode: http.StatusOK,
			count:    3,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				list, _ := m["Connections"].([]any)
				assert.Len(t, list, 3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)

			for i := range tt.count {
				rec := a1Do(t, h, "CreateConnection", map[string]any{
					"ConnectionName": "list-conn-" + string(rune('a'+i)),
					"ProviderType":   "GitHub",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := a1Do(t, h, "ListConnections", map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, a1Parse(t, rec))
			}
		})
	}
}

func TestAudit1_Connection_Delete(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	rec := a1Do(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "del-conn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	connArn := a1Parse(t, rec)["ConnectionArn"].(string)

	rec = a1Do(t, h, "DeleteConnection", map[string]any{"ConnectionArn": connArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1Do(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Host CRUD tests ---

func TestAudit1_Host_Create(t *testing.T) {
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
			name: "duplicate name returns error",
			body: map[string]any{
				"Name":             "dup-host",
				"ProviderType":     "GitHubEnterpriseServer",
				"ProviderEndpoint": "https://x.example.com",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing provider type returns error",
			body:     map[string]any{"Name": "bad-host", "ProviderEndpoint": "https://x.example.com"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)

			if tt.name == "duplicate name returns error" {
				rec := a1Do(t, h, "CreateHost", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := a1Do(t, h, "CreateHost", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, a1Parse(t, rec))
			}
		})
	}
}

func TestAudit1_Host_Get(t *testing.T) {
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

			h := a1Handler(t)

			hostArn := "arn:aws:codeconnections:us-east-1:000000000000:host/nonexistent"
			if tt.preload {
				rec := a1Do(t, h, "CreateHost", map[string]any{
					"Name":             "describe-host",
					"ProviderType":     "GitHubEnterpriseServer",
					"ProviderEndpoint": "https://ghe.example.com",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				hostArn = a1Parse(t, rec)["HostArn"].(string)
			}

			rec := a1Do(t, h, "GetHost", map[string]any{"HostArn": hostArn})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, a1Parse(t, rec))
			}
		})
	}
}

func TestAudit1_Host_Delete(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	rec := a1Do(t, h, "CreateHost", map[string]any{
		"Name":             "del-host",
		"ProviderType":     "GitHubEnterpriseServer",
		"ProviderEndpoint": "https://ghe.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	hostArn := a1Parse(t, rec)["HostArn"].(string)

	rec = a1Do(t, h, "DeleteHost", map[string]any{"HostArn": hostArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1Do(t, h, "GetHost", map[string]any{"HostArn": hostArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Tag round-trip tests ---

func TestAudit1_Tag_RoundTrip(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	rec := a1Do(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "tag-conn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	connArn := a1Parse(t, rec)["ConnectionArn"].(string)

	rec = a1Do(t, h, "TagResource", map[string]any{
		"ResourceArn": connArn,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1Do(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := a1Parse(t, rec)
	tags, _ := m["Tags"].([]any)
	assert.Len(t, tags, 2)

	tag0, _ := tags[0].(map[string]any)
	assert.NotEmpty(t, tag0["Key"])
	assert.NotEmpty(t, tag0["Value"])
}

func TestAudit1_Untag_Resource(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	rec := a1Do(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "untag-conn",
		"ProviderType":   "GitHub",
		"Tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	connArn := a1Parse(t, rec)["ConnectionArn"].(string)

	rec = a1Do(t, h, "UntagResource", map[string]any{
		"ResourceArn": connArn,
		"TagKeys":     []string{"remove"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1Do(t, h, "ListTagsForResource", map[string]any{"ResourceArn": connArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := a1Parse(t, rec)
	tags, _ := m["Tags"].([]any)
	assert.Len(t, tags, 1)

	remaining, _ := tags[0].(map[string]any)
	assert.Equal(t, "keep", remaining["Key"])
}

// --- RepositoryLink tests ---

func TestAudit1_RepositoryLink_Create(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	rec := a1Do(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "rl-conn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	connArn := a1Parse(t, rec)["ConnectionArn"].(string)

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, m map[string]any)
		name     string
		wantCode int
	}{
		{
			name: "creates repository link",
			body: map[string]any{
				"ConnectionArn":  connArn,
				"OwnerId":        "my-org",
				"RepositoryName": "my-repo",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, m map[string]any) {
				t.Helper()

				info, _ := m["RepositoryLinkInfo"].(map[string]any)
				require.NotNil(t, info)
				assert.NotEmpty(t, info["RepositoryLinkId"])
				assert.NotEmpty(t, info["RepositoryLinkArn"])
				assert.Equal(t, connArn, info["ConnectionArn"])
				assert.Equal(t, "my-org", info["OwnerId"])
				assert.Equal(t, "my-repo", info["RepositoryName"])
			},
		},
		{
			name:     "missing ConnectionArn returns error",
			body:     map[string]any{"OwnerId": "org", "RepositoryName": "repo"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing OwnerId returns error",
			body:     map[string]any{"ConnectionArn": connArn, "RepositoryName": "repo"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing RepositoryName returns error",
			body:     map[string]any{"ConnectionArn": connArn, "OwnerId": "org"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := a1Do(t, h, "CreateRepositoryLink", tt.body)
			assert.Equal(t, tt.wantCode, result.Code)

			if tt.check != nil {
				tt.check(t, a1Parse(t, result))
			}
		})
	}
}

func TestAudit1_RepositoryLink_GetDelete(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	rec := a1Do(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "rl-getdel-conn",
		"ProviderType":   "GitHub",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	connArn := a1Parse(t, rec)["ConnectionArn"].(string)

	rec = a1Do(t, h, "CreateRepositoryLink", map[string]any{
		"ConnectionArn":  connArn,
		"OwnerId":        "my-org",
		"RepositoryName": "my-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	info := a1Parse(t, rec)["RepositoryLinkInfo"].(map[string]any)
	rlID := info["RepositoryLinkId"].(string)

	rec = a1Do(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": rlID})
	assert.Equal(t, http.StatusOK, rec.Code)

	getInfo := a1Parse(t, rec)["RepositoryLinkInfo"].(map[string]any)
	assert.Equal(t, rlID, getInfo["RepositoryLinkId"])

	rec = a1Do(t, h, "DeleteRepositoryLink", map[string]any{"RepositoryLinkId": rlID})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1Do(t, h, "GetRepositoryLink", map[string]any{"RepositoryLinkId": rlID})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Error path tests ---

func TestAudit1_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		wantErrType string
		name        string
		action      string
		wantCode    int
	}{
		{
			name:        "GetConnection not found",
			action:      "GetConnection",
			body:        map[string]any{"ConnectionArn": a1ArnPrefix + "connection/nonexistent"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "GetHost not found",
			action:      "GetHost",
			body:        map[string]any{"HostArn": a1ArnPrefix + "host/nonexistent"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "DeleteConnection not found",
			action:      "DeleteConnection",
			body:        map[string]any{"ConnectionArn": a1ArnPrefix + "connection/nonexistent"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "CreateConnection missing provider type",
			action:      "CreateConnection",
			body:        map[string]any{"ConnectionName": "bad-conn"},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ValidationException",
		},
		{
			name:        "GetHost missing HostArn",
			action:      "GetHost",
			body:        map[string]any{},
			wantCode:    http.StatusBadRequest,
			wantErrType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)
			rec := a1Do(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			m := a1Parse(t, rec)
			assert.Equal(t, tt.wantErrType, m["__type"], "error envelope __type mismatch")
			assert.NotEmpty(t, m["message"], "error envelope message should not be empty")
		})
	}
}
