package serverlessrepo_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

var errNoSuchEntity = errors.New("NoSuchEntity")

type mockSERVERLESSREPOIAMBackend struct {
	users    map[string]*iam.User
	keyMap   map[string]string
	policies map[string][]string
}

func newMockSERVERLESSREPOIAMBackend() *mockSERVERLESSREPOIAMBackend {
	return &mockSERVERLESSREPOIAMBackend{
		users:    make(map[string]*iam.User),
		keyMap:   make(map[string]string),
		policies: make(map[string][]string),
	}
}

func (m *mockSERVERLESSREPOIAMBackend) GetUserByAccessKeyID(accessKeyID string) (*iam.User, error) {
	userName, ok := m.keyMap[accessKeyID]
	if !ok {
		return nil, errNoSuchEntity
	}
	user, ok := m.users[userName]
	if !ok {
		return nil, errNoSuchEntity
	}

	return user, nil
}

func (m *mockSERVERLESSREPOIAMBackend) GetPoliciesForUser(userName string) ([]string, error) {
	return m.policies[userName], nil
}

func (m *mockSERVERLESSREPOIAMBackend) GetPoliciesForRole(roleName string) ([]string, error) {
	return m.policies[roleName], nil
}

func (m *mockSERVERLESSREPOIAMBackend) GetGroupPoliciesForUser(_ string) ([]string, error) {
	return nil, nil
}

func setupSERVERLESSREPOEnforcementServer(t *testing.T, iamBackend *mockSERVERLESSREPOIAMBackend) *httptest.Server {
	t.Helper()

	backend := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
	handler := serverlessrepo.NewHandler(backend)

	e := echo.New()

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			meta := awsmeta.FromRequest(req, "us-east-1")
			ctx := awsmeta.Set(req.Context(), meta)
			c.SetRequest(req.WithContext(ctx))

			return next(c)
		}
	})

	cfg := iam.EnforcementConfig{
		Global: config.NewGlobalConfig("000000000000", "us-east-1", 0, 0, true, 0),
		ActionExtractors: []iam.ActionExtractor{
			iam.NewRegisterableActionExtractor(handler),
		},
	}

	e.Use(iam.EnforcementMiddleware(iamBackend, cfg))
	e.Any("/*", handler.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func TestIAMEnforcement(t *testing.T) {
	t.Parallel()

	scopedPolicy := `{
		"Version":"2012-10-17",
		"Statement":[
			{
				"Effect":"Allow",
				"Action":["serverlessrepo:ListApplications"],
				"Resource":["*"]
			}
		]
	}`

	tests := []struct {
		setupBackend  func(b *mockSERVERLESSREPOIAMBackend)
		name          string
		accessKeyID   string
		method        string
		path          string
		body          string
		wantBodyMatch string
		wantStatus    int
	}{
		{
			name:        "allowed_action_succeeds",
			accessKeyID: "AKIASERVERLESSREPOUSER1",
			method:      "GET",
			path:        "/applications",
			body:        ``,
			setupBackend: func(b *mockSERVERLESSREPOIAMBackend) {
				b.users["user1"] = &iam.User{UserName: "user1"}
				b.keyMap["AKIASERVERLESSREPOUSER1"] = "user1"
				b.policies["user1"] = []string{scopedPolicy}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "denied_action_returns_access_denied",
			accessKeyID: "AKIASERVERLESSREPOUSER2",
			method:      "POST",
			path:        "/applications",
			body:        `{"name":"test-app","author":"author","description":"desc"}`,
			setupBackend: func(b *mockSERVERLESSREPOIAMBackend) {
				b.users["user2"] = &iam.User{UserName: "user2"}
				b.keyMap["AKIASERVERLESSREPOUSER2"] = "user2"
				b.policies["user2"] = []string{scopedPolicy}
			},
			wantStatus:    http.StatusForbidden,
			wantBodyMatch: "AccessDenied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			iamBackend := newMockSERVERLESSREPOIAMBackend()
			tt.setupBackend(iamBackend)

			srv := setupSERVERLESSREPOEnforcementServer(t, iamBackend)
			ctx := t.Context()

			req, err := http.NewRequestWithContext(ctx, tt.method, srv.URL+tt.path, strings.NewReader(tt.body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential="+tt.accessKeyID+
					"/20260826/us-east-1/serverlessrepo/aws4_request, SignedHeaders=host, Signature=mock",
			)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}
