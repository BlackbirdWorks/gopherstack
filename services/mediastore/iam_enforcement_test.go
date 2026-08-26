package mediastore_test

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
	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

var errNoSuchEntity = errors.New("NoSuchEntity")

type mockMEDIASTOREIAMBackend struct {
	users    map[string]*iam.User
	keyMap   map[string]string
	policies map[string][]string
}

func newMockMEDIASTOREIAMBackend() *mockMEDIASTOREIAMBackend {
	return &mockMEDIASTOREIAMBackend{
		users:    make(map[string]*iam.User),
		keyMap:   make(map[string]string),
		policies: make(map[string][]string),
	}
}

func (m *mockMEDIASTOREIAMBackend) GetUserByAccessKeyID(accessKeyID string) (*iam.User, error) {
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

func (m *mockMEDIASTOREIAMBackend) GetPoliciesForUser(userName string) ([]string, error) {
	return m.policies[userName], nil
}

func (m *mockMEDIASTOREIAMBackend) GetPoliciesForRole(roleName string) ([]string, error) {
	return m.policies[roleName], nil
}

func (m *mockMEDIASTOREIAMBackend) GetGroupPoliciesForUser(_ string) ([]string, error) {
	return nil, nil
}

func setupMEDIASTOREEnforcementServer(t *testing.T, iamBackend *mockMEDIASTOREIAMBackend) *httptest.Server {
	t.Helper()

	backend := mediastore.NewInMemoryBackend()
	handler := mediastore.NewHandler(backend)

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
	}

	e.Use(iam.EnforcementMiddleware(iamBackend, cfg))
	e.POST("/", handler.Handler())

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
				"Action":["mediastore:ListContainers"],
				"Resource":["*"]
			}
		]
	}`

	tests := []struct {
		setupBackend  func(b *mockMEDIASTOREIAMBackend)
		name          string
		accessKeyID   string
		target        string
		body          string
		wantBodyMatch string
		wantStatus    int
	}{
		{
			name:        "allowed_action_succeeds",
			accessKeyID: "AKIAMEDIASTOREUSER1",
			target:      "MediaStore_20170901.ListContainers",
			body:        `{}`,
			setupBackend: func(b *mockMEDIASTOREIAMBackend) {
				b.users["user1"] = &iam.User{UserName: "user1"}
				b.keyMap["AKIAMEDIASTOREUSER1"] = "user1"
				b.policies["user1"] = []string{scopedPolicy}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "denied_action_returns_access_denied",
			accessKeyID: "AKIAMEDIASTOREUSER2",
			target:      "MediaStore_20170901.CreateContainer",
			body:        `{"ContainerName":"test-c"}`,
			setupBackend: func(b *mockMEDIASTOREIAMBackend) {
				b.users["user2"] = &iam.User{UserName: "user2"}
				b.keyMap["AKIAMEDIASTOREUSER2"] = "user2"
				b.policies["user2"] = []string{scopedPolicy}
			},
			wantStatus:    http.StatusBadRequest,
			wantBodyMatch: "AccessDeniedException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			iamBackend := newMockMEDIASTOREIAMBackend()
			tt.setupBackend(iamBackend)

			srv := setupMEDIASTOREEnforcementServer(t, iamBackend)
			ctx := t.Context()

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/", strings.NewReader(tt.body))
			require.NoError(t, err)
			req.Header.Set("X-Amz-Target", tt.target)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential="+tt.accessKeyID+
					"/20260826/us-east-1/mediastore/aws4_request, SignedHeaders=host, Signature=mock",
			)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}
