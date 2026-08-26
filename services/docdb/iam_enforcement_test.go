package docdb_test

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
	"github.com/blackbirdworks/gopherstack/services/docdb"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

var errNoSuchEntity = errors.New("NoSuchEntity")

type mockDOCDBIAMBackend struct {
	users    map[string]*iam.User
	keyMap   map[string]string
	policies map[string][]string
}

func newMockDOCDBIAMBackend() *mockDOCDBIAMBackend {
	return &mockDOCDBIAMBackend{
		users:    make(map[string]*iam.User),
		keyMap:   make(map[string]string),
		policies: make(map[string][]string),
	}
}

func (m *mockDOCDBIAMBackend) GetUserByAccessKeyID(accessKeyID string) (*iam.User, error) {
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

func (m *mockDOCDBIAMBackend) GetPoliciesForUser(userName string) ([]string, error) {
	return m.policies[userName], nil
}

func (m *mockDOCDBIAMBackend) GetPoliciesForRole(roleName string) ([]string, error) {
	return m.policies[roleName], nil
}

func (m *mockDOCDBIAMBackend) GetGroupPoliciesForUser(_ string) ([]string, error) {
	return nil, nil
}

func setupDOCDBEnforcementServer(t *testing.T, iamBackend *mockDOCDBIAMBackend) *httptest.Server {
	t.Helper()

	backend := docdb.NewInMemoryBackend("000000000000", "us-east-1")
	handler := docdb.NewHandler(backend)

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
				"Action":["rds:DescribeDBClusters"],
				"Resource":["*"]
			}
		]
	}`

	tests := []struct {
		setupBackend  func(b *mockDOCDBIAMBackend)
		name          string
		accessKeyID   string
		body          string
		wantBodyMatch string
		wantStatus    int
	}{
		{
			name:        "allowed_action_succeeds",
			accessKeyID: "AKIADOCDBUSER1",
			body:        "Action=DescribeDBClusters&Version=2014-10-31",
			setupBackend: func(b *mockDOCDBIAMBackend) {
				b.users["user1"] = &iam.User{UserName: "user1"}
				b.keyMap["AKIADOCDBUSER1"] = "user1"
				b.policies["user1"] = []string{scopedPolicy}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "denied_action_returns_access_denied",
			accessKeyID: "AKIADOCDBUSER2",
			body: "Action=CreateDBCluster&DBClusterIdentifier=test-cluster&" +
				"Engine=docdb&MasterUsername=admin&" +
				"MasterUserPassword=Pass1234&Version=2014-10-31",
			setupBackend: func(b *mockDOCDBIAMBackend) {
				b.users["user2"] = &iam.User{UserName: "user2"}
				b.keyMap["AKIADOCDBUSER2"] = "user2"
				b.policies["user2"] = []string{scopedPolicy}
			},
			wantStatus:    http.StatusForbidden,
			wantBodyMatch: "AccessDenied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			iamBackend := newMockDOCDBIAMBackend()
			tt.setupBackend(iamBackend)

			srv := setupDOCDBEnforcementServer(t, iamBackend)
			ctx := t.Context()

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/", strings.NewReader(tt.body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential="+tt.accessKeyID+
					"/20260826/us-east-1/rds/aws4_request, SignedHeaders=host, Signature=mock",
			)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}
