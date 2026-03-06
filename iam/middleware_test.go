package iam_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/iam"
)

var (
	errKeyNotFound  = errors.New("access key not found")
	errUserNotFound = errors.New("user not found")
)

// mockEnforcementBackend implements iam.EnforcementBackend for testing.
type mockEnforcementBackend struct {
	keyMap   map[string]string   // accessKeyID → userName
	policies map[string][]string // userName → []policyDoc
	users    map[string]*iam.User
}

func newMockEnforcementBackend() *mockEnforcementBackend {
	return &mockEnforcementBackend{
		users:    make(map[string]*iam.User),
		keyMap:   make(map[string]string),
		policies: make(map[string][]string),
	}
}

func (m *mockEnforcementBackend) GetUserByAccessKeyID(accessKeyID string) (*iam.User, error) {
	userName, ok := m.keyMap[accessKeyID]
	if !ok {
		return nil, errKeyNotFound
	}

	u, ok := m.users[userName]
	if !ok {
		return nil, errUserNotFound
	}

	return u, nil
}

func (m *mockEnforcementBackend) GetPoliciesForUser(userName string) ([]string, error) {
	return m.policies[userName], nil
}

func TestEnforcementMiddleware(t *testing.T) {
	t.Parallel()

	allowS3All := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	allowIAMAll := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"}]}`
	denyAll := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`

	tests := []struct {
		setupBackend  func(*mockEnforcementBackend)
		headers       map[string]string
		name          string
		requestPath   string
		requestMethod string
		body          string
		wantStatus    int
	}{
		{
			name:          "no_credentials_passes",
			setupBackend:  func(_ *mockEnforcementBackend) {},
			requestPath:   "/",
			requestMethod: http.MethodGet,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "unknown_key_passes",
			setupBackend:  func(_ *mockEnforcementBackend) {},
			requestPath:   "/",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=UNKNOWN_KEY/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "known_key_allow",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIATEST1"] = "alice"
				b.policies["alice"] = []string{allowS3All}
			},
			requestPath:   "/my-bucket/key",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIATEST1/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "known_key_implicit_deny",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIATEST2"] = "alice"
				b.policies["alice"] = []string{allowIAMAll} // only IAM, not S3
			},
			requestPath:   "/my-bucket/key",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIATEST2/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "known_key_explicit_deny",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIATEST3"] = "alice"
				b.policies["alice"] = []string{allowS3All, denyAll}
			},
			requestPath:   "/my-bucket/key",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIATEST3/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "dashboard_path_skipped",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIATEST4"] = "alice"
				b.policies["alice"] = []string{} // no policies
			},
			requestPath:   "/dashboard/iam",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIATEST4/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusOK, // skipped — dashboard path
		},
		{
			name: "health_path_skipped",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIATEST5"] = "alice"
				b.policies["alice"] = []string{} // no policies
			},
			requestPath:   "/_gopherstack/health",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIATEST5/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusOK, // skipped — internal path
		},
		{
			name: "no_policies_implicit_deny",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIATEST6"] = "alice"
				b.policies["alice"] = []string{}
			},
			requestPath:   "/my-bucket/key",
			requestMethod: http.MethodPut,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIATEST6/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newMockEnforcementBackend()
			tt.setupBackend(backend)

			e := echo.New()
			e.Use(iam.EnforcementMiddleware(backend))
			e.Any("/*", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})

			reqBody := strings.NewReader(tt.body)
			req := httptest.NewRequest(tt.requestMethod, tt.requestPath, reqBody)

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, "status code mismatch")
		})
	}
}

func TestExtractAccessKeyIDFromRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authorization string
		want          string
	}{
		{
			name: "valid_credential",
			authorization: "AWS4-HMAC-SHA256 Credential=AKIA1234/20230101/us-east-1/s3/aws4_request," +
				" SignedHeaders=host, Signature=xyz",
			want: "AKIA1234",
		},
		{
			name:          "empty",
			authorization: "",
			want:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}

			got := iam.ExtractAccessKeyID(req)
			require.Equal(t, tt.want, got)
		})
	}
}

// mockActionExtractor implements iam.ActionExtractor for testing.
type mockActionExtractor struct {
	action string
}

func (m *mockActionExtractor) IAMAction(_ *http.Request) string {
	return m.action
}

func TestEnforcementMiddleware_ActionExtractors(t *testing.T) {
	t.Parallel()

	allowLambda := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"lambda:*","Resource":"*"}]}`

	tests := []struct {
		setupBackend func(*mockEnforcementBackend)
		extractor    *mockActionExtractor
		name         string
		requestPath  string
		wantStatus   int
	}{
		{
			name: "extractor_returns_allowed_action",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIAEXT1"] = "alice"
				b.policies["alice"] = []string{allowLambda}
			},
			extractor:   &mockActionExtractor{action: "lambda:InvokeFunction"},
			requestPath: "/2015-03-31/functions/my-func/invocations",
			wantStatus:  http.StatusOK,
		},
		{
			name: "extractor_returns_denied_action",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIAEXT2"] = "alice"
				b.policies["alice"] = []string{allowLambda} // lambda allowed, not s3
			},
			extractor:   &mockActionExtractor{action: "s3:GetObject"}, // overrides to s3 → denied
			requestPath: "/2015-03-31/functions/my-func/invocations",
			wantStatus:  http.StatusForbidden,
		},
		{
			name: "extractor_returns_empty_passes_through",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIAEXT3"] = "alice"
				b.policies["alice"] = []string{} // no policies
			},
			extractor:   &mockActionExtractor{action: ""},            // empty → no enforcement
			requestPath: "/2015-03-31/functions/my-func/invocations", // Lambda path excluded from S3 detection
			wantStatus:  http.StatusOK,                               // passes through when action unknown
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newMockEnforcementBackend()
			tt.setupBackend(backend)

			cfg := iam.EnforcementConfig{
				ActionExtractors: []iam.ActionExtractor{tt.extractor},
			}

			e := echo.New()
			e.Use(iam.EnforcementMiddleware(backend, cfg))
			e.Any("/*", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})

			req := httptest.NewRequest(http.MethodPost, tt.requestPath, strings.NewReader(""))
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential="+getKey(backend)+"/20230101/us-east-1/lambda/aws4_request",
			)

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// getKey extracts the access key set by the setup function.
func getKey(b *mockEnforcementBackend) string {
	for k := range b.keyMap {
		return k
	}

	return "UNKNOWN"
}
