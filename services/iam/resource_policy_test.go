package iam_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// mockResourceProvider is a simple ResourcePolicyProvider for testing.
type mockResourceProvider struct {
	policies map[string]string // resourceARN → policyDoc
}

func (m *mockResourceProvider) GetResourcePolicy(_ context.Context, resourceARN string) (string, error) {
	if doc, ok := m.policies[resourceARN]; ok {
		return doc, nil
	}

	return "", nil
}

func TestEnforcementMiddleware_ResourceBasedPolicy(t *testing.T) {
	t.Parallel()

	// S3 bucket policy that allows public read on a specific bucket.
	bucketPolicy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:GetObject",
		"Resource":"arn:aws:s3:::public-bucket/*",
		"Principal":"*"
	}]}`

	// S3 bucket policy that explicitly denies all access to a restricted bucket.
	restrictedPolicy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Deny",
		"Action":"*",
		"Resource":"arn:aws:s3:::restricted-bucket/*",
		"Principal":"*"
	}]}`

	provider := &mockResourceProvider{
		policies: map[string]string{
			// extractResourceARN returns the full path ARN for object requests
			"arn:aws:s3:::public-bucket/file.txt":       bucketPolicy,
			"arn:aws:s3:::restricted-bucket/secret.txt": restrictedPolicy,
		},
	}

	tests := []struct {
		setupBackend  func(*mockEnforcementBackend)
		headers       map[string]string
		name          string
		requestPath   string
		requestMethod string
		wantStatus    int
	}{
		{
			name: "resource_policy_allows_without_identity_allow",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIARESOURCE1"] = "alice"
				b.policies["alice"] = []string{} // no identity policy
			},
			requestPath:   "/public-bucket/file.txt",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIARESOURCE1/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "resource_policy_explicit_deny_overrides_identity_allow",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIARESOURCE2"] = "alice"
				b.policies["alice"] = []string{
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
				}
			},
			requestPath:   "/restricted-bucket/secret.txt",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIARESOURCE2/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "identity_policy_allows_when_no_resource_policy",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIARESOURCE3"] = "alice"
				b.policies["alice"] = []string{
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
				}
			},
			requestPath:   "/other-bucket/file.txt",
			requestMethod: http.MethodGet,
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIARESOURCE3/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newMockEnforcementBackend()
			tt.setupBackend(backend)

			ecfg := iam.EnforcementConfig{
				AccountID:         "000000000000",
				Region:            "us-east-1",
				ResourceProviders: []iam.ResourcePolicyProvider{provider},
			}

			e := echo.New()
			e.Use(iam.EnforcementMiddleware(backend, ecfg))
			e.Any("/*", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})

			req := httptest.NewRequest(tt.requestMethod, tt.requestPath, strings.NewReader(""))
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestEnforcementMiddleware_ConditionContext(t *testing.T) {
	t.Parallel()

	// Policy that only allows from internal IP range.
	ipRestrictedPolicy := `{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow",
		"Action":"s3:*",
		"Resource":"*",
		"Condition":{
			"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}
		}
	}]}`

	tests := []struct {
		setupBackend  func(*mockEnforcementBackend)
		headers       map[string]string
		name          string
		requestPath   string
		requestMethod string
		remoteAddr    string
		wantStatus    int
	}{
		{
			name: "internal_ip_allowed",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIACOND1"] = "alice"
				b.policies["alice"] = []string{ipRestrictedPolicy}
			},
			requestPath:   "/my-bucket/file",
			requestMethod: http.MethodGet,
			remoteAddr:    "10.1.2.3:54321",
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIACOND1/20230101/us-east-1/s3/aws4_request",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "external_ip_denied",
			setupBackend: func(b *mockEnforcementBackend) {
				b.users["alice"] = &iam.User{UserName: "alice"}
				b.keyMap["AKIACOND2"] = "alice"
				b.policies["alice"] = []string{ipRestrictedPolicy}
			},
			requestPath:   "/my-bucket/file",
			requestMethod: http.MethodGet,
			remoteAddr:    "1.2.3.4:54321",
			headers: map[string]string{
				"Authorization": "AWS4-HMAC-SHA256 Credential=AKIACOND2/20230101/us-east-1/s3/aws4_request",
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

			req := httptest.NewRequest(tt.requestMethod, tt.requestPath, strings.NewReader(""))
			req.RemoteAddr = tt.remoteAddr

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
