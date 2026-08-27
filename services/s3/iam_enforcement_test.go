package s3_test

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

var errNoSuchEntity = errors.New("NoSuchEntity")

type mockS3IAMBackend struct {
	users    map[string]*iam.User
	keyMap   map[string]string
	policies map[string][]string
}

func newMockS3IAMBackend() *mockS3IAMBackend {
	return &mockS3IAMBackend{
		users:    make(map[string]*iam.User),
		keyMap:   make(map[string]string),
		policies: make(map[string][]string),
	}
}

func (m *mockS3IAMBackend) GetUserByAccessKeyID(accessKeyID string) (*iam.User, error) {
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

func (m *mockS3IAMBackend) GetPoliciesForUser(userName string) ([]string, error) {
	return m.policies[userName], nil
}

func (m *mockS3IAMBackend) GetPoliciesForRole(roleName string) ([]string, error) {
	return m.policies[roleName], nil
}

func (m *mockS3IAMBackend) GetGroupPoliciesForUser(_ string) ([]string, error) {
	return nil, nil
}

func setupS3EnforcementServer(t *testing.T, iamBackend *mockS3IAMBackend) (*httptest.Server, *s3.InMemoryBackend) {
	t.Helper()

	s3Backend := s3.NewInMemoryBackend(nil)
	s3Handler := s3.NewHandler(s3Backend)

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
	e.Any("/*", s3Handler.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv, s3Backend
}

func TestIAMEnforcement(t *testing.T) {
	t.Parallel()

	readPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":["s3:GetObject","s3:ListBucket"],` +
		`"Resource":["arn:aws:s3:::allowed-bucket/*","arn:aws:s3:::allowed-bucket"]}]}`

	tests := []struct {
		setupBackend  func(b *mockS3IAMBackend)
		name          string
		method        string
		path          string
		accessKeyID   string
		body          string
		wantBodyMatch string
		wantStatus    int
	}{
		{
			name:        "get_object_allowed_with_read_policy",
			method:      http.MethodGet,
			path:        "/allowed-bucket/test.txt",
			accessKeyID: "AKIAREADONLY1",
			setupBackend: func(b *mockS3IAMBackend) {
				b.users["reader"] = &iam.User{UserName: "reader"}
				b.keyMap["AKIAREADONLY1"] = "reader"
				b.policies["reader"] = []string{readPolicy}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "put_object_denied_with_read_only_policy",
			method:      http.MethodPut,
			path:        "/allowed-bucket/test.txt",
			accessKeyID: "AKIAREADONLY2",
			body:        "some data",
			setupBackend: func(b *mockS3IAMBackend) {
				b.users["reader2"] = &iam.User{UserName: "reader2"}
				b.keyMap["AKIAREADONLY2"] = "reader2"
				b.policies["reader2"] = []string{readPolicy}
			},
			wantStatus:    http.StatusForbidden,
			wantBodyMatch: "AccessDenied",
		},
		{
			name:        "get_object_denied_on_restricted_bucket",
			method:      http.MethodGet,
			path:        "/restricted-bucket/test.txt",
			accessKeyID: "AKIAREADONLY3",
			setupBackend: func(b *mockS3IAMBackend) {
				b.users["reader3"] = &iam.User{UserName: "reader3"}
				b.keyMap["AKIAREADONLY3"] = "reader3"
				b.policies["reader3"] = []string{readPolicy}
			},
			wantStatus:    http.StatusForbidden,
			wantBodyMatch: "AccessDenied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			iamBackend := newMockS3IAMBackend()
			tt.setupBackend(iamBackend)

			srv, s3Bk := setupS3EnforcementServer(t, iamBackend)

			ctx := t.Context()
			_, err := s3Bk.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("allowed-bucket")})
			require.NoError(t, err)
			_, err = s3Bk.PutObject(ctx, &awss3.PutObjectInput{
				Bucket: aws.String("allowed-bucket"),
				Key:    aws.String("test.txt"),
				Body:   bytes.NewReader([]byte("hello")),
			})
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(ctx, tt.method, srv.URL+tt.path, strings.NewReader(tt.body))
			require.NoError(t, err)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential="+tt.accessKeyID+
					"/20260826/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=mock",
			)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}
