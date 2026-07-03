package s3_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// serveInternal drives the full S3 Echo handler for a package-external test.
func serveInternal(h *s3.S3Handler, w http.ResponseWriter, r *http.Request) {
	ctx := logger.Save(r.Context(), slog.Default())
	r = r.WithContext(ctx)
	e := echo.New()
	c := e.NewContext(r, w)
	_ = h.Handler()(c)
}

// seedObject creates a bucket and stores a small object in it via the backend.
func seedObject(t *testing.T, b *s3.InMemoryBackend, bucket, key, body string) {
	t.Helper()

	_, err := b.CreateBucket(context.Background(),
		&awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = b.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	})
	require.NoError(t, err)
}

const denyGetPolicy = `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::authbkt/*"
  }]
}`

const allowPublicGetPolicy = `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::authbkt/*"
  }]
}`

func TestObjectAccess_DenyPolicyEnforced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "deny getobject blocks GET even for owner",
			policy:     denyGetPolicy,
			method:     http.MethodGet,
			path:       "/authbkt/secret.txt",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "deny getobject does not block DELETE",
			policy:     denyGetPolicy,
			method:     http.MethodDelete,
			path:       "/authbkt/secret.txt",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "no policy allows GET",
			policy:     "",
			method:     http.MethodGet,
			path:       "/authbkt/secret.txt",
			wantStatus: http.StatusOK,
		},
		{
			name:       "allow-only policy still allows GET",
			policy:     allowPublicGetPolicy,
			method:     http.MethodGet,
			path:       "/authbkt/secret.txt",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			h := s3.NewHandler(backend)
			seedObject(t, backend, "authbkt", "secret.txt", "classified")

			if tt.policy != "" {
				require.NoError(t,
					backend.PutBucketPolicy(context.Background(), "authbkt", tt.policy))
			}

			// Owner request: SigV4-style Authorization header present.
			r := httptest.NewRequest(tt.method, tt.path, nil)
			r.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, "+
					"SignedHeaders=host, Signature=x")
			rec := httptest.NewRecorder()
			serveInternal(h, rec, r)

			assert.Equal(t, tt.wantStatus, rec.Code, "body=%s", rec.Body.String())
			if tt.wantStatus == http.StatusForbidden {
				assert.Contains(t, rec.Body.String(), "AccessDenied")
			}
		})
	}
}

func TestObjectAccess_AnonymousEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		acl        string
		enforce    bool
		wantStatus int
	}{
		{
			name:       "anonymous GET private bucket denied in enforcement mode",
			enforce:    true,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "anonymous GET public-read bucket allowed",
			acl:        s3.ACLPublicRead,
			enforce:    true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "anonymous GET with public policy allowed",
			policy:     allowPublicGetPolicy,
			enforce:    true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "anonymous GET private bucket allowed when enforcement disabled",
			enforce:    false,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			h := s3.NewHandler(backend)
			if tt.enforce {
				h = h.WithPresignValidation(testSecret)
			}
			seedObject(t, backend, "authbkt", "secret.txt", "classified")

			if tt.policy != "" {
				require.NoError(t, backend.PutBucketPolicy(context.Background(), "authbkt", tt.policy))
			}
			if tt.acl != "" {
				require.NoError(t, backend.PutBucketACL(context.Background(), "authbkt", tt.acl))
			}

			// Anonymous request: no Authorization header, not presigned.
			r := httptest.NewRequest(http.MethodGet, "/authbkt/secret.txt", nil)
			rec := httptest.NewRecorder()
			serveInternal(h, rec, r)

			assert.Equal(t, tt.wantStatus, rec.Code, "body=%s", rec.Body.String())
		})
	}
}

func TestObjectAccess_PublicAccessBlockRestrictsPolicy(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	h := s3.NewHandler(backend).WithPresignValidation(testSecret)
	seedObject(t, backend, "authbkt", "secret.txt", "classified")

	require.NoError(t, backend.PutBucketPolicy(context.Background(), "authbkt", allowPublicGetPolicy))
	// RestrictPublicBuckets must suppress the public Allow, so anonymous GET is denied.
	pab := `<PublicAccessBlockConfiguration><RestrictPublicBuckets>true` +
		`</RestrictPublicBuckets></PublicAccessBlockConfiguration>`
	require.NoError(t, backend.PutPublicAccessBlock(context.Background(), "authbkt", pab))

	r := httptest.NewRequest(http.MethodGet, "/authbkt/secret.txt", nil)
	rec := httptest.NewRecorder()
	serveInternal(h, rec, r)

	assert.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
}

func TestEvaluateBucketPolicy_Matrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       string
		resDeny   string
		want      int
		anonymous bool
	}{
		{
			name:    "deny matches action and resource",
			key:     "obj",
			want:    s3.EffectDeny,
			resDeny: "arn:aws:s3:::authbkt/*",
		},
		{
			name:    "deny resource mismatch does not apply",
			key:     "obj",
			want:    s3.EffectDefault,
			resDeny: "arn:aws:s3:::otherbkt/*",
		},
		{
			name:      "deny applies to anonymous too",
			key:       "obj",
			anonymous: true,
			want:      s3.EffectDeny,
			resDeny:   "arn:aws:s3:::authbkt/*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			h := s3.NewHandler(backend)
			_, err := backend.CreateBucket(context.Background(),
				&awss3.CreateBucketInput{Bucket: aws.String("authbkt")})
			require.NoError(t, err)

			policy := `{"Statement":[{"Effect":"Deny","Principal":"*",` +
				`"Action":"s3:GetObject","Resource":"` + tt.resDeny + `"}]}`
			require.NoError(t, backend.PutBucketPolicy(context.Background(), "authbkt", policy))

			got := h.EvaluateBucketPolicyForTest(
				context.Background(), "authbkt", tt.key, s3.ActionGetObject, tt.anonymous)
			assert.Equal(t, tt.want, got)
		})
	}
}
