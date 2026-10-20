package iam_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	attrAllowS3   = `{"Statement":[{"Sid":"AllowS3","Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	attrDenyGet   = `{"Statement":[{"Sid":"NoGet","Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}`
	attrBoundList = `{"Statement":[{"Sid":"BoundList","Effect":"Allow","Action":"s3:List*","Resource":"*"}]}`
	attrBoundDeny = `{"Statement":[{"Sid":"BoundNoGet","Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}`
)

type attrSetup func(t *testing.T, b *iam.InMemoryBackend) (managedArn, boundaryArn string)

func attachManaged(t *testing.T, b *iam.InMemoryBackend, doc string) string {
	t.Helper()

	p, err := b.CreatePolicy("managed", "/", doc)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))

	return p.Arn
}

func setBoundary(t *testing.T, b *iam.InMemoryBackend, doc string) string {
	t.Helper()

	p, err := b.CreatePolicy("bound", "/", doc)
	require.NoError(t, err)
	require.NoError(t, b.PutUserPermissionsBoundary("alice", p.Arn))

	return p.Arn
}

func TestPolicyAttribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        attrSetup
		name         string
		wantType     string
		wantName     string
		wantDeny     string
		wantSid      string
		wantBoundSid string
		wantStatus   int
		wantPolicy   bool
		wantBoundary bool
		wantMatched  bool
		wantArnIsMan bool
		wantArnIsBnd bool
	}{
		{
			name: "managed_allow", wantStatus: http.StatusOK, wantPolicy: true,
			wantType: iampolicystream.PolicyManaged, wantSid: "AllowS3", wantArnIsMan: true,
			setup: func(t *testing.T, b *iam.InMemoryBackend) (string, string) {
				t.Helper()

				return attachManaged(t, b, attrAllowS3), ""
			},
		},
		{
			name: "inline_allow", wantStatus: http.StatusOK, wantPolicy: true,
			wantType: iampolicystream.PolicyInline, wantName: "inl", wantSid: "AllowS3",
			setup: func(t *testing.T, b *iam.InMemoryBackend) (string, string) {
				t.Helper()

				require.NoError(t, b.PutUserPolicy("alice", "inl", attrAllowS3))

				return "", ""
			},
		},
		{
			name: "explicit_deny_managed", wantStatus: http.StatusForbidden, wantPolicy: true,
			wantType: iampolicystream.PolicyManaged, wantSid: "NoGet", wantDeny: iampolicystream.DenyExplicit,
			wantArnIsMan: true,
			setup: func(t *testing.T, b *iam.InMemoryBackend) (string, string) {
				t.Helper()

				require.NoError(t, b.PutUserPolicy("alice", "inl", attrAllowS3))

				return attachManaged(t, b, attrDenyGet), ""
			},
		},
		{
			name: "boundary_implicit_deny", wantStatus: http.StatusForbidden, wantPolicy: true,
			wantType: iampolicystream.PolicyBoundary, wantDeny: iampolicystream.DenyImplicit,
			wantBoundary: true, wantArnIsBnd: true,
			setup: func(t *testing.T, b *iam.InMemoryBackend) (string, string) {
				t.Helper()

				require.NoError(t, b.PutUserPolicy("alice", "inl", attrAllowS3))

				return "", setBoundary(t, b, attrBoundList)
			},
		},
		{
			name: "boundary_explicit_deny_sid", wantStatus: http.StatusForbidden, wantPolicy: true,
			wantType: iampolicystream.PolicyBoundary, wantDeny: iampolicystream.DenyExplicit,
			wantSid: "BoundNoGet", wantBoundSid: "BoundNoGet", wantBoundary: true, wantMatched: true,
			wantArnIsBnd: true,
			setup: func(t *testing.T, b *iam.InMemoryBackend) (string, string) {
				t.Helper()

				require.NoError(t, b.PutUserPolicy("alice", "inl", attrAllowS3))

				return "", setBoundary(t, b, attrBoundDeny)
			},
		},
		{
			name: "implicit_deny_no_policy", wantStatus: http.StatusForbidden, wantDeny: iampolicystream.DenyImplicit,
			setup: func(*testing.T, *iam.InMemoryBackend) (string, string) { return "", "" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, err := b.CreateUser("alice", "/", "")
			require.NoError(t, err)

			key, err := b.CreateAccessKey("alice")
			require.NoError(t, err)

			managed, bound := tt.setup(t, b)
			stream := iampolicystream.New(10)
			e := echo.New()
			e.Use(iam.EnforcementMiddleware(b, iam.EnforcementConfig{Recorder: stream}))
			e.Any("/*", func(c *echo.Context) error { return c.String(http.StatusOK, "ok") })

			req := httptest.NewRequest(http.MethodGet, "/my-bucket/key", nil)
			req.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential="+key.AccessKeyID+"/20230101/us-east-1/s3/aws4_request")
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)

			events := stream.Snapshot(nil)
			require.Len(t, events, 1)
			ev := events[0]

			assert.Equal(t, tt.wantDeny, ev.DenyType)
			assert.Equal(t, tt.wantSid, ev.MatchedSid)
			assert.Equal(t, tt.wantPolicy, ev.Policy != nil)
			assert.Equal(t, tt.wantBoundary, ev.Boundary != nil)

			if ev.Policy != nil {
				assert.Equal(t, tt.wantType, ev.Policy.Type)
				assert.Equal(t, tt.wantName, ev.Policy.Name)
			}

			if tt.wantArnIsMan {
				assert.Equal(t, managed, ev.Policy.Arn)
			}

			if tt.wantArnIsBnd {
				assert.Equal(t, bound, ev.Policy.Arn)
				assert.Equal(t, bound, ev.Boundary.Arn)
				assert.Equal(t, tt.wantBoundSid, ev.Boundary.Sid)
				assert.Equal(t, tt.wantMatched, ev.Boundary.Matched)
			}
		})
	}
}
