package s3_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestObjectLambdaInputS3URLIsPresigned(t *testing.T) {
	t.Parallel()

	const (
		bucket = "olap-presign-bucket"
		alias  = "ps-abcd1234--ol-s3"
	)

	tests := []struct {
		mutate   func(u *url.URL)
		name     string
		method   string
		wantCode int
	}{
		{name: "valid", method: http.MethodHead, mutate: func(*url.URL) {}, wantCode: http.StatusOK},
		{
			name: "tampered signature", method: http.MethodHead, wantCode: http.StatusForbidden,
			mutate: func(u *url.URL) {
				q := u.Query()
				q.Set("X-Amz-Signature", strings.Repeat("0", 64))
				u.RawQuery = q.Encode()
			},
		},
		{name: "wrong method", method: http.MethodGet, mutate: func(*url.URL) {}, wantCode: http.StatusForbidden},
		{
			name: "other key", method: http.MethodHead, wantCode: http.StatusForbidden,
			mutate: func(u *url.URL) { u.Path = "/" + bucket + "/other.txt" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			handler := s3.NewHandler(backend).WithPresignValidation(testSecret)
			handler.Endpoint = "http://s3.local"
			mustCreateBucket(t, backend, bucket)
			mustPutObject(t, backend, bucket, "hello.txt", []byte("hello"))
			mustPutObject(t, backend, bucket, "other.txt", []byte("other"))

			fn := &recordingObjectLambda{response: `{"statusCode":200}`}
			handler.SetNotificationDispatcher(s3.NewNotificationDispatcher(
				&s3.NotificationTargets{LambdaInvoker: fn}, "us-east-1"))
			handler.SetObjectLambdaAccessPoint(bucket, s3.StoredObjectLambdaAccessPoint{
				Name: "ps", AccountID: "123456789012", Alias: alias, Actions: []string{"HeadObject"},
				LambdaARN: "arn:aws:lambda:us-east-1:000000000000:function:f",
			})

			req := httptest.NewRequest(http.MethodHead, "/"+alias+"/hello.txt", nil)
			req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
				AccessKeyID: "AKIAEXAMPLE",
				Principal: &awsmeta.Principal{
					Kind: awsmeta.PrincipalKindRoot, Arn: "arn:aws:iam::123456789012:root", AccountID: "123456789012",
				},
			}))
			serveS3Handler(handler, httptest.NewRecorder(), req)

			fn.mu.Lock()
			require.Len(t, fn.events, 1)
			in, _ := fn.events[0]["headObjectContext"].(map[string]any)["inputS3Url"].(string)
			fn.mu.Unlock()

			u, err := url.Parse(in)
			require.NoError(t, err)
			assert.Equal(t, "s3.local", u.Host)
			assert.Equal(t, "host", u.Query().Get("X-Amz-SignedHeaders"))
			assert.Equal(t, "AWS4-HMAC-SHA256", u.Query().Get("X-Amz-Algorithm"))
			assert.True(t, strings.HasPrefix(u.Query().Get("X-Amz-Credential"), "AKIAEXAMPLE/"))
			tt.mutate(u)

			fetch := httptest.NewRequest(tt.method, u.String(), nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, fetch)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestObjectLambdaEventSessionContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		principal   *awsmeta.Principal
		name        string
		wantIssuer  string
		wantSession bool
	}{
		{
			name: "assumed role",
			principal: &awsmeta.Principal{
				Kind: awsmeta.PrincipalKindAssumedRole, UserID: "AROAEXAMPLE:sess", AccountID: "123456789012",
				Arn: "arn:aws:sts::123456789012:assumed-role/Admin/sess",
			},
			wantSession: true, wantIssuer: "arn:aws:iam::123456789012:role/Admin",
		},
		{
			name: "user has no session",
			principal: &awsmeta.Principal{
				Kind: awsmeta.PrincipalKindUser, AccountID: "123456789012", Arn: "arn:aws:iam::123456789012:user/alice",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const alias = "sc-abcd1234--ol-s3"

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "sc-bucket")

			fn := &recordingObjectLambda{response: `{"statusCode":200}`}
			handler.SetNotificationDispatcher(s3.NewNotificationDispatcher(
				&s3.NotificationTargets{LambdaInvoker: fn}, "us-east-1"))
			handler.SetObjectLambdaAccessPoint("sc-bucket", s3.StoredObjectLambdaAccessPoint{
				Name: "sc", AccountID: "123456789012", Alias: alias, Actions: []string{"HeadObject"},
				LambdaARN: "arn:aws:lambda:us-east-1:000000000000:function:f",
			})

			req := httptest.NewRequest(http.MethodHead, "/"+alias+"/k", nil)
			req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Principal: tt.principal}))
			serveS3Handler(handler, httptest.NewRecorder(), req)

			fn.mu.Lock()
			defer fn.mu.Unlock()

			require.Len(t, fn.events, 1)

			ident, _ := fn.events[0]["userIdentity"].(map[string]any)
			sc, has := ident["sessionContext"].(map[string]any)
			assert.Equal(t, tt.wantSession, has)

			if !tt.wantSession {
				return
			}

			issuer, _ := sc["sessionIssuer"].(map[string]any)
			assert.Equal(t, tt.wantIssuer, issuer["arn"])
			assert.Equal(t, "Role", issuer["type"])
			assert.Equal(t, "Admin", issuer["userName"])

			attrs, _ := sc["attributes"].(map[string]any)
			assert.Equal(t, "false", attrs["mfaAuthenticated"])
			assert.NotEmpty(t, attrs["creationDate"])
		})
	}
}
