package s3_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestCannedACLGrants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		canned    string
		wantURIs  []string
		wantPerms []string
	}{
		{
			name:      "private owner only",
			canned:    "private",
			wantURIs:  nil,
			wantPerms: []string{s3.PermFullControl},
		},
		{
			name:      "public-read adds AllUsers READ",
			canned:    s3.ACLPublicRead,
			wantURIs:  []string{s3.URIAllUsers},
			wantPerms: []string{s3.PermFullControl, s3.PermRead},
		},
		{
			name:      "public-read-write adds AllUsers READ and WRITE",
			canned:    s3.ACLPublicReadWrite,
			wantURIs:  []string{s3.URIAllUsers, s3.URIAllUsers},
			wantPerms: []string{s3.PermFullControl, s3.PermRead, s3.PermWrite},
		},
		{
			name:      "authenticated-read adds AuthenticatedUsers READ",
			canned:    s3.ACLAuthenticatedRead,
			wantURIs:  []string{s3.URIAuthenticatedUsers},
			wantPerms: []string{s3.PermFullControl, s3.PermRead},
		},
		{
			name:      "log-delivery-write adds LogDelivery grants",
			canned:    s3.ACLLogDeliveryWrite,
			wantURIs:  []string{s3.URILogDelivery, s3.URILogDelivery},
			wantPerms: []string{s3.PermFullControl, s3.PermWrite, s3.PermReadACP},
		},
		{
			name:      "unknown canned falls back to private",
			canned:    "bucket-owner-full-control",
			wantURIs:  nil,
			wantPerms: []string{s3.PermFullControl},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			grants := s3.CannedACLGrants(tt.canned, "owner-id", "owner-name")

			var gotPerms, gotURIs []string
			for _, g := range grants {
				gotPerms = append(gotPerms, g.Permission)
				if g.Grantee.URI != "" {
					gotURIs = append(gotURIs, g.Grantee.URI)
				}
			}

			assert.Equal(t, tt.wantPerms, gotPerms)
			assert.Equal(t, tt.wantURIs, gotURIs)
			// Owner grant is always first and is a CanonicalUser.
			require.NotEmpty(t, grants)
			assert.Equal(t, s3.XsiTypeCanonicalUser, grants[0].Grantee.XsiType)
			assert.Equal(t, "owner-id", grants[0].Grantee.ID)
		})
	}
}

func TestGetBucketAcl_ReflectsStoredCannedACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		storedACL     string
		wantContains  string
		wantGrants    int
		wantNotPublic bool
	}{
		{
			name:          "private returns single owner grant",
			storedACL:     "",
			wantGrants:    1,
			wantNotPublic: true,
		},
		{
			name:         "public-read reflects AllUsers grant",
			storedACL:    s3.ACLPublicRead,
			wantGrants:   2,
			wantContains: s3.URIAllUsers,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			h := s3.NewHandler(backend)
			_, err := backend.CreateBucket(context.Background(),
				&awss3.CreateBucketInput{Bucket: aws.String("aclbkt")})
			require.NoError(t, err)

			if tt.storedACL != "" {
				require.NoError(t, backend.PutBucketACL(context.Background(), "aclbkt", tt.storedACL))
			}

			r := httptest.NewRequest(http.MethodGet, "/aclbkt?acl", nil)
			rec := httptest.NewRecorder()
			serveInternal(h, rec, r)

			require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())

			var acp s3.AccessControlPolicy
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &acp))
			assert.Len(t, acp.ACL.Grants, tt.wantGrants)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
			if tt.wantNotPublic {
				assert.NotContains(t, rec.Body.String(), s3.URIAllUsers)
			}
		})
	}
}

func TestSigV2QueryPresignRecognised(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		query         string
		wantPresigned bool
	}{
		{
			name:          "sigv2 query presign",
			query:         "AWSAccessKeyId=AKID&Signature=abc&Expires=9999999999",
			wantPresigned: true,
		},
		{
			name:          "sigv4 query presign",
			query:         "X-Amz-Signature=abc",
			wantPresigned: true,
		},
		{
			name:          "plain request",
			query:         "prefix=foo",
			wantPresigned: false,
		},
		{
			name:          "sigv2 missing expires",
			query:         "AWSAccessKeyId=AKID&Signature=abc",
			wantPresigned: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/bkt/key?"+tt.query, nil)
			assert.Equal(t, tt.wantPresigned, s3.IsPresignedRequestForTest(r))
		})
	}
}

func TestSigV2QueryPresignExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		expires    string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "valid unexpired sigv2 presign",
			expires:    "9999999999",
			wantStatus: http.StatusOK,
		},
		{
			name:       "expired sigv2 presign",
			expires:    "1",
			wantStatus: http.StatusForbidden,
			wantBody:   "Request has expired",
		},
		{
			name:       "non-numeric expires",
			expires:    "notanumber",
			wantStatus: http.StatusBadRequest,
			wantBody:   s3.ErrAuthQueryParams,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			h := s3.NewHandler(backend)
			seedObject(t, backend, "sigv2bkt", "obj", "data")

			r := httptest.NewRequest(http.MethodGet,
				"/sigv2bkt/obj?AWSAccessKeyId=AKID&Signature=abc&Expires="+tt.expires, nil)
			rec := httptest.NewRecorder()
			serveInternal(h, rec, r)

			assert.Equal(t, tt.wantStatus, rec.Code, "body=%s", rec.Body.String())
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}
