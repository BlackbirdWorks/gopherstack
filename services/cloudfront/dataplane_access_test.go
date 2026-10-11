package cloudfront_test

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const oaiRes = `"arn:aws:s3:::oaibkt/*"`

func oaiPrincipal(id string) string {
	return `{"AWS":"arn:aws:iam::cloudfront:user/CloudFront Origin Access Identity ` + id + `"}`
}

const publicReadPolicy = `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}`

func TestDataPlane_OriginAccessIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy     func(oaiID, canonical string) string
		name       string
		wantStatus int
	}{
		{
			name: "canonical_user", wantStatus: http.StatusOK,
			policy: func(_, c string) string {
				return policyDoc(stmt("Allow", `{"CanonicalUser":"`+c+`"}`, getOnly, oaiRes))
			},
		},
		{
			name: "iam_arn", wantStatus: http.StatusOK,
			policy: func(id, _ string) string {
				return policyDoc(stmt("Allow", oaiPrincipal(id), getOnly, oaiRes))
			},
		},
		{
			name: "other_oai", wantStatus: http.StatusForbidden,
			policy: func(string, string) string {
				return policyDoc(stmt("Allow", oaiPrincipal("EOTHER"), getOnly, oaiRes))
			},
		},
		{
			name: "explicit_deny", wantStatus: http.StatusForbidden,
			policy: func(_, c string) string {
				return policyDoc(
					stmt("Allow", `"*"`, getOnly, oaiRes),
					stmt("Deny", `{"CanonicalUser":"`+c+`"}`, getOnly, oaiRes),
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.bucket(t, "oaibkt", map[string]string{"a.txt": "oai-data"})

			oai, err := f.cf.CreateCloudFrontOriginAccessIdentity(
				t.Context(),
				&cfsdk.CreateCloudFrontOriginAccessIdentityInput{
					CloudFrontOriginAccessIdentityConfig: &types.CloudFrontOriginAccessIdentityConfig{
						CallerReference: aws.String("oai-ref"), Comment: aws.String("dp"),
					},
				},
			)
			require.NoError(t, err)

			id := aws.ToString(oai.CloudFrontOriginAccessIdentity.Id)
			f.policy(t, "oaibkt", tc.policy(id, aws.ToString(oai.CloudFrontOriginAccessIdentity.S3CanonicalUserId)))

			o := s3Origin("s3", "oaibkt", "", "")
			o.S3OriginConfig.OriginAccessIdentity = aws.String("origin-access-identity/cloudfront/" + id)
			d := f.dist(t, distSpec{origins: []types.Origin{o}, def: behavior("s3", managedCachingDisabled)})

			resp := f.viewer(t, d, http.MethodGet, "/a.txt", nil)
			assert.Equal(t, tc.wantStatus, resp.status)
		})
	}
}

func TestDataPlane_S3WebsiteOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantBody   string
		wantStatus int
		public     bool
	}{
		{name: "root_index", path: "/", public: true, wantStatus: 200, wantBody: "home"},
		{name: "dir_index", path: "/docs/", public: true, wantStatus: 200, wantBody: "docs-home"},
		{name: "object", path: "/a.txt", public: true, wantStatus: 200, wantBody: "alpha"},
		{name: "error_document", path: "/gone", public: true, wantStatus: 404, wantBody: "custom-404"},
		{name: "private_object", path: "/a.txt", public: false, wantStatus: 403},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.bucket(t, "webbkt", map[string]string{
				"index.html": "home", "docs/index.html": "docs-home", "a.txt": "alpha", "err.html": "custom-404",
			})

			_, err := f.s3.PutBucketWebsite(t.Context(), &s3sdk.PutBucketWebsiteInput{
				Bucket: aws.String("webbkt"),
				WebsiteConfiguration: &s3types.WebsiteConfiguration{
					IndexDocument: &s3types.IndexDocument{Suffix: aws.String("index.html")},
					ErrorDocument: &s3types.ErrorDocument{Key: aws.String("err.html")},
				},
			})
			require.NoError(t, err)

			if tc.public {
				f.policy(t, "webbkt", publicReadPolicy)
			}

			o := customOrigin("web", "webbkt.s3-website-us-east-1.amazonaws.com")
			d := f.dist(t, distSpec{origins: []types.Origin{o}, def: behavior("web", managedCachingDisabled)})

			resp := f.viewer(t, d, http.MethodGet, tc.path, nil)
			assert.Equal(t, tc.wantStatus, resp.status)

			if tc.wantBody != "" {
				assert.Equal(t, tc.wantBody, resp.body)
			}
		})
	}
}

func cfB64(b []byte) string {
	return strings.NewReplacer("+", "-", "=", "_", "/", "~").Replace(base64.StdEncoding.EncodeToString(b))
}

func signRSA(t *testing.T, key *rsa.PrivateKey, doc string) string {
	t.Helper()

	sum := sha1.Sum([]byte(doc))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA1, sum[:])
	require.NoError(t, err)

	return cfB64(sig)
}

func cannedDoc(resource string, expires int64) string {
	return fmt.Sprintf(
		`{"Statement":[{"Resource":%q,"Condition":{"DateLessThan":{"AWS:EpochTime":%d}}}]}`,
		resource,
		expires,
	)
}

func customDoc(resource, cidr string, expires int64) string {
	ip := ""
	if cidr != "" {
		ip = fmt.Sprintf(`,"IpAddress":{"AWS:SourceIp":%q}`, cidr)
	}

	return fmt.Sprintf(
		`{"Statement":[{"Resource":%q,"Condition":{"DateLessThan":{"AWS:EpochTime":%d}%s}}]}`,
		resource,
		expires,
		ip,
	)
}

func TestDataPlane_SignedURLsAndCookies(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	f.bucket(t, "signbkt", map[string]string{"a.txt": "secret", "b.txt": "other"})
	f.policy(t, "signbkt", publicReadPolicy)

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	require.NoError(t, err)

	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))

	pk, err := f.cf.CreatePublicKey(t.Context(), &cfsdk.CreatePublicKeyInput{
		PublicKeyConfig: &types.PublicKeyConfig{
			CallerReference: aws.String("pk"), Name: aws.String("signing"), EncodedKey: aws.String(pemKey),
		},
	})
	require.NoError(t, err)

	keyID := aws.ToString(pk.PublicKey.Id)

	kg, err := f.cf.CreateKeyGroup(t.Context(), &cfsdk.CreateKeyGroupInput{
		KeyGroupConfig: &types.KeyGroupConfig{Name: aws.String("kg"), Items: []string{keyID}},
	})
	require.NoError(t, err)

	def := behavior("s3", managedCachingDisabled)
	def.TrustedKeyGroups = &types.TrustedKeyGroups{
		Enabled: aws.Bool(true), Quantity: aws.Int32(1), Items: []string{aws.ToString(kg.KeyGroup.Id)},
	}
	d := f.dist(t, distSpec{origins: []types.Origin{s3Origin("s3", "signbkt", "", "")}, def: def})

	future := time.Now().Add(time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()
	url := func(p string) string { return "http://" + d.domain + p }
	canned := func(p string, exp int64) string {
		return fmt.Sprintf(
			"%s?Expires=%d&Signature=%s&Key-Pair-Id=%s",
			p,
			exp,
			signRSA(t, priv, cannedDoc(url("/a.txt"), exp)),
			keyID,
		)
	}
	custom := func(p, resource, cidr string) string {
		doc := customDoc(resource, cidr, future)

		return fmt.Sprintf(
			"%s?Policy=%s&Signature=%s&Key-Pair-Id=%s",
			p,
			cfB64([]byte(doc)),
			signRSA(t, priv, doc),
			keyID,
		)
	}

	tests := []struct {
		cookies    map[string]string
		name       string
		path       string
		wantStatus int
	}{
		{name: "unsigned_denied", path: "/a.txt", wantStatus: 403},
		{name: "canned_valid", path: canned("/a.txt", future), wantStatus: 200},
		{name: "canned_expired", path: canned("/a.txt", past), wantStatus: 403},
		{name: "canned_other_resource", path: canned("/b.txt", future), wantStatus: 403},
		{
			name:       "canned_unknown_key",
			path:       strings.Replace(canned("/a.txt", future), keyID, "KUNKNOWN", 1),
			wantStatus: 403,
		},
		{name: "custom_wildcard", path: custom("/b.txt", url("/*"), ""), wantStatus: 200},
		{name: "custom_wrong_resource", path: custom("/b.txt", url("/a.txt"), ""), wantStatus: 403},
		{name: "custom_ip_allowed", path: custom("/a.txt", url("/*"), "127.0.0.0/8"), wantStatus: 200},
		{name: "custom_ip_denied", path: custom("/a.txt", url("/*"), "10.0.0.0/8"), wantStatus: 403},
		{
			name: "signed_cookies", path: "/a.txt", wantStatus: 200,
			cookies: map[string]string{
				"CloudFront-Expires":     strconv.FormatInt(future, 10),
				"CloudFront-Signature":   signRSA(t, priv, cannedDoc(url("/a.txt"), future)),
				"CloudFront-Key-Pair-Id": keyID,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var cookie []string
			for k, v := range tc.cookies {
				cookie = append(cookie, k+"="+v)
			}

			hdr := map[string]string{}
			if len(cookie) > 0 {
				hdr["Cookie"] = strings.Join(cookie, "; ")
			}

			resp := f.viewer(t, d, http.MethodGet, tc.path, hdr)
			assert.Equal(t, tc.wantStatus, resp.status)
		})
	}
}
