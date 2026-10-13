package cloudfront_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	getOnly  = `"s3:GetObject"`
	getAndLs = `"s3:GetObject","s3:ListBucket"`
)

func TestDataPlane_S3OriginAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy     func(bucket, arn string) string
		name       string
		path       string
		wantBody   string
		wantStatus int
		useOAC     bool
	}{
		{
			name: "oac_policy_allows", useOAC: true, path: "/a.txt", wantStatus: http.StatusOK, wantBody: "alpha",
			policy: func(b, arn string) string { return cfPolicy(b, getOnly, arn) },
		},
		{
			name: "oac_no_policy_denied", useOAC: true, path: "/a.txt", wantStatus: http.StatusForbidden,
			policy: func(string, string) string { return "" },
		},
		{
			name: "oac_wrong_source_arn", useOAC: true, path: "/a.txt", wantStatus: http.StatusForbidden,
			policy: func(b, _ string) string {
				return cfPolicy(b, getOnly, "arn:aws:cloudfront::123456789012:distribution/EOTHER")
			},
		},
		{
			name: "oac_missing_key_without_list", useOAC: true, path: "/nope.txt", wantStatus: http.StatusForbidden,
			policy: func(b, arn string) string { return cfPolicy(b, getOnly, arn) },
		},
		{
			name: "oac_missing_key_with_list", useOAC: true, path: "/nope.txt", wantStatus: http.StatusNotFound,
			policy: func(b, arn string) string { return cfPolicy(b, getAndLs, arn) },
		},
		{
			name: "public_policy_without_oac", path: "/a.txt", wantStatus: http.StatusOK, wantBody: "alpha",
			policy: func(b, _ string) string {
				return policyDoc(stmt("Allow", `"*"`, getOnly, `"arn:aws:s3:::`+b+`/*"`))
			},
		},
		{
			name: "private_without_oac", path: "/a.txt", wantStatus: http.StatusForbidden,
			policy: func(string, string) string { return "" },
		},
		{
			name: "cloudfront_principal_not_public", path: "/a.txt", wantStatus: http.StatusForbidden,
			policy: func(b, arn string) string { return cfPolicy(b, getOnly, arn) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.bucket(t, "site", map[string]string{"a.txt": "alpha"})

			oacID := ""
			if tc.useOAC {
				oacID = f.oac(t)
			}

			spec := distSpec{
				origins: []types.Origin{s3Origin("s3", "site", "", oacID)},
				def:     behavior("s3", managedCachingDisabled),
			}
			d := f.dist(t, spec)

			if p := tc.policy("site", d.arn); p != "" {
				f.policy(t, "site", p)
			}

			resp := f.viewer(t, d, http.MethodGet, tc.path, nil)
			assert.Equal(t, tc.wantStatus, resp.status)

			if tc.wantBody != "" {
				assert.Equal(t, tc.wantBody, resp.body)
			}
		})
	}
}

func TestDataPlane_RoutingAndBehaviors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantBody   string
		wantStatus int
		pathStyle  bool
		disabled   bool
	}{
		{name: "default_root_object", method: http.MethodGet, path: "/", wantStatus: 200, wantBody: "home"},
		{name: "default_behavior", method: http.MethodGet, path: "/a.txt", wantStatus: 200, wantBody: "default-a"},
		{name: "path_pattern_behavior", method: http.MethodGet, path: "/img/a.txt", wantStatus: 200, wantBody: "img-a"},
		{name: "origin_path_prefix", method: http.MethodGet, path: "/p/a.txt", wantStatus: 200, wantBody: "prefixed-a"},
		{name: "head_allowed", method: http.MethodHead, path: "/a.txt", wantStatus: 200},
		{name: "post_denied", method: http.MethodPost, path: "/a.txt", wantStatus: 403},
		{name: "delete_denied", method: http.MethodDelete, path: "/a.txt", wantStatus: 403},
		{
			name:       "path_style_route",
			method:     http.MethodGet,
			path:       "/a.txt",
			wantStatus: 200,
			wantBody:   "default-a",
			pathStyle:  true,
		},
		{name: "disabled_distribution", method: http.MethodGet, path: "/a.txt", wantStatus: 403, disabled: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.bucket(t, "default", map[string]string{"index.html": "home", "a.txt": "default-a"})
			f.bucket(t, "images", map[string]string{"img/a.txt": "img-a", "pre/p/a.txt": "prefixed-a"})
			f.policy(
				t,
				"default",
				`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}`,
			)
			f.policy(
				t,
				"images",
				`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}`,
			)

			imgBehavior := types.CacheBehavior{
				PathPattern:          aws.String("img/*"),
				TargetOriginId:       aws.String("images"),
				ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
				CachePolicyId:        aws.String(managedCachingDisabled),
				AllowedMethods:       allowed(types.MethodGet, types.MethodHead),
			}
			preBehavior := imgBehavior
			preBehavior.PathPattern = aws.String("/p/*")
			preBehavior.TargetOriginId = aws.String("prefixed")

			d := f.dist(t, distSpec{
				root: "index.html", disabled: tc.disabled,
				origins: []types.Origin{
					s3Origin("default", "default", "", ""),
					s3Origin("images", "images", "", ""),
					s3Origin("prefixed", "images", "/pre", ""),
				},
				def:       behavior("default", managedCachingDisabled),
				behaviors: []types.CacheBehavior{imgBehavior, preBehavior},
			})

			var resp viewerResp
			if tc.pathStyle {
				resp = f.pathStyle(t, d, tc.path)
			} else {
				resp = f.viewer(t, d, tc.method, tc.path, nil)
			}

			assert.Equal(t, tc.wantStatus, resp.status)

			if tc.wantBody != "" {
				assert.Equal(t, tc.wantBody, resp.body)
			}

			assert.NotEmpty(t, resp.header.Get("X-Amz-Cf-Id"))
			assert.Contains(t, resp.header.Get("Via"), "(CloudFront)")
			assert.NotEmpty(t, resp.header.Get("X-Amz-Cf-Pop"))
		})
	}
}

func TestDataPlane_ViewerProtocolPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     types.ViewerProtocolPolicy
		proto      string
		method     string
		wantStatus int
		wantLoc    bool
	}{
		{
			name:       "redirect_get",
			policy:     types.ViewerProtocolPolicyRedirectToHttps,
			method:     http.MethodGet,
			wantStatus: 301,
			wantLoc:    true,
		},
		{
			name:       "redirect_skipped_on_https",
			policy:     types.ViewerProtocolPolicyRedirectToHttps,
			proto:      "https",
			method:     http.MethodGet,
			wantStatus: 200,
		},
		{
			name:       "https_only_rejects_http",
			policy:     types.ViewerProtocolPolicyHttpsOnly,
			method:     http.MethodGet,
			wantStatus: 403,
		},
		{
			name:       "https_only_accepts_https",
			policy:     types.ViewerProtocolPolicyHttpsOnly,
			proto:      "https",
			method:     http.MethodGet,
			wantStatus: 200,
		},
		{name: "allow_all", policy: types.ViewerProtocolPolicyAllowAll, method: http.MethodGet, wantStatus: 200},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.bucket(t, "bkt", map[string]string{"a.txt": "x"})
			f.policy(
				t,
				"bkt",
				`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}`,
			)

			def := behavior("s3", managedCachingDisabled)
			def.ViewerProtocolPolicy = tc.policy
			d := f.dist(t, distSpec{origins: []types.Origin{s3Origin("s3", "bkt", "", "")}, def: def})

			hdr := map[string]string{}
			if tc.proto != "" {
				hdr["X-Forwarded-Proto"] = tc.proto
			}

			resp := f.viewer(t, d, tc.method, "/a.txt?q=1", hdr)
			assert.Equal(t, tc.wantStatus, resp.status)

			if tc.wantLoc {
				assert.Equal(t, "https://"+d.domain+"/a.txt?q=1", resp.header.Get("Location"))
			}
		})
	}
}

func TestDataPlane_CustomErrorResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantBody   string
		errors     []types.CustomErrorResponse
		wantStatus int
	}{
		{
			name: "page_with_override", path: "/missing", wantStatus: 200, wantBody: "spa-shell",
			errors: []types.CustomErrorResponse{{
				ErrorCode: aws.Int32(404), ResponsePagePath: aws.String("/index.html"), ResponseCode: aws.String("200"),
			}},
		},
		{
			name: "page_keeps_status", path: "/missing", wantStatus: 404, wantBody: "spa-shell",
			errors: []types.CustomErrorResponse{{
				ErrorCode: aws.Int32(404), ResponsePagePath: aws.String("/index.html"),
			}},
		},
		{
			name: "code_only_override", path: "/missing", wantStatus: 410,
			errors: []types.CustomErrorResponse{{ErrorCode: aws.Int32(404), ResponseCode: aws.String("410")}},
		},
		{
			name: "unmatched_code_passes_through", path: "/missing", wantStatus: 404,
			errors: []types.CustomErrorResponse{{
				ErrorCode: aws.Int32(500), ResponsePagePath: aws.String("/index.html"), ResponseCode: aws.String("200"),
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.bucket(t, "spa", map[string]string{"index.html": "spa-shell"})
			f.policy(
				t,
				"spa",
				policyDoc(stmt("Allow", `"*"`, `[`+getAndLs+`]`, `["arn:aws:s3:::spa","arn:aws:s3:::spa/*"]`)),
			)

			d := f.dist(t, distSpec{
				origins: []types.Origin{s3Origin("s3", "spa", "", "")},
				def:     behavior("s3", managedCachingDisabled), errors: tc.errors,
			})

			resp := f.viewer(t, d, http.MethodGet, tc.path, nil)
			assert.Equal(t, tc.wantStatus, resp.status)

			if tc.wantBody != "" {
				assert.Equal(t, tc.wantBody, resp.body)
			}
		})
	}
}

func TestDataPlane_CacheAndInvalidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cachePolicy  string
		cacheControl string
		wantAfter    string
		invalidate   []string
	}{
		{name: "no_invalidation_serves_stale", cachePolicy: managedCachingOptimized, wantAfter: "v1"},
		{name: "exact_path", cachePolicy: managedCachingOptimized, invalidate: []string{"/a.txt"}, wantAfter: "v2"},
		{name: "wildcard", cachePolicy: managedCachingOptimized, invalidate: []string{"/*"}, wantAfter: "v2"},
		{
			name:        "other_path_keeps_entry",
			cachePolicy: managedCachingOptimized,
			invalidate:  []string{"/b.txt"},
			wantAfter:   "v1",
		},
		{name: "caching_disabled_never_caches", cachePolicy: managedCachingDisabled, wantAfter: "v2"},
		{name: "no_store_never_caches", cachePolicy: "zero-min", cacheControl: "no-store", wantAfter: "v2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.bucket(t, "cbk", nil)
			f.put(t, "cbk", "a.txt", "v1", tc.cacheControl)
			f.policy(
				t,
				"cbk",
				`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}`,
			)

			cachePolicy := tc.cachePolicy
			if cachePolicy == "zero-min" {
				cachePolicy = f.cachePolicy(t, 300)
			}

			d := f.dist(t, distSpec{
				origins: []types.Origin{s3Origin("s3", "cbk", "", "")}, def: behavior("s3", cachePolicy),
			})

			first := f.viewer(t, d, http.MethodGet, "/a.txt", nil)
			require.Equal(t, "v1", first.body)
			assert.Equal(t, "Miss from cloudfront", first.xcache())

			f.put(t, "cbk", "a.txt", "v2", tc.cacheControl)

			if len(tc.invalidate) > 0 {
				_, err := f.cf.CreateInvalidation(t.Context(), &cfsdk.CreateInvalidationInput{
					DistributionId: aws.String(d.id),
					InvalidationBatch: &types.InvalidationBatch{
						CallerReference: aws.String("inv-1"),
						Paths: &types.Paths{
							Quantity: aws.Int32(int32(len(tc.invalidate))),
							Items:    tc.invalidate,
						},
					},
				})
				require.NoError(t, err)
			}

			second := f.viewer(t, d, http.MethodGet, "/a.txt", nil)
			assert.Equal(t, tc.wantAfter, second.body)

			if tc.wantAfter == "v1" {
				assert.Equal(t, "Hit from cloudfront", second.xcache())
				assert.NotEmpty(t, second.header.Get("Age"))
			} else {
				assert.Equal(t, "Miss from cloudfront", second.xcache())
			}
		})
	}
}

func TestDataPlane_ResponseHeadersPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reqHdr  map[string]string
		wantHdr map[string]string
		name    string
		policy  string
	}{
		{
			name: "security_headers", policy: managedSecurityHeaders,
			wantHdr: map[string]string{
				"X-Content-Type-Options":    "nosniff",
				"X-Frame-Options":           "SAMEORIGIN",
				"Strict-Transport-Security": "max-age=31536000",
			},
		},
		{
			name: "simple_cors_with_origin", policy: managedSimpleCORS,
			reqHdr:  map[string]string{"Origin": "https://app.example"},
			wantHdr: map[string]string{"Access-Control-Allow-Origin": "*"},
		},
		{
			name: "simple_cors_without_origin", policy: managedSimpleCORS,
			wantHdr: map[string]string{"Access-Control-Allow-Origin": ""},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.bucket(t, "rbk", map[string]string{"a.txt": "x"})
			f.policy(
				t,
				"rbk",
				`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}`,
			)

			def := behavior("s3", managedCachingDisabled)
			def.ResponseHeadersPolicyId = aws.String(tc.policy)
			d := f.dist(t, distSpec{origins: []types.Origin{s3Origin("s3", "rbk", "", "")}, def: def})

			resp := f.viewer(t, d, http.MethodGet, "/a.txt", tc.reqHdr)
			require.Equal(t, http.StatusOK, resp.status)

			for k, v := range tc.wantHdr {
				assert.Equal(t, v, resp.header.Get(k), k)
			}
		})
	}
}
