package cloudfront_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type seenRequest struct {
	header http.Header
	method string
	uri    string
	host   string
	body   string
}

type originServer struct {
	srv  *httptest.Server
	seen []seenRequest
	mu   sync.Mutex
	hits atomic.Int32
}

func newOriginServer(t *testing.T, respond http.HandlerFunc) *originServer {
	t.Helper()

	o := &originServer{}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		o.hits.Add(1)
		o.mu.Lock()
		o.seen = append(
			o.seen,
			seenRequest{header: r.Header.Clone(), method: r.Method, uri: r.RequestURI, host: r.Host, body: string(b)},
		)
		o.mu.Unlock()
		respond(w, r)
	}))
	t.Cleanup(o.srv.Close)

	return o
}

func (o *originServer) hostPort() string { return strings.TrimPrefix(o.srv.URL, "http://") }

func (o *originServer) last(t *testing.T) seenRequest {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	require.NotEmpty(t, o.seen)

	return o.seen[len(o.seen)-1]
}

func (f *dpFixture) originRequestPolicy(t *testing.T) string {
	t.Helper()

	out, err := f.cf.CreateOriginRequestPolicy(t.Context(), &cfsdk.CreateOriginRequestPolicyInput{
		OriginRequestPolicyConfig: &types.OriginRequestPolicyConfig{
			Name: aws.String("orp-forward"),
			HeadersConfig: &types.OriginRequestPolicyHeadersConfig{
				HeaderBehavior: types.OriginRequestPolicyHeaderBehaviorWhitelist,
				Headers:        &types.Headers{Quantity: aws.Int32(1), Items: []string{"X-Custom"}},
			},
			CookiesConfig: &types.OriginRequestPolicyCookiesConfig{
				CookieBehavior: types.OriginRequestPolicyCookieBehaviorWhitelist,
				Cookies:        &types.CookieNames{Quantity: aws.Int32(1), Items: []string{"sid"}},
			},
			QueryStringsConfig: &types.OriginRequestPolicyQueryStringsConfig{
				QueryStringBehavior: types.OriginRequestPolicyQueryStringBehaviorWhitelist,
				QueryStrings:        &types.QueryStringNames{Quantity: aws.Int32(1), Items: []string{"keep"}},
			},
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.OriginRequestPolicy.Id)
}

func TestDataPlane_CustomOriginForwarding(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Origin", "yes")
		_, _ = io.WriteString(w, "from-origin")
	})

	orp := f.originRequestPolicy(t)
	o := customOrigin("api", origin.hostPort())
	o.OriginPath = aws.String("/v1")
	o.CustomHeaders = &types.CustomHeaders{
		Quantity: aws.Int32(1),
		Items: []types.OriginCustomHeader{
			{HeaderName: aws.String("X-Origin-Secret"), HeaderValue: aws.String("s3cret")},
		},
	}

	def := behavior("api", managedCachingDisabled)
	def.OriginRequestPolicyId = aws.String(orp)
	def.AllowedMethods = allowed(
		types.MethodGet,
		types.MethodHead,
		types.MethodOptions,
		types.MethodPut,
		types.MethodPatch,
		types.MethodPost,
		types.MethodDelete,
	)
	d := f.dist(t, distSpec{origins: []types.Origin{o}, def: def})

	resp := f.viewer(t, d, http.MethodGet, "/items/1?keep=a&drop=b", map[string]string{
		"X-Custom": "hello", "X-Other": "no", "Cookie": "sid=abc; other=zzz",
	})
	require.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "from-origin", resp.body)
	assert.Equal(t, "yes", resp.header.Get("X-Origin"))

	got := origin.last(t)
	assert.Equal(t, "/v1/items/1?keep=a", got.uri)
	assert.Equal(t, "hello", got.header.Get("X-Custom"))
	assert.Empty(t, got.header.Get("X-Other"))
	assert.Equal(t, "sid=abc", got.header.Get("Cookie"))
	assert.Equal(t, "s3cret", got.header.Get("X-Origin-Secret"))
	assert.Equal(t, "Amazon CloudFront", got.header.Get("User-Agent"))
	assert.Contains(t, got.header.Get("Via"), "CloudFront")
	assert.NotEmpty(t, got.header.Get("X-Amz-Cf-Id"))
	assert.Equal(t, strings.Split(origin.hostPort(), ":")[0], strings.Split(got.host, ":")[0])

	post := f.do(
		t,
		http.MethodPost,
		f.base+"/items",
		d.domain,
		strings.NewReader("payload"),
		map[string]string{"Content-Type": "text/plain"},
	)
	require.Equal(t, http.StatusOK, post.status)

	got = origin.last(t)
	assert.Equal(t, http.MethodPost, got.method)
	assert.Equal(t, "payload", got.body)
	assert.Equal(t, "text/plain", got.header.Get("Content-Type"))
}

func TestDataPlane_CustomOriginCaching(t *testing.T) {
	t.Parallel()

	none := types.CachePolicyQueryStringBehaviorNone
	all := types.CachePolicyQueryStringBehaviorAll

	tests := []struct {
		name       string
		cacheCtl   string
		second     string
		query      types.CachePolicyQueryStringBehavior
		wantXCache string
		minTTL     int64
		defTTL     int64
		status     int
		wantHits   int32
	}{
		{
			name: "max_age_hit", cacheCtl: "max-age=60", status: 200, second: "/x",
			defTTL: 5, query: none, wantHits: 1, wantXCache: "Hit from cloudfront",
		},
		{
			name: "default_ttl_hit", status: 200, second: "/x",
			defTTL: 300, query: none, wantHits: 1, wantXCache: "Hit from cloudfront",
		},
		{
			name: "max_age_zero_not_cached", cacheCtl: "max-age=0", status: 200, second: "/x",
			defTTL: 300, query: none, wantHits: 2, wantXCache: "Miss from cloudfront",
		},
		{
			name: "min_ttl_overrides_no_store", cacheCtl: "no-store", status: 200, second: "/x",
			minTTL: 30, defTTL: 300, query: none, wantHits: 1, wantXCache: "Hit from cloudfront",
		},
		{
			name: "query_in_key_misses", status: 200, second: "/x?v=2",
			defTTL: 300, query: all, wantHits: 2, wantXCache: "Miss from cloudfront",
		},
		{
			name: "query_ignored_in_key_hits", status: 200, second: "/x?v=2",
			defTTL: 300, query: none, wantHits: 1, wantXCache: "Hit from cloudfront",
		},
		{
			name: "errors_not_cached", status: 500, second: "/x",
			defTTL: 300, query: none, wantHits: 2, wantXCache: "Error from cloudfront",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) {
				if tc.cacheCtl != "" {
					w.Header().Set("Cache-Control", tc.cacheCtl)
				}

				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, "body")
			})

			d := f.dist(t, distSpec{
				origins: []types.Origin{customOrigin("api", origin.hostPort())},
				def:     behavior("api", f.cachePolicyQuery(t, tc.minTTL, tc.defTTL, tc.query)),
			})

			f.viewer(t, d, http.MethodGet, "/x?v=1", nil)
			second := f.viewer(t, d, http.MethodGet, tc.second, nil)

			assert.Equal(t, tc.wantHits, origin.hits.Load())
			assert.Equal(t, tc.wantXCache, second.xcache())
		})
	}
}

func TestDataPlane_OriginUnreachable(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	origin := newOriginServer(t, func(http.ResponseWriter, *http.Request) {})
	hostPort := origin.hostPort()
	origin.srv.Close()

	d := f.dist(t, distSpec{
		origins: []types.Origin{customOrigin("api", hostPort)},
		def:     behavior("api", managedCachingDisabled),
	})

	resp := f.viewer(t, d, http.MethodGet, "/", nil)
	assert.Equal(t, http.StatusBadGateway, resp.status)
	assert.Equal(t, "Error from cloudfront", resp.xcache())
}

func TestDataPlane_HostRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host       func(d distInfo) string
		name       string
		wantStatus int
	}{
		{name: "cloudfront_net", host: func(d distInfo) string { return d.domain }, wantStatus: 200},
		{name: "cloudfront_localhost", host: func(d distInfo) string {
			return strings.TrimSuffix(d.domain, ".cloudfront.net") + ".cloudfront.localhost:4566"
		}, wantStatus: 200},
		{name: "alias", host: func(distInfo) string { return "cdn.example.test" }, wantStatus: 200},
		{
			name:       "unknown_distribution",
			host:       func(distInfo) string { return "eunknown.cloudfront.net" },
			wantStatus: 404,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			origin := newOriginServer(
				t,
				func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") },
			)

			d := f.dist(t, distSpec{
				origins: []types.Origin{customOrigin("api", origin.hostPort())},
				def:     behavior("api", managedCachingDisabled),
				mutate: func(c *types.DistributionConfig) {
					c.Aliases = &types.Aliases{Quantity: aws.Int32(1), Items: []string{"cdn.example.test"}}
				},
			})

			resp := f.do(t, http.MethodGet, f.base+"/", tc.host(d), nil, nil)
			assert.Equal(t, tc.wantStatus, resp.status)
			assert.NotEmpty(t, resp.header.Get("X-Amz-Cf-Id"), "request must be answered by the edge")
		})
	}
}

func TestDataPlane_MethodEnforcement(t *testing.T) {
	t.Parallel()

	all := allowed(
		types.MethodGet,
		types.MethodHead,
		types.MethodOptions,
		types.MethodPut,
		types.MethodPatch,
		types.MethodPost,
		types.MethodDelete,
	)

	tests := []struct {
		name     string
		methods  *types.AllowedMethods
		method   string
		wantCode int
		wantHits int32
	}{
		{
			name:     "post_blocked_by_get_head",
			methods:  allowed(types.MethodGet, types.MethodHead),
			method:   http.MethodPost,
			wantCode: 403,
		},
		{
			name:     "put_blocked_by_get_head",
			methods:  allowed(types.MethodGet, types.MethodHead),
			method:   http.MethodPut,
			wantCode: 403,
		},
		{
			name:     "options_blocked_by_get_head",
			methods:  allowed(types.MethodGet, types.MethodHead),
			method:   http.MethodOptions,
			wantCode: 403,
		},
		{
			name:     "options_allowed",
			methods:  allowed(types.MethodGet, types.MethodHead, types.MethodOptions),
			method:   http.MethodOptions,
			wantCode: 200,
			wantHits: 1,
		},
		{name: "post_allowed_with_all", methods: all, method: http.MethodPost, wantCode: 200, wantHits: 1},
		{name: "delete_allowed_with_all", methods: all, method: http.MethodDelete, wantCode: 200, wantHits: 1},
		{
			name:     "get_allowed",
			methods:  allowed(types.MethodGet, types.MethodHead),
			method:   http.MethodGet,
			wantCode: 200,
			wantHits: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			origin := newOriginServer(
				t,
				func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") },
			)

			def := behavior("api", managedCachingDisabled)
			def.AllowedMethods = tc.methods
			d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("api", origin.hostPort())}, def: def})

			resp := f.do(t, tc.method, f.base+"/x", d.domain, nil, nil)
			assert.Equal(t, tc.wantCode, resp.status)
			assert.Equal(t, tc.wantHits, origin.hits.Load(), "denied methods must not reach the origin")
		})
	}
}
