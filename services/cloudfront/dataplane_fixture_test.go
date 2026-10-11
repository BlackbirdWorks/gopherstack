package cloudfront_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	managedCachingOptimized = "658327ea-f89d-4fab-a63d-7e88639e58f6"
	managedCachingDisabled  = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
	managedAllViewer        = "216adef6-5c7f-47e4-b989-5492eafa07d3"
	managedSecurityHeaders  = "67f7725c-6f97-4210-82d7-5512b31e9d03"
	managedSimpleCORS       = "60669652-455b-4ae9-85a4-c4c02393f86c"
)

type dpFixture struct {
	h      *cloudfront.Handler
	cf     *cfsdk.Client
	s3     *s3sdk.Client
	http   *http.Client
	base   string
	callID int
}

func newDPFixture(t *testing.T) *dpFixture {
	t.Helper()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	t.Cleanup(backend.Close)

	h := cloudfront.NewHandler(backend)
	s3be := s3backend.NewInMemoryBackend(nil)
	s3h := s3backend.NewHandler(s3be)
	h.SetS3Backend(func() s3backend.StorageBackend { return s3be })

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	require.NoError(t, registry.Register(s3h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	tr := &http.Transport{DisableKeepAlives: true}
	t.Cleanup(tr.CloseIdleConnections)

	return &dpFixture{
		h: h, base: srv.URL,
		cf: cfsdk.NewFromConfig(cfg, func(o *cfsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
		s3: s3sdk.NewFromConfig(cfg, func(o *s3sdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
			o.UsePathStyle = true
		}),
		http: &http.Client{
			Transport:     tr,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (f *dpFixture) bucket(t *testing.T, name string, objects map[string]string) {
	t.Helper()

	_, err := f.s3.CreateBucket(t.Context(), &s3sdk.CreateBucketInput{Bucket: aws.String(name)})
	require.NoError(t, err)

	for k, v := range objects {
		f.put(t, name, k, v, "")
	}
}

func (f *dpFixture) put(t *testing.T, bucket, key, body, cacheControl string) {
	t.Helper()

	in := &s3sdk.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body),
		ContentType: aws.String("text/plain"),
	}
	if cacheControl != "" {
		in.CacheControl = aws.String(cacheControl)
	}

	_, err := f.s3.PutObject(t.Context(), in)
	require.NoError(t, err)
}

func (f *dpFixture) policy(t *testing.T, bucket, doc string) {
	t.Helper()

	_, err := f.s3.PutBucketPolicy(t.Context(), &s3sdk.PutBucketPolicyInput{
		Bucket: aws.String(bucket), Policy: aws.String(doc),
	})
	require.NoError(t, err)
}

func stmt(effect, principal, action, resource string) string {
	return `{"Effect":"` + effect + `","Principal":` + principal + `,"Action":` + action + `,"Resource":` + resource + `}`
}

func policyDoc(stmts ...string) string {
	return `{"Statement":[` + strings.Join(stmts, ",") + `]}`
}

func cfPolicy(bucket, extraActions, distARN string) string {
	cond := ""
	if distARN != "" {
		cond = `,"Condition":{"StringEquals":{"AWS:SourceArn":"` + distARN + `"}}`
	}

	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"cloudfront.amazonaws.com"},` +
		`"Action":[` + extraActions + `],"Resource":["arn:aws:s3:::` + bucket + `/*","arn:aws:s3:::` + bucket + `"]` +
		cond + `}]}`
}

func (f *dpFixture) oac(t *testing.T) string {
	t.Helper()

	f.callID++

	out, err := f.cf.CreateOriginAccessControl(t.Context(), &cfsdk.CreateOriginAccessControlInput{
		OriginAccessControlConfig: &types.OriginAccessControlConfig{
			Name:                          aws.String("oac-" + strings.Repeat("x", f.callID)),
			OriginAccessControlOriginType: types.OriginAccessControlOriginTypesS3,
			SigningBehavior:               types.OriginAccessControlSigningBehaviorsAlways,
			SigningProtocol:               types.OriginAccessControlSigningProtocolsSigv4,
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.OriginAccessControl.Id)
}

func s3Origin(id, bucket, path, oacID string) types.Origin {
	return types.Origin{
		Id:                    aws.String(id),
		DomainName:            aws.String(bucket + ".s3.us-east-1.amazonaws.com"),
		OriginPath:            aws.String(path),
		OriginAccessControlId: aws.String(oacID),
		S3OriginConfig:        &types.S3OriginConfig{OriginAccessIdentity: aws.String("")},
	}
}

func customOrigin(id, hostPort string) types.Origin {
	host, port, _ := strings.Cut(hostPort, ":")

	return types.Origin{
		Id:         aws.String(id),
		DomainName: aws.String(host),
		CustomOriginConfig: &types.CustomOriginConfig{
			HTTPPort:             aws.Int32(atoi32(port)),
			HTTPSPort:            aws.Int32(443),
			OriginProtocolPolicy: types.OriginProtocolPolicyHttpOnly,
		},
	}
}

func atoi32(s string) int32 {
	var n int32

	for _, c := range s {
		n = n*10 + c - '0'
	}

	return n
}

func allowed(methods ...types.Method) *types.AllowedMethods {
	return &types.AllowedMethods{Quantity: aws.Int32(int32(len(methods))), Items: methods}
}

func behavior(origin, cachePolicy string) types.DefaultCacheBehavior {
	return types.DefaultCacheBehavior{
		TargetOriginId:       aws.String(origin),
		ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
		CachePolicyId:        aws.String(cachePolicy),
		AllowedMethods:       allowed(types.MethodGet, types.MethodHead),
	}
}

type distSpec struct {
	mutate    func(*types.DistributionConfig)
	def       types.DefaultCacheBehavior
	root      string
	origins   []types.Origin
	behaviors []types.CacheBehavior
	errors    []types.CustomErrorResponse
	disabled  bool
}

type distInfo struct{ id, arn, domain string }

func (f *dpFixture) dist(t *testing.T, s distSpec) distInfo {
	t.Helper()

	f.callID++

	cfg := &types.DistributionConfig{
		CallerReference:      aws.String("ref-" + strings.Repeat("r", f.callID)),
		Comment:              aws.String("dp"),
		Enabled:              aws.Bool(!s.disabled),
		DefaultRootObject:    aws.String(s.root),
		Origins:              &types.Origins{Quantity: aws.Int32(int32(len(s.origins))), Items: s.origins},
		DefaultCacheBehavior: &s.def,
	}

	if len(s.behaviors) > 0 {
		cfg.CacheBehaviors = &types.CacheBehaviors{Quantity: aws.Int32(int32(len(s.behaviors))), Items: s.behaviors}
	}

	if len(s.errors) > 0 {
		cfg.CustomErrorResponses = &types.CustomErrorResponses{
			Quantity: aws.Int32(int32(len(s.errors))), Items: s.errors,
		}
	}

	if s.mutate != nil {
		s.mutate(cfg)
	}

	out, err := f.cf.CreateDistribution(t.Context(), &cfsdk.CreateDistributionInput{DistributionConfig: cfg})
	require.NoError(t, err)

	return distInfo{
		id: aws.ToString(out.Distribution.Id), arn: aws.ToString(out.Distribution.ARN),
		domain: aws.ToString(out.Distribution.DomainName),
	}
}

type viewerResp struct {
	header http.Header
	body   string
	status int
}

func (r viewerResp) xcache() string { return r.header.Get("X-Cache") }

// viewer issues a request through the host-based route (Host: <id>.cloudfront.net).
func (f *dpFixture) viewer(t *testing.T, d distInfo, method, path string, hdr map[string]string) viewerResp {
	t.Helper()

	return f.do(t, method, f.base+path, d.domain, nil, hdr)
}

// pathStyle issues a request through the emulator path-style route.
func (f *dpFixture) pathStyle(t *testing.T, d distInfo, path string) viewerResp {
	t.Helper()

	return f.do(t, http.MethodGet, f.base+"/_gopherstack/cloudfront/"+d.id+path, "", nil, nil)
}

func (f *dpFixture) do(
	t *testing.T, method, url, host string, body io.Reader, hdr map[string]string,
) viewerResp {
	t.Helper()

	return doRequest(t.Context(), t, f.http, method, url, host, body, hdr)
}

func doRequest(
	ctx context.Context, t *testing.T, c *http.Client, method, url, host string, body io.Reader, hdr map[string]string,
) viewerResp {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	require.NoError(t, err)

	if host != "" {
		req.Host = host
	}

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := c.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return viewerResp{status: resp.StatusCode, header: resp.Header, body: string(b)}
}

// cachePolicy creates a cache policy with the given TTLs and no forwarded values.
func (f *dpFixture) cachePolicy(t *testing.T, defTTL int64) string {
	t.Helper()

	return f.cachePolicyQuery(t, 0, defTTL, types.CachePolicyQueryStringBehaviorNone)
}

func (f *dpFixture) cachePolicyQuery(
	t *testing.T, minTTL, defTTL int64, query types.CachePolicyQueryStringBehavior,
) string {
	t.Helper()

	f.callID++

	out, err := f.cf.CreateCachePolicy(t.Context(), &cfsdk.CreateCachePolicyInput{
		CachePolicyConfig: &types.CachePolicyConfig{
			Name:       aws.String("cp-" + strings.Repeat("c", f.callID)),
			MinTTL:     aws.Int64(minTTL),
			DefaultTTL: aws.Int64(defTTL),
			MaxTTL:     aws.Int64(3600),
			ParametersInCacheKeyAndForwardedToOrigin: &types.ParametersInCacheKeyAndForwardedToOrigin{
				CookiesConfig: &types.CachePolicyCookiesConfig{
					CookieBehavior: types.CachePolicyCookieBehaviorNone,
				},
				HeadersConfig: &types.CachePolicyHeadersConfig{
					HeaderBehavior: types.CachePolicyHeaderBehaviorNone,
				},
				QueryStringsConfig:       &types.CachePolicyQueryStringsConfig{QueryStringBehavior: query},
				EnableAcceptEncodingGzip: aws.Bool(false),
			},
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.CachePolicy.Id)
}
