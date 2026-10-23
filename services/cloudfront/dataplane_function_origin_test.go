package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
)

func originGroup(id string, primary, secondary string, codes ...int32) types.OriginGroup {
	sc := make([]int32, len(codes))
	copy(sc, codes)

	return types.OriginGroup{
		Id: aws.String(id),
		FailoverCriteria: &types.OriginGroupFailoverCriteria{
			StatusCodes: &types.StatusCodes{Quantity: aws.Int32(int32(len(sc))), Items: sc},
		},
		Members: &types.OriginGroupMembers{
			Quantity: aws.Int32(2),
			Items:    []types.OriginGroupMember{{OriginId: aws.String(primary)}, {OriginId: aws.String(secondary)}},
		},
	}
}

func withGroups(groups ...types.OriginGroup) func(*types.DistributionConfig) {
	return func(c *types.DistributionConfig) {
		c.OriginGroups = &types.OriginGroups{Quantity: aws.Int32(int32(len(groups))), Items: groups}
	}
}

func TestDataPlane_FunctionOriginSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		code       string
		path       string
		wantBody   string
		wantA      int
		wantB      int
		wantStatus int
	}{
		{
			name: "no_helper_uses_behavior_origin", path: "/x",
			code:     `function handler(e) { return e.request; }`,
			wantBody: "A:/x", wantA: 1, wantStatus: http.StatusOK,
		},
		{
			name: "select_request_origin_by_id", path: "/x",
			code:     cfFn(`cf.selectRequestOriginById('b')`),
			wantBody: "B:/x", wantB: 1, wantStatus: http.StatusOK,
		},
		{
			name: "select_with_origin_path_override", path: "/x",
			code: `import cf from 'cloudfront';
function handler(e) { cf.selectRequestOriginById('b', {originPath: '/pre'}); return e.request; }`,
			wantBody: "B:/pre/x", wantB: 1, wantStatus: http.StatusOK,
		},
		{
			name: "update_request_origin_domain_and_port", path: "/x",
			code: `import cf from 'cloudfront';
function handler(e) {
  cf.updateRequestOrigin({domainName: '127.0.0.1', customOriginConfig: {port: PORT_B, protocol: 'http'}});
  return e.request;
}`,
			wantBody: "B:/x", wantB: 1, wantStatus: http.StatusOK,
		},
		{
			name: "update_custom_headers_reach_origin", path: "/hdr",
			code: `import cf from 'cloudfront';
function handler(e) { cf.updateRequestOrigin({customHeaders: {'x-from-fn': 'yes'}}); return e.request; }`,
			wantBody: "A:/hdr", wantA: 1, wantStatus: http.StatusOK,
		},
		{
			name: "invalid_params_fail_function", path: "/x",
			code:       cfFn(`cf.updateRequestOrigin({timeouts: {readTimeout: 99}})`),
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "unknown_origin_id_fails_function", path: "/x",
			code:       cfFn(`cf.selectRequestOriginById('zzz')`),
			wantStatus: http.StatusBadGateway,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			a := newOriginServer(t, echoOrigin("A:"))
			b := newOriginServer(t, echoOrigin("B:"))

			code := replacePort(tt.code, b.hostPort())
			def := behavior("a", f.cachePolicy(t, 0))
			def.FunctionAssociations = &types.FunctionAssociations{
				Quantity: aws.Int32(1),
				Items: []types.FunctionAssociation{{
					EventType: types.EventTypeViewerRequest, FunctionARN: aws.String(publishFn(t, f, "sel", code)),
				}},
			}

			d := f.dist(t, distSpec{
				origins: []types.Origin{customOrigin("a", a.hostPort()), customOrigin("b", b.hostPort())},
				def:     def,
			})

			got := f.viewer(t, d, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, got.status, got.body)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, got.body)
			}

			assert.EqualValues(t, tt.wantA, a.hits.Load())
			assert.EqualValues(t, tt.wantB, b.hits.Load())

			if tt.name == "update_custom_headers_reach_origin" {
				assert.Equal(t, "yes", a.last(t).header.Get("X-From-Fn"))
			}
		})
	}
}

func TestDataPlane_FunctionOriginSwitchesS3Bucket(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	f.bucket(t, "bucket-a", map[string]string{"obj": "from-a"})
	f.bucket(t, "bucket-b", map[string]string{"obj": "from-b"})

	code := `import cf from 'cloudfront';
function handler(e) {
  cf.updateRequestOrigin({domainName: 'bucket-b.s3.us-east-1.amazonaws.com'});
  return e.request;
}`

	def := behavior("a", f.cachePolicy(t, 0))
	def.FunctionAssociations = &types.FunctionAssociations{
		Quantity: aws.Int32(1),
		Items: []types.FunctionAssociation{{
			EventType: types.EventTypeViewerRequest, FunctionARN: aws.String(publishFn(t, f, "s3sw", code)),
		}},
	}

	oac := f.oac(t)
	f.policy(t, "bucket-a", cfPolicy("bucket-a", `"s3:GetObject"`, ""))
	f.policy(t, "bucket-b", cfPolicy("bucket-b", `"s3:GetObject"`, ""))

	d := f.dist(t, distSpec{origins: []types.Origin{s3Origin("a", "bucket-a", "", oac)}, def: def})

	got := f.viewer(t, d, http.MethodGet, "/obj", nil)
	assert.Equal(t, http.StatusOK, got.status, got.body)
	assert.Equal(t, "from-b", got.body)
}

func TestDataPlane_OriginGroupFailover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		fnCode      string
		method      string
		wantBody    string
		primary     int
		secondary   int
		wantStatus  int
		wantPrimary int
		wantSecond  int
	}{
		{
			name: "primary_ok_no_failover", primary: 200, secondary: 200, method: http.MethodGet,
			wantStatus: 200, wantBody: "P", wantPrimary: 1,
		},
		{
			name: "primary_5xx_fails_over", primary: 503, secondary: 200, method: http.MethodGet,
			wantStatus: 200, wantBody: "S", wantPrimary: 1, wantSecond: 1,
		},
		{
			name: "status_not_in_criteria_passes_through", primary: 500, secondary: 200, method: http.MethodGet,
			wantStatus: 500, wantBody: "P", wantPrimary: 1,
		},
		{
			name: "both_fail_returns_secondary", primary: 503, secondary: 502, method: http.MethodGet,
			wantStatus: 502, wantBody: "S", wantPrimary: 1, wantSecond: 1,
		},
		{
			name: "function_created_group_fails_over", primary: 503, secondary: 200, method: http.MethodGet,
			fnCode: `import cf from 'cloudfront';
function handler(e) {
  cf.createRequestOriginGroup({originIds: ['p', 's'], failoverCriteria: {statusCodes: [503]}});
  return e.request;
}`,
			wantStatus: 200, wantBody: "S", wantPrimary: 1, wantSecond: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			p := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.primary)
				_, _ = w.Write([]byte("P"))
			})
			s := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.secondary)
				_, _ = w.Write([]byte("S"))
			})

			target := "grp"
			if tt.fnCode != "" {
				target = "p"
			}

			def := behavior(target, f.cachePolicy(t, 0))
			if tt.fnCode != "" {
				def.FunctionAssociations = &types.FunctionAssociations{
					Quantity: aws.Int32(1),
					Items: []types.FunctionAssociation{
						{
							EventType:   types.EventTypeViewerRequest,
							FunctionARN: aws.String(publishFn(t, f, "grp", tt.fnCode)),
						},
					},
				}
			}

			d := f.dist(t, distSpec{
				origins: []types.Origin{customOrigin("p", p.hostPort()), customOrigin("s", s.hostPort())},
				def:     def,
				mutate:  withGroups(originGroup("grp", "p", "s", 503)),
			})

			got := f.viewer(t, d, tt.method, "/x", nil)
			assert.Equal(t, tt.wantStatus, got.status, got.body)
			assert.Equal(t, tt.wantBody, got.body)
			assert.EqualValues(t, tt.wantPrimary, p.hits.Load())
			assert.EqualValues(t, tt.wantSecond, s.hits.Load())
		})
	}
}

func TestDataPlane_FunctionComputeLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mult       int
		wantStatus int
	}{
		{name: "strict_limit_is_503", mult: 1, wantStatus: http.StatusServiceUnavailable},
		{name: "lenient_default_passes_short_function", mult: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDPFixture(t)
			f.h.Backend.SetFunctionComputeMultiplier(tt.mult)

			origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

			code := `function handler(e) { return e.request; }`
			if tt.mult == 1 {
				code = `function handler(e) { while (true) {} }`
			}

			def := behavior("o", f.cachePolicy(t, 0))
			def.FunctionAssociations = &types.FunctionAssociations{
				Quantity: aws.Int32(1),
				Items: []types.FunctionAssociation{{
					EventType: types.EventTypeViewerRequest, FunctionARN: aws.String(publishFn(t, f, "cl", code)),
				}},
			}

			d := f.dist(t, distSpec{origins: []types.Origin{customOrigin("o", origin.hostPort())}, def: def})

			got := f.viewer(t, d, http.MethodGet, "/x", nil)
			assert.Equal(t, tt.wantStatus, got.status, got.body)

			if tt.wantStatus == http.StatusServiceUnavailable {
				assert.EqualValues(t, 0, origin.hits.Load())
			}
		})
	}
}

func replacePort(code, hostPort string) string {
	_, port, _ := strings.Cut(hostPort, ":")

	return strings.ReplaceAll(code, "PORT_B", port)
}

func echoOrigin(prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(prefix + r.URL.Path)) }
}

func cfFn(call string) string {
	return "import cf from 'cloudfront'; function handler(e) { " + call + "; return e.request; }"
}
