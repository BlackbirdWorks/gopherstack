package app

import (
	"github.com/blackbirdworks/gopherstack/dashboard/api/v1/dashboardv1connect"
	"github.com/blackbirdworks/gopherstack/internal/routegates"

	"math/rand/v2"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	indexProbeSeg   = "zzq9"
	indexCrossCases = 16000
	indexChunks     = 4

	indexVariantsPerCase = 20
	indexShortStride     = 8
)

func splitURI(uri string) (string, string) {
	path, query, ok := strings.Cut(uri, "?")
	if ok {
		query = "?" + query
	}

	return path, query
}

func firstSegment(path string) string {
	seg, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")

	return seg
}

func withPath(rc routingCase, path string) routingCase {
	_, query := splitURI(rc.uri)
	rc.uri = path + query

	return rc
}

func pathVariants(rc routingCase) []routingCase {
	path, _ := splitURI(rc.uri)
	seg := firstSegment(path)
	rest := strings.TrimPrefix(path, "/"+seg)

	variants := []routingCase{
		withPath(rc, "/"),
		withPath(rc, "/"+indexProbeSeg+rest),
		withPath(rc, path+"x"),
		withPath(rc, "/"+seg+"x"+rest),
	}

	if len(seg) > 1 {
		variants = append(variants, withPath(rc, "/"+seg[:len(seg)-1]+rest))
	}

	return variants
}

func arnVariants(rc routingCase) []routingCase {
	parts := strings.Split(firstPath(rc), "/")
	tokens := []string{"svc"}

	if rc.authSvc != "" {
		tokens = append(tokens, rc.authSvc)
	}

	var out []routingCase

	for _, tk := range tokens {
		arn := "arn:aws:" + tk + ":us-east-1:000000000000:res"

		for i := 2; i < len(parts); i++ {
			if strings.HasPrefix(parts[i], "x") {
				q := slices.Clone(parts)
				q[i] = arn
				out = append(out, withPath(rc, strings.Join(q, "/")))
			}
		}

		if len(parts) > 1 {
			out = append(out, withPath(rc, "/"+arn+strings.TrimPrefix(firstPath(rc), "/"+firstSegment(firstPath(rc)))))
		}
	}

	return out
}

func fieldVariants(rc routingCase) []routingCase {
	mutators := []func(*routingCase){
		func(m *routingCase) { m.authSvc = "" },
		func(m *routingCase) { m.authSvc = indexProbeSeg },
		func(m *routingCase) { m.ctype = "" },
		func(m *routingCase) { m.target = "" },
		func(m *routingCase) { m.target = "Zz.Op" },
		func(m *routingCase) { m.host = "localhost:4566" },
		func(m *routingCase) { m.host = "s3.us-east-1.amazonaws.com" },
		func(m *routingCase) { m.body = "" },
		func(m *routingCase) { m.headers = "" },
		func(m *routingCase) { m.method = "GET" },
	}

	out := make([]routingCase, 0, len(mutators))

	for _, mut := range mutators {
		m := rc
		mut(&m)
		out = append(out, m)
	}

	return out
}

func crossCases(cases []routingCase, n int) []routingCase {
	rng := rand.New(rand.NewPCG(7, 11))
	out := make([]routingCase, 0, n)

	for range n {
		m := cases[rng.IntN(len(cases))]
		o := cases[rng.IntN(len(cases))]

		switch rng.IntN(5) {
		case 0:
			m = withPath(m, firstPath(o))
		case 1:
			m.target = o.target
		case 2:
			m.authSvc, m.ctype = o.authSvc, o.ctype
		case 3:
			m.host, m.headers = o.host, o.headers
		default:
			m.body, m.method = o.body, o.method
		}

		out = append(out, m)
	}

	return out
}

func firstPath(rc routingCase) string {
	path, _ := splitURI(rc.uri)

	return path
}

func TestRoutingIndexMatchesLinearScan(t *testing.T) {
	t.Parallel()

	reg, entries := routingFixture(t)
	cases := loadRoutingCorpus(t)
	router := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())

	all := make([]routingCase, 0, len(cases)*(1+indexVariantsPerCase)+indexCrossCases)

	for _, rc := range cases {
		all = append(all, rc)
		all = append(all, pathVariants(rc)...)
		all = append(all, arnVariants(rc)...)
		all = append(all, fieldVariants(rc)...)
	}

	all = append(all, crossCases(cases, indexCrossCases)...)

	// -short (CI unit job) checks every 8th case so the race build stays inside its timeout.
	if testing.Short() {
		all = strideSample(all, indexShortStride)
	}

	chunk := (len(all) + indexChunks - 1) / indexChunks

	for i := range indexChunks {
		part := all[min(i*chunk, len(all)):min((i+1)*chunk, len(all))]

		t.Run("chunk", func(t *testing.T) {
			t.Parallel()

			var diffs []string

			for _, rc := range part {
				scan := scanSelect(entries, echo.NewContext(rc.request(t), httptest.NewRecorder()))
				got := entryName(router.Lookup(echo.NewContext(rc.request(t), httptest.NewRecorder())))

				if got != scan && len(diffs) < routingMaxDiffs {
					diffs = append(diffs, rc.method+" "+rc.host+rc.uri+" target="+rc.target+": got "+got+" want "+scan)
				}
			}

			assert.Empty(t, diffs, "indexed Lookup diverged from the ungated priority scan")
		})
	}
}

func TestRoutingPathGatedAlternatives(t *testing.T) {
	t.Parallel()

	reg, _ := routingFixture(t)
	router := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())

	tests := []struct {
		name, method, uri, target, want string
	}{
		{"resource_groups", "POST", "/", "ResourceGroups.GetGroup", "ResourceGroups"},
		{"scheduler", "POST", "/", "AWSScheduler.GetSchedule", "Scheduler"},
		{"apigateway", "POST", "/", "APIGateway.GetRestApis", "APIGateway"},
		{"lambda", "POST", "/", "AWSLambda.ListFunctions", "Lambda"},
		{"iotdataplane_admin", "POST", "/_admin/connections/client-1", "", "IoTDataPlane"},
		{
			"dashboard_stream_console", "POST",
			dashboardv1connect.DashboardServiceStreamConsoleProcedure, "", "Dashboard",
		},
		{
			"dashboard_stream_metrics", "POST",
			dashboardv1connect.DashboardServiceStreamMetricsProcedure, "", "Dashboard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := routingCase{
				method: tt.method, host: "localhost:4566", uri: tt.uri, ctype: "application/x-amz-json-1.1",
				target: tt.target, body: "{}",
			}

			got := router.Lookup(echo.NewContext(rc.request(t), httptest.NewRecorder()))
			assert.Equal(t, tt.want, entryName(got))
		})
	}
}

func strideSample(in []routingCase, stride int) []routingCase {
	out := make([]routingCase, 0, len(in)/stride+1)
	for i := 0; i < len(in); i += stride {
		out = append(out, in[i])
	}

	return out
}

func TestRoutingLambdaFunctionURLHost(t *testing.T) {
	t.Parallel()

	reg, _ := routingFixture(t)
	router := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())

	tests := []struct {
		name, method, host, uri string
		wantLambda              bool
	}{
		{"on_aws_root", "GET", "abcdef0123456789abcdef0123456789.lambda-url.us-east-1.on.aws", "/", true},
		{"on_aws_path_post", "POST", "abcdef0123456789abcdef0123456789.lambda-url.eu-west-1.on.aws", "/a/b?x=1", true},
		{"localhost_zone", "GET", "abcdef0123456789abcdef0123456789.lambda-url.us-east-1.localhost:4566", "/", true},
		{"s3_path_style", "GET", "localhost:4566", "/bucket/key", false},
		{"s3_vhost", "GET", "bkt.s3.us-east-1.amazonaws.com", "/key", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := routingCase{method: tt.method, host: tt.host, uri: tt.uri}
			got := router.Lookup(echo.NewContext(rc.request(t), httptest.NewRecorder()))
			assert.Equal(t, tt.wantLambda, entryName(got) == "Lambda")
		})
	}
}

func TestRoutingRPCv2CBORGates(t *testing.T) {
	t.Parallel()

	reg, _ := routingFixture(t)
	router := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())

	tests := []struct {
		name, uri, want, notWant string
	}{
		{"codeconnections", "/service/CodeConnections_20231201/operation/ListConnections", "CodeConnections", ""},
		{"translate", "/service/AWSShineFrontendService_20170701/operation/ListLanguages", "Translate", ""},
		{"unknown_shape", "/service/Unknown_20200101/operation/ListConnections", "", "CodeConnections"},
		{"unknown_shape_translate", "/service/Unknown_20200101/operation/ListLanguages", "", "Translate"},
		{"suffixed_shape", "/service/CodeConnections_20231201x/operation/ListConnections", "", "CodeConnections"},
		{"suffixed_translate", "/service/AWSShineFrontendService_20170701x/operation/ListLanguages", "", "Translate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := routingCase{
				method: "POST", host: "localhost:4566", uri: tt.uri,
				ctype: "application/cbor", body: "",
			}
			req := rc.request(t)
			req.Header.Set("Smithy-Protocol", "rpc-v2-cbor")

			got := router.Lookup(echo.NewContext(req, httptest.NewRecorder()))

			if tt.want != "" {
				assert.Equal(t, tt.want, entryName(got))
			}

			if tt.notWant != "" {
				assert.NotEqual(t, tt.notWant, entryName(got))
			}
		})
	}
}

func gatePrefixCases(rc routingCase, prefixes []string) []routingCase {
	out := make([]routingCase, 0, 9*len(prefixes))

	for _, p := range prefixes {
		rest := strings.TrimLeft(p, "/")

		for _, lead := range []string{"/", "//", "///"} {
			for _, tail := range []string{"", "/x", "x"} {
				v := rc
				v.uri = lead + rest + tail
				out = append(out, v)
			}
		}
	}

	return out
}

func TestRoutingIndexLeadingSlashes(t *testing.T) {
	t.Parallel()

	reg, entries := routingFixture(t)
	router := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())

	var cases []routingCase

	for _, rc := range loadRoutingCorpus(t) {
		path, query := splitURI(rc.uri)

		for _, extra := range []string{"/", "//"} {
			v := rc
			v.uri = extra + path + query
			cases = append(cases, v)
		}
	}

	for _, prefixes := range routegates.PathGates() {
		for _, method := range []string{"GET", "POST", "PUT", "DELETE"} {
			base := routingCase{method: method, host: "localhost:4566"}
			cases = append(cases, gatePrefixCases(base, prefixes)...)
		}
	}

	if testing.Short() {
		cases = strideSample(cases, indexShortStride)
	}

	var diffs []string

	for _, rc := range cases {
		scan := scanSelect(entries, echo.NewContext(rc.request(t), httptest.NewRecorder()))
		got := entryName(router.Lookup(echo.NewContext(rc.request(t), httptest.NewRecorder())))

		if got != scan && len(diffs) < routingMaxDiffs {
			diffs = append(diffs, rc.method+" "+rc.host+rc.uri+": got "+got+" want "+scan)
		}
	}

	assert.Empty(t, diffs, "indexed Lookup diverged from the ungated scan on leading-slash paths")
}
