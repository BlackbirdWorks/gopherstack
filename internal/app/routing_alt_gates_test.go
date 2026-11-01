package app

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/routegates"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	altLambdaHost = "abcdef0123456789abcdef0123456789.lambda-url.us-east-1.on.aws"
	altRowStride  = 16
	altFormCT     = "application/x-www-form-urlencoded"
	altMaxASCII   = 0x7f
)

// altAdmits is an independent reference for service.AltGate semantics.
func altAdmits(g service.AltGate, req *http.Request, rc routingCase) bool {
	path := req.URL.Path
	collapsed := "/" + strings.TrimLeft(path, "/")
	lowerUA := strings.ToLower(req.Header.Get("User-Agent")) + "\x00" +
		strings.ToLower(req.Header.Get("X-Amz-User-Agent"))
	nonASCII := strings.ContainsFunc(req.Host+lowerUA, func(r rune) bool { return r > altMaxASCII })
	svc := httputils.ExtractServiceFromRequest(req)

	return slices.ContainsFunc(g.Target, func(p string) bool { return strings.HasPrefix(rc.target, p) }) ||
		slices.ContainsFunc(g.Path, func(p string) bool { return strings.HasPrefix(collapsed, p) }) ||
		slices.ContainsFunc(g.PathSuffix, func(p string) bool { return strings.HasSuffix(path, p) }) ||
		slices.ContainsFunc(g.Host, func(p string) bool {
			return nonASCII || strings.Contains(strings.ToLower(req.Host), p)
		}) ||
		slices.ContainsFunc(g.UserAgent, func(p string) bool { return nonASCII || strings.Contains(lowerUA, p) }) ||
		slices.ContainsFunc(g.ContentType, func(p string) bool {
			return strings.Contains(strings.ToLower(req.Header.Get("Content-Type")), p)
		}) ||
		slices.ContainsFunc(g.Query, func(p string) bool { return strings.Contains(req.URL.RawQuery, p) }) ||
		slices.Contains(g.Scope, svc)
}

func altVariants(rc routingCase, g service.AltGate) []routingCase {
	_, query := splitURI(rc.uri)

	var out []routingCase

	mut := func(f func(*routingCase)) {
		v := rc
		f(&v)
		out = append(out, v)
	}

	mut(func(v *routingCase) { v.target = "" })
	mut(func(v *routingCase) { v.target = "Zz.Op" })
	mut(func(v *routingCase) { v.authSvc = "" })
	mut(func(v *routingCase) { v.authSvc = "s3" })

	for _, p := range g.Target {
		mut(func(v *routingCase) { v.target = p + "Op" })
	}

	for _, s := range append([]string{"s3"}, g.Scope...) {
		mut(func(v *routingCase) { v.authSvc = s })
		mut(func(v *routingCase) {
			sep := "?"
			if query != "" {
				sep = "&"
			}

			v.authSvc = ""
			v.uri += sep + "X-Amz-Credential=AKIA%2F20260101%2Fus-east-1%2F" + s + "%2Faws4_request&X-Amz-Signature=00"
		})
	}

	for _, h := range []string{
		"localhost:4566", altLambdaHost, "ABC.LAMBDA-URL.eu-west-1.LOCALHOST:4566", "x.lambda-url.y.on.aws:443",
	} {
		mut(func(v *routingCase) { v.host = h })
	}

	for _, ua := range []string{
		"", "User-Agent:MediaStoreData", "X-Amz-User-Agent:aws-sdk-js mediastore-data", "User-Agent:xİmediastoredata",
		"User-Agent:aws-sdk-go-v2 api/mediastoredata#1", "User-Agent:aws-sdk-go-v2 api/mediastore#1",
	} {
		mut(func(v *routingCase) { v.headers = ua })
	}

	formBodies := []string{
		"Action=ListQueues&Version=2012-11-05", "Action=GetCallerIdentity&Version=2011-06-15",
		"Action=ListTopics&Version=2010-03-31",
	}

	for _, ct := range []string{"", altFormCT, "APPLICATION/X-WWW-FORM-URLENCODED; charset=utf-8"} {
		mut(func(v *routingCase) { v.ctype = ct })
		mut(func(v *routingCase) { v.ctype, v.method = ct, "POST" })

		for _, body := range formBodies {
			mut(func(v *routingCase) { v.ctype, v.method, v.body = ct, "POST", body })
		}
	}

	for _, q := range []string{
		"?Action=ConfirmSubscription&TopicArn=x", "?Act%69on=ConfirmSubscription&TopicArn=x", "?x=1",
	} {
		path, _ := splitURI(rc.uri)

		mut(func(v *routingCase) { v.method, v.uri = "GET", path+q })
	}

	for _, p := range g.Path {
		mut(func(v *routingCase) { v.uri = p + "x" + query })
		mut(func(v *routingCase) { v.uri = "/" + p + query })
	}

	for _, sfx := range g.PathSuffix {
		s := "/" + strings.TrimLeft(sfx, "/")

		mut(func(v *routingCase) { v.uri = s + query })
		mut(func(v *routingCase) { v.uri = "/pool" + s + query })
		mut(func(v *routingCase) { v.uri = "/a/b" + s + "x" + query })
		mut(func(v *routingCase) { v.uri = "/a/b" + strings.TrimLeft(sfx, "/") + query })
	}

	return out
}

func TestRoutingAltGatedLookupMatchesScan(t *testing.T) {
	t.Parallel()

	reg, entries := routingFixture(t)
	router := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())

	cases := loadRoutingCorpus(t)
	if testing.Short() {
		cases = strideSample(cases, altRowStride)
	}

	gates := routegates.AltGates()

	// Lookup and the scan consider every service, so the per-service variant sets are
	// unioned and deduplicated; each distinct request is checked exactly once.
	chunk := (len(cases) + holdChunks - 1) / holdChunks

	var done sync.Map

	for i := range holdChunks {
		part := cases[min(i*chunk, len(cases)):min((i+1)*chunk, len(cases))]

		t.Run("chunk", func(t *testing.T) {
			t.Parallel()

			for _, rc := range part {
				seen := map[routingCase]bool{rc: true}
				all := []routingCase{rc}

				for _, g := range gates {
					for _, v := range altVariants(rc, g) {
						if !seen[v] {
							seen[v] = true
							all = append(all, v)
						}
					}
				}

				for _, v := range all {
					if _, dup := done.LoadOrStore(v, struct{}{}); dup {
						continue
					}

					scan := scanSelect(entries, echo.NewContext(v.request(t), httptest.NewRecorder()))
					got := entryName(router.Lookup(echo.NewContext(v.request(t), httptest.NewRecorder())))
					require.Equal(t, scan, got, "%s %s%s target=%q auth=%q headers=%q",
						v.method, v.host, v.uri, v.target, v.authSvc, v.headers)
				}
			}
		})
	}
}

func BenchmarkRouterLookupS3Object(b *testing.B) {
	_, byName := benchServer(b)
	reg := service.NewRegistry()

	for _, svc := range byName {
		require.NoError(b, reg.Register(svc))
	}

	base := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates())
	withAlt := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())

	routers := []struct {
		r    *service.Router
		name string
	}{{base, "no_alt"}, {withAlt, "alt"}}

	for _, method := range []string{"GET", "PUT"} {
		for _, rt := range routers {
			b.Run(method+"/"+rt.name, func(b *testing.B) {
				b.ReportAllocs()

				req := httptest.NewRequest(method, "http://localhost:4566/bench-server-bucket/obj.txt", nil)
				req.Header.Set("Authorization", strings.Replace(benchAuth, "%s", "s3", 1))

				c := echo.NewContext(req, httptest.NewRecorder())

				for range b.N {
					if e := rt.r.Lookup(c); e == nil || e.Registerable.Name() != "S3" {
						b.Fatal("not S3")
					}
				}
			})
		}
	}
}
