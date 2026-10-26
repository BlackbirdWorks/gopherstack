package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func twoBackendMap(name string) *computepb.UrlMap {
	return &computepb.UrlMap{
		Name:           s(name),
		DefaultService: s(bsRef),
		HostRules:      []*computepb.HostRule{{Hosts: []string{"*.example.com"}, PathMatcher: s("pm")}},
		PathMatchers: []*computepb.PathMatcher{{
			Name:           s("pm"),
			DefaultService: s(bsRef),
			PathRules:      []*computepb.PathRule{{Paths: []string{"/api/*"}, Service: s(gl + "backendServices/api")}},
		}},
	}
}

func TestURLMapInsert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(m *computepb.UrlMap)
		name   string
		reason string
		status int
	}{
		{name: "valid", mutate: func(*computepb.UrlMap) {}},
		{
			name: "no default target", status: http.StatusBadRequest, reason: "invalid",
			mutate: func(m *computepb.UrlMap) { m.DefaultService = nil },
		},
		{
			name: "unknown service", status: http.StatusNotFound, reason: "notFound",
			mutate: func(m *computepb.UrlMap) { m.DefaultService = s(gl + "backendServices/missing") },
		},
		{
			name: "host rule names missing matcher", status: http.StatusBadRequest, reason: "invalid",
			mutate: func(m *computepb.UrlMap) { m.HostRules[0].PathMatcher = s("nope") },
		},
		{
			name: "duplicate host", status: http.StatusBadRequest, reason: "invalid",
			mutate: func(m *computepb.UrlMap) {
				m.HostRules = append(m.HostRules, &computepb.HostRule{Hosts: []string{"*.example.com"}, PathMatcher: s("pm")})
			},
		},
		{
			name: "path without slash", status: http.StatusBadRequest, reason: "invalid",
			mutate: func(m *computepb.UrlMap) { m.PathMatchers[0].PathRules[0].Paths = []string{"api"} },
		},
		{
			name: "path and route rules together", status: http.StatusBadRequest, reason: "invalid",
			mutate: func(m *computepb.UrlMap) {
				m.PathMatchers[0].RouteRules = []*computepb.HttpRouteRule{{Priority: new(int32(1)), Service: s(bsRef)}}
			},
		},
		{
			name: "service and redirect", status: http.StatusBadRequest, reason: "invalid",
			mutate: func(m *computepb.UrlMap) {
				m.DefaultUrlRedirect = &computepb.HttpRedirectAction{HttpsRedirect: new(true)}
			},
		},
		{
			name: "failing test", status: http.StatusBadRequest, reason: "invalid",
			mutate: func(m *computepb.UrlMap) {
				m.Tests = []*computepb.UrlMapTest{{Host: s("a.example.com"), Path: s("/api/x"), Service: s(bsRef)}}
			},
		},
		{
			name: "passing test",
			mutate: func(m *computepb.UrlMap) {
				m.Tests = []*computepb.UrlMapTest{
					{Host: s("a.example.com"), Path: s("/api/x"), Service: s(gl + "backendServices/api")},
					{Host: s("other.org"), Path: s("/api/x"), Service: s(bsRef)},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.lbStack(t)
			require.NoError(t, e.lbBS(t, &computepb.BackendService{Name: s("api")}))

			m := twoBackendMap("um2")
			tt.mutate(m)

			err := e.lbMap(t, m)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			require.NoError(t, err)

			got, gerr := client(t, e, compute.NewUrlMapsRESTClient).Get(t.Context(), &computepb.GetUrlMapRequest{Project: project, UrlMap: "um2"})
			require.NoError(t, gerr)
			assert.Contains(t, got.GetDefaultService(), "/global/backendServices/bs")
			assert.Contains(t, got.GetPathMatchers()[0].GetPathRules()[0].GetService(), "/backendServices/api")
		})
	}
}

func TestURLMapValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(m *computepb.UrlMap)
		name     string
		loaded   bool
		passed   bool
		failures int
	}{
		{name: "ok", loaded: true, passed: true, mutate: func(*computepb.UrlMap) {}},
		{
			name: "unknown service", loaded: false, passed: false,
			mutate: func(m *computepb.UrlMap) { m.DefaultService = s(gl + "backendServices/missing") },
		},
		{
			name: "test failure", loaded: true, passed: false, failures: 1,
			mutate: func(m *computepb.UrlMap) {
				m.Tests = []*computepb.UrlMapTest{{Host: s("a.example.com"), Path: s("/api/x"), Service: s(bsRef)}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.lbStack(t)
			require.NoError(t, e.lbBS(t, &computepb.BackendService{Name: s("api")}))

			m := twoBackendMap("probe")
			tt.mutate(m)

			res, err := client(t, e, compute.NewUrlMapsRESTClient).Validate(t.Context(), &computepb.ValidateUrlMapRequest{
				Project: project, UrlMap: "um",
				UrlMapsValidateRequestResource: &computepb.UrlMapsValidateRequest{Resource: m},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.loaded, res.GetResult().GetLoadSucceeded())
			assert.Equal(t, tt.passed, res.GetResult().GetTestPassed())
			assert.Len(t, res.GetResult().GetTestFailures(), tt.failures)

			if !tt.loaded {
				assert.NotEmpty(t, res.GetResult().GetLoadErrors())
			}
		})
	}
}

func TestRegionalURLMap(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	rbs := client(t, e, compute.NewRegionBackendServicesRESTClient)
	rum := client(t, e, compute.NewRegionUrlMapsRESTClient)

	op, err := rbs.Insert(t.Context(), &computepb.InsertRegionBackendServiceRequest{
		Project: project, Region: region, BackendServiceResource: &computepb.BackendService{
			Name: s("rbs"), LoadBalancingScheme: s("INTERNAL_MANAGED"),
		},
	})
	e.done(t, op, err)

	op, err = rum.Insert(t.Context(), &computepb.InsertRegionUrlMapRequest{
		Project: project, Region: region, UrlMapResource: &computepb.UrlMap{
			Name: s("rum"), DefaultService: s(regBase + "backendServices/rbs"),
		},
	})
	e.done(t, op, err)

	got, err := rum.Get(t.Context(), &computepb.GetRegionUrlMapRequest{Project: project, Region: region, UrlMap: "rum"})
	require.NoError(t, err)
	assert.Contains(t, got.GetSelfLink(), "/regions/"+region+"/urlMaps/rum")

	e.lbHC(t, "hc")
	e.group(t, "ig")
	require.NoError(t, e.lbBS(t, &computepb.BackendService{Name: s("bs"), HealthChecks: []string{hcRef}}))

	_, err = rum.Insert(t.Context(), &computepb.InsertRegionUrlMapRequest{
		Project: project, Region: region, UrlMapResource: &computepb.UrlMap{Name: s("bad"), DefaultService: s(bsRef)},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
}
