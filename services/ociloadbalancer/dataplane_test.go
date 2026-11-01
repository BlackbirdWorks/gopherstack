package ociloadbalancer_test

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const httpPort = 80

func listener() loadbalancer.ListenerDetails {
	return loadbalancer.ListenerDetails{DefaultBackendSetName: new("bs"), Port: new(httpPort), Protocol: new("HTTP")}
}

func tally(t *testing.T, e *env, lbID string, n int) map[string]int {
	t.Helper()

	out := map[string]int{}

	for range n {
		r := e.call(t, lbID, httpPort, "", "/x", nil)
		require.Equal(t, http.StatusOK, r.status)
		out[r.header.Get("X-Backend")]++
	}

	return out
}

func TestBalancingPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want     map[string]int
		name     string
		policy   string
		wa       int
		wb       int
		n        int
		distinct int
	}{
		{name: "round robin", policy: "ROUND_ROBIN", wa: 1, wb: 1, n: 6, want: map[string]int{"a": 3, "b": 3}},
		{name: "weighted round robin", policy: "ROUND_ROBIN", wa: 3, wb: 1, n: 8, want: map[string]int{"a": 6, "b": 2}},
		{name: "ip hash", policy: "IP_HASH", wa: 1, wb: 1, n: 5, distinct: 1},
		{name: "least connections", policy: "LEAST_CONNECTIONS", wa: 1, wb: 1, n: 4, distinct: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			a, b := startBackend(t, "a"), startBackend(t, "b")
			set := backendSet(tt.policy, a, b)
			set.Backends[0].Weight, set.Backends[1].Weight = new(tt.wa), new(tt.wb)

			d := baseDetails(tt.name)
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
			id := e.mkLB(t, d)

			got := tally(t, e, id, tt.n)
			if tt.want != nil {
				assert.Equal(t, tt.want, got)

				return
			}

			assert.LessOrEqual(t, len(got), tt.distinct)
			assert.Equal(t, tt.n, got["a"]+got["b"])
		})
	}
}

func TestHealthCheckFailover(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a, b := startBackend(t, "a"), startBackend(t, "b")
	d := baseDetails("failover")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": backendSet("ROUND_ROBIN", a, b)}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
	id := e.mkLB(t, d)

	assert.Equal(t, map[string]int{"a": 2, "b": 2}, tally(t, e, id, 4))

	health := func() loadbalancer.BackendSetHealthStatusEnum {
		resp, err := e.lb.GetBackendSetHealth(t.Context(), loadbalancer.GetBackendSetHealthRequest{
			LoadBalancerId: &id, BackendSetName: new("bs"),
		})
		require.NoError(t, err)

		return resp.Status
	}

	assert.Equal(t, loadbalancer.BackendSetHealthStatusOk, health())

	a.healthy.Store(false)
	e.clock.Advance(2 * time.Second)

	assert.Equal(t, map[string]int{"b": 4}, tally(t, e, id, 4))
	assert.Equal(t, loadbalancer.BackendSetHealthStatusWarning, health())

	bname := a.ip() + ":" + itoa(a.port())
	bh, err := e.lb.GetBackendHealth(t.Context(), loadbalancer.GetBackendHealthRequest{
		LoadBalancerId: &id, BackendSetName: new("bs"), BackendName: &bname,
	})
	require.NoError(t, err)
	assert.Equal(t, loadbalancer.BackendHealthStatusCritical, bh.Status)
	require.Len(t, bh.HealthCheckResults, 1)
	assert.Equal(
		t,
		loadbalancer.HealthCheckResultHealthCheckStatusInvalidStatusCode,
		bh.HealthCheckResults[0].HealthCheckStatus,
	)

	lbh, err := e.lb.GetLoadBalancerHealth(t.Context(), loadbalancer.GetLoadBalancerHealthRequest{LoadBalancerId: &id})
	require.NoError(t, err)
	assert.Equal(t, loadbalancer.LoadBalancerHealthStatusWarning, lbh.Status)
	assert.Equal(t, []string{"bs"}, lbh.WarningStateBackendSetNames)

	summaries, err := e.lb.ListLoadBalancerHealths(
		t.Context(),
		loadbalancer.ListLoadBalancerHealthsRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, summaries.Items, 1)

	a.healthy.Store(true)
	e.clock.Advance(2 * time.Second)

	assert.Equal(t, map[string]int{"a": 2, "b": 2}, tally(t, e, id, 4))
	assert.Equal(t, loadbalancer.BackendSetHealthStatusOk, health())

	b.srv.Close()
	a.srv.Close()
	e.clock.Advance(2 * time.Second)

	r := e.call(t, id, httpPort, "", "/x", nil)
	assert.Equal(t, http.StatusServiceUnavailable, r.status)
	assert.Equal(t, loadbalancer.BackendSetHealthStatusCritical, health())
}

func TestBackupDrainOffline(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a, b, c := startBackend(t, "a"), startBackend(t, "b"), startBackend(t, "c")
	set := backendSet("ROUND_ROBIN", a, b, c)
	set.Backends[1].Backup = new(true)
	set.Backends[2].Offline = new(true)

	d := baseDetails("roles")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
	id := e.mkLB(t, d)

	assert.Equal(t, map[string]int{"a": 3}, tally(t, e, id, 3))

	a.healthy.Store(false)
	e.clock.Advance(2 * time.Second)
	assert.Equal(t, map[string]int{"b": 3}, tally(t, e, id, 3))
}

func TestRoutingByPathPolicyAndHostname(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	web, api, admin := startBackend(t, "web"), startBackend(t, "api"), startBackend(t, "admin")
	d := baseDetails("routes")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{
		"web": backendSet(
			"ROUND_ROBIN",
			web,
		),
		"api":   backendSet("ROUND_ROBIN", api),
		"admin": backendSet("ROUND_ROBIN", admin),
	}
	d.Hostnames = map[string]loadbalancer.HostnameDetails{
		"hApi":   {Name: new("hApi"), Hostname: new("api.example.com")},
		"hAdmin": {Name: new("hAdmin"), Hostname: new("*.admin.example.com")},
	}
	d.PathRouteSets = map[string]loadbalancer.PathRouteSetDetails{
		"paths": {PathRoutes: []loadbalancer.PathRoute{
			prefix("/api", "api"),
			{
				Path:           new("/exact"),
				BackendSetName: new("admin"),
				PathMatchType:  &loadbalancer.PathMatchType{MatchType: loadbalancer.PathMatchTypeMatchTypeExactMatch},
			},
			{
				Path:           new(".css"),
				BackendSetName: new("admin"),
				PathMatchType:  &loadbalancer.PathMatchType{MatchType: loadbalancer.PathMatchTypeMatchTypeSuffixMatch},
			},
		}},
	}
	d.Listeners = map[string]loadbalancer.ListenerDetails{
		"vhostApi": {
			DefaultBackendSetName: new("api"),
			Port:                  new(81),
			Protocol:              new("HTTP"),
			HostnameNames:         []string{"hApi"},
		},
		"vhostAdmin": {
			DefaultBackendSetName: new("admin"),
			Port:                  new(81),
			Protocol:              new("HTTP"),
			HostnameNames:         []string{"hAdmin"},
		},
		"paths": {
			DefaultBackendSetName: new("web"),
			Port:                  new(80),
			Protocol:              new("HTTP"),
			PathRouteSetName:      new("paths"),
		},
	}
	id := e.mkLB(t, d)

	tests := []struct {
		name   string
		host   string
		path   string
		want   string
		port   int
		status int
	}{
		{name: "default set", port: 80, host: "", path: "/home", want: "web", status: 200},
		{name: "prefix route", port: 80, host: "", path: "/API/v1", want: "api", status: 200},
		{name: "exact route", port: 80, host: "", path: "/exact", want: "admin", status: 200},
		{name: "suffix route", port: 80, host: "", path: "/site/app.css", want: "admin", status: 200},
		{name: "exact miss falls to default", port: 80, host: "", path: "/exact/more", want: "web", status: 200},
		{name: "exact hostname", port: 81, host: "api.example.com", path: "/", want: "api", status: 200},
		{name: "wildcard hostname", port: 81, host: "x.admin.example.com", path: "/", want: "admin", status: 200},
		{name: "hostname with port", port: 81, host: "api.example.com:81", path: "/", want: "api", status: 200},
		{name: "unmatched hostname", port: 81, host: "other.example.com", path: "/", want: "", status: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := e.call(t, id, tt.port, tt.host, tt.path, nil)
			assert.Equal(t, tt.status, r.status)

			if tt.want != "" {
				assert.Equal(t, tt.want, r.header.Get("X-Backend"))
			}
		})
	}

	t.Run("routing policy", func(t *testing.T) {
		e.settle()

		_, err := e.lb.CreateRoutingPolicy(t.Context(), loadbalancer.CreateRoutingPolicyRequest{
			LoadBalancerId: &id,
			Name: new(
				"rp",
			),
			ConditionLanguageVersion: loadbalancer.CreateRoutingPolicyDetailsConditionLanguageVersionV1,
			Rules: []loadbalancer.RoutingRule{
				{
					Name: new("byHeader"),
					Condition: new(
						"any(http.request.headers[(i 'X-Tier')] eq 'gold', http.request.url.query['tier'] eq 'gold')",
					),
					Actions: []loadbalancer.Action{loadbalancer.ForwardToBackendSet{BackendSetName: new("api")}},
				},
				{
					Name:      new("byPath"),
					Condition: new("all(http.request.url.path ew '.png', http.request.method eq 'GET')"),
					Actions:   []loadbalancer.Action{loadbalancer.ForwardToBackendSet{BackendSetName: new("admin")}},
				},
			},
		})
		require.NoError(t, err)
		e.settle()

		_, err = e.lb.CreateListener(t.Context(), loadbalancer.CreateListenerRequest{
			LoadBalancerId: &id,
			Name: new(
				"policy",
			),
			DefaultBackendSetName: new("web"),
			Port:                  new(82),
			Protocol:              new("HTTP"),
			RoutingPolicyName:     new("rp"),
		})
		require.NoError(t, err)
		e.settle()

		tests := []struct {
			path    string
			headers map[string]string
			want    string
		}{
			{"/", map[string]string{"x-tier": "gold"}, "api"},
			{"/?tier=gold", nil, "api"},
			{"/logo.png", nil, "admin"},
			{"/logo.png", map[string]string{"X-Tier": "gold"}, "api"},
			{"/other", nil, "web"},
		}
		for _, c := range tests {
			r := e.call(t, id, 82, "", c.path, c.headers)
			assert.Equal(t, c.want, r.header.Get("X-Backend"), c.path)
		}
	})
}

func TestRuleSets(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := startBackend(t, "a")
	d := baseDetails("rules")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": backendSet("ROUND_ROBIN", a)}
	d.RuleSets = map[string]loadbalancer.RuleSetDetails{
		"headers": {Items: []loadbalancer.Rule{
			loadbalancer.AddHttpRequestHeaderRule{Header: new("X-Added"), Value: new("yes")},
			loadbalancer.ExtendHttpRequestHeaderValueRule{Header: new("X-Added"), Prefix: new("<"), Suffix: new(">")},
			loadbalancer.AddHttpResponseHeaderRule{Header: new("X-Resp"), Value: new("lb")},
			loadbalancer.RemoveHttpResponseHeaderRule{Header: new("Set-Cookie")},
		}},
		"redirects": {Items: []loadbalancer.Rule{
			loadbalancer.RedirectRule{
				Conditions: []loadbalancer.RuleCondition{loadbalancer.PathMatchCondition{
					AttributeValue: new("/old"), Operator: loadbalancer.PathMatchConditionOperatorPrefixMatch,
				}},
				ResponseCode: new(301),
				RedirectUri: &loadbalancer.RedirectUri{
					Protocol: new("https"),
					Host:     new("new.example.com"),
					Port:     new(8443),
					Path:     new("/moved/{path}"),
				},
			},
		}},
		"methods": {Items: []loadbalancer.Rule{
			loadbalancer.ControlAccessUsingHttpMethodsRule{
				AllowedMethods: []string{"GET", "HEAD"},
				StatusCode:     new(405),
			},
		}},
		"allow": {Items: []loadbalancer.Rule{
			loadbalancer.AllowRule{
				Conditions: []loadbalancer.RuleCondition{
					loadbalancer.SourceIpAddressCondition{AttributeValue: new("10.0.0.0/8")},
				},
			},
		}},
		"allowLocal": {Items: []loadbalancer.Rule{
			loadbalancer.AllowRule{
				Conditions: []loadbalancer.RuleCondition{
					loadbalancer.SourceIpAddressCondition{AttributeValue: new("127.0.0.0/8")},
				},
			},
		}},
	}
	mk := func(port int, sets ...string) loadbalancer.ListenerDetails {
		l := listener()
		l.Port, l.RuleSetNames = new(port), sets

		return l
	}
	d.Listeners = map[string]loadbalancer.ListenerDetails{
		"headers": mk(80, "headers"), "redirect": mk(81, "redirects"), "methods": mk(82, "methods"),
		"deny": mk(83, "allow"), "permit": mk(84, "allowLocal"),
	}
	id := e.mkLB(t, d)

	t.Run("headers", func(t *testing.T) {
		t.Parallel()

		r := e.call(t, id, 80, "", "/x", nil)
		assert.Equal(t, 200, r.status)
		assert.Equal(t, "<yes>", r.header.Get("Echo-X-Added"))
		assert.Equal(t, "lb", r.header.Get("X-Resp"))
		assert.Empty(t, r.header.Get("Set-Cookie"))
		assert.NotEmpty(t, r.header.Get("Echo-Xff"))
	})

	t.Run("redirect", func(t *testing.T) {
		t.Parallel()

		r := e.call(t, id, 81, "lb.example.com", "/old/page", nil)
		assert.Equal(t, 301, r.status)
		assert.Equal(t, "https://new.example.com:8443/moved/old/page", r.header.Get("Location"))

		r = e.call(t, id, 81, "", "/keep", nil)
		assert.Equal(t, 200, r.status)
	})

	t.Run("methods", func(t *testing.T) {
		t.Parallel()

		r := e.do(t, "http://"+e.addr(t, id, 82)+"/x", "", http.MethodPost, nil, nil)
		assert.Equal(t, 405, r.status)

		r = e.do(t, "http://"+e.addr(t, id, 82)+"/x", "", http.MethodGet, nil, nil)
		assert.Equal(t, 200, r.status)
	})

	t.Run("allow", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 403, e.call(t, id, 83, "", "/x", nil).status)
		assert.Equal(t, 200, e.call(t, id, 84, "", "/x", nil).status)
	})
}

func TestTLSTermination(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := genCert(t)
	e := newEnv(t, envOpts{})
	a := startBackend(t, "a")
	d := baseDetails("tls")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": backendSet("ROUND_ROBIN", a)}
	d.Certificates = map[string]loadbalancer.CertificateDetails{
		"cert": {
			CertificateName:   new("cert"),
			PublicCertificate: new(string(certPEM)),
			PrivateKey:        new(string(keyPEM)),
		},
	}
	ssl := &loadbalancer.SslConfigurationDetails{CertificateName: new("cert")}
	d.Listeners = map[string]loadbalancer.ListenerDetails{
		"https":  {DefaultBackendSetName: new("bs"), Port: new(443), Protocol: new("HTTP"), SslConfiguration: ssl},
		"h2":     {DefaultBackendSetName: new("bs"), Port: new(444), Protocol: new("HTTP2"), SslConfiguration: ssl},
		"tcptls": {DefaultBackendSetName: new("bs"), Port: new(445), Protocol: new("TCP"), SslConfiguration: ssl},
	}
	id := e.mkLB(t, d)

	t.Run("https terminates and forwards plain", func(t *testing.T) {
		t.Parallel()

		r := e.do(
			t,
			"https://"+e.addr(t, id, 443)+"/secure",
			"",
			http.MethodGet,
			nil,
			tlsClient(t, certPEM),
		)
		assert.Equal(t, 200, r.status)
		assert.Equal(t, "a /secure", r.body)
	})

	t.Run("untrusted client rejected", func(t *testing.T) {
		t.Parallel()

		other, _ := genCert(t)
		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodGet,
			"https://"+e.addr(t, id, 443)+"/",
			http.NoBody,
		)
		require.NoError(t, err)

		_, err = tlsClient(t, other).Do(req)
		require.Error(t, err)
	})

	t.Run("http2 negotiated", func(t *testing.T) {
		t.Parallel()

		c := tlsClient(t, certPEM)
		tr, ok := c.Transport.(*http.Transport)
		require.True(t, ok)

		tr.ForceAttemptHTTP2 = true
		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodGet,
			"https://"+e.addr(t, id, 444)+"/",
			http.NoBody,
		)
		require.NoError(t, err)

		resp, err := c.Do(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		_, _ = io.Copy(io.Discard, resp.Body)
		assert.Equal(t, 2, resp.ProtoMajor)
	})

	t.Run("tls terminated tcp", func(t *testing.T) {
		t.Parallel()

		pool := tlsClient(t, certPEM).Transport.(*http.Transport).TLSClientConfig
		conn, err := tls.Dial("tcp", e.addr(t, id, 445), pool)
		require.NoError(t, err)

		defer conn.Close()

		_, err = io.WriteString(conn, "GET /raw HTTP/1.0\r\n\r\n")
		require.NoError(t, err)

		body, _ := io.ReadAll(conn)
		assert.Contains(t, string(body), "a /raw")
	})

	t.Run("no private key keeps listener unusable", func(t *testing.T) {
		t.Parallel()

		snap := e.backend.Snapshot(t.Context())
		assert.NotContains(t, string(snap), "PRIVATE KEY")
	})
}

func TestBackendTLSVerification(t *testing.T) {
	t.Parallel()

	good, goodKey := genCert(t)
	wrong, _ := genCert(t)
	pair, err := tls.X509KeyPair(good, goodKey)
	require.NoError(t, err)

	tests := []struct {
		name   string
		ca     string
		status int
	}{
		{"trusted bundle", string(good), http.StatusOK},
		{"wrong bundle", string(wrong), http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			be := startTLSBackend(t, "secure", pair)
			set := backendSet("ROUND_ROBIN", be)
			set.SslConfiguration = &loadbalancer.SslConfigurationDetails{
				CertificateName: new("ca"), VerifyPeerCertificate: new(true),
			}

			d := baseDetails("backend-tls")
			d.Certificates = map[string]loadbalancer.CertificateDetails{
				"ca": {CertificateName: new("ca"), CaCertificate: new(tt.ca)},
			}
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
			id := e.mkLB(t, d)

			r := e.call(t, id, httpPort, "", "/tls", nil)
			assert.Equal(t, tt.status, r.status)
		})
	}
}

func TestExternalBackendRefused(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	set := loadbalancer.BackendSetDetails{
		Policy: new("ROUND_ROBIN"), HealthChecker: hc(),
		Backends: []loadbalancer.BackendDetails{{IpAddress: new("192.0.2.1"), Port: new(80)}},
	}
	d := baseDetails("external")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
	id := e.mkLB(t, d)

	start := time.Now()
	r := e.call(t, id, httpPort, "", "/", nil)
	assert.Equal(t, http.StatusServiceUnavailable, r.status)
	assert.Less(t, time.Since(start), 5*time.Second)

	bname := "192.0.2.1:80"
	bh, err := e.lb.GetBackendHealth(t.Context(), loadbalancer.GetBackendHealthRequest{
		LoadBalancerId: &id, BackendSetName: new("bs"), BackendName: &bname,
	})
	require.NoError(t, err)
	assert.Equal(t, loadbalancer.BackendHealthStatusCritical, bh.Status)
	assert.Equal(
		t,
		loadbalancer.HealthCheckResultHealthCheckStatusConnectFailed,
		bh.HealthCheckResults[0].HealthCheckStatus,
	)
}

func TestUnhealthyButUndecidedBackendIsDialled(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	set := backendSet("ROUND_ROBIN")
	set.Backends = []loadbalancer.BackendDetails{{IpAddress: new("192.0.2.1"), Port: new(80)}}
	set.HealthChecker.Retries = new(3)

	d := baseDetails("undecided")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
	id := e.mkLB(t, d)

	r := e.call(t, id, httpPort, "", "/", nil)
	assert.Equal(t, http.StatusBadGateway, r.status, "the dial guard refuses the external peer")
}

func TestTCPListener(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := startBackend(t, "a")
	set := backendSet("ROUND_ROBIN", a)
	set.HealthChecker = &loadbalancer.HealthCheckerDetails{
		Protocol:         new("TCP"),
		Retries:          new(0),
		IntervalInMillis: new(1000),
		TimeoutInMillis:  new(1000),
	}

	d := baseDetails("tcp")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": {
		DefaultBackendSetName: new("bs"), Port: new(7000), Protocol: new("TCP"),
		ConnectionConfiguration: &loadbalancer.ConnectionConfiguration{IdleTimeout: new(int64(30))},
	}}
	id := e.mkLB(t, d)

	conn, err := net.Dial("tcp", e.addr(t, id, 7000))
	require.NoError(t, err)

	defer conn.Close()

	_, err = io.WriteString(conn, "GET /tcp HTTP/1.0\r\n\r\n")
	require.NoError(t, err)

	body, _ := io.ReadAll(conn)
	assert.True(t, strings.HasSuffix(string(body), "a /tcp"), string(body))
}

func TestSessionPersistenceAndRequestID(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a, b := startBackend(t, "a"), startBackend(t, "b")
	set := backendSet("ROUND_ROBIN", a, b)
	set.LbCookieSessionPersistenceConfiguration = &loadbalancer.LbCookieSessionPersistenceConfigurationDetails{}

	d := baseDetails("sticky")
	d.IsRequestIdEnabled = new(true)
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
	id := e.mkLB(t, d)

	first := e.call(t, id, httpPort, "", "/", nil)
	cookie := ""

	for _, c := range first.header.Values("Set-Cookie") {
		if strings.HasPrefix(c, "X-Oracle-BMC-LBS-Route=") {
			cookie = strings.SplitN(c, ";", 2)[0]
		}
	}

	require.NotEmpty(t, cookie)

	for range 4 {
		r := e.call(t, id, httpPort, "", "/", map[string]string{"Cookie": cookie})
		assert.Equal(t, first.header.Get("X-Backend"), r.header.Get("X-Backend"))
	}
}

func TestLifecycleOfListeners(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := startBackend(t, "a")
	d := baseDetails("serving")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": backendSet("ROUND_ROBIN", a)}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
	id, _ := e.create(t, d)

	assert.Empty(t, e.backend.Endpoints(), "creating load balancers do not serve")

	e.settle()
	require.Len(t, e.backend.Endpoints(), 1)

	{ // admin endpoint
		status, body := rawDo(t, e.base+"/_gopherstack/loadbalancer/endpoints")
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, body, id)
		assert.Contains(t, body, "127.0.0.1:")
	}

	{ // listener address in load balancer view
		status, body := rawDo(t, e.base+"/20170115/loadBalancers/"+id)
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, body, "gopherstackAddress")
	}

	{ // delete stops serving
		_, err := e.lb.DeleteLoadBalancer(t.Context(), loadbalancer.DeleteLoadBalancerRequest{LoadBalancerId: &id})
		require.NoError(t, err)

		e.settle()
		assert.Empty(t, e.backend.Endpoints())
	}
}
