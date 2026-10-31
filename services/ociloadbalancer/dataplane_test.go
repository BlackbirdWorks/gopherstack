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

func listener(set string) loadbalancer.ListenerDetails {
	return loadbalancer.ListenerDetails{DefaultBackendSetName: sp(set), Port: sp(httpPort), Protocol: sp("HTTP")}
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
		name   string
		policy string
		wa, wb int
		n      int
		check  func(t *testing.T, got map[string]int)
	}{
		{"round robin", "ROUND_ROBIN", 1, 1, 6, func(t *testing.T, got map[string]int) { assert.Equal(t, map[string]int{"a": 3, "b": 3}, got) }},
		{"weighted round robin", "ROUND_ROBIN", 3, 1, 8, func(t *testing.T, got map[string]int) { assert.Equal(t, map[string]int{"a": 6, "b": 2}, got) }},
		{"ip hash", "IP_HASH", 1, 1, 5, func(t *testing.T, got map[string]int) { assert.Len(t, got, 1) }},
		{"least connections", "LEAST_CONNECTIONS", 1, 1, 4, func(t *testing.T, got map[string]int) { assert.Equal(t, 4, got["a"]+got["b"]) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			a, b := startBackend(t, "a"), startBackend(t, "b")
			set := backendSet(tt.policy, a, b)
			set.Backends[0].Weight, set.Backends[1].Weight = sp(tt.wa), sp(tt.wb)

			d := baseDetails(tt.name)
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener("bs")}
			id := e.mkLB(t, d)

			tt.check(t, tally(t, e, id, tt.n))
		})
	}
}

func TestHealthCheckFailover(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a, b := startBackend(t, "a"), startBackend(t, "b")
	d := baseDetails("failover")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": backendSet("ROUND_ROBIN", a, b)}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener("bs")}
	id := e.mkLB(t, d)

	assert.Equal(t, map[string]int{"a": 2, "b": 2}, tally(t, e, id, 4))

	health := func() loadbalancer.BackendSetHealthStatusEnum {
		resp, err := e.lb.GetBackendSetHealth(t.Context(), loadbalancer.GetBackendSetHealthRequest{
			LoadBalancerId: &id, BackendSetName: sp("bs"),
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
		LoadBalancerId: &id, BackendSetName: sp("bs"), BackendName: &bname,
	})
	require.NoError(t, err)
	assert.Equal(t, loadbalancer.BackendHealthStatusCritical, bh.Status)
	require.Len(t, bh.HealthCheckResults, 1)
	assert.Equal(t, loadbalancer.HealthCheckResultHealthCheckStatusInvalidStatusCode, bh.HealthCheckResults[0].HealthCheckStatus)

	lbh, err := e.lb.GetLoadBalancerHealth(t.Context(), loadbalancer.GetLoadBalancerHealthRequest{LoadBalancerId: &id})
	require.NoError(t, err)
	assert.Equal(t, loadbalancer.LoadBalancerHealthStatusWarning, lbh.Status)
	assert.Equal(t, []string{"bs"}, lbh.WarningStateBackendSetNames)

	summaries, err := e.lb.ListLoadBalancerHealths(t.Context(), loadbalancer.ListLoadBalancerHealthsRequest{CompartmentId: sp(testCompartment)})
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
	set.Backends[1].Backup = sp(true)
	set.Backends[2].Offline = sp(true)

	d := baseDetails("roles")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener("bs")}
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
		"web": backendSet("ROUND_ROBIN", web), "api": backendSet("ROUND_ROBIN", api), "admin": backendSet("ROUND_ROBIN", admin),
	}
	d.Hostnames = map[string]loadbalancer.HostnameDetails{
		"hApi":   {Name: sp("hApi"), Hostname: sp("api.example.com")},
		"hAdmin": {Name: sp("hAdmin"), Hostname: sp("*.admin.example.com")},
	}
	d.PathRouteSets = map[string]loadbalancer.PathRouteSetDetails{
		"paths": {PathRoutes: []loadbalancer.PathRoute{
			prefix("/api", "api"),
			{Path: sp("/exact"), BackendSetName: sp("admin"), PathMatchType: &loadbalancer.PathMatchType{MatchType: loadbalancer.PathMatchTypeMatchTypeExactMatch}},
			{Path: sp(".css"), BackendSetName: sp("admin"), PathMatchType: &loadbalancer.PathMatchType{MatchType: loadbalancer.PathMatchTypeMatchTypeSuffixMatch}},
		}},
	}
	d.Listeners = map[string]loadbalancer.ListenerDetails{
		"vhostApi":   {DefaultBackendSetName: sp("api"), Port: sp(81), Protocol: sp("HTTP"), HostnameNames: []string{"hApi"}},
		"vhostAdmin": {DefaultBackendSetName: sp("admin"), Port: sp(81), Protocol: sp("HTTP"), HostnameNames: []string{"hAdmin"}},
		"paths":      {DefaultBackendSetName: sp("web"), Port: sp(80), Protocol: sp("HTTP"), PathRouteSetName: sp("paths")},
	}
	id := e.mkLB(t, d)

	tests := []struct {
		name   string
		port   int
		host   string
		path   string
		want   string
		status int
	}{
		{"default set", 80, "", "/home", "web", 200},
		{"prefix route", 80, "", "/API/v1", "api", 200},
		{"exact route", 80, "", "/exact", "admin", 200},
		{"suffix route", 80, "", "/site/app.css", "admin", 200},
		{"exact miss falls to default", 80, "", "/exact/more", "web", 200},
		{"exact hostname", 81, "api.example.com", "/", "api", 200},
		{"wildcard hostname", 81, "x.admin.example.com", "/", "admin", 200},
		{"hostname with port", 81, "api.example.com:81", "/", "api", 200},
		{"unmatched hostname", 81, "other.example.com", "/", "", 404},
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
			CreateRoutingPolicyDetails: loadbalancer.CreateRoutingPolicyDetails{
				Name: sp("rp"), ConditionLanguageVersion: loadbalancer.CreateRoutingPolicyDetailsConditionLanguageVersionV1,
				Rules: []loadbalancer.RoutingRule{
					{Name: sp("byHeader"), Condition: sp("any(http.request.headers[(i 'X-Tier')] eq 'gold', http.request.url.query['tier'] eq 'gold')"),
						Actions: []loadbalancer.Action{loadbalancer.ForwardToBackendSet{BackendSetName: sp("api")}}},
					{Name: sp("byPath"), Condition: sp("all(http.request.url.path ew '.png', http.request.method eq 'GET')"),
						Actions: []loadbalancer.Action{loadbalancer.ForwardToBackendSet{BackendSetName: sp("admin")}}},
				},
			},
		})
		require.NoError(t, err)
		e.settle()

		_, err = e.lb.CreateListener(t.Context(), loadbalancer.CreateListenerRequest{
			LoadBalancerId: &id,
			CreateListenerDetails: loadbalancer.CreateListenerDetails{
				Name: sp("policy"), DefaultBackendSetName: sp("web"), Port: sp(82), Protocol: sp("HTTP"), RoutingPolicyName: sp("rp"),
			},
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
			loadbalancer.AddHttpRequestHeaderRule{Header: sp("X-Added"), Value: sp("yes")},
			loadbalancer.ExtendHttpRequestHeaderValueRule{Header: sp("X-Added"), Prefix: sp("<"), Suffix: sp(">")},
			loadbalancer.AddHttpResponseHeaderRule{Header: sp("X-Resp"), Value: sp("lb")},
			loadbalancer.RemoveHttpResponseHeaderRule{Header: sp("Set-Cookie")},
		}},
		"redirects": {Items: []loadbalancer.Rule{
			loadbalancer.RedirectRule{
				Conditions: []loadbalancer.RuleCondition{loadbalancer.PathMatchCondition{
					AttributeValue: sp("/old"), Operator: loadbalancer.PathMatchConditionOperatorPrefixMatch,
				}},
				ResponseCode: sp(301),
				RedirectUri:  &loadbalancer.RedirectUri{Protocol: sp("https"), Host: sp("new.example.com"), Port: sp(8443), Path: sp("/moved/{path}")},
			},
		}},
		"methods": {Items: []loadbalancer.Rule{
			loadbalancer.ControlAccessUsingHttpMethodsRule{AllowedMethods: []string{"GET", "HEAD"}, StatusCode: sp(405)},
		}},
		"allow": {Items: []loadbalancer.Rule{
			loadbalancer.AllowRule{Conditions: []loadbalancer.RuleCondition{loadbalancer.SourceIpAddressCondition{AttributeValue: sp("10.0.0.0/8")}}},
		}},
		"allowLocal": {Items: []loadbalancer.Rule{
			loadbalancer.AllowRule{Conditions: []loadbalancer.RuleCondition{loadbalancer.SourceIpAddressCondition{AttributeValue: sp("127.0.0.0/8")}}},
		}},
	}
	mk := func(port int, sets ...string) loadbalancer.ListenerDetails {
		l := listener("bs")
		l.Port, l.RuleSetNames = sp(port), sets

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
		assert.NotEmpty(t, r.header.Get("Echo-XFF"))
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
		"cert": {CertificateName: sp("cert"), PublicCertificate: sp(string(certPEM)), PrivateKey: sp(string(keyPEM))},
	}
	ssl := &loadbalancer.SslConfigurationDetails{CertificateName: sp("cert")}
	d.Listeners = map[string]loadbalancer.ListenerDetails{
		"https":  {DefaultBackendSetName: sp("bs"), Port: sp(443), Protocol: sp("HTTP"), SslConfiguration: ssl},
		"h2":     {DefaultBackendSetName: sp("bs"), Port: sp(444), Protocol: sp("HTTP2"), SslConfiguration: ssl},
		"tcptls": {DefaultBackendSetName: sp("bs"), Port: sp(445), Protocol: sp("TCP"), SslConfiguration: ssl},
	}
	id := e.mkLB(t, d)

	t.Run("https terminates and forwards plain", func(t *testing.T) {
		t.Parallel()

		r := e.do(t, "https://"+e.addr(t, id, 443)+"/secure", "", http.MethodGet, nil, tlsClient(t, certPEM, "localhost"))
		assert.Equal(t, 200, r.status)
		assert.Equal(t, "a /secure", r.body)
	})

	t.Run("untrusted client rejected", func(t *testing.T) {
		t.Parallel()

		other, _ := genCert(t)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+e.addr(t, id, 443)+"/", http.NoBody)
		require.NoError(t, err)

		_, err = tlsClient(t, other, "localhost").Do(req)
		require.Error(t, err)
	})

	t.Run("http2 negotiated", func(t *testing.T) {
		t.Parallel()

		c := tlsClient(t, certPEM, "localhost")
		tr, ok := c.Transport.(*http.Transport)
		require.True(t, ok)

		tr.ForceAttemptHTTP2 = true
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+e.addr(t, id, 444)+"/", http.NoBody)
		require.NoError(t, err)

		resp, err := c.Do(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		_, _ = io.Copy(io.Discard, resp.Body)
		assert.Equal(t, 2, resp.ProtoMajor)
	})

	t.Run("tls terminated tcp", func(t *testing.T) {
		t.Parallel()

		pool := tlsClient(t, certPEM, "localhost").Transport.(*http.Transport).TLSClientConfig
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
				CertificateName: sp("ca"), VerifyPeerCertificate: sp(true),
			}

			d := baseDetails("backend-tls")
			d.Certificates = map[string]loadbalancer.CertificateDetails{"ca": {CertificateName: sp("ca"), CaCertificate: sp(tt.ca)}}
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener("bs")}
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
		Policy: sp("ROUND_ROBIN"), HealthChecker: hc(),
		Backends: []loadbalancer.BackendDetails{{IpAddress: sp("192.0.2.1"), Port: sp(80)}},
	}
	d := baseDetails("external")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener("bs")}
	id := e.mkLB(t, d)

	start := time.Now()
	r := e.call(t, id, httpPort, "", "/", nil)
	assert.Equal(t, http.StatusServiceUnavailable, r.status)
	assert.Less(t, time.Since(start), 5*time.Second)

	bname := "192.0.2.1:80"
	bh, err := e.lb.GetBackendHealth(t.Context(), loadbalancer.GetBackendHealthRequest{
		LoadBalancerId: &id, BackendSetName: sp("bs"), BackendName: &bname,
	})
	require.NoError(t, err)
	assert.Equal(t, loadbalancer.BackendHealthStatusCritical, bh.Status)
	assert.Equal(t, loadbalancer.HealthCheckResultHealthCheckStatusConnectFailed, bh.HealthCheckResults[0].HealthCheckStatus)
}

func TestUnhealthyButUndecidedBackendIsDialled(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	set := backendSet("ROUND_ROBIN")
	set.Backends = []loadbalancer.BackendDetails{{IpAddress: sp("192.0.2.1"), Port: sp(80)}}
	set.HealthChecker.Retries = sp(3)

	d := baseDetails("undecided")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener("bs")}
	id := e.mkLB(t, d)

	r := e.call(t, id, httpPort, "", "/", nil)
	assert.Equal(t, http.StatusBadGateway, r.status, "the dial guard refuses the external peer")
}

func TestTCPListener(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := startBackend(t, "a")
	set := backendSet("ROUND_ROBIN", a)
	set.HealthChecker = &loadbalancer.HealthCheckerDetails{Protocol: sp("TCP"), Retries: sp(0), IntervalInMillis: sp(1000), TimeoutInMillis: sp(1000)}

	d := baseDetails("tcp")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": {
		DefaultBackendSetName: sp("bs"), Port: sp(7000), Protocol: sp("TCP"),
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
	d.IsRequestIdEnabled = sp(true)
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": set}
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener("bs")}
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
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener("bs")}
	id, _ := e.create(t, d)

	assert.Empty(t, e.backend.Endpoints(), "creating load balancers do not serve")

	e.settle()
	require.Len(t, e.backend.Endpoints(), 1)

	t.Run("admin endpoint", func(t *testing.T) {
		status, body, _ := rawDo(t, http.MethodGet, e.base+"/_gopherstack/loadbalancer/endpoints", "")
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, body, id)
		assert.Contains(t, body, "127.0.0.1:")
	})

	t.Run("listener address in load balancer view", func(t *testing.T) {
		status, body, _ := rawDo(t, http.MethodGet, e.base+"/20170115/loadBalancers/"+id, "")
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, body, "gopherstackAddress")
	})

	t.Run("delete stops serving", func(t *testing.T) {
		_, err := e.lb.DeleteLoadBalancer(t.Context(), loadbalancer.DeleteLoadBalancerRequest{LoadBalancerId: &id})
		require.NoError(t, err)

		e.settle()
		assert.Empty(t, e.backend.Endpoints())
	})
}
