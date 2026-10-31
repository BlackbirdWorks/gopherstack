package ociloadbalancer_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// routedLB returns an active load balancer with two backend sets.
func routedLB(t *testing.T, e *env, name string) string {
	t.Helper()

	a, b := startBackend(t, "a"), startBackend(t, "b")
	d := baseDetails(name)
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{
		"web": backendSet("ROUND_ROBIN", a), "api": backendSet("ROUND_ROBIN", b),
	}

	return e.mkLB(t, d)
}

func TestListenersAndRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	id := routedLB(t, e, "routing")
	ctx := t.Context()

	next := func() { e.settle() }

	t.Run("hostnames", func(t *testing.T) {
		next()

		_, err := e.lb.CreateHostname(ctx, loadbalancer.CreateHostnameRequest{
			LoadBalancerId:        &id,
			CreateHostnameDetails: loadbalancer.CreateHostnameDetails{Name: sp("h1"), Hostname: sp("app.example.com")},
		})
		require.NoError(t, err)
		next()

		_, err = e.lb.CreateHostname(ctx, loadbalancer.CreateHostnameRequest{
			LoadBalancerId:        &id,
			CreateHostnameDetails: loadbalancer.CreateHostnameDetails{Name: sp("h2"), Hostname: sp("bad host!")},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.UpdateHostname(ctx, loadbalancer.UpdateHostnameRequest{
			LoadBalancerId: &id, Name: sp("h1"), UpdateHostnameDetails: loadbalancer.UpdateHostnameDetails{Hostname: sp("*.example.com")},
		})
		require.NoError(t, err)
		next()

		got, err := e.lb.GetHostname(ctx, loadbalancer.GetHostnameRequest{LoadBalancerId: &id, Name: sp("h1")})
		require.NoError(t, err)
		assert.Equal(t, "*.example.com", *got.Hostname.Hostname)

		list, err := e.lb.ListHostnames(ctx, loadbalancer.ListHostnamesRequest{LoadBalancerId: &id})
		require.NoError(t, err)
		assert.Len(t, list.Items, 1)
	})

	t.Run("path route sets", func(t *testing.T) {
		next()

		_, err := e.lb.CreatePathRouteSet(ctx, loadbalancer.CreatePathRouteSetRequest{
			LoadBalancerId: &id,
			CreatePathRouteSetDetails: loadbalancer.CreatePathRouteSetDetails{
				Name: sp("paths"), PathRoutes: []loadbalancer.PathRoute{prefix("/api", "api")},
			},
		})
		require.NoError(t, err)
		next()

		_, err = e.lb.CreatePathRouteSet(ctx, loadbalancer.CreatePathRouteSetRequest{
			LoadBalancerId: &id,
			CreatePathRouteSetDetails: loadbalancer.CreatePathRouteSetDetails{
				Name: sp("dangling"), PathRoutes: []loadbalancer.PathRoute{prefix("/x", "missing")},
			},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.UpdatePathRouteSet(ctx, loadbalancer.UpdatePathRouteSetRequest{
			LoadBalancerId: &id, PathRouteSetName: sp("paths"),
			UpdatePathRouteSetDetails: loadbalancer.UpdatePathRouteSetDetails{
				PathRoutes: []loadbalancer.PathRoute{prefix("/api", "api"), prefix("/web", "web")},
			},
		})
		require.NoError(t, err)
		next()

		got, err := e.lb.GetPathRouteSet(ctx, loadbalancer.GetPathRouteSetRequest{LoadBalancerId: &id, PathRouteSetName: sp("paths")})
		require.NoError(t, err)
		assert.Len(t, got.PathRoutes, 2)
	})

	t.Run("routing policies", func(t *testing.T) {
		next()

		rules := func(cond string) []loadbalancer.RoutingRule {
			return []loadbalancer.RoutingRule{{
				Name: sp("r1"), Condition: sp(cond), Actions: []loadbalancer.Action{loadbalancer.ForwardToBackendSet{BackendSetName: sp("api")}},
			}}
		}

		_, err := e.lb.CreateRoutingPolicy(ctx, loadbalancer.CreateRoutingPolicyRequest{
			LoadBalancerId: &id,
			CreateRoutingPolicyDetails: loadbalancer.CreateRoutingPolicyDetails{
				Name: sp("rp"), ConditionLanguageVersion: loadbalancer.CreateRoutingPolicyDetailsConditionLanguageVersionV1,
				Rules: rules("all(http.request.url.path sw '/api')"),
			},
		})
		require.NoError(t, err)
		next()

		_, err = e.lb.CreateRoutingPolicy(ctx, loadbalancer.CreateRoutingPolicyRequest{
			LoadBalancerId: &id,
			CreateRoutingPolicyDetails: loadbalancer.CreateRoutingPolicyDetails{
				Name: sp("bad"), ConditionLanguageVersion: loadbalancer.CreateRoutingPolicyDetailsConditionLanguageVersionV1,
				Rules: rules("http.request.url.path matches '/api'"),
			},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		got, err := e.lb.GetRoutingPolicy(ctx, loadbalancer.GetRoutingPolicyRequest{LoadBalancerId: &id, RoutingPolicyName: sp("rp")})
		require.NoError(t, err)
		require.Len(t, got.Rules, 1)
		assert.Equal(t, "all(http.request.url.path sw '/api')", *got.Rules[0].Condition)
		require.Len(t, got.Rules[0].Actions, 1)
		assert.IsType(t, loadbalancer.ForwardToBackendSet{}, got.Rules[0].Actions[0])
	})

	t.Run("rule sets", func(t *testing.T) {
		next()

		_, err := e.lb.CreateRuleSet(ctx, loadbalancer.CreateRuleSetRequest{
			LoadBalancerId: &id,
			CreateRuleSetDetails: loadbalancer.CreateRuleSetDetails{Name: sp("rs"), Items: []loadbalancer.Rule{
				loadbalancer.AddHttpRequestHeaderRule{Header: sp("X-Added"), Value: sp("1")},
				loadbalancer.RedirectRule{
					Conditions: []loadbalancer.RuleCondition{loadbalancer.PathMatchCondition{
						AttributeValue: sp("/old"), Operator: loadbalancer.PathMatchConditionOperatorPrefixMatch,
					}},
					RedirectUri: &loadbalancer.RedirectUri{Path: sp("/new")},
				},
			}},
		})
		require.NoError(t, err)
		next()

		got, err := e.lb.GetRuleSet(ctx, loadbalancer.GetRuleSetRequest{LoadBalancerId: &id, RuleSetName: sp("rs")})
		require.NoError(t, err)
		require.Len(t, got.Items, 2)
		assert.IsType(t, loadbalancer.AddHttpRequestHeaderRule{}, got.Items[0])
		redir, ok := got.Items[1].(loadbalancer.RedirectRule)
		require.True(t, ok)
		assert.Equal(t, 302, *redir.ResponseCode)

		bad := []loadbalancer.Rule{
			loadbalancer.IpBasedMaxConnectionsRule{DefaultMaxConnections: sp(5)},
			loadbalancer.AddHttpRequestHeaderRule{Header: sp("Host"), Value: sp("x")},
			loadbalancer.ControlAccessUsingHttpMethodsRule{AllowedMethods: []string{}},
			loadbalancer.AllowRule{Conditions: []loadbalancer.RuleCondition{loadbalancer.SourceIpAddressCondition{AttributeValue: sp("nope")}}},
		}
		for _, r := range bad {
			next()

			_, err = e.lb.CreateRuleSet(ctx, loadbalancer.CreateRuleSetRequest{
				LoadBalancerId:       &id,
				CreateRuleSetDetails: loadbalancer.CreateRuleSetDetails{Name: sp("badrs"), Items: []loadbalancer.Rule{r}},
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "%T", r)
		}
	})

	t.Run("listener", func(t *testing.T) {
		next()

		_, err := e.lb.CreateListener(ctx, loadbalancer.CreateListenerRequest{
			LoadBalancerId: &id,
			CreateListenerDetails: loadbalancer.CreateListenerDetails{
				Name: sp("front"), DefaultBackendSetName: sp("web"), Port: sp(8080), Protocol: sp("HTTP"),
				HostnameNames: []string{"h1"}, PathRouteSetName: sp("paths"), RuleSetNames: []string{"rs"},
			},
		})
		require.NoError(t, err)
		next()

		list, err := e.lb.ListListenerRules(ctx, loadbalancer.ListListenerRulesRequest{LoadBalancerId: &id, ListenerName: sp("front")})
		require.NoError(t, err)
		require.Len(t, list.Items, 2)
		assert.Equal(t, "rs", *list.Items[0].RuleSetName)

		cases := []struct {
			name string
			d    loadbalancer.CreateListenerDetails
			want int
		}{
			{"port in use", loadbalancer.CreateListenerDetails{
				Name: sp("dup"), DefaultBackendSetName: sp("web"), Port: sp(8080), Protocol: sp("HTTP"),
			}, http.StatusConflict},
			{"both path set and policy", loadbalancer.CreateListenerDetails{
				Name: sp("both"), DefaultBackendSetName: sp("web"), Port: sp(8081), Protocol: sp("HTTP"),
				PathRouteSetName: sp("paths"), RoutingPolicyName: sp("rp"),
			}, http.StatusBadRequest},
			{"unknown rule set", loadbalancer.CreateListenerDetails{
				Name: sp("norules"), DefaultBackendSetName: sp("web"), Port: sp(8081), Protocol: sp("HTTP"), RuleSetNames: []string{"zzz"},
			}, http.StatusBadRequest},
			{"tcp with hostnames", loadbalancer.CreateListenerDetails{
				Name: sp("tcph"), DefaultBackendSetName: sp("web"), Port: sp(8081), Protocol: sp("TCP"), HostnameNames: []string{"h1"},
			}, http.StatusBadRequest},
			{"unknown protocol", loadbalancer.CreateListenerDetails{
				Name: sp("udp"), DefaultBackendSetName: sp("web"), Port: sp(8081), Protocol: sp("UDP"),
			}, http.StatusBadRequest},
			{"unknown certificate", loadbalancer.CreateListenerDetails{
				Name: sp("tls"), DefaultBackendSetName: sp("web"), Port: sp(8443), Protocol: sp("HTTP"),
				SslConfiguration: &loadbalancer.SslConfigurationDetails{CertificateName: sp("nocert")},
			}, http.StatusBadRequest},
		}
		for _, c := range cases {
			next()

			_, err = e.lb.CreateListener(ctx, loadbalancer.CreateListenerRequest{LoadBalancerId: &id, CreateListenerDetails: c.d})
			assert.Equal(t, c.want, svcErr(t, err).GetHTTPStatusCode(), c.name)
		}
	})

	t.Run("references block deletes", func(t *testing.T) {
		next()

		for _, del := range []func() error{
			func() error {
				_, err := e.lb.DeleteBackendSet(ctx, loadbalancer.DeleteBackendSetRequest{LoadBalancerId: &id, BackendSetName: sp("web")})

				return err
			},
			func() error {
				_, err := e.lb.DeleteHostname(ctx, loadbalancer.DeleteHostnameRequest{LoadBalancerId: &id, Name: sp("h1")})

				return err
			},
			func() error {
				_, err := e.lb.DeleteRuleSet(ctx, loadbalancer.DeleteRuleSetRequest{LoadBalancerId: &id, RuleSetName: sp("rs")})

				return err
			},
			func() error {
				_, err := e.lb.DeletePathRouteSet(ctx, loadbalancer.DeletePathRouteSetRequest{LoadBalancerId: &id, PathRouteSetName: sp("paths")})

				return err
			},
		} {
			assert.Equal(t, http.StatusConflict, svcErr(t, del()).GetHTTPStatusCode())
		}

		_, err := e.lb.DeleteListener(ctx, loadbalancer.DeleteListenerRequest{LoadBalancerId: &id, ListenerName: sp("front")})
		require.NoError(t, err)
		next()

		_, err = e.lb.DeleteRuleSet(ctx, loadbalancer.DeleteRuleSetRequest{LoadBalancerId: &id, RuleSetName: sp("rs")})
		require.NoError(t, err)
	})
}

func TestCertificatesAndCipherSuites(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	id := routedLB(t, e, "certs")
	ctx := t.Context()
	certPEM, keyPEM := genCert(t)
	otherCert, otherKey := genCert(t)

	create := func(d loadbalancer.CreateCertificateDetails) error {
		e.settle()

		_, err := e.lb.CreateCertificate(ctx, loadbalancer.CreateCertificateRequest{LoadBalancerId: &id, CreateCertificateDetails: d})

		return err
	}

	require.NoError(t, create(loadbalancer.CreateCertificateDetails{
		CertificateName: sp("good"), PublicCertificate: sp(string(certPEM)), PrivateKey: sp(string(keyPEM)),
	}))
	require.NoError(t, create(loadbalancer.CreateCertificateDetails{CertificateName: sp("ca-only"), CaCertificate: sp(string(otherCert))}))

	errs := []struct {
		name string
		d    loadbalancer.CreateCertificateDetails
		want int
	}{
		{"duplicate", loadbalancer.CreateCertificateDetails{CertificateName: sp("good"), PublicCertificate: sp(string(certPEM))}, 409},
		{"mismatched key", loadbalancer.CreateCertificateDetails{
			CertificateName: sp("mm"), PublicCertificate: sp(string(certPEM)), PrivateKey: sp(string(otherKey)),
		}, 400},
		{"garbage cert", loadbalancer.CreateCertificateDetails{CertificateName: sp("junk"), PublicCertificate: sp("not pem")}, 400},
		{"key without cert", loadbalancer.CreateCertificateDetails{CertificateName: sp("k"), PrivateKey: sp(string(keyPEM))}, 400},
		{"empty", loadbalancer.CreateCertificateDetails{CertificateName: sp("empty")}, 400},
	}
	for _, c := range errs {
		assert.Equal(t, c.want, svcErr(t, create(c.d)).GetHTTPStatusCode(), c.name)
	}

	t.Run("private key never returned", func(t *testing.T) {
		got := e.get(t, id)
		require.Contains(t, got.Certificates, "good")
		assert.Equal(t, string(certPEM), *got.Certificates["good"].PublicCertificate)

		status, body, _ := rawDo(t, http.MethodGet, e.base+"/20170115/loadBalancers/"+id, "")
		assert.Equal(t, http.StatusOK, status)
		assert.NotContains(t, body, "PRIVATE KEY")
		assert.NotContains(t, body, "privateKey")

		list, err := e.lb.ListCertificates(ctx, loadbalancer.ListCertificatesRequest{LoadBalancerId: &id})
		require.NoError(t, err)
		assert.Len(t, list.Items, 2)
	})

	t.Run("cipher suites", func(t *testing.T) {
		e.settle()

		_, err := e.lb.CreateSSLCipherSuite(ctx, loadbalancer.CreateSSLCipherSuiteRequest{
			LoadBalancerId: &id,
			CreateSslCipherSuiteDetails: loadbalancer.CreateSslCipherSuiteDetails{
				Name: sp("suite"), Ciphers: []string{"ECDHE-RSA-AES128-GCM-SHA256"},
			},
		})
		require.NoError(t, err)
		e.settle()

		_, err = e.lb.CreateSSLCipherSuite(ctx, loadbalancer.CreateSSLCipherSuiteRequest{
			LoadBalancerId:              &id,
			CreateSslCipherSuiteDetails: loadbalancer.CreateSslCipherSuiteDetails{Name: sp("weird"), Ciphers: []string{"NULL-MD5"}},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.CreateSSLCipherSuite(ctx, loadbalancer.CreateSSLCipherSuiteRequest{
			LoadBalancerId:              &id,
			CreateSslCipherSuiteDetails: loadbalancer.CreateSslCipherSuiteDetails{Name: sp("oci-mine"), Ciphers: []string{"AES128-SHA"}},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.UpdateSSLCipherSuite(ctx, loadbalancer.UpdateSSLCipherSuiteRequest{
			LoadBalancerId: &id, Name: sp("suite"),
			UpdateSslCipherSuiteDetails: loadbalancer.UpdateSslCipherSuiteDetails{Ciphers: []string{"AES256-GCM-SHA384"}},
		})
		require.NoError(t, err)
		e.settle()

		got, err := e.lb.GetSSLCipherSuite(ctx, loadbalancer.GetSSLCipherSuiteRequest{LoadBalancerId: &id, Name: sp("suite")})
		require.NoError(t, err)
		assert.Equal(t, []string{"AES256-GCM-SHA384"}, got.Ciphers)

		_, err = e.lb.DeleteSSLCipherSuite(ctx, loadbalancer.DeleteSSLCipherSuiteRequest{LoadBalancerId: &id, Name: sp("suite")})
		require.NoError(t, err)
	})

	t.Run("in-use certificate cannot be deleted", func(t *testing.T) {
		e.settle()

		_, err := e.lb.CreateListener(ctx, loadbalancer.CreateListenerRequest{
			LoadBalancerId: &id,
			CreateListenerDetails: loadbalancer.CreateListenerDetails{
				Name: sp("tls"), DefaultBackendSetName: sp("web"), Port: sp(8443), Protocol: sp("HTTP"),
				SslConfiguration: &loadbalancer.SslConfigurationDetails{CertificateName: sp("good")},
			},
		})
		require.NoError(t, err)
		e.settle()

		_, err = e.lb.DeleteCertificate(ctx, loadbalancer.DeleteCertificateRequest{LoadBalancerId: &id, CertificateName: sp("good")})
		assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.CreateListener(ctx, loadbalancer.CreateListenerRequest{
			LoadBalancerId: &id,
			CreateListenerDetails: loadbalancer.CreateListenerDetails{
				Name: sp("noKey"), DefaultBackendSetName: sp("web"), Port: sp(8444), Protocol: sp("HTTP"),
				SslConfiguration: &loadbalancer.SslConfigurationDetails{CertificateName: sp("ca-only")},
			},
		})
		require.Error(t, err)

		_, err = e.lb.DeleteCertificate(ctx, loadbalancer.DeleteCertificateRequest{LoadBalancerId: &id, CertificateName: sp("ca-only")})
		require.NoError(t, err)
	})
}
