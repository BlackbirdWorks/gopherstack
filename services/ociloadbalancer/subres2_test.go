//nolint:lll // SDK request literals are naturally wide
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

	{ // hostnames
		next()

		_, err := e.lb.CreateHostname(ctx, loadbalancer.CreateHostnameRequest{
			LoadBalancerId: &id,
			Name:           new("h1"), Hostname: new("app.example.com"),
		})
		require.NoError(t, err)
		next()

		_, err = e.lb.CreateHostname(ctx, loadbalancer.CreateHostnameRequest{
			LoadBalancerId: &id,
			Name:           new("h2"), Hostname: new("bad host!"),
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.UpdateHostname(ctx, loadbalancer.UpdateHostnameRequest{
			LoadBalancerId: &id, Name: new("h1"), Hostname: new("*.example.com"),
		})
		require.NoError(t, err)
		next()

		got, err := e.lb.GetHostname(ctx, loadbalancer.GetHostnameRequest{LoadBalancerId: &id, Name: new("h1")})
		require.NoError(t, err)
		assert.Equal(t, "*.example.com", *got.Hostname.Hostname)

		list, err := e.lb.ListHostnames(ctx, loadbalancer.ListHostnamesRequest{LoadBalancerId: &id})
		require.NoError(t, err)
		assert.Len(t, list.Items, 1)
	}

	{ // path route sets
		next()

		_, err := e.lb.CreatePathRouteSet(ctx, loadbalancer.CreatePathRouteSetRequest{
			LoadBalancerId: &id,
			Name:           new("paths"), PathRoutes: []loadbalancer.PathRoute{prefix("/api", "api")},
		})
		require.NoError(t, err)
		next()

		_, err = e.lb.CreatePathRouteSet(ctx, loadbalancer.CreatePathRouteSetRequest{
			LoadBalancerId: &id,
			Name:           new("dangling"), PathRoutes: []loadbalancer.PathRoute{prefix("/x", "missing")},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.UpdatePathRouteSet(ctx, loadbalancer.UpdatePathRouteSetRequest{
			LoadBalancerId: &id, PathRouteSetName: new("paths"),
			PathRoutes: []loadbalancer.PathRoute{prefix("/api", "api"), prefix("/web", "web")},
		})
		require.NoError(t, err)
		next()

		got, err := e.lb.GetPathRouteSet(
			ctx,
			loadbalancer.GetPathRouteSetRequest{LoadBalancerId: &id, PathRouteSetName: new("paths")},
		)
		require.NoError(t, err)
		assert.Len(t, got.PathRoutes, 2)
	}

	{ // routing policies
		next()

		rules := func(cond string) []loadbalancer.RoutingRule {
			return []loadbalancer.RoutingRule{
				{
					Name: new(
						"r1",
					),
					Condition: new(cond),
					Actions:   []loadbalancer.Action{loadbalancer.ForwardToBackendSet{BackendSetName: new("api")}},
				},
			}
		}

		_, err := e.lb.CreateRoutingPolicy(ctx, loadbalancer.CreateRoutingPolicyRequest{
			LoadBalancerId: &id,
			Name: new(
				"rp",
			),
			ConditionLanguageVersion: loadbalancer.CreateRoutingPolicyDetailsConditionLanguageVersionV1,
			Rules:                    rules("all(http.request.url.path sw '/api')"),
		})
		require.NoError(t, err)
		next()

		_, err = e.lb.CreateRoutingPolicy(ctx, loadbalancer.CreateRoutingPolicyRequest{
			LoadBalancerId: &id,
			Name: new(
				"bad",
			),
			ConditionLanguageVersion: loadbalancer.CreateRoutingPolicyDetailsConditionLanguageVersionV1,
			Rules:                    rules("http.request.url.path matches '/api'"),
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		got, err := e.lb.GetRoutingPolicy(
			ctx,
			loadbalancer.GetRoutingPolicyRequest{LoadBalancerId: &id, RoutingPolicyName: new("rp")},
		)
		require.NoError(t, err)
		require.Len(t, got.Rules, 1)
		assert.Equal(t, "all(http.request.url.path sw '/api')", *got.Rules[0].Condition)
		require.Len(t, got.Rules[0].Actions, 1)
		assert.IsType(t, loadbalancer.ForwardToBackendSet{}, got.Rules[0].Actions[0])
	}

	{ // rule sets
		next()

		_, err := e.lb.CreateRuleSet(ctx, loadbalancer.CreateRuleSetRequest{
			LoadBalancerId: &id,
			Name:           new("rs"), Items: []loadbalancer.Rule{
				loadbalancer.AddHttpRequestHeaderRule{Header: new("X-Added"), Value: new("1")},
				loadbalancer.RedirectRule{
					Conditions: []loadbalancer.RuleCondition{loadbalancer.PathMatchCondition{
						AttributeValue: new("/old"), Operator: loadbalancer.PathMatchConditionOperatorPrefixMatch,
					}},
					RedirectUri: &loadbalancer.RedirectUri{Path: new("/new")},
				},
			},
		})
		require.NoError(t, err)
		next()

		got, err := e.lb.GetRuleSet(ctx, loadbalancer.GetRuleSetRequest{LoadBalancerId: &id, RuleSetName: new("rs")})
		require.NoError(t, err)
		require.Len(t, got.Items, 2)
		assert.IsType(t, loadbalancer.AddHttpRequestHeaderRule{}, got.Items[0])
		redir, ok := got.Items[1].(loadbalancer.RedirectRule)
		require.True(t, ok)
		assert.Equal(t, 302, *redir.ResponseCode)

		bad := []loadbalancer.Rule{
			loadbalancer.IpBasedMaxConnectionsRule{DefaultMaxConnections: new(5)},
			loadbalancer.AddHttpRequestHeaderRule{Header: new("Host"), Value: new("x")},
			loadbalancer.ControlAccessUsingHttpMethodsRule{AllowedMethods: []string{}},
			loadbalancer.AllowRule{
				Conditions: []loadbalancer.RuleCondition{
					loadbalancer.SourceIpAddressCondition{AttributeValue: new("nope")},
				},
			},
		}
		for _, r := range bad {
			next()

			_, err = e.lb.CreateRuleSet(ctx, loadbalancer.CreateRuleSetRequest{
				LoadBalancerId: &id,
				Name:           new("badrs"), Items: []loadbalancer.Rule{r},
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "%T", r)
		}
	}

	{ // listener
		next()

		_, err := e.lb.CreateListener(ctx, loadbalancer.CreateListenerRequest{
			LoadBalancerId: &id,
			Name:           new("front"), DefaultBackendSetName: new("web"), Port: new(8080), Protocol: new("HTTP"),
			HostnameNames: []string{"h1"}, PathRouteSetName: new("paths"), RuleSetNames: []string{"rs"},
		})
		require.NoError(t, err)
		next()

		list, err := e.lb.ListListenerRules(
			ctx,
			loadbalancer.ListListenerRulesRequest{LoadBalancerId: &id, ListenerName: new("front")},
		)
		require.NoError(t, err)
		require.Len(t, list.Items, 2)
		assert.Equal(t, "rs", *list.Items[0].RuleSetName)

		cases := []struct {
			name string
			d    loadbalancer.CreateListenerDetails
			want int
		}{
			{"port in use", loadbalancer.CreateListenerDetails{
				Name: new("dup"), DefaultBackendSetName: new("web"), Port: new(8080), Protocol: new("HTTP"),
			}, http.StatusConflict},
			{"both path set and policy", loadbalancer.CreateListenerDetails{
				Name: new("both"), DefaultBackendSetName: new("web"), Port: new(8081), Protocol: new("HTTP"),
				PathRouteSetName: new("paths"), RoutingPolicyName: new("rp"),
			}, http.StatusBadRequest},
			{
				"unknown rule set",
				loadbalancer.CreateListenerDetails{
					Name: new(
						"norules",
					),
					DefaultBackendSetName: new("web"),
					Port:                  new(8081),
					Protocol:              new("HTTP"),
					RuleSetNames:          []string{"zzz"},
				},
				http.StatusBadRequest,
			},
			{
				"tcp with hostnames",
				loadbalancer.CreateListenerDetails{
					Name: new(
						"tcph",
					),
					DefaultBackendSetName: new("web"),
					Port:                  new(8081),
					Protocol:              new("TCP"),
					HostnameNames:         []string{"h1"},
				},
				http.StatusBadRequest,
			},
			{"unknown protocol", loadbalancer.CreateListenerDetails{
				Name: new("udp"), DefaultBackendSetName: new("web"), Port: new(8081), Protocol: new("UDP"),
			}, http.StatusBadRequest},
			{"unknown certificate", loadbalancer.CreateListenerDetails{
				Name: new("tls"), DefaultBackendSetName: new("web"), Port: new(8443), Protocol: new("HTTP"),
				SslConfiguration: &loadbalancer.SslConfigurationDetails{CertificateName: new("nocert")},
			}, http.StatusBadRequest},
		}
		for _, c := range cases {
			next()

			_, err = e.lb.CreateListener(
				ctx,
				loadbalancer.CreateListenerRequest{LoadBalancerId: &id, CreateListenerDetails: c.d},
			)
			assert.Equal(t, c.want, svcErr(t, err).GetHTTPStatusCode(), c.name)
		}
	}

	{ // references block deletes
		next()

		for _, del := range []func() error{
			func() error {
				_, err := e.lb.DeleteBackendSet(ctx, loadbalancer.DeleteBackendSetRequest{LoadBalancerId: &id, BackendSetName: new("web")})

				return err
			},
			func() error {
				_, err := e.lb.DeleteHostname(ctx, loadbalancer.DeleteHostnameRequest{LoadBalancerId: &id, Name: new("h1")})

				return err
			},
			func() error {
				_, err := e.lb.DeleteRuleSet(ctx, loadbalancer.DeleteRuleSetRequest{LoadBalancerId: &id, RuleSetName: new("rs")})

				return err
			},
			func() error {
				_, err := e.lb.DeletePathRouteSet(ctx, loadbalancer.DeletePathRouteSetRequest{LoadBalancerId: &id, PathRouteSetName: new("paths")})

				return err
			},
		} {
			assert.Equal(t, http.StatusConflict, svcErr(t, del()).GetHTTPStatusCode())
		}

		_, err := e.lb.DeleteListener(
			ctx,
			loadbalancer.DeleteListenerRequest{LoadBalancerId: &id, ListenerName: new("front")},
		)
		require.NoError(t, err)
		next()

		_, err = e.lb.DeleteRuleSet(ctx, loadbalancer.DeleteRuleSetRequest{LoadBalancerId: &id, RuleSetName: new("rs")})
		require.NoError(t, err)
	}
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

		_, err := e.lb.CreateCertificate(
			ctx,
			loadbalancer.CreateCertificateRequest{LoadBalancerId: &id, CreateCertificateDetails: d},
		)

		return err
	}

	require.NoError(t, create(loadbalancer.CreateCertificateDetails{
		CertificateName: new("good"), PublicCertificate: new(string(certPEM)), PrivateKey: new(string(keyPEM)),
	}))
	require.NoError(
		t,
		create(
			loadbalancer.CreateCertificateDetails{
				CertificateName: new("ca-only"),
				CaCertificate:   new(string(otherCert)),
			},
		),
	)

	pubOnly := loadbalancer.CreateCertificateDetails{
		CertificateName: new("good"), PublicCertificate: new(string(certPEM)),
	}
	errs := []struct {
		d    loadbalancer.CreateCertificateDetails
		name string
		want int
	}{
		{name: "duplicate", d: pubOnly, want: 409},
		{name: "mismatched key", want: 400, d: loadbalancer.CreateCertificateDetails{
			CertificateName: new("mm"), PublicCertificate: new(string(certPEM)), PrivateKey: new(string(otherKey)),
		}},
		{name: "garbage cert", want: 400, d: loadbalancer.CreateCertificateDetails{
			CertificateName: new("junk"), PublicCertificate: new("not pem"),
		}},
		{name: "key without cert", want: 400, d: loadbalancer.CreateCertificateDetails{
			CertificateName: new("k"), PrivateKey: new(string(keyPEM)),
		}},
		{name: "empty", want: 400, d: loadbalancer.CreateCertificateDetails{CertificateName: new("empty")}},
	}

	for _, c := range errs {
		assert.Equal(t, c.want, svcErr(t, create(c.d)).GetHTTPStatusCode(), c.name)
	}

	{ // private key never returned
		got := e.get(t, id)
		require.Contains(t, got.Certificates, "good")
		assert.Equal(t, string(certPEM), *got.Certificates["good"].PublicCertificate)

		status, body := rawDo(t, e.base+"/20170115/loadBalancers/"+id)
		assert.Equal(t, http.StatusOK, status)
		assert.NotContains(t, body, "PRIVATE KEY")
		assert.NotContains(t, body, "privateKey")

		list, err := e.lb.ListCertificates(ctx, loadbalancer.ListCertificatesRequest{LoadBalancerId: &id})
		require.NoError(t, err)
		assert.Len(t, list.Items, 2)
	}

	{ // cipher suites
		e.settle()

		_, err := e.lb.CreateSSLCipherSuite(ctx, loadbalancer.CreateSSLCipherSuiteRequest{
			LoadBalancerId: &id,
			Name:           new("suite"), Ciphers: []string{"ECDHE-RSA-AES128-GCM-SHA256"},
		})
		require.NoError(t, err)
		e.settle()

		_, err = e.lb.CreateSSLCipherSuite(ctx, loadbalancer.CreateSSLCipherSuiteRequest{
			LoadBalancerId: &id,
			Name:           new("weird"), Ciphers: []string{"NULL-MD5"},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.CreateSSLCipherSuite(ctx, loadbalancer.CreateSSLCipherSuiteRequest{
			LoadBalancerId: &id,
			Name:           new("oci-mine"), Ciphers: []string{"AES128-SHA"},
		})
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.UpdateSSLCipherSuite(ctx, loadbalancer.UpdateSSLCipherSuiteRequest{
			LoadBalancerId: &id, Name: new("suite"),
			Ciphers: []string{"AES256-GCM-SHA384"},
		})
		require.NoError(t, err)
		e.settle()

		got, err := e.lb.GetSSLCipherSuite(
			ctx,
			loadbalancer.GetSSLCipherSuiteRequest{LoadBalancerId: &id, Name: new("suite")},
		)
		require.NoError(t, err)
		assert.Equal(t, []string{"AES256-GCM-SHA384"}, got.Ciphers)

		_, err = e.lb.DeleteSSLCipherSuite(
			ctx,
			loadbalancer.DeleteSSLCipherSuiteRequest{LoadBalancerId: &id, Name: new("suite")},
		)
		require.NoError(t, err)
	}

	{ // in-use certificate cannot be deleted
		e.settle()

		_, err := e.lb.CreateListener(ctx, loadbalancer.CreateListenerRequest{
			LoadBalancerId: &id,
			Name:           new("tls"), DefaultBackendSetName: new("web"), Port: new(8443), Protocol: new("HTTP"),
			SslConfiguration: &loadbalancer.SslConfigurationDetails{CertificateName: new("good")},
		})
		require.NoError(t, err)
		e.settle()

		_, err = e.lb.DeleteCertificate(
			ctx,
			loadbalancer.DeleteCertificateRequest{LoadBalancerId: &id, CertificateName: new("good")},
		)
		assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.lb.CreateListener(ctx, loadbalancer.CreateListenerRequest{
			LoadBalancerId: &id,
			Name:           new("noKey"), DefaultBackendSetName: new("web"), Port: new(8444), Protocol: new("HTTP"),
			SslConfiguration: &loadbalancer.SslConfigurationDetails{CertificateName: new("ca-only")},
		})
		require.Error(t, err)

		_, err = e.lb.DeleteCertificate(
			ctx,
			loadbalancer.DeleteCertificateRequest{LoadBalancerId: &id, CertificateName: new("ca-only")},
		)
		require.NoError(t, err)
	}
}
