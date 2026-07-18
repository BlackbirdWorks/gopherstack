package elbv2_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestCreateListener tests listener creation.
func TestCreateListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		name       string
		wantStatus int
	}{
		{
			name: "creates_successfully",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "listener-lb")
				tgArn := mustCreateTG(t, h, "listener-tg")

				return url.Values{
					"Action":                                 {"CreateListener"},
					"Version":                                {"2015-12-01"},
					"LoadBalancerArn":                        {lbArn},
					"Protocol":                               {"HTTP"},
					"Port":                                   {"80"},
					"DefaultActions.member.1.Type":           {"forward"},
					"DefaultActions.member.1.TargetGroupArn": {tgArn},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_lb_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"CreateListener"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "nonexistent_lb_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"CreateListener"},
					"Version": {"2015-12-01"},
					"LoadBalancerArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/no-such/0",
					},
					"Protocol":                     {"HTTP"},
					"Port":                         {"80"},
					"DefaultActions.member.1.Type": {"fixed-response"},
					"DefaultActions.member.1.FixedResponseConfig.StatusCode": {"200"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)
			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDeleteListener tests listener deletion.
func TestDeleteListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		name       string
		wantStatus int
	}{
		{
			name: "delete_existing",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "del-listener-lb")
				tgArn := mustCreateTG(t, h, "del-listener-tg")

				rec := doELBv2(t, h, url.Values{
					"Action":                                 {"CreateListener"},
					"Version":                                {"2015-12-01"},
					"LoadBalancerArn":                        {lbArn},
					"Protocol":                               {"HTTP"},
					"Port":                                   {"80"},
					"DefaultActions.member.1.Type":           {"forward"},
					"DefaultActions.member.1.TargetGroupArn": {tgArn},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp struct {
					Result struct {
						Listeners struct {
							Members []struct {
								ListenerArn string `xml:"ListenerArn"`
							} `xml:"member"`
						} `xml:"Listeners"`
					} `xml:"CreateListenerResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				listenerArn := resp.Result.Listeners.Members[0].ListenerArn

				return url.Values{
					"Action":      {"DeleteListener"},
					"Version":     {"2015-12-01"},
					"ListenerArn": {listenerArn},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"DeleteListener"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":      {"DeleteListener"},
					"Version":     {"2015-12-01"},
					"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no/0/no"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)
			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeListeners tests listener describe.
func TestDescribeListeners(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "desc-listener-lb")
	tgArn := mustCreateTG(t, h, "desc-listener-tg")

	doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	parseXMLBody(t, rec, &resp)
	assert.Len(t, resp.Result.Listeners.Members, 1)
}

// TestModifyListener tests listener modification.
func TestModifyListener(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "mod-listener-lb")
	tgArn := mustCreateTG(t, h, "mod-listener-tg")

	listenerRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, listenerRec.Code)

	var listenerResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listenerRec.Body.Bytes(), &listenerResp))
	listenerArn := listenerResp.Result.Listeners.Members[0].ListenerArn

	rec := doELBv2(t, h, url.Values{
		"Action":      {"ModifyListener"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Test not found case
	rec2 := doELBv2(t, h, url.Values{
		"Action":      {"ModifyListener"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no/0/no"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	// Test missing arn
	rec3 := doELBv2(t, h, url.Values{
		"Action":  {"ModifyListener"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

// TestModifyListenerSyncDefaultRule verifies that changing listener default actions
// also updates the auto-created default rule's actions.
func TestModifyListenerSyncDefaultRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sync-rule-lb")
	tgArn1 := mustCreateTG(t, h, "sync-rule-tg1")
	tgArn2 := mustCreateTG(t, h, "sync-rule-tg2")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn1)

	// Modify listener to point to tgArn2.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"ModifyListener"},
		"Version":                                {"2015-12-01"},
		"ListenerArn":                            {listenerArn},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn2},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// The default rule must also reference tgArn2.
	rulesRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rulesRec.Code)

	var rulesResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							TargetGroupArn string `xml:"TargetGroupArn"`
						} `xml:"member"`
					} `xml:"Actions"`
					IsDefault bool `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rulesRec.Body.Bytes(), &rulesResp))

	for _, r := range rulesResp.Result.Rules.Members {
		if r.IsDefault {
			require.Len(t, r.Actions.Members, 1)
			assert.Equal(t, tgArn2, r.Actions.Members[0].TargetGroupArn,
				"default rule action must be updated when listener default actions change")
		}
	}
}

// TestDescribeListenersSortedByPort verifies that DescribeListeners returns listeners
// sorted by port in ascending order.
func TestDescribeListenersSortedByPort(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sort-listen-lb")
	tgArn := mustCreateTG(t, h, "sort-listen-tg")
	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/sort-cert"

	// Create listeners out of order: 8080, 80, 443.
	for _, portProto := range []struct{ port, proto string }{
		{"8080", "HTTP"},
		{"80", "HTTP"},
	} {
		rec := doELBv2(t, h, url.Values{
			"Action":                                 {"CreateListener"},
			"Version":                                {"2015-12-01"},
			"LoadBalancerArn":                        {lbArn},
			"Protocol":                               {portProto.proto},
			"Port":                                   {portProto.port},
			"DefaultActions.member.1.Type":           {"forward"},
			"DefaultActions.member.1.TargetGroupArn": {tgArn},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Also create an HTTPS listener on 443.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {certArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe all listeners on the LB.
	descRec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					Port int `xml:"Port"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
	require.Len(t, descResp.Result.Listeners.Members, 3)

	ports := []int{
		descResp.Result.Listeners.Members[0].Port,
		descResp.Result.Listeners.Members[1].Port,
		descResp.Result.Listeners.Members[2].Port,
	}
	assert.Equal(t, []int{80, 443, 8080}, ports, "listeners must be sorted by port ascending")
}

// Test_AlpnPolicyWireShape verifies AlpnPolicy is wire-encoded as an AWS list
// (<AlpnPolicy><member>...</member></AlpnPolicy>, request field AlpnPolicy.member.N),
// not a bare string element — matching aws-sdk-go-v2 types.Listener.AlpnPolicy ([]string) —
// and that CreateListener, DescribeListeners, and ModifyListener all agree on that shape.
func Test_AlpnPolicyWireShape(t *testing.T) {
	t.Parallel()

	type alpnListenerResp struct {
		ListenerArn string `xml:"ListenerArn"`
		AlpnPolicy  struct {
			Members []string `xml:"member"`
		} `xml:"AlpnPolicy"`
	}

	cases := []struct {
		name    string
		values  []string
		wantNil bool
	}{
		{name: "single_policy", values: []string{"HTTP2Optional"}},
		{name: "multiple_policies", values: []string{"HTTP2Optional", "HTTP1Only"}},
		{name: "no_policy_omits_element", values: nil, wantNil: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nameSuffix := strings.ReplaceAll(tc.name, "_", "-")

			h := newTestHandler()
			lbArn := mustCreateNLB(t, h, "alpn-"+nameSuffix)
			tgArn := mustCreateTG(t, h, "alpn-tg-"+nameSuffix)

			vals := url.Values{
				"Action":                                 {"CreateListener"},
				"Version":                                {"2015-12-01"},
				"LoadBalancerArn":                        {lbArn},
				"Protocol":                               {"TLS"},
				"Port":                                   {"443"},
				"DefaultActions.member.1.Type":           {"forward"},
				"DefaultActions.member.1.TargetGroupArn": {tgArn},
				"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123456789012:certificate/abc"},
			}

			for i, p := range tc.values {
				vals.Set(fmt.Sprintf("AlpnPolicy.member.%d", i+1), p)
			}

			rec := doELBv2(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var createResp struct {
				Result struct {
					Listeners struct {
						Members []alpnListenerResp `xml:"member"`
					} `xml:"Listeners"`
				} `xml:"CreateListenerResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
			require.Len(t, createResp.Result.Listeners.Members, 1)

			if tc.wantNil {
				assert.Empty(t, createResp.Result.Listeners.Members[0].AlpnPolicy.Members)
			} else {
				assert.Equal(t, tc.values, createResp.Result.Listeners.Members[0].AlpnPolicy.Members)
			}

			// DescribeListeners must round-trip the same list shape.
			listenerArn := createResp.Result.Listeners.Members[0].ListenerArn
			descRec := doELBv2(t, h, url.Values{
				"Action":                {"DescribeListeners"},
				"Version":               {"2015-12-01"},
				"ListenerArns.member.1": {listenerArn},
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp struct {
				Result struct {
					Listeners struct {
						Members []alpnListenerResp `xml:"member"`
					} `xml:"Listeners"`
				} `xml:"DescribeListenersResult"`
			}
			require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
			require.Len(t, descResp.Result.Listeners.Members, 1)

			if tc.wantNil {
				assert.Empty(t, descResp.Result.Listeners.Members[0].AlpnPolicy.Members)
			} else {
				assert.Equal(t, tc.values, descResp.Result.Listeners.Members[0].AlpnPolicy.Members)
			}
		})
	}
}

// TestDescribeListenersPagination verifies Marker/PageSize pagination for DescribeListeners.
func TestDescribeListenersPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "pag-listeners-lb")
	tgArn := mustCreateTG(t, h, "pag-listeners-tg")

	// Create 3 listeners on ports 80, 81, 82.
	for _, port := range []string{"80", "81", "82"} {
		rec := doELBv2(t, h, url.Values{
			"Action":                                 {"CreateListener"},
			"Version":                                {"2015-12-01"},
			"LoadBalancerArn":                        {lbArn},
			"Protocol":                               {"HTTP"},
			"Port":                                   {port},
			"DefaultActions.member.1.Type":           {"forward"},
			"DefaultActions.member.1.TargetGroupArn": {tgArn},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: PageSize=2
	rec1 := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"PageSize":        {"2"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 struct {
		Result struct {
			NextMarker string `xml:"NextMarker"`
			Listeners  struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
					Port        int32  `xml:"Port"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.Result.Listeners.Members, 2)
	assert.NotEmpty(t, page1.Result.NextMarker)

	// Page 2
	rec2 := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"PageSize":        {"2"},
		"Marker":          {page1.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Result struct {
			NextMarker string `xml:"NextMarker"`
			Listeners  struct {
				Members []struct {
					Port int32 `xml:"Port"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.Result.Listeners.Members, 1)
	assert.Empty(t, page2.Result.NextMarker)
}

func TestCreateListener_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-http-lb")
	tgArn := b1CreateTG(t, h, "listener-http-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					Protocol string `xml:"Protocol"`
					Port     int32  `xml:"Port"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	l := resp.Result.Listeners.Members[0]
	assert.Equal(t, "HTTP", l.Protocol)
	assert.Equal(t, int32(80), l.Port)
}

func TestDeleteListener_Smoke(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "del-listener-lb")
	tgArn := b1CreateTG(t, h, "del-listener-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DeleteListener"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestDescribeListeners_Smoke(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "desc-listener-lb")
	tgArn := b1CreateTG(t, h, "desc-listener-tg")
	b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), lbArn)
}

func TestModifyListener_ChangePort(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mod-listener-lb")
	tgArn := b1CreateTG(t, h, "mod-listener-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"ModifyListener"},
		"Version":                                {"2015-12-01"},
		"ListenerArn":                            {lArn},
		"Port":                                   {"8080"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "8080")
}

func TestDescribeListeners_Pagination(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "pag-listeners-lb")
	tgArn := b1CreateTG(t, h, "pag-listeners-tg")

	// Create listeners on different ports
	for _, port := range []string{"80", "8080", "8081", "8082"} {
		doELBv2(t, h, url.Values{
			"Action":                                 {"CreateListener"},
			"Version":                                {"2015-12-01"},
			"LoadBalancerArn":                        {lbArn},
			"Protocol":                               {"HTTP"},
			"Port":                                   {port},
			"DefaultActions.member.1.Type":           {"forward"},
			"DefaultActions.member.1.TargetGroupArn": {tgArn},
		})
	}

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"PageSize":        {"2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			NextMarker string `xml:"NextMarker"`
			Listeners  struct {
				Members []struct{} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Listeners.Members, 2)
	assert.NotEmpty(t, resp.Result.NextMarker)
}
