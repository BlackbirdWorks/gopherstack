package elb_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestAttachLoadBalancerToSubnets tests attaching subnets to an existing LB.
func TestAttachLoadBalancerToSubnets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, h *elb.Handler)
		vals          url.Values
		name          string
		wantSubnetLen int
		wantStatus    int
	}{
		{
			// AttachLoadBalancerToSubnets requires a VPC LB (created with subnets).
			name: "attaches_subnets",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateVPCLB(t, h, "subnet-lb")
			},
			vals: url.Values{
				"Action":           {"AttachLoadBalancerToSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"subnet-lb"},
				"Subnets.member.1": {"subnet-aaa"},
				"Subnets.member.2": {"subnet-bbb"},
			},
			wantStatus:    http.StatusOK,
			wantSubnetLen: 3, // subnet-00001 (from create) + subnet-aaa + subnet-bbb
		},
		{
			name: "idempotent_attach",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateVPCLB(t, h, "subnet-idem-lb")
				doELB(t, h, url.Values{
					"Action":           {"AttachLoadBalancerToSubnets"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"subnet-idem-lb"},
					"Subnets.member.1": {"subnet-aaa"},
				})
			},
			vals: url.Values{
				"Action":           {"AttachLoadBalancerToSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"subnet-idem-lb"},
				"Subnets.member.1": {"subnet-aaa"},
			},
			wantStatus:    http.StatusOK,
			wantSubnetLen: 2, // subnet-00001 (from create) + subnet-aaa
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"AttachLoadBalancerToSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
				"Subnets.member.1": {"subnet-aaa"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"AttachLoadBalancerToSubnets"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSubnetLen > 0 {
				var resp struct {
					XMLName xml.Name `xml:"AttachLoadBalancerToSubnetsResponse"`
					Result  struct {
						Subnets struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"Subnets"`
					} `xml:"AttachLoadBalancerToSubnetsResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.Subnets.Members, tt.wantSubnetLen)
			}
		})
	}
}

func TestSubnetsAttachDetachCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"subnet-cycle-lb"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	// Attach second subnet.
	doELB(t, h, url.Values{
		"Action":           {"AttachLoadBalancerToSubnets"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"subnet-cycle-lb"},
		"Subnets.member.1": {"subnet-bbb"},
	})

	// Detach first subnet.
	rec := doELB(t, h, url.Values{
		"Action":           {"DetachLoadBalancerFromSubnets"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"subnet-cycle-lb"},
		"Subnets.member.1": {"subnet-aaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DetachLoadBalancerFromSubnetsResponse"`
		Result  struct {
			Subnets struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"Subnets"`
		} `xml:"DetachLoadBalancerFromSubnetsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Subnets.Members, 1)
	assert.Equal(t, "subnet-bbb", resp.Result.Subnets.Members[0].Value)
}

func TestSubnetsAttachIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"subnet-idem-lb"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	// Attaching the same subnet again is idempotent.
	rec := doELB(t, h, url.Values{
		"Action":           {"AttachLoadBalancerToSubnets"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"subnet-idem-lb"},
		"Subnets.member.1": {"subnet-aaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"AttachLoadBalancerToSubnetsResponse"`
		Result  struct {
			Subnets struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"Subnets"`
		} `xml:"AttachLoadBalancerToSubnetsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Subnets.Members, 1, "idempotent attach must not duplicate")
}

// TestAttachSubnetsEC2ClassicRejected verifies that AttachSubnets on an
// EC2-Classic LB is rejected.
func TestAttachSubnetsEC2ClassicRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		isVPC      bool
	}{
		{"ec2_classic_rejected", http.StatusBadRequest, false},
		{"vpc_lb_accepted", http.StatusOK, true},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := fmt.Sprintf("subnet-vpc-%d", i)

			if tt.isVPC {
				mustCreateVPCLB(t, h, lbName)
			} else {
				mustCreateLB(t, h, lbName)
			}

			rec := doELB(t, h, url.Values{
				"Action":           {"AttachLoadBalancerToSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {lbName},
				"Subnets.member.1": {"subnet-new1"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDetachLoadBalancerFromSubnets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "detaches_existing_subnet",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()

				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"detach-lb"},
					"Subnets.member.1":                    {"subnet-aaa"},
					"Subnets.member.2":                    {"subnet-bbb"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"80"},
				})
			},
			vals: url.Values{
				"Action":           {"DetachLoadBalancerFromSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"detach-lb"},
				"Subnets.member.1": {"subnet-aaa"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "not_found_returns_404",
			vals: url.Values{
				"Action":           {"DetachLoadBalancerFromSubnets"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-such-lb"},
				"Subnets.member.1": {"subnet-aaa"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_name_returns_400",
			vals: url.Values{
				"Action":  {"DetachLoadBalancerFromSubnets"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantCount > 0 {
				var resp struct {
					XMLName xml.Name `xml:"DetachLoadBalancerFromSubnetsResponse"`
					Result  struct {
						Subnets struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"Subnets"`
					} `xml:"DetachLoadBalancerFromSubnetsResult"`
				}

				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.Subnets.Members, tt.wantCount)
			}
		})
	}
}

// TestDetachSubnetsSortedResult verifies that DetachLoadBalancerFromSubnets
// returns subnets in sorted order.
func TestDetachSubnetsSortedResult(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"sorted-detach-lb"},
		"Subnets.member.1":                    {"subnet-zzz"},
		"Subnets.member.2":                    {"subnet-aaa"},
		"Subnets.member.3":                    {"subnet-mmm"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"DetachLoadBalancerFromSubnets"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"sorted-detach-lb"},
		"Subnets.member.1": {"subnet-mmm"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DetachLoadBalancerFromSubnetsResponse"`
		Result  struct {
			Subnets struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"Subnets"`
		} `xml:"DetachLoadBalancerFromSubnetsResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Subnets.Members, 2)

	subnets := []string{
		resp.Result.Subnets.Members[0].Value,
		resp.Result.Subnets.Members[1].Value,
	}
	assert.Equal(t, []string{"subnet-aaa", "subnet-zzz"}, subnets, "subnets must be sorted")
}
