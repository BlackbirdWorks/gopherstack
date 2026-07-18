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

func TestAvailabilityZonesEnableDisableCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"az-cycle-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	// Enable a second AZ.
	rec := doELB(t, h, url.Values{
		"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"az-cycle-lb"},
		"AvailabilityZones.member.1": {"us-east-1b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var enableResp struct {
		XMLName xml.Name `xml:"EnableAvailabilityZonesForLoadBalancerResponse"`
		Result  struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"EnableAvailabilityZonesForLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &enableResp))
	assert.Len(t, enableResp.Result.AvailabilityZones.Members, 2)

	// Disable the second AZ.
	rec2 := doELB(t, h, url.Values{
		"Action":                     {"DisableAvailabilityZonesForLoadBalancer"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"az-cycle-lb"},
		"AvailabilityZones.member.1": {"us-east-1b"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var disableResp struct {
		XMLName xml.Name `xml:"DisableAvailabilityZonesForLoadBalancerResponse"`
		Result  struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"DisableAvailabilityZonesForLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &disableResp))
	assert.Len(t, disableResp.Result.AvailabilityZones.Members, 1)
}

func TestAvailabilityZonesDisableLastReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "az-last-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DisableAvailabilityZonesForLoadBalancer"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"az-last-lb"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAvailabilityZonesEnableReturnsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "az-sort-lb")

	// EnableAvailabilityZonesForLoadBalancer returns sorted result.
	rec := doELB(t, h, url.Values{
		"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"az-sort-lb"},
		"AvailabilityZones.member.1": {"us-east-1c"},
		"AvailabilityZones.member.2": {"us-east-1b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"EnableAvailabilityZonesForLoadBalancerResponse"`
		Result  struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"EnableAvailabilityZonesForLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	azs := resp.Result.AvailabilityZones.Members
	require.GreaterOrEqual(t, len(azs), 2)
	for i := 1; i < len(azs); i++ {
		assert.LessOrEqual(t, azs[i-1].Value, azs[i].Value, "returned AZs must be sorted")
	}
}

// TestEnableAZVPCLBRejected verifies that EnableAvailabilityZones on a
// VPC LB (created with subnets) is rejected with InvalidConfigurationRequest.
func TestEnableAZVPCLBRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		isVPC      bool
	}{
		{"vpc_lb_rejected", http.StatusBadRequest, true},
		{"ec2_classic_lb_accepted", http.StatusOK, false},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := fmt.Sprintf("az-vpc-%d", i)

			if tt.isVPC {
				mustCreateVPCLB(t, h, lbName)
			} else {
				mustCreateLB(t, h, lbName)
			}

			rec := doELB(t, h, url.Values{
				"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {lbName},
				"AvailabilityZones.member.1": {"us-east-1b"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestEnableAvailabilityZonesForLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantAZs    []string
		wantStatus int
	}{
		{
			name: "adds_new_az",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "enable-az-lb")
			},
			vals: url.Values{
				"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"enable-az-lb"},
				"AvailabilityZones.member.1": {"us-east-1b"},
			},
			wantStatus: http.StatusOK,
			wantAZs:    []string{"us-east-1a", "us-east-1b"},
		},
		{
			name: "idempotent_for_existing_az",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "enable-az-idem")
			},
			vals: url.Values{
				"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"enable-az-idem"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantStatus: http.StatusOK,
			wantAZs:    []string{"us-east-1a"},
		},
		{
			name: "not_found_returns_404",
			vals: url.Values{
				"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"no-lb"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantStatus: http.StatusNotFound,
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

			if tt.wantStatus == http.StatusOK && len(tt.wantAZs) > 0 {
				var resp struct {
					XMLName xml.Name `xml:"EnableAvailabilityZonesForLoadBalancerResponse"`
					Result  struct {
						AvailabilityZones struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"AvailabilityZones"`
					} `xml:"EnableAvailabilityZonesForLoadBalancerResult"`
				}

				parseXMLBody(t, rec, &resp)
				gotAZs := make([]string, 0, len(resp.Result.AvailabilityZones.Members))
				for _, m := range resp.Result.AvailabilityZones.Members {
					gotAZs = append(gotAZs, m.Value)
				}

				for _, az := range tt.wantAZs {
					assert.Contains(t, gotAZs, az)
				}
			}
		})
	}
}

func TestDisableAvailabilityZonesForLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "removes_az",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()

				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"disable-az-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"AvailabilityZones.member.2":          {"us-east-1b"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"80"},
				})
			},
			vals: url.Values{
				"Action":                     {"DisableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"disable-az-lb"},
				"AvailabilityZones.member.1": {"us-east-1b"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "not_found_returns_404",
			vals: url.Values{
				"Action":                     {"DisableAvailabilityZonesForLoadBalancer"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerName":           {"no-lb"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantStatus: http.StatusNotFound,
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
					XMLName xml.Name `xml:"DisableAvailabilityZonesForLoadBalancerResponse"`
					Result  struct {
						AvailabilityZones struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"AvailabilityZones"`
					} `xml:"DisableAvailabilityZonesForLoadBalancerResult"`
				}

				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.AvailabilityZones.Members, tt.wantCount)
			}
		})
	}
}

// TestDisableAZsEmptyListNoOp verifies that DisableAZs with empty list is a no-op.
func TestDisableAZsEmptyListNoOp(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"az-noop-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"AvailabilityZones.member.2":          {"us-east-1b"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"DisableAvailabilityZonesForLoadBalancer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"az-noop-lb"},
		// No AZs provided → no-op
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DisableAvailabilityZonesForLoadBalancerResponse"`
		Result  struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"DisableAvailabilityZonesForLoadBalancerResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.AvailabilityZones.Members, 2, "both AZs must remain after no-op disable")
}
