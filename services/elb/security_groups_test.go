package elb_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestApplySecurityGroupsToLoadBalancer tests replacing security groups on a VPC LB.
func TestApplySecurityGroupsToLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantSGLen  int
		wantStatus int
	}{
		{
			// ApplySecurityGroups requires a VPC LB (created with subnets).
			name: "applies_security_groups",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateVPCLB(t, h, "sg-lb")
			},
			vals: url.Values{
				"Action":                  {"ApplySecurityGroupsToLoadBalancer"},
				"Version":                 {"2012-06-01"},
				"LoadBalancerName":        {"sg-lb"},
				"SecurityGroups.member.1": {"sg-aaa"},
				"SecurityGroups.member.2": {"sg-bbb"},
			},
			wantStatus: http.StatusOK,
			wantSGLen:  2,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":                  {"ApplySecurityGroupsToLoadBalancer"},
				"Version":                 {"2012-06-01"},
				"LoadBalancerName":        {"no-lb"},
				"SecurityGroups.member.1": {"sg-aaa"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"ApplySecurityGroupsToLoadBalancer"},
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

			if tt.wantSGLen > 0 {
				var resp struct {
					XMLName xml.Name `xml:"ApplySecurityGroupsToLoadBalancerResponse"`
					Result  struct {
						SecurityGroups struct {
							Members []struct {
								Value string `xml:",chardata"`
							} `xml:"member"`
						} `xml:"SecurityGroups"`
					} `xml:"ApplySecurityGroupsToLoadBalancerResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.SecurityGroups.Members, tt.wantSGLen)
			}
		})
	}
}

func TestSecurityGroupsApplyReplaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"sg-replace-lb"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"SecurityGroups.member.1":             {"sg-old"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	rec := doELB(t, h, url.Values{
		"Action":                  {"ApplySecurityGroupsToLoadBalancer"},
		"Version":                 {"2012-06-01"},
		"LoadBalancerName":        {"sg-replace-lb"},
		"SecurityGroups.member.1": {"sg-new1"},
		"SecurityGroups.member.2": {"sg-new2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ApplySecurityGroupsToLoadBalancerResponse"`
		Result  struct {
			SecurityGroups struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"SecurityGroups"`
		} `xml:"ApplySecurityGroupsToLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.SecurityGroups.Members, 2)

	vals := []string{
		resp.Result.SecurityGroups.Members[0].Value,
		resp.Result.SecurityGroups.Members[1].Value,
	}
	assert.Contains(t, vals, "sg-new1")
	assert.Contains(t, vals, "sg-new2")
}

func TestSecurityGroupsApplyEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"sg-empty-lb"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"SecurityGroups.member.1":             {"sg-init"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"ApplySecurityGroupsToLoadBalancer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"sg-empty-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ApplySecurityGroupsToLoadBalancerResponse"`
		Result  struct {
			SecurityGroups struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"SecurityGroups"`
		} `xml:"ApplySecurityGroupsToLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.SecurityGroups.Members)
}
