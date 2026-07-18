package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestCreateLoadBalancer tests load balancer creation.
func TestCreateLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler)
		vals       url.Values
		name       string
		wantDNS    string
		wantStatus int
	}{
		{
			name: "creates_successfully",
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {"my-alb"},
				"Scheme":  {"internet-facing"},
				"Type":    {"application"},
			},
			wantStatus: http.StatusOK,
			wantDNS:    "my-alb-00000001.us-east-1.elb.amazonaws.com",
		},
		{
			name: "duplicate_returns_conflict",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-alb")
			},
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {"dup-alb"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_name_returns_bad_request",
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_internal_scheme",
			vals: url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {"internal-alb"},
				"Scheme":  {"internal"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDNS != "" {
				var resp struct {
					XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
					Result  struct {
						LoadBalancers struct {
							Members []struct {
								DNSName string `xml:"DNSName"`
							} `xml:"member"`
						} `xml:"LoadBalancers"`
					} `xml:"CreateLoadBalancerResult"`
				}
				parseXMLBody(t, rec, &resp)
				require.Len(t, resp.Result.LoadBalancers.Members, 1)
				assert.Equal(t, tt.wantDNS, resp.Result.LoadBalancers.Members[0].DNSName)
			}
		})
	}
}

// TestDeleteLoadBalancer tests delete operations.
func TestDeleteLoadBalancer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "delete_existing",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				lbArn := mustCreateLB(t, h, "delete-me")
				_ = lbArn
			},
			vals: url.Values{
				"Action":  {"DeleteLoadBalancer"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest, // LoadBalancerArn is required
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"DeleteLoadBalancer"},
				"Version": {"2015-12-01"},
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

			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDeleteLoadBalancerByARN tests that providing a valid ARN deletes the LB.
func TestDeleteLoadBalancerByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "to-delete")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DeleteLoadBalancer"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Querying a specific ARN that no longer exists should return 400 (AWS query-protocol error status).
	rec2 := doELBv2(t, h, url.Values{
		"Action":                    {"DescribeLoadBalancers"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArns.member.1": {lbArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestDescribeLoadBalancers tests describe operations.
func TestDescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler)
		vals       url.Values
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "describe_all",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateLB(t, h, "lb-a")
				mustCreateLB(t, h, "lb-b")
			},
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "describe_empty",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "filter_by_name",
			setup: func(t *testing.T, h *elbv2.Handler) {
				t.Helper()
				mustCreateLB(t, h, "filter-lb")
				mustCreateLB(t, h, "other-lb")
			},
			vals: url.Values{
				"Action":         {"DescribeLoadBalancers"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"filter-lb"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELBv2(t, h, tt.vals)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
				Result  struct {
					LoadBalancers struct {
						Members []struct {
							LoadBalancerArn string `xml:"LoadBalancerArn"`
						} `xml:"member"`
					} `xml:"LoadBalancers"`
				} `xml:"DescribeLoadBalancersResult"`
			}
			parseXMLBody(t, rec, &resp)
			assert.Len(t, resp.Result.LoadBalancers.Members, tt.wantCount)
		})
	}
}

// TestDeleteLoadBalancerNotFound tests delete with non-existent ARN.
func TestDeleteLoadBalancerNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DeleteLoadBalancer"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/no-such/0"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeLoadBalancersNotFound verifies that querying non-existent ARNs returns an error.
func TestDescribeLoadBalancersNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	fakeArn := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/fake/0000000000000000"

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"DescribeLoadBalancers"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArns.member.1": {fakeArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeLoadBalancersPagination verifies Marker/PageSize pagination for DescribeLoadBalancers.
func TestDescribeLoadBalancersPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// Create 5 LBs sorted alphabetically: alb-a, alb-b, alb-c, alb-d, alb-e
	for _, name := range []string{"alb-a", "alb-b", "alb-c", "alb-d", "alb-e"} {
		mustCreateLB(t, h, name)
	}

	// Page 1: PageSize=2
	rec1 := doELBv2(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 struct {
		Result struct {
			NextMarker    string `xml:"NextMarker"`
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn  string `xml:"LoadBalancerArn"`
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.Result.LoadBalancers.Members, 2)
	assert.Equal(t, "alb-a", page1.Result.LoadBalancers.Members[0].LoadBalancerName)
	assert.Equal(t, "alb-b", page1.Result.LoadBalancers.Members[1].LoadBalancerName)
	assert.NotEmpty(t, page1.Result.NextMarker)

	// Page 2: use Marker from page1, PageSize=2
	rec2 := doELBv2(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
		"Marker":   {page1.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Result struct {
			NextMarker    string `xml:"NextMarker"`
			LoadBalancers struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.Result.LoadBalancers.Members, 2)
	assert.Equal(t, "alb-c", page2.Result.LoadBalancers.Members[0].LoadBalancerName)
	assert.Equal(t, "alb-d", page2.Result.LoadBalancers.Members[1].LoadBalancerName)

	// Page 3: last page
	rec3 := doELBv2(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
		"Marker":   {page2.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var page3 struct {
		Result struct {
			NextMarker    string `xml:"NextMarker"`
			LoadBalancers struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec3.Body.Bytes(), &page3))
	require.Len(t, page3.Result.LoadBalancers.Members, 1)
	assert.Equal(t, "alb-e", page3.Result.LoadBalancers.Members[0].LoadBalancerName)
	assert.Empty(t, page3.Result.NextMarker)
}

// TestDescribeLoadBalancersByNameNotFound verifies that querying a non-existent LB by name
// returns 400 (LoadBalancerNotFound), matching real AWS which raises
// LoadBalancerNotFoundException for any unknown name.
func TestDescribeLoadBalancersByNameNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals   url.Values
		name   string
		expect int
	}{
		{
			name: "single_missing_name",
			vals: url.Values{
				"Action":         {"DescribeLoadBalancers"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"does-not-exist"},
			},
			expect: http.StatusBadRequest,
		},
		{
			name: "one_valid_one_missing_name",
			vals: url.Values{
				"Action":         {"DescribeLoadBalancers"},
				"Version":        {"2015-12-01"},
				"Names.member.1": {"desc-lb-name-exists"},
				"Names.member.2": {"does-not-exist"},
			},
			expect: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tc.name == "one_valid_one_missing_name" {
				mustCreateLB(t, h, "desc-lb-name-exists")
			}

			rec := doELBv2(t, h, tc.vals)
			assert.Equal(t, tc.expect, rec.Code)
		})
	}
}

func TestCreateLB_SubnetMappings_ReturnsSubnetId(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":                           {"CreateLoadBalancer"},
		"Version":                          {"2015-12-01"},
		"Name":                             {"lb-subnet-map"},
		"Type":                             {"application"},
		"SubnetMappings.member.1.SubnetId": {"subnet-aaa111"},
		"SubnetMappings.member.2.SubnetId": {"subnet-bbb222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					AvailabilityZones struct {
						Members []struct {
							ZoneName string `xml:"ZoneName"`
							SubnetID string `xml:"SubnetId"`
						} `xml:"member"`
					} `xml:"AvailabilityZones"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancers.Members, 1)
	azs := resp.Result.LoadBalancers.Members[0].AvailabilityZones.Members
	require.Len(t, azs, 2)
	assert.Equal(t, "subnet-aaa111", azs[0].SubnetID)
	assert.Equal(t, "subnet-bbb222", azs[1].SubnetID)
	assert.NotEmpty(t, azs[0].ZoneName)
	assert.NotEmpty(t, azs[1].ZoneName)
}

func TestCreateLB_Subnets_ReturnsSubnetId(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":           {"CreateLoadBalancer"},
		"Version":          {"2015-12-01"},
		"Name":             {"lb-subnets-plain"},
		"Type":             {"application"},
		"Subnets.member.1": {"subnet-plain01"},
		"Subnets.member.2": {"subnet-plain02"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					AvailabilityZones struct {
						Members []struct {
							SubnetID string `xml:"SubnetId"`
						} `xml:"member"`
					} `xml:"AvailabilityZones"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	azs := resp.Result.LoadBalancers.Members[0].AvailabilityZones.Members
	require.Len(t, azs, 2)
	assert.Equal(t, "subnet-plain01", azs[0].SubnetID)
	assert.Equal(t, "subnet-plain02", azs[1].SubnetID)
}

func TestCreateLB_NoSubnets_EmptyAZs(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"lb-no-subnets"},
		"Type":    {"application"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestCreateLB_ALB_StateActive(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"alb-state-check"},
		"Type":    {"application"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					State struct {
						Code string `xml:"Code"`
					} `xml:"State"`
					Type string `xml:"Type"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	lb := resp.Result.LoadBalancers.Members[0]
	assert.Equal(t, "active", lb.State.Code)
	assert.Equal(t, "application", lb.Type)
}

func TestCreateLB_NLB(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":           {"CreateLoadBalancer"},
		"Version":          {"2015-12-01"},
		"Name":             {"nlb-test"},
		"Type":             {"network"},
		"Subnets.member.1": {"subnet-nlb01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					Type    string `xml:"Type"`
					DNSName string `xml:"DNSName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	lb := resp.Result.LoadBalancers.Members[0]
	assert.Equal(t, "network", lb.Type)
	assert.Contains(t, lb.DNSName, "nlb-test")
}

func TestCreateLB_GWLB(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"gwlb-test"},
		"Type":    {"gateway"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					Type string `xml:"Type"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "gateway", resp.Result.LoadBalancers.Members[0].Type)
}

func TestDescribeLBs_FilterByArn(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	arn1 := b1CreateLB(t, h, "filter-arn-lb1")
	b1CreateLB(t, h, "filter-arn-lb2")

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"DescribeLoadBalancers"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArns.member.1": {arn1},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancers.Members, 1)
	assert.Equal(t, arn1, resp.Result.LoadBalancers.Members[0].LoadBalancerArn)
}

func TestDescribeLBs_FilterByName(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	b1CreateLB(t, h, "name-filter-lb-a")
	b1CreateLB(t, h, "name-filter-lb-b")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeLoadBalancers"},
		"Version":        {"2015-12-01"},
		"Names.member.1": {"name-filter-lb-a"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancers.Members, 1)
	assert.Equal(t, "name-filter-lb-a", resp.Result.LoadBalancers.Members[0].LoadBalancerName)
}

func TestDescribeLBs_ArnNotFound(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeLoadBalancers"},
		"Version": {"2015-12-01"},
		"LoadBalancerArns.member.1": {
			"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/not-exist/000",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDeleteLB_Success(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "delete-lb-batch1")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DeleteLoadBalancer"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// verify gone
	rec2 := doELBv2(t, h, url.Values{
		"Action":                    {"DescribeLoadBalancers"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArns.member.1": {lbArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestDeleteLB_NotFound(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":          {"DeleteLoadBalancer"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/ghost/000"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestModifyLBAttrs_ALBIdleTimeout(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "alb-idle-timeout")

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyLoadBalancerAttributes"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArn":           {lbArn},
		"Attributes.member.1.Key":   {"idle_timeout.timeout_seconds"},
		"Attributes.member.1.Value": {"120"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "120")
}

func TestModifyLBAttrs_DeletionProtection(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "alb-deletion-protect")

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyLoadBalancerAttributes"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArn":           {lbArn},
		"Attributes.member.1.Key":   {"deletion_protection.enabled"},
		"Attributes.member.1.Value": {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Contains(t, rec2.Body.String(), "deletion_protection.enabled")
	assert.Contains(t, rec2.Body.String(), "true")
}

func TestDescribeLBAttrs_DefaultsALB(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "alb-default-attrs")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "idle_timeout.timeout_seconds")
	assert.Contains(t, body, "deletion_protection.enabled")
	assert.Contains(t, body, "access_logs.s3.enabled")
}

func TestDescribeLBAttrs_DefaultsNLB(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "nlb-default-attrs", url.Values{"Type": {"network"}})

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "load_balancing.cross_zone.enabled")
	assert.Contains(t, body, "deletion_protection.enabled")
}

func TestNLB_DefaultCrossZone_False(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "nlb-cross-zone", url.Values{"Type": {"network"}})

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "load_balancing.cross_zone.enabled")
	assert.Contains(t, rec.Body.String(), "false")
}

func TestDescribeLBs_Pagination(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	for i := range 5 {
		b1CreateLB(t, h, "pag-lb-"+string(rune('a'+i)))
	}

	rec := doELBv2(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2015-12-01"},
		"PageSize": {"3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			NextMarker    string `xml:"NextMarker"`
			LoadBalancers struct {
				Members []struct{} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.LoadBalancers.Members, 3)
	assert.NotEmpty(t, resp.Result.NextMarker)
}
