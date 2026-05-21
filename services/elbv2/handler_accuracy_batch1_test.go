package elbv2_test

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

func newBatch1Handler() *elbv2.Handler {
	b := elbv2.NewInMemoryBackend("000000000000", config.DefaultRegion)

	return elbv2.NewHandler(b)
}

func newBatch1Backend() *elbv2.InMemoryBackend {
	return elbv2.NewInMemoryBackend("000000000000", config.DefaultRegion)
}

// helpers

func b1CreateLB(t *testing.T, h *elbv2.Handler, name string, extra ...url.Values) string {
	t.Helper()
	vals := url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {name},
		"Type":    {"application"},
	}
	if len(extra) > 0 {
		maps.Copy(vals, extra[0])
	}
	rec := doELBv2(t, h, vals)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancers.Members, 1)

	return resp.Result.LoadBalancers.Members[0].LoadBalancerArn
}

func b1CreateTG(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {name},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)

	return resp.Result.TargetGroups.Members[0].TargetGroupArn
}

func b1CreateListener(t *testing.T, h *elbv2.Handler, lbArn, tgArn string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
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
	require.Len(t, resp.Result.Listeners.Members, 1)

	return resp.Result.Listeners.Members[0].ListenerArn
}

// ---- LoadBalancer CRUD ----

func TestBatch1_CreateLB_SubnetMappings_ReturnsSubnetId(t *testing.T) {
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

func TestBatch1_CreateLB_Subnets_ReturnsSubnetId(t *testing.T) {
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

func TestBatch1_CreateLB_NoSubnets_EmptyAZs(t *testing.T) {
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

func TestBatch1_CreateLB_ALB_StateActive(t *testing.T) {
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

func TestBatch1_CreateLB_NLB(t *testing.T) {
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

func TestBatch1_CreateLB_GWLB(t *testing.T) {
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

func TestBatch1_CreateLB_InvalidType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"bad-type-lb"},
		"Type":    {"invalid"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_CreateLB_DuplicateName(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	b1CreateLB(t, h, "dup-lb-batch1")
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"dup-lb-batch1"},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestBatch1_DescribeLBs_FilterByArn(t *testing.T) {
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

func TestBatch1_DescribeLBs_FilterByName(t *testing.T) {
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

func TestBatch1_DescribeLBs_ArnNotFound(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeLoadBalancers"},
		"Version": {"2015-12-01"},
		"LoadBalancerArns.member.1": {
			"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/not-exist/000",
		},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_DeleteLB_Success(t *testing.T) {
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
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestBatch1_DeleteLB_NotFound(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":          {"DeleteLoadBalancer"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/ghost/000"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- LB Attributes ----

func TestBatch1_ModifyLBAttrs_ALBIdleTimeout(t *testing.T) {
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

func TestBatch1_ModifyLBAttrs_DeletionProtection(t *testing.T) {
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

func TestBatch1_DescribeLBAttrs_DefaultsALB(t *testing.T) {
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

func TestBatch1_DescribeLBAttrs_DefaultsNLB(t *testing.T) {
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

// ---- SecurityGroups / Subnets / IpAddressType ----

func TestBatch1_SetSecurityGroups_ALB(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "sg-alb-batch1")

	rec := doELBv2(t, h, url.Values{
		"Action":                  {"SetSecurityGroups"},
		"Version":                 {"2015-12-01"},
		"LoadBalancerArn":         {lbArn},
		"SecurityGroups.member.1": {"sg-11111111"},
		"SecurityGroups.member.2": {"sg-22222222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "sg-11111111")
}

func TestBatch1_SetSecurityGroups_NLBRejected(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "sg-nlb-reject", url.Values{"Type": {"network"}})

	rec := doELBv2(t, h, url.Values{
		"Action":                  {"SetSecurityGroups"},
		"Version":                 {"2015-12-01"},
		"LoadBalancerArn":         {lbArn},
		"SecurityGroups.member.1": {"sg-11111111"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_SetSubnets_SubnetMappings_ReturnsSubnetId(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "set-subnets-map")

	rec := doELBv2(t, h, url.Values{
		"Action":                           {"SetSubnets"},
		"Version":                          {"2015-12-01"},
		"LoadBalancerArn":                  {lbArn},
		"SubnetMappings.member.1.SubnetId": {"subnet-map01"},
		"SubnetMappings.member.2.SubnetId": {"subnet-map02"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			AvailabilityZones struct {
				Members []struct {
					SubnetID string `xml:"SubnetId"`
					ZoneName string `xml:"ZoneName"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"SetSubnetsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	azs := resp.Result.AvailabilityZones.Members
	require.Len(t, azs, 2)
	assert.Equal(t, "subnet-map01", azs[0].SubnetID)
	assert.Equal(t, "subnet-map02", azs[1].SubnetID)
	assert.NotEmpty(t, azs[0].ZoneName)
}

func TestBatch1_SetSubnets_PlainSubnets(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "set-subnets-plain")

	rec := doELBv2(t, h, url.Values{
		"Action":           {"SetSubnets"},
		"Version":          {"2015-12-01"},
		"LoadBalancerArn":  {lbArn},
		"Subnets.member.1": {"subnet-pl01"},
		"Subnets.member.2": {"subnet-pl02"},
		"Subnets.member.3": {"subnet-pl03"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			AvailabilityZones struct {
				Members []struct {
					SubnetID string `xml:"SubnetId"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"SetSubnetsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.AvailabilityZones.Members, 3)
}

func TestBatch1_SetIpAddressType_Dualstack(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "iptype-dualstack")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"SetIpAddressType"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"IpAddressType":   {"dualstack"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "dualstack")
}

// ---- TargetGroup CRUD ----

func TestBatch1_CreateTG_InstanceType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"tg-instance"},
		"Protocol":   {"HTTP"},
		"Port":       {"8080"},
		"VpcId":      {"vpc-11111111"},
		"TargetType": {"instance"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetType string `xml:"TargetType"`
					VpcID      string `xml:"VpcId"`
					Port       int32  `xml:"Port"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	tg := resp.Result.TargetGroups.Members[0]
	assert.Equal(t, "instance", tg.TargetType)
	assert.Equal(t, "vpc-11111111", tg.VpcID)
	assert.Equal(t, int32(8080), tg.Port)
}

func TestBatch1_CreateTG_IPType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"tg-ip"},
		"Protocol":   {"HTTP"},
		"Port":       {"443"},
		"VpcId":      {"vpc-00000000"},
		"TargetType": {"ip"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ip")
}

func TestBatch1_CreateTG_LambdaType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"tg-lambda"},
		"TargetType": {"lambda"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lambda")
}

func TestBatch1_CreateTG_ALBType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"tg-alb"},
		"Protocol":   {"HTTP"},
		"Port":       {"80"},
		"VpcId":      {"vpc-00000000"},
		"TargetType": {"alb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alb")
}

func TestBatch1_CreateTG_DefaultHealthCheck(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {"tg-hc-defaults"},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					HealthCheckProtocol        string `xml:"HealthCheckProtocol"`
					HealthCheckIntervalSeconds int32  `xml:"HealthCheckIntervalSeconds"`
					HealthCheckTimeoutSeconds  int32  `xml:"HealthCheckTimeoutSeconds"`
					HealthyThresholdCount      int32  `xml:"HealthyThresholdCount"`
					UnhealthyThresholdCount    int32  `xml:"UnhealthyThresholdCount"`
					HealthCheckEnabled         bool   `xml:"HealthCheckEnabled"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	tg := resp.Result.TargetGroups.Members[0]
	assert.True(t, tg.HealthCheckEnabled)
	assert.Equal(t, "HTTP", tg.HealthCheckProtocol)
	assert.Equal(t, int32(30), tg.HealthCheckIntervalSeconds)
	assert.Equal(t, int32(5), tg.HealthCheckTimeoutSeconds)
	assert.Equal(t, int32(3), tg.HealthyThresholdCount)
	assert.Equal(t, int32(3), tg.UnhealthyThresholdCount)
}

func TestBatch1_CreateTG_CustomHealthCheck(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":                     {"CreateTargetGroup"},
		"Version":                    {"2015-12-01"},
		"Name":                       {"tg-hc-custom"},
		"Protocol":                   {"HTTP"},
		"Port":                       {"80"},
		"VpcId":                      {"vpc-00000000"},
		"HealthCheckPath":            {"/healthz"},
		"HealthCheckIntervalSeconds": {"10"},
		"HealthCheckTimeoutSeconds":  {"3"},
		"HealthyThresholdCount":      {"3"},
		"UnhealthyThresholdCount":    {"2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "/healthz")
	assert.Contains(t, body, "10")
}

func TestBatch1_DescribeTGs_ByArn(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "describe-tg-arn")
	b1CreateTG(t, h, "other-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	assert.Equal(t, tgArn, resp.Result.TargetGroups.Members[0].TargetGroupArn)
}

func TestBatch1_DeleteTG_Success(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "del-tg-batch1")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch1_ModifyTG_HealthCheck(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "modify-tg-hc")

	rec := doELBv2(t, h, url.Values{
		"Action":                     {"ModifyTargetGroup"},
		"Version":                    {"2015-12-01"},
		"TargetGroupArn":             {tgArn},
		"HealthCheckPath":            {"/api/health"},
		"HealthCheckIntervalSeconds": {"15"},
		"HealthyThresholdCount":      {"4"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "/api/health")
	assert.Contains(t, body, "15")
}

func TestBatch1_TGAttributes_DefaultStickiness(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "tg-sticky-default")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "stickiness.enabled")
	assert.Contains(t, body, "deregistration_delay.timeout_seconds")
}

func TestBatch1_TGAttributes_EnableStickiness(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "tg-sticky-enable")

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyTargetGroupAttributes"},
		"Version":                   {"2015-12-01"},
		"TargetGroupArn":            {tgArn},
		"Attributes.member.1.Key":   {"stickiness.enabled"},
		"Attributes.member.1.Value": {"true"},
		"Attributes.member.2.Key":   {"stickiness.type"},
		"Attributes.member.2.Value": {"lb_cookie"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	body := rec2.Body.String()
	assert.Contains(t, body, "stickiness.enabled")
	assert.Contains(t, body, "lb_cookie")
}

// ---- Target health lifecycle ----

func TestBatch1_RegisterTargets_InitialHealthState(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "hc-initial-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-00000001"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	th := resp.Result.TargetHealthDescriptions.Members[0]
	assert.Equal(t, "i-00000001", th.Target.ID)
	assert.Equal(t, "initial", th.TargetHealth.State)
	assert.Equal(t, "Elb.InitialHealthChecking", th.TargetHealth.Reason)
}

func TestBatch1_TargetHealth_SetHealthy(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	h := elbv2.NewHandler(b)

	tgArn := b1CreateTG(t, h, "hc-set-healthy")

	rec := doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-healthy-01"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, b.SetTargetHealthState(tgArn, "i-healthy-01", 80, "healthy", ""))

	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	th := resp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "healthy", th.State)
	assert.Empty(t, th.Reason)
}

func TestBatch1_TargetHealth_SetUnhealthy(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	h := elbv2.NewHandler(b)

	tgArn := b1CreateTG(t, h, "hc-set-unhealthy")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-unhealthy-01"},
		"Targets.member.1.Port": {"80"},
	})

	require.NoError(t, b.SetTargetHealthState(tgArn, "i-unhealthy-01", 80, "unhealthy", "Target.ResponseCodeMismatch"))

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					TargetHealth struct {
						State  string `xml:"State"`
						Reason string `xml:"Reason"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	th := resp.Result.TargetHealthDescriptions.Members[0].TargetHealth
	assert.Equal(t, "unhealthy", th.State)
	assert.Equal(t, "Target.ResponseCodeMismatch", th.Reason)
}

func TestBatch1_TargetHealth_MultipleDifferentStates(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend()
	h := elbv2.NewHandler(b)

	tgArn := b1CreateTG(t, h, "hc-multi-state")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-initial-01"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-healthy-02"},
		"Targets.member.2.Port": {"80"},
	})

	require.NoError(t, b.SetTargetHealthState(tgArn, "i-healthy-02", 80, "healthy", ""))

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
					TargetHealth struct {
						State string `xml:"State"`
					} `xml:"TargetHealth"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 2)

	states := map[string]string{}
	for _, m := range resp.Result.TargetHealthDescriptions.Members {
		states[m.Target.ID] = m.TargetHealth.State
	}
	assert.Equal(t, "initial", states["i-initial-01"])
	assert.Equal(t, "healthy", states["i-healthy-02"])
}

func TestBatch1_RegisterTargets_Dedup(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "register-dedup")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-dedup-01"},
		"Targets.member.1.Port": {"80"},
	})
	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-dedup-01"},
		"Targets.member.1.Port": {"80"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct{} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
}

func TestBatch1_DeregisterTargets_Success(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "dereg-tg")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-to-dereg"},
		"Targets.member.1.Port": {"80"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DeregisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-to-dereg"},
		"Targets.member.1.Port": {"80"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetHealth"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct{} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.TargetHealthDescriptions.Members)
}

// ---- Listener CRUD ----

func TestBatch1_CreateListener_HTTP(t *testing.T) {
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

func TestBatch1_CreateListener_HTTPS_RequiresCert(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-https-no-cert")
	tgArn := b1CreateTG(t, h, "listener-https-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_CreateListener_HTTPS_WithCert(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-https-cert")
	tgArn := b1CreateTG(t, h, "listener-https-cert-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:000000000000:certificate/aaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "HTTPS")
}

func TestBatch1_CreateListener_DefaultSSLPolicy(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-default-ssl")
	tgArn := b1CreateTG(t, h, "listener-default-ssl-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:000000000000:certificate/bbb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ELBSecurityPolicy")
}

func TestBatch1_CreateListener_DuplicatePortRejected(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "dup-port-lb")
	tgArn := b1CreateTG(t, h, "dup-port-tg")

	b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestBatch1_DeleteListener(t *testing.T) {
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

func TestBatch1_DescribeListeners(t *testing.T) {
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

func TestBatch1_ModifyListener_ChangePort(t *testing.T) {
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

func TestBatch1_ListenerAttributes_DefaultRouting(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-attr-lb")
	tgArn := b1CreateTG(t, h, "listener-attr-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerAttributes"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "routing.http")
}

func TestBatch1_ModifyListenerAttributes(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mod-listener-attr-lb")
	tgArn := b1CreateTG(t, h, "mod-listener-attr-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyListenerAttributes"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Attributes.member.1.Key":   {"routing.http.response.server.enabled"},
		"Attributes.member.1.Value": {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerAttributes"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	assert.Contains(t, rec2.Body.String(), "routing.http.response.server.enabled")
}

// ---- MutualAuthentication (mTLS) ----

func TestBatch1_MutualAuth_OnListener(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mtls-lb")
	tgArn := b1CreateTG(t, h, "mtls-tg")

	// Create trust store first
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"mtls-ts"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:000000000000:certificate/ccc"},
		"MutualAuthentication.Mode":              {"verify"},
		"MutualAuthentication.TrustStoreArn":     {tsArn},
		"MutualAuthentication.IgnoreClientCertificateExpiry": {"false"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "verify")
	assert.Contains(t, body, tsArn)
}

// ---- Rules ----

func TestBatch1_CreateRule_PathPattern(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-path-lb")
	tgArn := b1CreateTG(t, h, "rule-path-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"10"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/api/*"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/api/*")
}

func TestBatch1_CreateRule_HostHeader(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-host-lb")
	tgArn := b1CreateTG(t, h, "rule-host-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"20"},
		"Conditions.member.1.Field": {"host-header"},
		"Conditions.member.1.HostHeaderConfig.Values.member.1": {"api.example.com"},
		"Actions.member.1.Type":                                {"forward"},
		"Actions.member.1.TargetGroupArn":                      {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "api.example.com")
}

func TestBatch1_CreateRule_RedirectAction(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-redir-lb")
	tgArn := b1CreateTG(t, h, "rule-redir-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"30"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/old/*"},
		"Actions.member.1.Type":                                 {"redirect"},
		"Actions.member.1.RedirectConfig.StatusCode":            {"HTTP_301"},
		"Actions.member.1.RedirectConfig.Protocol":              {"HTTPS"},
		"Actions.member.1.RedirectConfig.Port":                  {"443"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "redirect")
	assert.Contains(t, body, "HTTP_301")
}

func TestBatch1_CreateRule_FixedResponse(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-fixed-lb")
	tgArn := b1CreateTG(t, h, "rule-fixed-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"40"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/health"},
		"Actions.member.1.Type":                                 {"fixed-response"},
		"Actions.member.1.FixedResponseConfig.StatusCode":       {"200"},
		"Actions.member.1.FixedResponseConfig.ContentType":      {"application/json"},
		"Actions.member.1.FixedResponseConfig.MessageBody":      {`{"status":"ok"}`},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "fixed-response")
	assert.Contains(t, body, "200")
}

func TestBatch1_CreateRule_AuthenticateCognito(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-cognito-lb")
	tgArn := b1CreateTG(t, h, "rule-cognito-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"50"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/secure/*"},
		"Actions.member.1.Type":                                 {"authenticate-cognito"},
		"Actions.member.1.Order":                                {"1"},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolArn": {
			"arn:aws:cognito-idp:us-east-1:000:userpool/us-east-1_abc",
		},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolClientId": {"clientid123"},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolDomain":   {"my-pool.auth.us-east-1.amazoncognito.com"},
		"Actions.member.2.Type":           {"forward"},
		"Actions.member.2.Order":          {"2"},
		"Actions.member.2.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "authenticate-cognito")
}

func TestBatch1_CreateRule_AuthenticateOIDC(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-oidc-lb")
	tgArn := b1CreateTG(t, h, "rule-oidc-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"60"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1":         {"/oidc/*"},
		"Actions.member.1.Type":                                         {"authenticate-oidc"},
		"Actions.member.1.Order":                                        {"1"},
		"Actions.member.1.AuthenticateOidcConfig.Issuer":                {"https://idp.example.com"},
		"Actions.member.1.AuthenticateOidcConfig.AuthorizationEndpoint": {"https://idp.example.com/auth"},
		"Actions.member.1.AuthenticateOidcConfig.TokenEndpoint":         {"https://idp.example.com/token"},
		"Actions.member.1.AuthenticateOidcConfig.UserInfoEndpoint":      {"https://idp.example.com/userinfo"},
		"Actions.member.1.AuthenticateOidcConfig.ClientId":              {"client-abc"},
		"Actions.member.1.AuthenticateOidcConfig.ClientSecret":          {"secret-xyz"},
		"Actions.member.2.Type":                                         {"forward"},
		"Actions.member.2.Order":                                        {"2"},
		"Actions.member.2.TargetGroupArn":                               {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "authenticate-oidc")
}

func TestBatch1_DescribeRules_SortedByPriority(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "sort-rules-lb")
	tgArn := b1CreateTG(t, h, "sort-rules-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	for _, prio := range []string{"30", "10", "20"} {
		doELBv2(t, h, url.Values{
			"Action":                    {"CreateRule"},
			"Version":                   {"2015-12-01"},
			"ListenerArn":               {lArn},
			"Priority":                  {prio},
			"Conditions.member.1.Field": {"path-pattern"},
			"Conditions.member.1.PathPatternConfig.Values.member.1": {"/p" + prio + "/*"},
			"Actions.member.1.Type":                                 {"forward"},
			"Actions.member.1.TargetGroupArn":                       {tgArn},
		})
	}

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeRules"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Priority string `xml:"Priority"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	priorities := make([]string, 0)
	for _, m := range resp.Result.Rules.Members {
		if m.Priority != "default" {
			priorities = append(priorities, m.Priority)
		}
	}
	require.Len(t, priorities, 3)
	assert.Equal(t, "10", priorities[0])
	assert.Equal(t, "20", priorities[1])
	assert.Equal(t, "30", priorities[2])
}

func TestBatch1_DeleteRule_Success(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "del-rule-lb")
	tgArn := b1CreateTG(t, h, "del-rule-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	createRec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"100"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/delete/*"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var ruleResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &ruleResp))
	ruleArn := ruleResp.Result.Rules.Members[0].RuleArn

	delRec := doELBv2(t, h, url.Values{
		"Action":  {"DeleteRule"},
		"Version": {"2015-12-01"},
		"RuleArn": {ruleArn},
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

func TestBatch1_SetRulePriorities(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "set-prio-lb")
	tgArn := b1CreateTG(t, h, "set-prio-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	ruleArns := make([]string, 0, 2)
	for _, prio := range []string{"1", "2"} {
		r := doELBv2(t, h, url.Values{
			"Action":                    {"CreateRule"},
			"Version":                   {"2015-12-01"},
			"ListenerArn":               {lArn},
			"Priority":                  {prio},
			"Conditions.member.1.Field": {"path-pattern"},
			"Conditions.member.1.PathPatternConfig.Values.member.1": {"/p" + prio},
			"Actions.member.1.Type":                                 {"forward"},
			"Actions.member.1.TargetGroupArn":                       {tgArn},
		})
		require.Equal(t, http.StatusOK, r.Code)
		var rr struct {
			Result struct {
				Rules struct {
					Members []struct {
						RuleArn string `xml:"RuleArn"`
					} `xml:"member"`
				} `xml:"Rules"`
			} `xml:"CreateRuleResult"`
		}
		require.NoError(t, xml.Unmarshal(r.Body.Bytes(), &rr))
		ruleArns = append(ruleArns, rr.Result.Rules.Members[0].RuleArn)
	}

	// Swap priorities
	rec := doELBv2(t, h, url.Values{
		"Action":                           {"SetRulePriorities"},
		"Version":                          {"2015-12-01"},
		"RulePriorities.member.1.RuleArn":  {ruleArns[0]},
		"RulePriorities.member.1.Priority": {"2"},
		"RulePriorities.member.2.RuleArn":  {ruleArns[1]},
		"RulePriorities.member.2.Priority": {"1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Tags ----

func TestBatch1_AddTags_LB(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "tag-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
		"Tags.member.1.Key":     {"env"},
		"Tags.member.1.Value":   {"prod"},
		"Tags.member.2.Key":     {"team"},
		"Tags.member.2.Value":   {"platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	body := rec2.Body.String()
	assert.Contains(t, body, "env")
	assert.Contains(t, body, "prod")
	assert.Contains(t, body, "team")
	assert.Contains(t, body, "platform")
}

func TestBatch1_RemoveTags_LB(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "remove-tag-lb")

	doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
		"Tags.member.1.Key":     {"to-remove"},
		"Tags.member.1.Value":   {"yes"},
		"Tags.member.2.Key":     {"to-keep"},
		"Tags.member.2.Value":   {"yes"},
	})

	doELBv2(t, h, url.Values{
		"Action":                {"RemoveTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
		"TagKeys.member.1":      {"to-remove"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
	})
	body := rec.Body.String()
	assert.NotContains(t, body, "to-remove")
	assert.Contains(t, body, "to-keep")
}

func TestBatch1_Tags_TG(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "tg-tag-test")

	doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {tgArn},
		"Tags.member.1.Key":     {"service"},
		"Tags.member.1.Value":   {"api"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {tgArn},
	})
	assert.Contains(t, rec.Body.String(), "service")
	assert.Contains(t, rec.Body.String(), "api")
}

func TestBatch1_Tags_Listener(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-tag-lb")
	tgArn := b1CreateTG(t, h, "listener-tag-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lArn},
		"Tags.member.1.Key":     {"listener-tag"},
		"Tags.member.1.Value":   {"true"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lArn},
	})
	assert.Contains(t, rec.Body.String(), "listener-tag")
}

func TestBatch1_AddTags_MissingResourceArns(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":            {"AddTags"},
		"Version":           {"2015-12-01"},
		"Tags.member.1.Key": {"k"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- SSL Policies ----

func TestBatch1_DescribeSSLPolicies_All(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeSSLPolicies"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ELBSecurityPolicy")
}

func TestBatch1_DescribeSSLPolicies_Filter(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeSSLPolicies"},
		"Version":        {"2015-12-01"},
		"Names.member.1": {"ELBSecurityPolicy-2016-08"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SSLPolicies struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"SslPolicies"`
		} `xml:"DescribeSSLPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.SSLPolicies.Members, 1)
	assert.Equal(t, "ELBSecurityPolicy-2016-08", resp.Result.SSLPolicies.Members[0].Name)
}

// ---- TrustStore lifecycle ----

func TestBatch1_TrustStore_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	// Create
	cRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-full-lifecycle"},
	})
	require.Equal(t, http.StatusOK, cRec.Code)
	var cResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
					Name          string `xml:"Name"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(cRec.Body.Bytes(), &cResp))
	tsArn := cResp.Result.TrustStores.Members[0].TrustStoreArn
	assert.NotEmpty(t, tsArn)

	// Describe
	dRec := doELBv2(t, h, url.Values{
		"Action":                  {"DescribeTrustStores"},
		"Version":                 {"2015-12-01"},
		"TrustStoreArns.member.1": {tsArn},
	})
	require.Equal(t, http.StatusOK, dRec.Code)
	assert.Contains(t, dRec.Body.String(), "ts-full-lifecycle")

	// Modify
	mRec := doELBv2(t, h, url.Values{
		"Action":        {"ModifyTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, mRec.Code)

	// Delete
	delRec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

func TestBatch1_TrustStore_Revocations(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-revocations"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	// Add revocations
	addRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddTrustStoreRevocations"},
		"Version":                              {"2015-12-01"},
		"TrustStoreArn":                        {tsArn},
		"RevocationContents.member.1.S3Bucket": {"my-bucket"},
		"RevocationContents.member.1.S3Key":    {"revocations.crl"},
		"RevocationContents.member.1.RevocationType": {"CRL"},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	// Describe revocations
	descRec := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreRevocations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, descRec.Code)
}

// ---- Listener certificates ----

func TestBatch1_ListenerCertificates_AddAndDescribe(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "cert-lb")
	tgArn := b1CreateTG(t, h, "cert-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn) // HTTP, no cert needed

	addRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {lArn},
		"Certificates.member.1.CertificateArn": {"arn:aws:acm:us-east-1:000000000000:certificate/extra1"},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	descRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "extra1")
}

func TestBatch1_ListenerCertificates_Remove(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rm-cert-lb")
	tgArn := b1CreateTG(t, h, "rm-cert-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	certArn := "arn:aws:acm:us-east-1:000000000000:certificate/to-remove"
	doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {lArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})

	rmRec := doELBv2(t, h, url.Values{
		"Action":                               {"RemoveListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {lArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})
	assert.Equal(t, http.StatusOK, rmRec.Code)
}

// ---- Account limits ----

func TestBatch1_DescribeAccountLimits(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2015-12-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Limits")
}

// ---- CapacityReservation ----

func TestBatch1_DescribeCapacityReservation(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "cap-res-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeCapacityReservation"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch1_ModifyCapacityReservation(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mod-cap-res-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"ModifyCapacityReservation"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"MinimumLoadBalancerCapacity.CapacityUnits": {"100"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Stub operations ----

func TestBatch1_GetResourcePolicy(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "grp-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":      {"GetResourcePolicy"},
		"Version":     {"2015-12-01"},
		"ResourceArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch1_GetResourcePolicy_MissingArn(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"GetResourcePolicy"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_GetTrustStoreCaCertificatesBundle(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-ca-bundle"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))

	rec := doELBv2(t, h, url.Values{
		"Action":        {"GetTrustStoreCaCertificatesBundle"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsResp.Result.TrustStores.Members[0].TrustStoreArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch1_ModifyIpPools(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "ippool-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"ModifyIpPools"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- ProtocolVersion ----

func TestBatch1_CreateTG_ProtocolVersionHTTP2(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":          {"CreateTargetGroup"},
		"Version":         {"2015-12-01"},
		"Name":            {"tg-http2"},
		"Protocol":        {"HTTP"},
		"Port":            {"80"},
		"VpcId":           {"vpc-00000000"},
		"ProtocolVersion": {"HTTP2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "HTTP2")
}

func TestBatch1_CreateTG_ProtocolVersionGRPC(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":          {"CreateTargetGroup"},
		"Version":         {"2015-12-01"},
		"Name":            {"tg-grpc"},
		"Protocol":        {"HTTP"},
		"Port":            {"50051"},
		"VpcId":           {"vpc-00000000"},
		"ProtocolVersion": {"GRPC"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GRPC")
}

// ---- NLB-specific ----

func TestBatch1_NLB_DNS_Format(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"my-nlb"},
		"Type":    {"network"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					DNSName string `xml:"DNSName"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	dns := resp.Result.LoadBalancers.Members[0].DNSName
	assert.Contains(t, dns, "my-nlb")
	assert.Contains(t, dns, "elb.")
}

func TestBatch1_NLB_DefaultCrossZone_False(t *testing.T) {
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

// ---- Pagination ----

func TestBatch1_DescribeLBs_Pagination(t *testing.T) {
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

func TestBatch1_DescribeTGs_Pagination(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	for i := range 4 {
		b1CreateTG(t, h, "pag-tg-"+string(rune('a'+i)))
	}

	rec := doELBv2(t, h, url.Values{
		"Action":   {"DescribeTargetGroups"},
		"Version":  {"2015-12-01"},
		"PageSize": {"2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			NextMarker   string `xml:"NextMarker"`
			TargetGroups struct {
				Members []struct{} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.TargetGroups.Members, 2)
	assert.NotEmpty(t, resp.Result.NextMarker)
}

func TestBatch1_DescribeListeners_Pagination(t *testing.T) {
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

// ---- Validation ----

func TestBatch1_CreateLB_NameTooLong(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"this-name-is-way-too-long-for-elb"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_CreateTG_NameTooLong(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {"this-name-is-definitely-too-long-yes"},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_CreateListener_InvalidPort(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "invalid-port-lb")
	tgArn := b1CreateTG(t, h, "invalid-port-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"99999"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_CreateRule_InvalidPriority_Zero(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-prio-zero-lb")
	tgArn := b1CreateTG(t, h, "rule-prio-zero-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"0"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/foo"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch1_CreateTG_InvalidTargetType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":     {"CreateTargetGroup"},
		"Version":    {"2015-12-01"},
		"Name":       {"bad-type-tg"},
		"Protocol":   {"HTTP"},
		"Port":       {"80"},
		"VpcId":      {"vpc-00000000"},
		"TargetType": {"invalid"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- DescribeTrustStoreAssociations ----

func TestBatch1_DescribeTrustStoreAssociations(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-associations"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreAssociations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- DeleteSharedTrustStoreAssociation ----

func TestBatch1_DeleteSharedTrustStoreAssociation(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-shared-assoc"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteSharedTrustStoreAssociation"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- RemoveTrustStoreRevocations ----

func TestBatch1_RemoveTrustStoreRevocations(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-rm-rev"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":                 {"RemoveTrustStoreRevocations"},
		"Version":                {"2015-12-01"},
		"TrustStoreArn":          {tsArn},
		"RevocationIds.member.1": {"1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- GetTrustStoreRevocationContent ----

func TestBatch1_GetTrustStoreRevocationContent(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-rev-content"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":        {"GetTrustStoreRevocationContent"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
		"RevocationId":  {"1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Forward with weighted target groups ----

func TestBatch1_ForwardWeightedTGs(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "weighted-lb")
	tgArn1 := b1CreateTG(t, h, "weighted-tg1")
	tgArn2 := b1CreateTG(t, h, "weighted-tg2")

	rec := doELBv2(t, h, url.Values{
		"Action":                       {"CreateListener"},
		"Version":                      {"2015-12-01"},
		"LoadBalancerArn":              {lbArn},
		"Protocol":                     {"HTTP"},
		"Port":                         {"80"},
		"DefaultActions.member.1.Type": {"forward"},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.1.TargetGroupArn": {tgArn1},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.1.Weight":         {"80"},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.2.TargetGroupArn": {tgArn2},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.2.Weight":         {"20"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, tgArn1)
	assert.Contains(t, body, tgArn2)
}

// ---- ModifyRule ----

func TestBatch1_ModifyRule_ChangeConditions(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mod-rule-lb")
	tgArn := b1CreateTG(t, h, "mod-rule-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	cRec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"200"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/old"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, cRec.Code)

	var cResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(cRec.Body.Bytes(), &cResp))
	ruleArn := cResp.Result.Rules.Members[0].RuleArn

	mRec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyRule"},
		"Version":                   {"2015-12-01"},
		"RuleArn":                   {ruleArn},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/new"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, mRec.Code)
	assert.Contains(t, mRec.Body.String(), "/new")
}

// ---- TG in use protection ----

func TestBatch1_DeleteTG_InUseRejected(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "tg-inuse-lb")
	tgArn := b1CreateTG(t, h, "tg-inuse")
	b1CreateListener(t, h, lbArn, tgArn) // forward to tgArn

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DeleteTargetGroup"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- DescribeTargetHealth filter ----

func TestBatch1_DescribeTargetHealth_FilterByTarget(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "hc-filter")

	doELBv2(t, h, url.Values{
		"Action":                {"RegisterTargets"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-target-a"},
		"Targets.member.1.Port": {"80"},
		"Targets.member.2.Id":   {"i-target-b"},
		"Targets.member.2.Port": {"80"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                {"DescribeTargetHealth"},
		"Version":               {"2015-12-01"},
		"TargetGroupArn":        {tgArn},
		"Targets.member.1.Id":   {"i-target-a"},
		"Targets.member.1.Port": {"80"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetHealthDescriptions struct {
				Members []struct {
					Target struct {
						ID string `xml:"Id"`
					} `xml:"Target"`
				} `xml:"member"`
			} `xml:"TargetHealthDescriptions"`
		} `xml:"DescribeTargetHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetHealthDescriptions.Members, 1)
	assert.Equal(t, "i-target-a", resp.Result.TargetHealthDescriptions.Members[0].Target.ID)
}
