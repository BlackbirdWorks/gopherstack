package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeLoadBalancerAttributes tests describe LB attributes.
func TestDescribeLoadBalancerAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "attrs-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestModifyLoadBalancerAttributes tests modify LB attributes.
func TestModifyLoadBalancerAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "mod-attrs-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"ModifyLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestSetSecurityGroups tests setting security groups.
func TestSetSecurityGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sg-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":                  {"SetSecurityGroups"},
		"Version":                 {"2015-12-01"},
		"LoadBalancerArn":         {lbArn},
		"SecurityGroups.member.1": {"sg-00000001"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestSetSubnets tests subnet setting.
func TestSetSubnets(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "subnet-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":           {"SetSubnets"},
		"Version":          {"2015-12-01"},
		"LoadBalancerArn":  {lbArn},
		"Subnets.member.1": {"subnet-00000001"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing arn
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"SetSubnets"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestSetIpAddressType tests IP address type setting.
func TestSetIpAddressType(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "iptype-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"SetIpAddressType"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"IpAddressType":   {"ipv4"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing arn
	rec2 := doELBv2(t, h, url.Values{
		"Action":  {"SetIpAddressType"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestSetSecurityGroupsMissingARN tests missing ARN for SetSecurityGroups.
func TestSetSecurityGroupsMissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"SetSecurityGroups"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestModifyLoadBalancerAttributesMissing tests missing ARN.
func TestModifyLoadBalancerAttributesMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"ModifyLoadBalancerAttributes"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeLoadBalancerAttributesMissing tests missing ARN.
func TestDescribeLoadBalancerAttributesMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"DescribeLoadBalancerAttributes"},
		"Version": {"2015-12-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDescribeLoadBalancerAttributesPersists verifies LB attrs are persisted and returned.
func TestDescribeLoadBalancerAttributesPersists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "lb-attrs-persist")

	// Verify defaults are set on creation.
	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "60", attrMap["idle_timeout.timeout_seconds"])
	assert.Equal(t, "defensive", attrMap["routing.http.desync_mitigation_mode"])
	assert.Equal(t, "false", attrMap["deletion_protection.enabled"])

	// Modify and verify the change is persisted.
	modRec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyLoadBalancerAttributes"},
		"Version":                   {"2015-12-01"},
		"LoadBalancerArn":           {lbArn},
		"Attributes.member.1.Key":   {"idle_timeout.timeout_seconds"},
		"Attributes.member.1.Value": {"120"},
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	// Describe again to verify persistence.
	rec2 := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	attrMap2 := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap2[a.Key] = a.Value
	}
	assert.Equal(t, "120", attrMap2["idle_timeout.timeout_seconds"])
}

// TestSetSecurityGroupsPersist verifies that SetSecurityGroups updates the LB state.
func TestSetSecurityGroupsPersist(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "sg-persist-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":                  {"SetSecurityGroups"},
		"Version":                 {"2015-12-01"},
		"LoadBalancerArn":         {lbArn},
		"SecurityGroups.member.1": {"sg-00000001"},
		"SecurityGroups.member.2": {"sg-00000002"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			SecurityGroupIDs struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"SecurityGroupIds"`
		} `xml:"SetSecurityGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.SecurityGroupIDs.Members, 2)
	assert.Equal(t, "sg-00000001", resp.Result.SecurityGroupIDs.Members[0].Value)
	assert.Equal(t, "sg-00000002", resp.Result.SecurityGroupIDs.Members[1].Value)
}

// TestSetSecurityGroupsNotFound verifies that SetSecurityGroups returns 400 (LoadBalancerNotFound) for missing LB.
func TestSetSecurityGroupsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"SetSecurityGroups"},
		"Version": {"2015-12-01"},
		"LoadBalancerArn": {
			"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/nonexistent/abc",
		},
		"SecurityGroups.member.1": {"sg-00000001"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSetSubnetsPersist verifies that SetSubnets updates the LB availability zones.
func TestSetSubnetsPersist(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "subnets-persist-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":           {"SetSubnets"},
		"Version":          {"2015-12-01"},
		"LoadBalancerArn":  {lbArn},
		"Subnets.member.1": {"subnet-00000001"},
		"Subnets.member.2": {"subnet-00000002"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"SetSubnetsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.AvailabilityZones.Members, 2)
}

// TestSetIpAddressTypePersist verifies that SetIpAddressType updates and validates.
func TestSetIpAddressTypePersist(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "iptype-persist-lb")

	// Valid type.
	rec := doELBv2(t, h, url.Values{
		"Action":          {"SetIpAddressType"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"IpAddressType":   {"dualstack"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			IPAddressType string `xml:"IpAddressType"`
		} `xml:"SetIpAddressTypeResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "dualstack", resp.Result.IPAddressType)

	// Invalid type.
	rec2 := doELBv2(t, h, url.Values{
		"Action":          {"SetIpAddressType"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"IpAddressType":   {"bogus"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestNLBDefaultAttributes verifies that NLBs have cross_zone=false by default.
func TestNLBDefaultAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateNLB(t, h, "nlb-attrs-test")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, m := range resp.Result.Attributes.Members {
		attrMap[m.Key] = m.Value
	}

	assert.Equal(t, "false", attrMap["load_balancing.cross_zone.enabled"])
	// NLB should not have HTTP-specific attributes.
	assert.NotContains(t, attrMap, "routing.http2.enabled")
	assert.NotContains(t, attrMap, "waf.fail_open.enabled")
}

// TestALBResponseHeaderAttributes verifies that ALBs have response header attributes.
func TestALBResponseHeaderAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "alb-resp-hdr-test")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, m := range resp.Result.Attributes.Members {
		attrMap[m.Key] = m.Value
	}

	assert.Equal(t, "true", attrMap["routing.http.response.server.enabled"])
	assert.Equal(t, "false", attrMap["routing.http.response.strict_transport_security.enabled"])
	assert.Contains(t, attrMap, "routing.http.response.x_frame_options.header_value")
	assert.Contains(t, attrMap, "routing.http.response.content_security_policy.header_value")
}

// TestSetSecurityGroupsNLBRejected verifies that SetSecurityGroups is rejected for NLBs.
func TestSetSecurityGroupsNLBRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateNLB(t, h, "nlb-sg-reject")

	rec := doELBv2(t, h, url.Values{
		"Action":                  {"SetSecurityGroups"},
		"Version":                 {"2015-12-01"},
		"LoadBalancerArn":         {lbArn},
		"SecurityGroups.member.1": {"sg-12345"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGWLBDefaultAttributes verifies that GWLB has cross_zone=false.
func TestGWLBDefaultAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {"gwlb-attrs"},
		"Type":    {"gateway"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
	lbArn := createResp.Result.LoadBalancers.Members[0].LoadBalancerArn

	recAttrs := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, recAttrs.Code)

	var attrsResp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(recAttrs.Body.Bytes(), &attrsResp))

	attrMap := make(map[string]string)
	for _, m := range attrsResp.Result.Attributes.Members {
		attrMap[m.Key] = m.Value
	}

	assert.Equal(t, "false", attrMap["load_balancing.cross_zone.enabled"])
	assert.NotContains(t, attrMap, "routing.http2.enabled")
}

// TestNLBAttributeDefaults verifies that NLB attributes don't include HTTP routing attrs.
func TestNLBAttributeDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateNLB(t, h, "nlb-attr-defaults")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeLoadBalancerAttributes"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, m := range resp.Result.Attributes.Members {
		attrMap[m.Key] = m.Value
	}

	// NLB must have these.
	assert.Equal(t, "false", attrMap["access_logs.s3.enabled"])
	assert.Equal(t, "false", attrMap["deletion_protection.enabled"])
	assert.Equal(t, "false", attrMap["load_balancing.cross_zone.enabled"])

	// NLB must not have these HTTP-specific ones.
	assert.NotContains(t, attrMap, "routing.http2.enabled")
	assert.NotContains(t, attrMap, "routing.http.desync_mitigation_mode")
	assert.NotContains(t, attrMap, "waf.fail_open.enabled")
	assert.NotContains(t, attrMap, "routing.http.response.server.enabled")
}

func TestSetSecurityGroups_ALB(t *testing.T) {
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

func TestSetSecurityGroups_NLBRejected(t *testing.T) {
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

func TestSetSubnets_SubnetMappings_ReturnsSubnetId(t *testing.T) {
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

func TestSetSubnets_PlainSubnets(t *testing.T) {
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

func TestSetIpAddressType_Dualstack(t *testing.T) {
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

func TestModifyIpPools(t *testing.T) {
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
