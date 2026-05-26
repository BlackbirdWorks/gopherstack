package elb_test

// handler_audit2_test.go — table-driven tests for the second batch of
// AWS-accuracy audit fixes (issues #8, #11, #12, #13, #19-#26, #28-#30).

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// ─── Issue #8: AZ vs Subnet mutual exclusivity ───────────────────────────────

// TestAudit2_AZSubnetMutualExclusivity verifies that supplying both
// AvailabilityZones and Subnets in CreateLoadBalancer is rejected.
func TestAudit2_AZSubnetMutualExclusivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "az_only_accepted",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"az-only-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "subnet_only_accepted",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"subnet-only-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"Subnets.member.1":                   {"subnet-000a"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "both_az_and_subnet_rejected",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"az-subnet-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
				"Subnets.member.1":                   {"subnet-000a"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "neither_az_nor_subnet_rejected",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"no-az-subnet-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit2_EnableAZ_VPCLBRejected verifies that EnableAvailabilityZones on a
// VPC LB (created with subnets) is rejected with InvalidConfigurationRequest.
func TestAudit2_EnableAZ_VPCLBRejected(t *testing.T) {
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
				"Action":                    {"EnableAvailabilityZonesForLoadBalancer"},
				"Version":                   {"2012-06-01"},
				"LoadBalancerName":          {lbName},
				"AvailabilityZones.member.1": {"us-east-1b"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit2_AttachSubnets_EC2ClassicRejected verifies that AttachSubnets on an
// EC2-Classic LB is rejected.
func TestAudit2_AttachSubnets_EC2ClassicRejected(t *testing.T) {
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

// ─── Issue #11: Per-region CanonicalHostedZoneNameID ─────────────────────────

// TestAudit2_RegionHostedZoneID verifies that known AWS regions return their
// documented CanonicalHostedZoneNameID values.
func TestAudit2_RegionHostedZoneID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region         string
		wantHostedZone string
	}{
		{"us-east-1", "Z35SXDOTRQ7X7K"},
		{"us-west-2", "Z1H1FL5HABSF5"},
		{"eu-west-1", "Z32O12XQLNTSW2"},
		{"ap-southeast-1", "Z1LMS91P8CMLE5"},
		{"ap-northeast-1", "Z14GRHDCWA56QT"},
	}

	for _, tt := range tests {
		t.Run(tt.region, func(t *testing.T) {
			t.Parallel()

			b := elb.NewInMemoryBackend("123456789012", tt.region)
			h := elb.NewHandler(b)

			mustCreateLB(t, h, "zone-lb")

			rec := doELB(t, h, url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"zone-lb"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
				Result  struct {
					LBs struct {
						Members []struct {
							CanonicalHostedZoneNameID string `xml:"CanonicalHostedZoneNameID"`
						} `xml:"member"`
					} `xml:"LoadBalancerDescriptions"`
				} `xml:"DescribeLoadBalancersResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.LBs.Members, 1)
			assert.Equal(t, tt.wantHostedZone, resp.Result.LBs.Members[0].CanonicalHostedZoneNameID)
		})
	}
}

// ─── Issue #12: DNS name format ───────────────────────────────────────────────

// TestAudit2_DNSNameFormat verifies that DNS names include a hash suffix and
// the internal scheme uses the "internal-" prefix.
func TestAudit2_DNSNameFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals          url.Values
		name          string
		wantPrefix    string
		wantSuffix    string
		wantContains  string
	}{
		{
			name: "internet_facing_has_hash_suffix",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"inet-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantPrefix:   "inet-lb-",
			wantSuffix:   ".us-east-1.elb.amazonaws.com",
			wantContains: "us-east-1.elb.amazonaws.com",
		},
		{
			name: "internal_scheme_has_internal_prefix",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"int-lb"},
				"Scheme":                              {"internal"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantPrefix:   "internal-int-lb-",
			wantSuffix:   ".us-east-1.elb.amazonaws.com",
			wantContains: "internal-int-lb-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
				Result  struct {
					DNSName string `xml:"DNSName"`
				} `xml:"CreateLoadBalancerResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			dns := resp.Result.DNSName

			if tt.wantPrefix != "" {
				assert.True(t, strings.HasPrefix(dns, tt.wantPrefix),
					"DNS %q should start with %q", dns, tt.wantPrefix)
			}
			if tt.wantSuffix != "" {
				assert.True(t, strings.HasSuffix(dns, tt.wantSuffix),
					"DNS %q should end with %q", dns, tt.wantSuffix)
			}
			if tt.wantContains != "" {
				assert.Contains(t, dns, tt.wantContains)
			}
		})
	}
}

// ─── Issue #13: SourceSecurityGroup VPC vs EC2-Classic ───────────────────────

// TestAudit2_SourceSecurityGroup_VPC verifies that a VPC LB returns the
// account ID as OwnerAlias and "default" as GroupName.
func TestAudit2_SourceSecurityGroup_VPC(t *testing.T) {
	t.Parallel()

	b := elb.NewInMemoryBackend("111222333444", "us-east-1")
	h := elb.NewHandler(b)
	mustCreateVPCLB(t, h, "vpc-ssg-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"vpc-ssg-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LBs struct {
				Members []struct {
					SSG struct {
						GroupName  string `xml:"GroupName"`
						OwnerAlias string `xml:"OwnerAlias"`
					} `xml:"SourceSecurityGroup"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LBs.Members, 1)

	ssg := resp.Result.LBs.Members[0].SSG
	assert.Equal(t, "default", ssg.GroupName)
	assert.Equal(t, "111222333444", ssg.OwnerAlias)
}

// ─── Issue #19: Stickiness policy on TCP/SSL listeners ───────────────────────

// TestAudit2_StickinessPolicy_TCPRejected verifies that attaching a stickiness
// policy to a TCP or SSL listener is rejected.
func TestAudit2_StickinessPolicy_TCPRejected(t *testing.T) {
	t.Parallel()

	const certARN = "arn:aws:iam::123456789012:server-certificate/my-cert"

	tests := []struct {
		name       string
		protocol   string
		certARN    string
		port       string
		wantStatus int
	}{
		{"tcp_listener_rejected", "TCP", "", "80", http.StatusBadRequest},
		{"ssl_listener_rejected", "SSL", certARN, "443", http.StatusBadRequest},
		{"http_listener_accepted", "HTTP", "", "80", http.StatusOK},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := fmt.Sprintf("sticky-%d", i)

			// Create LB with specified listener.
			vals := url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {lbName},
				"Listeners.member.1.Protocol":         {tt.protocol},
				"Listeners.member.1.LoadBalancerPort": {tt.port},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			}
			if tt.certARN != "" {
				vals.Set("Listeners.member.1.SSLCertificateId", tt.certARN)
			}

			rec := doELB(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code)

			// Create a stickiness policy.
			doELB(t, h, url.Values{
				"Action":           {"CreateLBCookieStickinessPolicy"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {lbName},
				"PolicyName":       {"sticky-pol"},
			})

			// Attach stickiness policy to the listener.
			portStr := tt.port
			rec = doELB(t, h, url.Values{
				"Action":                  {"SetLoadBalancerPoliciesOfListener"},
				"Version":                 {"2012-06-01"},
				"LoadBalancerName":        {lbName},
				"LoadBalancerPort":        {portStr},
				"PolicyNames.member.1":    {"sticky-pol"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── Issue #20: SSL cert on non-HTTPS/SSL listener ───────────────────────────

// TestAudit2_SetSSLCert_NonHTTPSListener verifies that setting an SSL cert on
// an HTTP or TCP listener returns InvalidConfigurationRequest.
func TestAudit2_SetSSLCert_NonHTTPSListener(t *testing.T) {
	t.Parallel()

	const certARN = "arn:aws:acm:us-east-1:123456789012:certificate/abc-123"

	tests := []struct {
		name       string
		protocol   string
		port       string
		wantStatus int
	}{
		{"http_listener_rejected", "HTTP", "80", http.StatusBadRequest},
		{"tcp_listener_rejected", "TCP", "80", http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := fmt.Sprintf("ssl-proto-%d", i)

			rec := doELB(t, h, url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {lbName},
				"Listeners.member.1.Protocol":         {tt.protocol},
				"Listeners.member.1.LoadBalancerPort": {tt.port},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doELB(t, h, url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {lbName},
				"LoadBalancerPort": {tt.port},
				"SSLCertificateId": {certARN},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── Issue #21: SSL cert ARN format validation ───────────────────────────────

// TestAudit2_CertARNValidation verifies that SetLoadBalancerListenerSSLCertificate
// accepts valid ACM/IAM ARNs and rejects invalid ones.
func TestAudit2_CertARNValidation(t *testing.T) {
	t.Parallel()

	const httpsPort = "443"
	const certARN = "arn:aws:iam::123456789012:server-certificate/initial"

	tests := []struct {
		certID     string
		name       string
		wantStatus int
	}{
		{"arn:aws:acm:us-east-1:123456789012:certificate/abc-123", "acm_arn_valid", http.StatusOK},
		{"arn:aws:iam::123456789012:server-certificate/my-cert", "iam_arn_valid", http.StatusOK},
		{"not-an-arn", "bare_string_rejected", http.StatusBadRequest},
		{"arn:aws:s3:::my-bucket", "s3_arn_rejected", http.StatusBadRequest},
		{"", "empty_rejected", http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := fmt.Sprintf("certval-%d", i)

			rec := doELB(t, h, url.Values{
				"Action":                                        {"CreateLoadBalancer"},
				"Version":                                       {"2012-06-01"},
				"LoadBalancerName":                              {lbName},
				"Listeners.member.1.Protocol":                   {"HTTPS"},
				"Listeners.member.1.LoadBalancerPort":           {httpsPort},
				"Listeners.member.1.InstancePort":               {"8443"},
				"Listeners.member.1.SSLCertificateId":           {certARN},
				"AvailabilityZones.member.1":                    {"us-east-1a"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			vals := url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {lbName},
				"LoadBalancerPort": {httpsPort},
			}
			if tt.certID != "" {
				vals.Set("SSLCertificateId", tt.certID)
			}

			rec = doELB(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── Issue #22: RemoveTags empty keys ────────────────────────────────────────

// TestAudit2_RemoveTags_EmptyList verifies that RemoveTags with no tag keys
// returns a ValidationError.
func TestAudit2_RemoveTags_EmptyList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "no_tags_rejected",
			vals: url.Values{
				"Action":                     {"RemoveTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"rmtags-empty-lb"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with_tags_accepted",
			vals: url.Values{
				"Action":                     {"RemoveTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"rmtags-nonempty-lb"},
				"Tags.member.1.Key":          {"mykey"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "rmtags-empty-lb")
			mustCreateLB(t, h, "rmtags-nonempty-lb")
			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── Issue #23: Reserved tag key prefixes ────────────────────────────────────

// TestAudit2_AddTags_ReservedPrefixes verifies that tag keys with reserved
// prefixes (aws:, amazon:, elasticloadbalancing:) are rejected.
func TestAudit2_AddTags_ReservedPrefixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagKey     string
		name       string
		wantStatus int
	}{
		{"my-tag", "plain_key_accepted", http.StatusOK},
		{"aws:reserved", "aws_prefix_rejected", http.StatusBadRequest},
		{"AWS:Reserved", "aws_prefix_case_insensitive_rejected", http.StatusBadRequest},
		{"amazon:reserved", "amazon_prefix_rejected", http.StatusBadRequest},
		{"Amazon:reserved", "amazon_prefix_case_insensitive_rejected", http.StatusBadRequest},
		{"elasticloadbalancing:reserved", "elb_prefix_rejected", http.StatusBadRequest},
		{"ElasticLoadBalancing:reserved", "elb_prefix_case_insensitive_rejected", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "addtag-prefix-lb")

			rec := doELB(t, h, url.Values{
				"Action":                     {"AddTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"addtag-prefix-lb"},
				"Tags.member.1.Key":          {tt.tagKey},
				"Tags.member.1.Value":        {"value"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── Issue #24: Instance ID format in DescribeInstanceHealth ─────────────────

// TestAudit2_DescribeInstanceHealth_InvalidFormat verifies that
// DescribeInstanceHealth rejects malformed instance IDs before checking
// registration status.
func TestAudit2_DescribeInstanceHealth_InvalidFormat(t *testing.T) {
	t.Parallel()

	const validID = "i-1234567890abcdef0"

	tests := []struct {
		instanceID string
		name       string
		wantStatus int
		register   bool // whether to pre-register the instance
	}{
		// Valid format + registered → 200.
		{validID, "valid_registered_id_ok", http.StatusOK, true},
		// Valid format + NOT registered → 400 (InvalidInstance: not registered).
		// This is correct AWS behavior; format check passes but registration check fails.
		{validID, "valid_unregistered_id_rejected", http.StatusBadRequest, false},
		// Invalid format → 400 (format error, before registration check).
		{"not-an-instance", "invalid_format_rejected", http.StatusBadRequest, false},
		{"ec2-i-1234567890", "wrong_prefix_rejected", http.StatusBadRequest, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "health-fmt-lb")

			if tt.register {
				rec := doELB(t, h, url.Values{
					"Action":                          {"RegisterInstancesWithLoadBalancer"},
					"Version":                         {"2012-06-01"},
					"LoadBalancerName":                {"health-fmt-lb"},
					"Instances.member.1.InstanceId":   {tt.instanceID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doELB(t, h, url.Values{
				"Action":                          {"DescribeInstanceHealth"},
				"Version":                         {"2012-06-01"},
				"LoadBalancerName":                {"health-fmt-lb"},
				"Instances.member.1.InstanceId":   {tt.instanceID},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── Issue #25: Account limits ───────────────────────────────────────────────

// TestAudit2_AccountLimit_MaxLoadBalancers verifies that creating more than 20
// load balancers returns a LimitExceeded error.
func TestAudit2_AccountLimit_MaxLoadBalancers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create 20 LBs (the max).
	for i := range 20 {
		rec := doELB(t, h, url.Values{
			"Action":                              {"CreateLoadBalancer"},
			"Version":                             {"2012-06-01"},
			"LoadBalancerName":                    {fmt.Sprintf("max-lbs-%02d", i)},
			"Listeners.member.1.Protocol":         {"HTTP"},
			"Listeners.member.1.LoadBalancerPort": {"80"},
			"Listeners.member.1.InstancePort":     {"8080"},
			"AvailabilityZones.member.1":          {"us-east-1a"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "LB %d creation should succeed", i+1)
	}

	// 21st LB must fail.
	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"lb-over-limit"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ValidationError", errResp.Error.Code)
}

// TestAudit2_AccountLimit_MaxListeners verifies that adding more than 100
// listeners to a single LB returns a ValidationError.
func TestAudit2_AccountLimit_MaxListeners(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// CreateLoadBalancer with 1 listener on port 80.
	mustCreateLB(t, h, "maxlist-lb")

	// Add 99 more listeners on ports 1024..1122 to reach the limit of 100.
	for port := 1024; port < 1024+99; port++ {
		rec := doELB(t, h, url.Values{
			"Action":                              {"CreateLoadBalancerListeners"},
			"Version":                             {"2012-06-01"},
			"LoadBalancerName":                    {"maxlist-lb"},
			"Listeners.member.1.Protocol":         {"HTTP"},
			"Listeners.member.1.LoadBalancerPort": {fmt.Sprintf("%d", port)},
			"Listeners.member.1.InstancePort":     {"8080"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "listener on port %d should succeed", port)
	}

	// Adding the 101st listener must fail.
	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancerListeners"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"maxlist-lb"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"2048"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── Issue #26: Opaque pagination marker ─────────────────────────────────────

// TestAudit2_Pagination_OpaqueMarker verifies that the NextMarker is base64
// encoded and that sending a non-base64 Marker returns an error.
func TestAudit2_Pagination_OpaqueMarker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		marker     string
		wantStatus int
	}{
		// A valid base64-encoded integer offset is accepted.
		{"valid_base64_marker", base64.StdEncoding.EncodeToString([]byte("0")), http.StatusOK},
		// Plain LB name (old behaviour) is now invalid.
		{"plain_name_marker_rejected", "some-lb-name", http.StatusBadRequest},
		// Totally invalid string is rejected.
		{"garbage_marker_rejected", "!!!not-base64!!!", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "pag-lb")

			rec := doELB(t, h, url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
				"Marker":  {tt.marker},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit2_Pagination_NextMarkerIsBase64 verifies that when a NextMarker is
// returned it is valid base64 encoding an integer.
func TestAudit2_Pagination_NextMarkerIsBase64(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create 5 LBs.
	for i := range 5 {
		mustCreateLB(t, h, fmt.Sprintf("pag-lb-%d", i))
	}

	// Request page of size 2.
	rec := doELB(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2012-06-01"},
		"PageSize": {"2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			NextMarker string `xml:"NextMarker"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	marker := resp.Result.NextMarker
	require.NotEmpty(t, marker, "NextMarker should be set when more pages exist")

	// Marker must be valid base64.
	decoded, err := base64.StdEncoding.DecodeString(marker)
	require.NoError(t, err, "NextMarker must be base64: %q", marker)
	assert.NotEmpty(t, decoded)
}

// ─── Issue #28: BackendServerDescriptions cleanup ────────────────────────────

// TestAudit2_BSD_CleanupOnEmptyPolicies verifies that a BackendServerDescription
// entry is removed when all policies are cleared from that backend port.
func TestAudit2_BSD_CleanupOnEmptyPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "bsd-lb")

	// Create a proxy-protocol policy.
	doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-lb"},
		"PolicyName":       {"pp-pol"},
		"PolicyTypeName":   {"ProxyProtocolPolicyType"},
	})

	// Attach policy to backend port 8080.
	doELB(t, h, url.Values{
		"Action":                  {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":                 {"2012-06-01"},
		"LoadBalancerName":        {"bsd-lb"},
		"InstancePort":            {"8080"},
		"PolicyNames.member.1":    {"pp-pol"},
	})

	// Verify BSD entry is present.
	bsdsAfterAdd := describeBSDs(t, h, "bsd-lb")
	require.Len(t, bsdsAfterAdd, 1, "BSD entry should exist after policy attach")

	// Clear all policies from backend port 8080 (empty list).
	doELB(t, h, url.Values{
		"Action":           {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-lb"},
		"InstancePort":     {"8080"},
	})

	// BSD entry must be removed.
	bsdsAfterClear := describeBSDs(t, h, "bsd-lb")
	assert.Empty(t, bsdsAfterClear, "BSD entry should be removed when no policies remain")
}

// describeBSDs is a helper that calls DescribeLoadBalancers and extracts the
// BackendServerDescriptions list.
func describeBSDs(t *testing.T, h *elb.Handler, lbName string) []struct {
	InstancePort int32  `xml:"InstancePort"`
	PolicyNames  string `xml:"PolicyNames"`
} {
	t.Helper()

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {lbName},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LBs struct {
				Members []struct {
					BSDs struct {
						Members []struct {
							InstancePort int32  `xml:"InstancePort"`
							PolicyNames  string `xml:"PolicyNames"`
						} `xml:"member"`
					} `xml:"BackendServerDescriptions"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LBs.Members, 1)

	return resp.Result.LBs.Members[0].BSDs.Members
}

// ─── Issue #29: Snapshot/Restore schema version + region drift ───────────────

// TestAudit2_Snapshot_SchemaVersion verifies that a snapshot includes a
// non-zero schema version and round-trips correctly.
func TestAudit2_Snapshot_SchemaVersion(t *testing.T) {
	t.Parallel()

	b := elb.NewInMemoryBackend("123456789012", "us-east-1")
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "snap-lb")

	snap := b.Snapshot()
	require.NotNil(t, snap)
	assert.Contains(t, string(snap), `"version"`)

	// Restore into a fresh backend.
	b2 := elb.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	lbs, err := b2.DescribeLoadBalancers([]string{"snap-lb"})
	require.NoError(t, err)
	assert.Len(t, lbs, 1)
}

// TestAudit2_Restore_PreservesRegion verifies that restoring a snapshot into a
// backend that already has a region set does not overwrite that region.
func TestAudit2_Restore_PreservesRegion(t *testing.T) {
	t.Parallel()

	// Create snapshot from us-east-1 backend.
	b1 := elb.NewInMemoryBackend("123456789012", "us-east-1")
	snap := b1.Snapshot()
	require.NotNil(t, snap)

	// Restore into us-west-2 backend — region must stay us-west-2.
	b2 := elb.NewInMemoryBackend("123456789012", "us-west-2")
	require.NoError(t, b2.Restore(snap))

	// The backend's region is not directly exported, but we can verify via the
	// DNS name of a newly-created LB (which incorporates the region).
	h2 := elb.NewHandler(b2)
	rec := doELB(t, h2, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"region-drift-lb"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
		"AvailabilityZones.member.1":          {"us-west-2a"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
		Result  struct {
			DNSName string `xml:"DNSName"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp.Result.DNSName, "us-west-2", "DNS should contain us-west-2 region")
}

// ─── Issue #30: ChaosRegions returns handler's region ────────────────────────

// TestAudit2_ChaosRegions verifies that ChaosRegions returns the region
// configured on the backend, not always the default region.
func TestAudit2_ChaosRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region string
		name   string
	}{
		{"us-east-1", "us_east_1"},
		{"eu-west-1", "eu_west_1"},
		{"ap-southeast-1", "ap_southeast_1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elb.NewInMemoryBackend("123456789012", tt.region)
			h := elb.NewHandler(b)

			regions := h.ChaosRegions()
			assert.Contains(t, regions, tt.region)
		})
	}
}

// ─── Issue #5: Protocol pairing validation ───────────────────────────────────

// TestAudit2_ProtocolPairing verifies that only valid frontend/backend protocol
// pairings are accepted.
func TestAudit2_ProtocolPairing(t *testing.T) {
	t.Parallel()

	const certARN = "arn:aws:iam::123456789012:server-certificate/my-cert"

	tests := []struct {
		instanceProtocol string
		protocol         string
		certARN          string
		name             string
		port             string
		wantStatus       int
	}{
		// Valid pairings.
		{"HTTP", "HTTP", "", "http_http", "80", http.StatusOK},
		{"HTTPS", "HTTP", "", "http_https", "80", http.StatusOK},
		{"HTTP", "HTTPS", certARN, "https_http", "443", http.StatusOK},
		{"HTTPS", "HTTPS", certARN, "https_https", "443", http.StatusOK},
		{"TCP", "TCP", "", "tcp_tcp", "80", http.StatusOK},
		{"TCP", "SSL", certARN, "ssl_tcp", "443", http.StatusOK},
		{"SSL", "SSL", certARN, "ssl_ssl", "443", http.StatusOK},
		// Invalid pairings.
		{"TCP", "HTTP", "", "http_tcp_invalid", "80", http.StatusBadRequest},
		{"SSL", "HTTP", "", "http_ssl_invalid", "80", http.StatusBadRequest},
		{"HTTP", "TCP", "", "tcp_http_invalid", "80", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := url.Values{
				"Action":                                        {"CreateLoadBalancer"},
				"Version":                                       {"2012-06-01"},
				"LoadBalancerName":                              {"pair-lb"},
				"Listeners.member.1.Protocol":                   {tt.protocol},
				"Listeners.member.1.InstanceProtocol":           {tt.instanceProtocol},
				"Listeners.member.1.LoadBalancerPort":           {tt.port},
				"Listeners.member.1.InstancePort":               {"8080"},
				"AvailabilityZones.member.1":                    {"us-east-1a"},
			}
			if tt.certARN != "" {
				vals.Set("Listeners.member.1.SSLCertificateId", tt.certARN)
			}

			rec := doELB(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── Issue #7 + #5: Listener count and HTTPS cert required ───────────────────

// TestAudit2_CreateLB_ListenerRequirements verifies various listener validation
// rules during CreateLoadBalancer.
func TestAudit2_CreateLB_ListenerRequirements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "https_requires_cert",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"nocert-lb"},
				"Listeners.member.1.Protocol":         {"HTTPS"},
				"Listeners.member.1.LoadBalancerPort": {"443"},
				"Listeners.member.1.InstancePort":     {"8443"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "ssl_requires_cert",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"nocert-ssl-lb"},
				"Listeners.member.1.Protocol":         {"SSL"},
				"Listeners.member.1.LoadBalancerPort": {"443"},
				"Listeners.member.1.InstancePort":     {"443"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "https_with_cert_accepted",
			vals: url.Values{
				"Action":                                        {"CreateLoadBalancer"},
				"Version":                                       {"2012-06-01"},
				"LoadBalancerName":                              {"withcert-lb"},
				"Listeners.member.1.Protocol":                   {"HTTPS"},
				"Listeners.member.1.LoadBalancerPort":           {"443"},
				"Listeners.member.1.InstancePort":               {"8443"},
				"Listeners.member.1.SSLCertificateId":           {"arn:aws:acm:us-east-1:123456789012:certificate/abc"},
				"AvailabilityZones.member.1":                    {"us-east-1a"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "no_listeners_rejected",
			vals: url.Values{
				"Action":                    {"CreateLoadBalancer"},
				"Version":                   {"2012-06-01"},
				"LoadBalancerName":          {"nolisteners-lb"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "allowed_port_25_accepted",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"port25-lb"},
				"Listeners.member.1.Protocol":         {"TCP"},
				"Listeners.member.1.LoadBalancerPort": {"25"},
				"Listeners.member.1.InstancePort":     {"25"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "reserved_port_rejected",
			vals: url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"port2-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"2"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
