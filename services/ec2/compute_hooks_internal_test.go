package ec2

import (
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testInstanceIDKey = "InstanceId.1"

// stubCompute records calls and returns canned LaunchResults so the
// handler/backend wiring can be exercised without a real Docker daemon.
type stubCompute struct {
	launchErr      error
	result         LaunchResult
	launchCalls    []LaunchRequest
	terminateCalls []string
	startCalls     []string
	stopCalls      []string
}

func (s *stubCompute) Launch(_ context.Context, req LaunchRequest) (LaunchResult, error) {
	s.launchCalls = append(s.launchCalls, req)

	return s.result, s.launchErr
}

func (s *stubCompute) Terminate(_ context.Context, _, providerID string) error {
	s.terminateCalls = append(s.terminateCalls, providerID)

	return nil
}

func (s *stubCompute) Start(_ context.Context, _, providerID string) error {
	s.startCalls = append(s.startCalls, providerID)

	return nil
}

func (s *stubCompute) Stop(_ context.Context, _, providerID string) error {
	s.stopCalls = append(s.stopCalls, providerID)

	return nil
}

func TestComputeHookLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, b *InMemoryBackend, c *stubCompute)
		assertAfter func(t *testing.T, b *InMemoryBackend, c *stubCompute)
		name        string
	}{
		{
			name: "run_terminate_lifecycle",
			setup: func(t *testing.T, _ *InMemoryBackend, c *stubCompute) {
				t.Helper()
				c.result = LaunchResult{
					ProviderID:      "ctr-abc",
					PrivateIP:       "172.20.0.5",
					PublicIPAddress: "127.0.0.1",
					SSHPort:         32100,
				}
			},
			assertAfter: func(t *testing.T, b *InMemoryBackend, c *stubCompute) {
				t.Helper()

				kp, err := b.CreateKeyPair("demo")
				require.NoError(t, err)
				assert.NotEmpty(t, kp.PublicKey, "CreateKeyPair must derive an OpenSSH public key")

				h := NewHandler(b)
				vals := map[string][]string{
					"ImageId":      {"ami-1"},
					"InstanceType": {"t3.micro"},
					"MinCount":     {"1"},
					"KeyName":      {"demo"},
				}

				resp, err := h.handleRunInstances(vals, "req-1")
				require.NoError(t, err)
				require.NotNil(t, resp)

				assert.Len(t, c.launchCalls, 1)
				assert.Equal(t, "demo", c.launchCalls[0].KeyName)
				assert.Contains(t, c.launchCalls[0].AuthorizedKey, "ssh-rsa")

				// verify the instance reflects the docker-assigned IP/port
				instances := b.DescribeInstances(nil, "")
				require.Len(t, instances, 1)
				assert.Equal(t, "ctr-abc", instances[0].ProviderID)
				assert.Equal(t, "172.20.0.5", instances[0].PrivateIP)
				assert.Equal(t, 32100, instances[0].SSHPort)

				// terminate goes through the compute hook
				_, terr := h.handleTerminateInstances(map[string][]string{
					testInstanceIDKey: {instances[0].ID},
				}, "req-2")
				require.NoError(t, terr)
				assert.Equal(t, []string{"ctr-abc"}, c.terminateCalls)
			},
		},
		{
			name:  "start_stop_dispatch",
			setup: func(_ *testing.T, _ *InMemoryBackend, _ *stubCompute) {},
			assertAfter: func(t *testing.T, b *InMemoryBackend, c *stubCompute) {
				t.Helper()
				c.result = LaunchResult{ProviderID: "ctr-xyz"}

				h := NewHandler(b)
				resp, err := h.handleRunInstances(map[string][]string{
					"ImageId": {"ami-2"}, "MinCount": {"1"},
				}, "req")
				require.NoError(t, err)
				require.NotNil(t, resp)

				instances := b.DescribeInstances(nil, "")
				require.Len(t, instances, 1)
				id := instances[0].ID
				b.reconcileInstanceLifecycle() // pending → running

				_, err = h.handleStopInstances(map[string][]string{testInstanceIDKey: {id}}, "req")
				require.NoError(t, err)
				assert.Equal(t, []string{"ctr-xyz"}, c.stopCalls)
				b.reconcileInstanceLifecycle() // stopping → stopped

				_, err = h.handleStartInstances(map[string][]string{testInstanceIDKey: {id}}, "req")
				require.NoError(t, err)
				assert.Equal(t, []string{"ctr-xyz"}, c.startCalls)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			c := &stubCompute{}
			b.WithCompute(c)
			tt.setup(t, b, c)
			tt.assertAfter(t, b, c)
		})
	}
}

// stubDNSRegistrar records register/deregister calls for assertion.
type stubDNSRegistrar struct {
	registered   []string
	deregistered []string
}

func (s *stubDNSRegistrar) Register(name string)   { s.registered = append(s.registered, name) }
func (s *stubDNSRegistrar) Deregister(name string) { s.deregistered = append(s.deregistered, name) }

func TestComputeHookPublishesDNSAndTags(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	c := &stubCompute{
		result: LaunchResult{
			ProviderID:      "ctr-dns",
			PrivateIP:       "172.20.0.7",
			PublicIPAddress: "127.0.0.1",
			PublicDNSName:   "ec2-test1.compute-1.amazonaws.com",
			SSHPort:         22001,
		},
	}

	dns := &stubDNSRegistrar{}
	b.WithCompute(c)
	b.SetDNSRegistrar(dns)

	h := NewHandler(b)

	resp, err := h.handleRunInstances(map[string][]string{
		"ImageId":      {"ami-1"},
		"InstanceType": {"t3.micro"},
		"MinCount":     {"1"},
	}, "req-dns")
	require.NoError(t, err)
	require.NotNil(t, resp)

	instances := b.DescribeInstances(nil, "")
	require.Len(t, instances, 1)
	id := instances[0].ID

	tags := b.TagsForResource(id)
	assert.Equal(t, "22001", tags[tagKeySSHPort])
	assert.Equal(t, "127.0.0.1", tags[tagKeySSHHost])

	assert.Equal(t, []string{"ec2-test1.compute-1.amazonaws.com"}, dns.registered)

	_, terr := h.handleTerminateInstances(map[string][]string{
		testInstanceIDKey: {id},
	}, "req-term")
	require.NoError(t, terr)

	assert.Equal(t, []string{"ec2-test1.compute-1.amazonaws.com"}, dns.deregistered)
	assert.Equal(t, []string{"ctr-dns"}, c.terminateCalls)
}

// TestGeneratedResourceIDs_HexOnlyShape guards against gopherstack-28ce: IDs
// built as "<prefix>-" + uuid.New().String()[:N] embed literal "-"
// characters once N crosses a hyphen boundary in the 8-4-4-4-12 hyphenated
// UUID string, producing shapes real AWS never returns (e.g.
// "subnet-44eea3bc-ae2c-4c2"). Every generator below must strip the UUID's
// hyphens first, so the suffix is hex-only. This covers a representative
// set of resource families across the package, including ones that use a
// named prefix-length constant instead of a literal.
func TestGeneratedResourceIDs_HexOnlyShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		generate func() string
		pattern  *regexp.Regexp
		name     string
	}{
		{name: "subnet", generate: newSubnetID, pattern: regexp.MustCompile(`^subnet-[0-9a-f]{17}$`)},
		{name: "instance", generate: newInstanceID, pattern: regexp.MustCompile(`^i-[0-9a-f]{17}$`)},
		{name: "reservation", generate: newReservationID, pattern: regexp.MustCompile(`^r-[0-9a-f]{17}$`)},
		{name: "vpc", generate: newVPCID, pattern: regexp.MustCompile(`^vpc-[0-9a-f]{17}$`)},
		{name: "security_group", generate: newSecurityGroupID, pattern: regexp.MustCompile(`^sg-[0-9a-f]{17}$`)},
		{name: "ami", generate: newAMIID, pattern: regexp.MustCompile(`^ami-[0-9a-f]{17}$`)},
		{name: "snapshot", generate: newSnapshotID, pattern: regexp.MustCompile(`^snap-[0-9a-f]{17}$`)},
		{name: "volume", generate: newVolumeID, pattern: regexp.MustCompile(`^vol-[0-9a-f]{17}$`)},
		{name: "eni", generate: newENIID, pattern: regexp.MustCompile(`^eni-[0-9a-f]{17}$`)},
		{name: "route_table", generate: newRouteTableID, pattern: regexp.MustCompile(`^rtb-[0-9a-f]{17}$`)},
		{name: "transit_gateway", generate: newTransitGatewayID, pattern: regexp.MustCompile(`^tgw-[0-9a-f]{17}$`)},
		{
			name:     "local_gateway_rtb_vif_group_assoc",
			generate: newLocalGatewayRouteTableVirtualInterfaceGroupAssociationID,
			pattern:  regexp.MustCompile(`^lgw-route-table-virtual-interface-group-assoc-[0-9a-f]{17}$`),
		},
		{
			name:     "ipam_verification_token",
			generate: newIPAMVerificationTokenName,
			pattern:  regexp.MustCompile(`^ipam-verify-[0-9a-f]{12}$`),
		},
		{name: "host_reservation", generate: newHostReservationID, pattern: regexp.MustCompile(`^hr-[0-9a-f]{17}$`)},
		{
			name:     "declarative_policies_report",
			generate: newDeclarativePoliciesReportID,
			pattern:  regexp.MustCompile(`^report-[0-9a-f]{17}$`),
		},
		{
			name:     "vpc_bpa_exclusion",
			generate: newVPCBPAExclusionID,
			pattern:  regexp.MustCompile(`^vpcbpa-exclusion-[0-9a-f]{17}$`),
		},
		{
			name:     "vpc_encryption_control",
			generate: newVPCEncryptionControlID,
			pattern:  regexp.MustCompile(`^vpc-ec-[0-9a-f]{17}$`),
		},
		{name: "vpn_concentrator", generate: newVPNConcentratorID, pattern: regexp.MustCompile(`^vpnc-[0-9a-f]{17}$`)},
		{name: "vpn_pre_shared_key", generate: newVPNPreSharedKey, pattern: regexp.MustCompile(`^[0-9a-f]{24}$`)},
		{
			name:     "key_pair_fingerprint",
			generate: newKeyPairFingerprint,
			pattern:  regexp.MustCompile(`^aa:bb:cc:dd:[0-9a-f]{11}$`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.generate()
			assert.Regexp(t, tt.pattern, got, "generated %s ID must be hex-only after the prefix", tt.name)
		})
	}
}
