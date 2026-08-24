package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Transit Gateway Multicast Domains ----

func TestTGWMulticast_CreateDomainValidation(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	tests := []struct {
		name    string
		tgwID   string
		wantErr bool
	}{
		{name: "empty transit gateway id", tgwID: "", wantErr: true},
		{name: "unknown transit gateway id", tgwID: "tgw-nonexistent", wantErr: true},
		{name: "valid transit gateway id", tgwID: tgw.ID, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			domain, createErr := bk.CreateTransitGatewayMulticastDomain(tt.tgwID, "", "", "", nil)
			if tt.wantErr {
				require.Error(t, createErr)
				assert.Nil(t, domain)

				return
			}

			require.NoError(t, createErr)
			require.NotNil(t, domain)
			assert.Contains(t, domain.ID, "tgw-mcast-domain-")
			assert.Equal(t, tt.tgwID, domain.TransitGatewayID)
			assert.Equal(t, "available", domain.State)
			// Defaults applied when options are omitted.
			assert.Equal(t, "disable", domain.AutoAcceptSharedAssociations)
			assert.Equal(t, "disable", domain.Igmpv2Support)
			assert.Equal(t, "disable", domain.StaticSourcesSupport)
		})
	}
}

func TestTGWMulticast_CreateDomainWithOptions(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	domain, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "enable", "enable", "enable", nil)
	require.NoError(t, err)
	assert.Equal(t, "enable", domain.AutoAcceptSharedAssociations)
	assert.Equal(t, "enable", domain.Igmpv2Support)
	assert.Equal(t, "enable", domain.StaticSourcesSupport)
}

func TestTGWMulticast_DescribeAndDeleteDomain(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	d1, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	d2, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	// Describe all.
	all := bk.DescribeTransitGatewayMulticastDomains(nil)
	assert.Len(t, all, 2)

	// Describe filtered.
	filtered := bk.DescribeTransitGatewayMulticastDomains([]string{d1.ID})
	require.Len(t, filtered, 1)
	assert.Equal(t, d1.ID, filtered[0].ID)

	// Delete.
	require.NoError(t, bk.DeleteTransitGatewayMulticastDomain(d1.ID))
	remaining := bk.DescribeTransitGatewayMulticastDomains(nil)
	require.Len(t, remaining, 1)
	assert.Equal(t, d2.ID, remaining[0].ID)

	// Delete not found.
	require.Error(t, bk.DeleteTransitGatewayMulticastDomain(d1.ID))

	// Delete empty ID.
	require.Error(t, bk.DeleteTransitGatewayMulticastDomain(""))
}

func TestTGWMulticast_DeleteDomainCascadesAssociationsAndGroups(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	domain, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	_, err = bk.AssociateTransitGatewayMulticastDomain(domain.ID, "tgw-attach-1", []string{"subnet-1"})
	require.NoError(t, err)

	_, err = bk.RegisterTransitGatewayMulticastGroupMembers(domain.ID, "224.0.0.1", []string{"eni-1"})
	require.NoError(t, err)

	require.NoError(t, bk.DeleteTransitGatewayMulticastDomain(domain.ID))

	assert.Empty(t, bk.GetTransitGatewayMulticastDomainAssociations(domain.ID))
	assert.Empty(t, bk.SearchTransitGatewayMulticastGroups(domain.ID))
}

// ---- Transit Gateway Multicast Domain associations ----

func TestTGWMulticast_AssociateDisassociateGetAssociations(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	domain, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	assocs, err := bk.AssociateTransitGatewayMulticastDomain(
		domain.ID, "tgw-attach-1", []string{"subnet-1", "subnet-2"},
	)
	require.NoError(t, err)
	require.Len(t, assocs, 2)

	for _, a := range assocs {
		assert.Equal(t, "associated", a.State)
		assert.Equal(t, domain.ID, a.TransitGatewayMulticastDomainID)
		assert.Equal(t, "tgw-attach-1", a.TransitGatewayAttachmentID)
	}

	got := bk.GetTransitGatewayMulticastDomainAssociations(domain.ID)
	require.Len(t, got, 2)
	assert.Equal(t, "subnet-1", got[0].SubnetID)
	assert.Equal(t, "subnet-2", got[1].SubnetID)

	// Disassociate one subnet.
	disassocs, err := bk.DisassociateTransitGatewayMulticastDomain(
		domain.ID, "tgw-attach-1", []string{"subnet-1"},
	)
	require.NoError(t, err)
	require.Len(t, disassocs, 1)
	assert.Equal(t, "disassociated", disassocs[0].State)

	remaining := bk.GetTransitGatewayMulticastDomainAssociations(domain.ID)
	require.Len(t, remaining, 1)
	assert.Equal(t, "subnet-2", remaining[0].SubnetID)

	// Disassociate an association that never existed: no error, still returns
	// a disassociated record reflecting the request.
	unknown, err := bk.DisassociateTransitGatewayMulticastDomain(
		domain.ID, "tgw-attach-9", []string{"subnet-9"},
	)
	require.NoError(t, err)
	require.Len(t, unknown, 1)
	assert.Equal(t, "disassociated", unknown[0].State)
}

func TestTGWMulticast_AssociateValidation(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	domain, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	tests := []struct {
		name         string
		domainID     string
		attachmentID string
		subnetIDs    []string
	}{
		{name: "empty domain id", domainID: "", attachmentID: "tgw-attach-1", subnetIDs: []string{"subnet-1"}},
		{name: "empty attachment id", domainID: domain.ID, attachmentID: "", subnetIDs: []string{"subnet-1"}},
		{name: "empty subnet ids", domainID: domain.ID, attachmentID: "tgw-attach-1", subnetIDs: nil},
		{name: "unknown domain id", domainID: "tgw-mcast-domain-nope", attachmentID: "a", subnetIDs: []string{"s"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, assocErr := bk.AssociateTransitGatewayMulticastDomain(tt.domainID, tt.attachmentID, tt.subnetIDs)
			require.Error(t, assocErr)
		})
	}
}

// ---- Transit Gateway Multicast Group members / sources ----

func TestTGWMulticast_RegisterDeregisterMembersAndSources(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	domain, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	memberResp, err := bk.RegisterTransitGatewayMulticastGroupMembers(
		domain.ID, "224.0.0.1", []string{"eni-1", "eni-2"},
	)
	require.NoError(t, err)
	assert.Equal(t, "224.0.0.1", memberResp.GroupIPAddress)
	assert.ElementsMatch(t, []string{"eni-1", "eni-2"}, memberResp.NetworkInterfaceIDs)

	sourceResp, err := bk.RegisterTransitGatewayMulticastGroupSources(
		domain.ID, "224.0.0.1", []string{"eni-1"},
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"eni-1"}, sourceResp.NetworkInterfaceIDs)

	groups := bk.SearchTransitGatewayMulticastGroups(domain.ID)
	require.Len(t, groups, 2)

	var eni1, eni2 *ec2.TransitGatewayMulticastGroupEntry

	for _, g := range groups {
		switch g.NetworkInterfaceID {
		case "eni-1":
			eni1 = g
		case "eni-2":
			eni2 = g
		}
	}

	require.NotNil(t, eni1)
	require.NotNil(t, eni2)
	assert.True(t, eni1.IsMember)
	assert.True(t, eni1.IsSource)
	assert.True(t, eni2.IsMember)
	assert.False(t, eni2.IsSource)

	// Deregister eni-1 as a source: it remains registered as a member.
	deregSource, err := bk.DeregisterTransitGatewayMulticastGroupSources(
		domain.ID, "224.0.0.1", []string{"eni-1"},
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"eni-1"}, deregSource.NetworkInterfaceIDs)

	groupsAfter := bk.SearchTransitGatewayMulticastGroups(domain.ID)
	require.Len(t, groupsAfter, 2)

	// Deregister eni-1 and eni-2 as members: entries with no remaining
	// membership/source flags are removed entirely.
	_, err = bk.DeregisterTransitGatewayMulticastGroupMembers(
		domain.ID, "224.0.0.1", []string{"eni-1", "eni-2"},
	)
	require.NoError(t, err)
	assert.Empty(t, bk.SearchTransitGatewayMulticastGroups(domain.ID))

	// Deregistering an unknown ENI is a no-op, not an error.
	deregUnknown, err := bk.DeregisterTransitGatewayMulticastGroupMembers(
		domain.ID, "224.0.0.1", []string{"eni-unknown"},
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"eni-unknown"}, deregUnknown.NetworkInterfaceIDs)
}

func TestTGWMulticast_RegisterValidation(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	domain, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	tests := []struct {
		name     string
		domainID string
		eniIDs   []string
	}{
		{name: "empty domain id", domainID: "", eniIDs: []string{"eni-1"}},
		{name: "empty eni ids", domainID: domain.ID, eniIDs: nil},
		{name: "unknown domain id", domainID: "tgw-mcast-domain-nope", eniIDs: []string{"eni-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, registerErr := bk.RegisterTransitGatewayMulticastGroupMembers(tt.domainID, "224.0.0.1", tt.eniIDs)
			require.Error(t, registerErr)
		})
	}
}

// ---- Transit Gateway Metering Policies ----

func TestTGWMulticast_MeteringPolicyCRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	policy, err := bk.CreateTransitGatewayMeteringPolicy(tgw.ID, []string{"tgw-attach-1"}, nil)
	require.NoError(t, err)
	assert.Contains(t, policy.ID, "tgw-metering-policy-")
	assert.Equal(t, "available", policy.State)
	assert.Equal(t, []string{"tgw-attach-1"}, policy.MiddleboxAttachmentIDs)

	all := bk.DescribeTransitGatewayMeteringPolicies(nil)
	require.Len(t, all, 1)

	filtered := bk.DescribeTransitGatewayMeteringPolicies([]string{policy.ID})
	require.Len(t, filtered, 1)

	// Mutating the returned slice must not corrupt backend state (deep copy check).
	filtered[0].MiddleboxAttachmentIDs[0] = "mutated"
	again := bk.DescribeTransitGatewayMeteringPolicies([]string{policy.ID})
	require.Len(t, again, 1)
	assert.Equal(t, "tgw-attach-1", again[0].MiddleboxAttachmentIDs[0])

	require.NoError(t, bk.DeleteTransitGatewayMeteringPolicy(policy.ID))
	assert.Empty(t, bk.DescribeTransitGatewayMeteringPolicies(nil))

	require.Error(t, bk.DeleteTransitGatewayMeteringPolicy(policy.ID))
	require.Error(t, bk.DeleteTransitGatewayMeteringPolicy(""))
}

func TestTGWMulticast_CreatePolicyValidation(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tests := []struct {
		name  string
		tgwID string
	}{
		{name: "empty transit gateway id", tgwID: ""},
		{name: "unknown transit gateway id", tgwID: "tgw-nonexistent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := bk.CreateTransitGatewayMeteringPolicy(tt.tgwID, nil, nil)
			require.Error(t, err)
		})
	}
}

// ---- Transit Gateway Metering Policy entries ----

func TestTGWMulticast_MeteringPolicyEntryCRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	policy, err := bk.CreateTransitGatewayMeteringPolicy(tgw.ID, nil, nil)
	require.NoError(t, err)

	entry, err := bk.CreateTransitGatewayMeteringPolicyEntry(policy.ID, &ec2.TransitGatewayMeteringPolicyEntry{
		PolicyRuleNumber:     1,
		MeteredAccount:       "source-attachment-owner",
		DestinationCidrBlock: "10.0.0.0/8",
		SourceCidrBlock:      "192.168.0.0/16",
		Protocol:             "6",
	})
	require.NoError(t, err)
	assert.Equal(t, policy.ID, entry.TransitGatewayMeteringPolicyID)
	assert.Equal(t, "available", entry.State)
	assert.Equal(t, 1, entry.PolicyRuleNumber)
	assert.Equal(t, "10.0.0.0/8", entry.DestinationCidrBlock)

	// Create entry: policy not found.
	_, err = bk.CreateTransitGatewayMeteringPolicyEntry("nonexistent", &ec2.TransitGatewayMeteringPolicyEntry{
		PolicyRuleNumber: 1,
	})
	require.Error(t, err)

	// Create entry: missing rule number.
	_, err = bk.CreateTransitGatewayMeteringPolicyEntry(policy.ID, &ec2.TransitGatewayMeteringPolicyEntry{})
	require.Error(t, err)

	// Create entry: empty policy id.
	_, err = bk.CreateTransitGatewayMeteringPolicyEntry("", &ec2.TransitGatewayMeteringPolicyEntry{
		PolicyRuleNumber: 1,
	})
	require.Error(t, err)

	// Delete entry.
	deleted, err := bk.DeleteTransitGatewayMeteringPolicyEntry(policy.ID, 1)
	require.NoError(t, err)
	assert.Equal(t, "deleted", deleted.State)

	// Delete entry: not found the second time.
	_, err = bk.DeleteTransitGatewayMeteringPolicyEntry(policy.ID, 1)
	require.Error(t, err)

	// Delete entry: empty policy id.
	_, err = bk.DeleteTransitGatewayMeteringPolicyEntry("", 1)
	require.Error(t, err)
}

func TestTGWMulticast_DeletePolicyCascadesEntries(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	policy, err := bk.CreateTransitGatewayMeteringPolicy(tgw.ID, nil, nil)
	require.NoError(t, err)

	_, err = bk.CreateTransitGatewayMeteringPolicyEntry(policy.ID, &ec2.TransitGatewayMeteringPolicyEntry{
		PolicyRuleNumber: 1,
	})
	require.NoError(t, err)

	require.NoError(t, bk.DeleteTransitGatewayMeteringPolicy(policy.ID))

	// The entry is gone along with the policy: a fresh policy re-using rule
	// number 1 should not collide with stale state.
	policy2, err := bk.CreateTransitGatewayMeteringPolicy(tgw.ID, nil, nil)
	require.NoError(t, err)

	_, err = bk.DeleteTransitGatewayMeteringPolicyEntry(policy2.ID, 1)
	require.Error(t, err, "entry from the deleted policy must not leak into a new policy")
}

// ---- Persistence ----

func TestTGWMulticast_SnapshotRestore(t *testing.T) {
	t.Parallel()

	original := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	tgw, err := original.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	domain, err := original.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	_, err = original.AssociateTransitGatewayMulticastDomain(domain.ID, "tgw-attach-1", []string{"subnet-1"})
	require.NoError(t, err)

	_, err = original.RegisterTransitGatewayMulticastGroupMembers(domain.ID, "224.0.0.1", []string{"eni-1"})
	require.NoError(t, err)

	policy, err := original.CreateTransitGatewayMeteringPolicy(tgw.ID, []string{"tgw-attach-1"}, nil)
	require.NoError(t, err)

	_, err = original.CreateTransitGatewayMeteringPolicyEntry(policy.ID, &ec2.TransitGatewayMeteringPolicyEntry{
		PolicyRuleNumber: 1,
		MeteredAccount:   "source-attachment-owner",
	})
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	domains := fresh.DescribeTransitGatewayMulticastDomains(nil)
	require.Len(t, domains, 1)
	assert.Equal(t, domain.ID, domains[0].ID)

	assocs := fresh.GetTransitGatewayMulticastDomainAssociations(domain.ID)
	require.Len(t, assocs, 1)
	assert.Equal(t, "subnet-1", assocs[0].SubnetID)

	groups := fresh.SearchTransitGatewayMulticastGroups(domain.ID)
	require.Len(t, groups, 1)
	assert.Equal(t, "eni-1", groups[0].NetworkInterfaceID)

	policies := fresh.DescribeTransitGatewayMeteringPolicies(nil)
	require.Len(t, policies, 1)
	assert.Equal(t, policy.ID, policies[0].ID)

	deleted, err := fresh.DeleteTransitGatewayMeteringPolicyEntry(policy.ID, 1)
	require.NoError(t, err, "metering policy entry must have survived the snapshot round trip")
	assert.Equal(t, "source-attachment-owner", deleted.MeteredAccount)
}
