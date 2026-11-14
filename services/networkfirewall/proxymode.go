package networkfirewall

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// maxReferencedCIDRs is the documented limit of CIDRs across all IP set references of one firewall.
const maxReferencedCIDRs = 1_000_000

// NetworkResolver looks up the EC2 resources a firewall refers to: NAT gateways and managed prefix lists.
type NetworkResolver interface {
	NatGatewayNetwork(id string) (vpcID, zone string, ok bool)
	PrefixListCIDRCount(arn string) (count int, ok bool)
}

func (b *InMemoryBackend) netResolver() NetworkResolver {
	b.mu.RLock("netResolver")
	defer b.mu.RUnlock()

	return b.nets
}

// SetNetworkResolver wires EC2 NAT gateway and prefix list lookups; nil keeps standalone behaviour.
func (b *InMemoryBackend) SetNetworkResolver(r NetworkResolver) {
	b.mu.Lock("SetNetworkResolver")
	defer b.mu.Unlock()

	b.nets = r
}

// SetNetworkResolver wires EC2 lookups into the home backend and all current and future siblings.
func (h *Handler) SetNetworkResolver(r NetworkResolver) {
	h.Backend.SetNetworkResolver(r)

	for _, p := range h.peers.All() {
		p.Backend.SetNetworkResolver(r)
	}
}

func validateProxyFirewall(in *CreateFirewallInput) error {
	if !in.NoSourcePreservation {
		if len(in.NatGatewayMappings) > 0 || in.VpcEndpoint != nil {
			return invalid("NatGatewayMappings and VpcEndpoint require NoSourcePreservation")
		}

		return nil
	}

	if in.TransitGatewayID != "" || in.VpcID != "" || len(in.SubnetMappings) > 0 ||
		len(in.AvailabilityZoneMappings) > 0 {
		return invalid("A NoSourcePreservation firewall takes NatGatewayMappings and VpcEndpoint, not VpcId, " +
			"SubnetMappings, AvailabilityZoneMappings or TransitGatewayId")
	}

	if in.VpcEndpoint == nil || in.VpcEndpoint.VpcID == "" || len(in.VpcEndpoint.SubnetMappings) == 0 {
		return invalid("VpcEndpoint with a VpcId and at least one SubnetMapping is required")
	}

	if len(in.NatGatewayMappings) == 0 {
		return invalid("NatGatewayMappings is required")
	}

	seen := make(map[string]bool, len(in.NatGatewayMappings))

	for _, m := range in.NatGatewayMappings {
		if m.NatGatewayID == "" {
			return invalid("NatGatewayMapping.NatGatewayId is required")
		}

		if seen[m.NatGatewayID] {
			return invalid("NAT gateway %s is specified more than once", m.NatGatewayID)
		}

		seen[m.NatGatewayID] = true
	}

	return validateSubnetMappings(in.VpcEndpoint.SubnetMappings)
}

// proxyPlacement returns the VPC and subnets a firewall endpoint is created in.
func proxyPlacement(in *CreateFirewallInput) (string, []SubnetMapping) {
	if in.NoSourcePreservation {
		return in.VpcEndpoint.VpcID, in.VpcEndpoint.SubnetMappings
	}

	return in.VpcID, in.SubnetMappings
}

// attachNatGatewaysLocked records the zone of each NAT gateway; the gateways must exist in EC2 when it is wired.
func (b *InMemoryBackend) attachNatGatewaysLocked(r *firewallRecord, in *CreateFirewallInput) error {
	r.NoSourcePreservation = true
	r.NatZones = make(map[string]string, len(in.NatGatewayMappings))

	for _, m := range in.NatGatewayMappings {
		zone := ""

		if b.nets != nil {
			vpcID, z, ok := b.nets.NatGatewayNetwork(m.NatGatewayID)
			if !ok {
				return invalid("NAT gateway %s does not exist", m.NatGatewayID)
			}

			if vpcID != r.VpcID {
				return invalid("NAT gateway %s does not belong to VPC %s", m.NatGatewayID, r.VpcID)
			}

			zone = z
		}

		r.NatGateways = append(r.NatGateways, m.NatGatewayID)
		r.NatZones[m.NatGatewayID] = zone
	}

	return nil
}

func (r *firewallRecord) natGatewayMappings() []NatGatewayMapping {
	out := make([]NatGatewayMapping, len(r.NatGateways))
	for i, id := range r.NatGateways {
		out[i] = NatGatewayMapping{NatGatewayID: id}
	}

	return out
}

// applyProxyView adds the proxy mode members to a firewall view and its NAT gateway attachments to the sync states.
func (r *firewallRecord) applyProxyView(fw *Firewall, states map[string]SyncState, attachment string) {
	if !r.NoSourcePreservation {
		return
	}

	fw.NoSourcePreservation = true
	fw.VpcID = ""
	fw.NatGatewayMappings = r.natGatewayMappings()
	fw.VpcEndpoint = &VpcEndpoint{VpcID: r.VpcID, SubnetMappings: fw.SubnetMappings}
	fw.SubnetMappings = nil

	for _, id := range r.NatGateways {
		zone := r.NatZones[id]
		if zone == "" {
			zone = r.firstZone()
		}

		st := states[zone]
		st.NatGatewayAttachments = append(st.NatGatewayAttachments, NatGatewayAttachment{
			NatGatewayID: id, Status: attachment,
		})
		states[zone] = st
	}
}

func (r *firewallRecord) firstZone() string {
	if len(r.SubnetMappings) > 0 {
		return r.Zones[r.SubnetMappings[0].SubnetID]
	}

	return ""
}

// capacityUsageLocked sums the CIDRs behind the IP set references of the firewall's stateful rule groups.
func (b *InMemoryBackend) capacityUsageLocked(r *firewallRecord) *CapacityUsageSummary {
	refs := map[string]IPSetMetadata{}
	used := int32(0)

	if p, ok := b.policies.Get(r.PolicyARN); ok {
		for _, ref := range p.Policy.StatefulRuleGroupReferences {
			g, found := b.ruleGroups.Get(ref.ResourceArn)
			if !found || g.RuleGroup.ReferenceSets == nil {
				continue
			}

			for _, key := range collections.SortedKeys(g.RuleGroup.ReferenceSets.IPSetReferences) {
				arnStr := g.RuleGroup.ReferenceSets.IPSetReferences[key].ReferenceArn
				if _, seen := refs[arnStr]; seen || arnStr == "" {
					continue
				}

				n := b.resolvedCIDRs(arnStr)
				refs[arnStr] = IPSetMetadata{ResolvedCIDRCount: n}
				used += n
			}
		}
	}

	return &CapacityUsageSummary{CIDRs: &CIDRSummary{
		AvailableCIDRCount: max(maxReferencedCIDRs-used, 0), UtilizedCIDRCount: used, IPSetReferences: refs,
	}}
}

// resolvedCIDRs counts the entries of a referenced managed prefix list; other reference kinds resolve to none.
func (b *InMemoryBackend) resolvedCIDRs(referenceARN string) int32 {
	if b.nets == nil || !strings.Contains(referenceARN, ":prefix-list/") {
		return 0
	}

	n, ok := b.nets.PrefixListCIDRCount(referenceARN)
	if !ok {
		return 0
	}

	return toInt32(min(n, maxReferencedCIDRs))
}
