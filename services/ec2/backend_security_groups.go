package ec2

import (
	"fmt"
	"sort"
)

// AssociateSecurityGroupVpc extends a security group to an additional VPC.
func (b *InMemoryBackend) AssociateSecurityGroupVpc(
	sgID, vpcID string,
) (*SGVpcAssociationState, error) {
	if sgID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateSecurityGroupVpc")
	defer b.mu.Unlock()

	if _, ok := b.securityGroups.Get(sgID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, sgID)
	}
	if _, ok := b.vpcs.Get(vpcID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	if b.sgVpcAssociations[sgID] == nil {
		b.sgVpcAssociations[sgID] = make(map[string]string)
	}
	b.sgVpcAssociations[sgID][vpcID] = stateAssociated

	return &SGVpcAssociationState{SGID: sgID, VPCID: vpcID, State: stateAssociated}, nil
}

// ---- DisassociateSecurityGroupVpc ----

// DisassociateSecurityGroupVpc removes a security group from a VPC.
func (b *InMemoryBackend) DisassociateSecurityGroupVpc(sgID, vpcID string) error {
	if sgID == "" {
		return fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}
	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateSecurityGroupVpc")
	defer b.mu.Unlock()

	if m, ok := b.sgVpcAssociations[sgID]; ok {
		delete(m, vpcID)

		return nil
	}

	return fmt.Errorf("%w: %s is not associated with %s", ErrInvalidParameter, sgID, vpcID)
}

// ---- DescribeSecurityGroupReferences ----

// SGReference represents a reference to a security group from another VPC.
type SGReference struct {
	GroupID                string `json:"groupID,omitempty"`
	ReferencingVPCID       string `json:"referencingVPCID,omitempty"`
	VpcPeeringConnectionID string `json:"vpcPeeringConnectionID,omitempty"`
}

// DescribeSecurityGroupReferences returns cross-VPC references for the given SG IDs.
// Returns empty for SGs not referenced externally (stub-level accuracy).
func (b *InMemoryBackend) DescribeSecurityGroupReferences(sgIDs []string) []SGReference {
	b.mu.RLock("DescribeSecurityGroupReferences")
	defer b.mu.RUnlock()

	// Return references based on sg-vpc associations as proxy for cross-VPC rules.
	var out []SGReference
	filter := make(map[string]bool, len(sgIDs))
	for _, id := range sgIDs {
		filter[id] = true
	}
	for sgID, vpcMap := range b.sgVpcAssociations {
		if len(filter) > 0 && !filter[sgID] {
			continue
		}
		for vpcID := range vpcMap {
			if sg, ok := b.securityGroups.Get(sgID); ok && sg.VPCID != vpcID {
				out = append(out, SGReference{
					GroupID:          sgID,
					ReferencingVPCID: vpcID,
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })

	return out
}

// ---- DescribeStaleSecurityGroups ----

// StaleSGItem is a stale security group entry.
type StaleSGItem struct {
	GroupID     string `json:"groupID,omitempty"`
	GroupName   string `json:"groupName,omitempty"`
	Description string `json:"description,omitempty"`
	VPCID       string `json:"vpcid,omitempty"`
}

// findDeletedPeerVPCsLocked returns VPC IDs with terminated peering connections to vpcID.
func (b *InMemoryBackend) findDeletedPeerVPCsLocked(vpcID string) map[string]bool {
	result := make(map[string]bool)
	for _, pc := range b.vpcPeeringConnections.All() {
		if pc.State != tgwRouteStateDeleted && pc.State != "rejected" && pc.State != "failed" {
			continue
		}
		if pc.RequesterVpcID == vpcID {
			result[pc.AccepterVpcID] = true
		} else if pc.AccepterVpcID == vpcID {
			result[pc.RequesterVpcID] = true
		}
	}

	return result
}

// hasStaleRuleLocked returns true if sg has any rule referencing a group in a deleted-peer VPC.
func (b *InMemoryBackend) hasStaleRuleLocked(
	sg *SecurityGroup,
	deletedPeerVPCs map[string]bool,
) bool {
	for _, rule := range append(sg.IngressRules, sg.EgressRules...) {
		if rule.SourceGroupID == "" {
			continue
		}
		srcSG, ok := b.securityGroups.Get(rule.SourceGroupID)
		if ok && deletedPeerVPCs[srcSG.VPCID] {
			return true
		}
	}

	return false
}

// DescribeStaleSecurityGroups returns security groups in stale peering state for the VPC.
// In this implementation, stale SGs are those with dangling VPC peering references.
func (b *InMemoryBackend) DescribeStaleSecurityGroups(vpcID string) []StaleSGItem {
	b.mu.RLock("DescribeStaleSecurityGroups")
	defer b.mu.RUnlock()

	deletedPeerVPCs := b.findDeletedPeerVPCsLocked(vpcID)

	var out []StaleSGItem
	for _, sg := range b.securityGroups.All() {
		if sg.VPCID != vpcID || !b.hasStaleRuleLocked(sg, deletedPeerVPCs) {
			continue
		}
		out = append(out, StaleSGItem{
			GroupID:     sg.ID,
			GroupName:   sg.Name,
			Description: sg.Description,
			VPCID:       sg.VPCID,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })

	return out
}

// ---- DescribeSecurityGroupVpcAssociations ----

// SGVpcAssocItem is an entry returned by DescribeSecurityGroupVpcAssociations.
type SGVpcAssocItem struct {
	SGID  string `json:"sgid,omitempty"`
	VPCID string `json:"vpcid,omitempty"`
	State string `json:"state,omitempty"`
}

// DescribeSecurityGroupVpcAssociations returns SG-VPC associations for the given SG IDs.
func (b *InMemoryBackend) DescribeSecurityGroupVpcAssociations(sgIDs []string) []SGVpcAssocItem {
	b.mu.RLock("DescribeSecurityGroupVpcAssociations")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(sgIDs))
	for _, id := range sgIDs {
		filter[id] = true
	}

	var out []SGVpcAssocItem
	for sgID, vpcMap := range b.sgVpcAssociations {
		if len(filter) > 0 && !filter[sgID] {
			continue
		}
		for vpcID, state := range vpcMap {
			out = append(out, SGVpcAssocItem{SGID: sgID, VPCID: vpcID, State: state})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SGID != out[j].SGID {
			return out[i].SGID < out[j].SGID
		}

		return out[i].VPCID < out[j].VPCID
	})

	return out
}

// ---- ModifyVpcTenancy ----

// GetSecurityGroupsForVpc returns security groups associated with a VPC.
func (b *InMemoryBackend) GetSecurityGroupsForVpc(vpcID string) ([]SecurityGroupForVpcItem, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetSecurityGroupsForVpc")
	defer b.mu.RUnlock()

	var out []SecurityGroupForVpcItem
	for _, sg := range b.securityGroups.All() {
		if sg.VPCID == vpcID {
			out = append(out, SecurityGroupForVpcItem{
				GroupID:     sg.ID,
				GroupName:   sg.Name,
				Description: sg.Description,
				VPCID:       sg.VPCID,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })

	return out, nil
}

// ---- ReplaceRoute ----

// UpdateSecurityGroupRuleDescriptionsIngress updates descriptions of ingress rules.
func (b *InMemoryBackend) UpdateSecurityGroupRuleDescriptionsIngress(
	groupID string,
	updates []SecurityGroupRule,
) error {
	if groupID == "" {
		return fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateSecurityGroupRuleDescriptionsIngress")
	defer b.mu.Unlock()

	sg, ok := b.securityGroups.Get(groupID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	applyRuleDescriptions(sg.IngressRules, updates)

	return nil
}

// UpdateSecurityGroupRuleDescriptionsEgress updates descriptions of egress rules.
func (b *InMemoryBackend) UpdateSecurityGroupRuleDescriptionsEgress(
	groupID string,
	updates []SecurityGroupRule,
) error {
	if groupID == "" {
		return fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateSecurityGroupRuleDescriptionsEgress")
	defer b.mu.Unlock()

	sg, ok := b.securityGroups.Get(groupID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	applyRuleDescriptions(sg.EgressRules, updates)

	return nil
}

// applyRuleDescriptions sets Description on each stored rule whose identity
// (protocol/ports/CIDR/source-group — see ruleKey) matches an incoming
// update, ignoring the incoming rule's own Description for matching purposes.
func applyRuleDescriptions(stored []SecurityGroupRule, updates []SecurityGroupRule) {
	for i := range stored {
		key := ruleKey(stored[i])

		for _, u := range updates {
			if ruleKey(u) == key {
				stored[i].Description = u.Description
			}
		}
	}
}

// ---- Volume recycle bin ----

// DescribeSecurityGroupRules returns all ingress and egress rules for the given group.
func (b *InMemoryBackend) DescribeSecurityGroupRules(
	groupID string,
) ([]*SecurityGroupRuleDetail, error) {
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeSecurityGroupRules")
	defer b.mu.RUnlock()

	sg, ok := b.securityGroups.Get(groupID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	var out []*SecurityGroupRuleDetail

	for i, r := range sg.IngressRules {
		out = append(out, &SecurityGroupRuleDetail{
			SecurityGroupRuleID: fmt.Sprintf("sgr-%s-in-%d", groupID, i),
			GroupID:             groupID,
			Protocol:            r.Protocol,
			CIDRIPv4:            r.IPRange,
			Description:         r.Description,
			FromPort:            r.FromPort,
			ToPort:              r.ToPort,
			IsEgress:            false,
		})
	}

	for i, r := range sg.EgressRules {
		out = append(out, &SecurityGroupRuleDetail{
			SecurityGroupRuleID: fmt.Sprintf("sgr-%s-out-%d", groupID, i),
			GroupID:             groupID,
			Protocol:            r.Protocol,
			CIDRIPv4:            r.IPRange,
			Description:         r.Description,
			FromPort:            r.FromPort,
			ToPort:              r.ToPort,
			IsEgress:            true,
		})
	}

	return out, nil
}

// ModifySecurityGroupRules updates one or more rules (by position index) within a security group.
// Only protocol, IPRange, and port range can be mutated; egress/ingress direction is immutable.
func (b *InMemoryBackend) ModifySecurityGroupRules(
	groupID string,
	updates []SecurityGroupRule,
	egress bool,
) error {
	if groupID == "" {
		return fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifySecurityGroupRules")
	defer b.mu.Unlock()

	sg, ok := b.securityGroups.Get(groupID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSecurityGroupNotFound, groupID)
	}

	if egress {
		sg.EgressRules = updates
	} else {
		sg.IngressRules = updates
	}

	return nil
}

// ---- Launch template delete + versions ----
