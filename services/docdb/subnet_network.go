package docdb

import (
	"fmt"
	"slices"
)

// SubnetResolver looks up the EC2 subnets behind a DB subnet group.
type SubnetResolver interface {
	// SubnetNetwork returns the subnet's VPC and whether it has both IPv4 and IPv6 CIDRs; found is false for an
	// unknown subnet.
	SubnetNetwork(id string) (vpcID string, dual, found bool)
}

// SetSubnetResolver wires the EC2 lookup used for subnet-group VpcId and SupportedNetworkTypes.
func (b *InMemoryBackend) SetSubnetResolver(r SubnetResolver) {
	b.mu.Lock("SetSubnetResolver")
	defer b.mu.Unlock()

	b.subnets = r
}

// subnetNetwork is the EC2-derived network view of a subnet group.
type subnetNetwork struct {
	vpcID string
	types []string
	// known is false when no resolver is wired or any subnet is not an EC2 subnet.
	known bool
}

func (b *InMemoryBackend) subnetGroupNetwork(subnetIDs []string) subnetNetwork {
	if b.subnets == nil || len(subnetIDs) == 0 {
		return subnetNetwork{}
	}

	out := subnetNetwork{known: true}
	allDual := true

	for _, id := range subnetIDs {
		v, dual, found := b.subnets.SubnetNetwork(id)
		if !found {
			out.known, allDual = false, false

			continue
		}

		if out.vpcID == "" {
			out.vpcID = v
		}

		allDual = allDual && dual
	}

	out.types = []string{networkTypeIPv4}
	if allDual {
		out.types = append(out.types, networkTypeDual)
	}

	return out
}

// decorateSubnetGroup fills the EC2-derived VpcID and SupportedNetworkTypes on a subnet group copy.
func (b *InMemoryBackend) decorateSubnetGroup(sg *DBSubnetGroup) {
	n := b.subnetGroupNetwork(sg.SubnetIDs)
	if n.vpcID != "" {
		sg.VpcID = n.vpcID
	}

	sg.SupportedNetworkTypes = n.types
}

// checkNetworkType returns NetworkTypeNotSupported when networkType is DUAL and the named subnet group is known
// not to support it.
func (b *InMemoryBackend) checkNetworkType(region, networkType, subnetGroupName string) error {
	if networkType != networkTypeDual || subnetGroupName == "" {
		return nil
	}

	sg, ok := b.subnetGroupGet(region, subnetGroupName)
	if !ok {
		return nil
	}

	n := b.subnetGroupNetwork(sg.SubnetIDs)
	if !n.known || slices.Contains(n.types, networkTypeDual) {
		return nil
	}

	return fmt.Errorf(
		"%w: subnet group %s does not support NetworkType DUAL", ErrNetworkTypeNotSupported, subnetGroupName,
	)
}

func (b *InMemoryBackend) subnetGroupVpcID(region, subnetGroupName string) string {
	if sg, ok := b.subnetGroupGet(region, subnetGroupName); ok {
		return sg.VpcID
	}

	return ""
}

type xmlNetworkTypes struct {
	Members []string `xml:"member"`
}

func networkTypesXML(types []string) *xmlNetworkTypes {
	if len(types) == 0 {
		return nil
	}

	return &xmlNetworkTypes{Members: types}
}
