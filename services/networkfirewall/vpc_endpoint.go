package networkfirewall

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

// VpcEndpointAssociation is the VpcEndpointAssociation response shape.
type VpcEndpointAssociation struct {
	SubnetMapping             *SubnetMapping `json:"SubnetMapping,omitempty"`
	FirewallArn               string         `json:"FirewallArn"`
	VpcEndpointAssociationArn string         `json:"VpcEndpointAssociationArn"`
	VpcEndpointAssociationID  string         `json:"VpcEndpointAssociationId,omitempty"`
	VpcID                     string         `json:"VpcId"`
	Description               string         `json:"Description,omitempty"`
	Tags                      []Tag          `json:"Tags,omitempty"`
}

// AZSyncState is the endpoint attachment in one Availability Zone.
type AZSyncState struct {
	Attachment *Attachment `json:"Attachment,omitempty"`
}

// VpcEndpointAssociationStatus is the VpcEndpointAssociationStatus response shape.
type VpcEndpointAssociationStatus struct {
	AssociationSyncState map[string]AZSyncState `json:"AssociationSyncState,omitempty"`
	Status               string                 `json:"Status"`
}

// VpcEndpointAssociationOutput is the Create/Describe/DeleteVpcEndpointAssociation response.
type VpcEndpointAssociationOutput struct {
	VpcEndpointAssociationStatus VpcEndpointAssociationStatus `json:"VpcEndpointAssociationStatus"`
	VpcEndpointAssociation       VpcEndpointAssociation       `json:"VpcEndpointAssociation"`
}

// CreateVpcEndpointAssociationInput is the CreateVpcEndpointAssociation request.
type CreateVpcEndpointAssociationInput struct {
	SubnetMapping *SubnetMapping `json:"SubnetMapping"`
	FirewallArn   string         `json:"FirewallArn"`
	VpcID         string         `json:"VpcId"`
	Description   string         `json:"Description"`
	Tags          []Tag          `json:"Tags"`
}

// DescribeVpcEndpointAssociationInput is the DescribeVpcEndpointAssociation request.
type DescribeVpcEndpointAssociationInput struct {
	VpcEndpointAssociationArn string `json:"VpcEndpointAssociationArn"`
}

// DeleteVpcEndpointAssociationInput is the DeleteVpcEndpointAssociation request.
type DeleteVpcEndpointAssociationInput = DescribeVpcEndpointAssociationInput

// ListVpcEndpointAssociationsInput is the ListVpcEndpointAssociations request.
type ListVpcEndpointAssociationsInput struct {
	MaxResults  *int32 `json:"MaxResults"`
	FirewallArn string `json:"FirewallArn"`
	NextToken   string `json:"NextToken"`
}

// VpcEndpointAssociationMetadata is a ListVpcEndpointAssociations entry.
type VpcEndpointAssociationMetadata struct {
	VpcEndpointAssociationArn string `json:"VpcEndpointAssociationArn"`
}

// ListVpcEndpointAssociationsOutput is the ListVpcEndpointAssociations response.
type ListVpcEndpointAssociationsOutput struct {
	NextToken               string                           `json:"NextToken,omitempty"`
	VpcEndpointAssociations []VpcEndpointAssociationMetadata `json:"VpcEndpointAssociations"`
}

func (r *vpcEndpointRecord) expired(now time.Time) bool {
	return !r.DeleteUntil.IsZero() && !now.Before(r.DeleteUntil)
}

// orphaned reports whether the owning firewall is gone; sweeps only run on writes.
func (b *InMemoryBackend) orphaned(r *vpcEndpointRecord, now time.Time) bool {
	fw, ok := b.firewalls.Get(r.FirewallARN)

	return !ok || fw.expired(now)
}

func (r *vpcEndpointRecord) statusAt(now time.Time) (string, string) {
	switch {
	case !r.DeleteUntil.IsZero():
		return statusDeleting, statusDeleting
	case now.Before(r.ProvisionUntil):
		return statusProvisioning, attachmentCreating
	default:
		return statusReady, attachmentReady
	}
}

func (r *vpcEndpointRecord) output(now time.Time) *VpcEndpointAssociationOutput {
	status, attachment := r.statusAt(now)
	subnet := r.Subnet

	return &VpcEndpointAssociationOutput{
		VpcEndpointAssociation: VpcEndpointAssociation{
			SubnetMapping:             &subnet,
			FirewallArn:               r.FirewallARN,
			VpcEndpointAssociationArn: r.ARN,
			VpcEndpointAssociationID:  r.ID,
			VpcID:                     r.VpcID,
			Description:               r.Description,
			Tags:                      tagList(r.Tags),
		},
		VpcEndpointAssociationStatus: VpcEndpointAssociationStatus{
			Status: status,
			AssociationSyncState: map[string]AZSyncState{
				r.Zone: {
					Attachment: &Attachment{EndpointID: r.EndpointID, Status: attachment, SubnetID: r.Subnet.SubnetID},
				},
			},
		},
	}
}

func (b *InMemoryBackend) findVpcEndpointLocked(arnStr string, now time.Time) (*vpcEndpointRecord, error) {
	if arnStr == "" {
		return nil, invalid("VpcEndpointAssociationArn is required")
	}

	r, err := getOrNotFound(b.vpcEndpoints, "VPC endpoint association", arnStr)
	if err != nil {
		return nil, err
	}

	if r.expired(now) || b.orphaned(r, now) {
		return nil, notFound("VPC endpoint association", arnStr)
	}

	return r, nil
}

func (b *InMemoryBackend) endpointsOfLocked(firewallARN string, now time.Time) []*vpcEndpointRecord {
	return slices.DeleteFunc(b.vpcEndpoints.All(), func(r *vpcEndpointRecord) bool {
		return r.FirewallARN != firewallARN || r.expired(now)
	})
}

// removeFirewallLocked drops a firewall and everything scoped to it.
func (b *InMemoryBackend) removeFirewallLocked(firewallARN string) {
	b.firewalls.Delete(firewallARN)

	for _, r := range b.vpcEndpoints.All() {
		if r.FirewallARN == firewallARN {
			b.vpcEndpoints.Delete(r.ARN)
		}
	}

	for _, r := range b.flowOps.All() {
		if r.FirewallARN == firewallARN {
			b.flowOps.Delete(r.Key)
		}
	}

	for _, r := range b.analysisReports.All() {
		if r.FirewallARN == firewallARN {
			b.analysisReports.Delete(r.Key)
		}
	}
}

// CreateVpcEndpointAssociation creates an additional firewall endpoint in another VPC.
func (b *InMemoryBackend) CreateVpcEndpointAssociation(
	_ context.Context, in *CreateVpcEndpointAssociationInput,
) (*VpcEndpointAssociationOutput, error) {
	if in.FirewallArn == "" || in.VpcID == "" || in.SubnetMapping == nil {
		return nil, invalid("FirewallArn, VpcId and SubnetMapping are required")
	}

	if err := validateSubnetMappings([]SubnetMapping{*in.SubnetMapping}); err != nil {
		return nil, err
	}

	if err := validateDescription(in.Description); err != nil {
		return nil, err
	}

	if err := validateTags(tagMap(in.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateVpcEndpointAssociation")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	fw, err := b.findFirewallLocked(in.FirewallArn, "", now)
	if err != nil {
		return nil, err
	}

	existing := b.endpointsOfLocked(fw.ARN, now)

	for _, e := range existing {
		if e.Subnet.SubnetID == in.SubnetMapping.SubnetID {
			return nil, invalid("Subnet %s already has an endpoint for this firewall", e.Subnet.SubnetID)
		}
	}

	zone, err := b.zoneForLocked(len(existing)+len(fw.Zones), in.VpcID, in.SubnetMapping.SubnetID)
	if err != nil {
		return nil, err
	}

	id := awsid.New("vpce-assoc-")
	subnet := *in.SubnetMapping

	if subnet.IPAddressType == "" {
		subnet.IPAddressType = ipv4
	}

	r := &vpcEndpointRecord{
		ARN:            b.arnFor(kindVpcEndpoint, id),
		ID:             id,
		FirewallARN:    fw.ARN,
		VpcID:          in.VpcID,
		Subnet:         subnet,
		Zone:           zone,
		EndpointID:     awsid.New("vpce-"),
		Description:    in.Description,
		Tags:           tagMap(in.Tags),
		ProvisionUntil: b.deadline(now),
	}

	b.vpcEndpoints.Put(r)

	return r.output(now), nil
}

// DescribeVpcEndpointAssociation returns an association and its status.
func (b *InMemoryBackend) DescribeVpcEndpointAssociation(
	_ context.Context, in *DescribeVpcEndpointAssociationInput,
) (*VpcEndpointAssociationOutput, error) {
	b.mu.RLock("DescribeVpcEndpointAssociation")
	defer b.mu.RUnlock()

	now := time.Now()

	r, err := b.findVpcEndpointLocked(in.VpcEndpointAssociationArn, now)
	if err != nil {
		return nil, err
	}

	return r.output(now), nil
}

// ListVpcEndpointAssociations lists associations, optionally for one firewall.
func (b *InMemoryBackend) ListVpcEndpointAssociations(
	_ context.Context, in *ListVpcEndpointAssociationsInput,
) (*ListVpcEndpointAssociationsOutput, error) {
	b.mu.RLock("ListVpcEndpointAssociations")
	defer b.mu.RUnlock()

	now := time.Now()
	all := make([]VpcEndpointAssociationMetadata, 0, b.vpcEndpoints.Len())

	for _, r := range b.vpcEndpoints.All() {
		if r.expired(now) || b.orphaned(r, now) || (in.FirewallArn != "" && r.FirewallARN != in.FirewallArn) {
			continue
		}

		all = append(all, VpcEndpointAssociationMetadata{VpcEndpointAssociationArn: r.ARN})
	}

	slices.SortFunc(all, func(a, c VpcEndpointAssociationMetadata) int {
		return strings.Compare(a.VpcEndpointAssociationArn, c.VpcEndpointAssociationArn)
	})

	pg, err := paginate(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	if pg.Data == nil {
		pg.Data = []VpcEndpointAssociationMetadata{}
	}

	return &ListVpcEndpointAssociationsOutput{VpcEndpointAssociations: pg.Data, NextToken: pg.Next}, nil
}

// DeleteVpcEndpointAssociation starts deletion; the association disappears once DELETING passes.
func (b *InMemoryBackend) DeleteVpcEndpointAssociation(
	_ context.Context, in *DeleteVpcEndpointAssociationInput,
) (*VpcEndpointAssociationOutput, error) {
	b.mu.Lock("DeleteVpcEndpointAssociation")
	defer b.mu.Unlock()

	now := time.Now()

	r, err := b.findVpcEndpointLocked(in.VpcEndpointAssociationArn, now)
	if err != nil {
		return nil, err
	}

	if r.DeleteUntil.IsZero() {
		r.DeleteUntil = b.deadline(now)
	}

	out := r.output(now)

	if b.delay <= 0 {
		b.vpcEndpoints.Delete(r.ARN)
	}

	return out, nil
}
