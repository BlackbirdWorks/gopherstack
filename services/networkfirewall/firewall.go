package networkfirewall

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	zoneLetters = 26
	ipv4        = "IPV4"
)

// CreateFirewallInput is the CreateFirewall request.
type CreateFirewallInput struct {
	EncryptionConfiguration          *EncryptionConfiguration  `json:"EncryptionConfiguration,omitempty"`
	ProxySettings                    *ProxySettings            `json:"ProxySettings,omitempty"`
	VpcEndpoint                      *VpcEndpoint              `json:"VpcEndpoint,omitempty"`
	FirewallName                     string                    `json:"FirewallName"`
	FirewallPolicyArn                string                    `json:"FirewallPolicyArn"`
	VpcID                            string                    `json:"VpcId"`
	Description                      string                    `json:"Description"`
	TransitGatewayID                 string                    `json:"TransitGatewayId"`
	SubnetMappings                   []SubnetMapping           `json:"SubnetMappings"`
	AvailabilityZoneMappings         []AvailabilityZoneMapping `json:"AvailabilityZoneMappings"`
	NatGatewayMappings               []NatGatewayMapping       `json:"NatGatewayMappings"`
	EnabledAnalysisTypes             []string                  `json:"EnabledAnalysisTypes"`
	Tags                             []Tag                     `json:"Tags"`
	DeleteProtection                 bool                      `json:"DeleteProtection"`
	SubnetChangeProtection           bool                      `json:"SubnetChangeProtection"`
	FirewallPolicyChangeProtection   bool                      `json:"FirewallPolicyChangeProtection"`
	AvailabilityZoneChangeProtection bool                      `json:"AvailabilityZoneChangeProtection"`
}

// FirewallOutput is the CreateFirewall and DeleteFirewall response.
type FirewallOutput struct {
	FirewallStatus FirewallStatus `json:"FirewallStatus"`
	Firewall       Firewall       `json:"Firewall"`
}

// DescribeFirewallInput is the DescribeFirewall request.
type DescribeFirewallInput struct {
	FirewallArn  string `json:"FirewallArn"`
	FirewallName string `json:"FirewallName"`
}

// DescribeFirewallOutput is the DescribeFirewall response.
type DescribeFirewallOutput struct {
	FirewallStatus FirewallStatus `json:"FirewallStatus"`
	UpdateToken    string         `json:"UpdateToken"`
	Firewall       Firewall       `json:"Firewall"`
}

// DescribeFirewallMetadataInput is the DescribeFirewallMetadata request.
type DescribeFirewallMetadataInput struct {
	FirewallArn string `json:"FirewallArn"`
}

// DescribeFirewallMetadataOutput is the DescribeFirewallMetadata response.
type DescribeFirewallMetadataOutput struct {
	Description       string `json:"Description,omitempty"`
	FirewallArn       string `json:"FirewallArn"`
	FirewallPolicyArn string `json:"FirewallPolicyArn"`
	Status            string `json:"Status"`

	SupportedAvailabilityZones map[string]AvailabilityZoneMetadata `json:"SupportedAvailabilityZones,omitempty"`
	TransitGatewayAttachmentID string                              `json:"TransitGatewayAttachmentId,omitempty"`
}

// ListFirewallsInput is the ListFirewalls request.
type ListFirewallsInput struct {
	MaxResults *int32   `json:"MaxResults"`
	NextToken  string   `json:"NextToken"`
	VpcIDs     []string `json:"VpcIds"`
}

// ListFirewallsOutput is the ListFirewalls response.
type ListFirewallsOutput struct {
	NextToken string             `json:"NextToken,omitempty"`
	Firewalls []FirewallMetadata `json:"Firewalls"`
}

// DeleteFirewallInput is the DeleteFirewall request.
type DeleteFirewallInput = DescribeFirewallInput

func validateCreateFirewall(in *CreateFirewallInput) error {
	if err := validateName("FirewallName", in.FirewallName); err != nil {
		return err
	}

	if in.FirewallPolicyArn == "" {
		return invalid("FirewallPolicyArn is required")
	}

	if len(in.NatGatewayMappings) > 0 || in.VpcEndpoint != nil {
		return unsupported("NatGatewayMappings and VpcEndpoint are not supported")
	}

	if err := validateProxySettings(in.ProxySettings); err != nil {
		return err
	}

	if err := validateFirewallPlacement(in); err != nil {
		return err
	}

	if err := validateDescription(in.Description); err != nil {
		return err
	}

	if err := validateEncryption(in.EncryptionConfiguration); err != nil {
		return err
	}

	if err := validateAnalysisTypes(in.EnabledAnalysisTypes); err != nil {
		return err
	}

	return validateTags(tagMap(in.Tags))
}

func validateFirewallPlacement(in *CreateFirewallInput) error {
	if in.TransitGatewayID != "" {
		if in.VpcID != "" || len(in.SubnetMappings) > 0 {
			return invalid("VpcId and SubnetMappings cannot be combined with TransitGatewayId")
		}

		return validateZoneMappings(in.AvailabilityZoneMappings)
	}

	if len(in.AvailabilityZoneMappings) > 0 {
		return invalid("AvailabilityZoneMappings requires TransitGatewayId")
	}

	if in.VpcID == "" || len(in.SubnetMappings) == 0 {
		return invalid("VpcId and at least one SubnetMapping are required")
	}

	return validateSubnetMappings(in.SubnetMappings)
}

func validateSubnetMappings(sm []SubnetMapping) error {
	seen := make(map[string]bool, len(sm))

	for _, m := range sm {
		if m.SubnetID == "" {
			return invalid("SubnetMapping.SubnetId is required")
		}

		switch m.IPAddressType {
		case "", ipv4, "IPV6", "DUALSTACK":
		default:
			return invalid("IPAddressType %q is not valid", m.IPAddressType)
		}

		if seen[m.SubnetID] {
			return invalid("Subnet %s is specified more than once", m.SubnetID)
		}

		seen[m.SubnetID] = true
	}

	return nil
}

// resolveSubnetsLocked assigns an endpoint and zone to each new subnet; r is untouched on error.
func (b *InMemoryBackend) resolveSubnetsLocked(r *firewallRecord, vpcID string, add []SubnetMapping) error {
	zones := maps.Clone(r.Zones)
	endpoints := make(map[string]string, len(add))

	for _, m := range add {
		zone, err := b.zoneForLocked(zones, vpcID, m.SubnetID)
		if err != nil {
			return err
		}

		for _, other := range zones {
			if other == zone {
				return invalid("Only one subnet per Availability Zone is allowed; %s is already used", zone)
			}
		}

		zones[m.SubnetID] = zone
		endpoints[m.SubnetID] = awsid.New("vpce-")
	}

	r.Zones = zones
	maps.Copy(r.Endpoints, endpoints)

	return nil
}

func (b *InMemoryBackend) zoneForLocked(used map[string]string, vpcID, subnetID string) (string, error) {
	if b.subnets == nil {
		return b.freeZone(used), nil
	}

	subnetVpc, _, ok := b.subnets.SubnetNetwork(subnetID)
	if !ok {
		return "", invalid("Subnet %s does not exist", subnetID)
	}

	if subnetVpc != vpcID {
		return "", invalid("Subnet %s does not belong to VPC %s", subnetID, vpcID)
	}

	zone, _ := b.subnets.SubnetAZ(subnetID)

	return zone, nil
}

func (b *InMemoryBackend) freeZone(used map[string]string) string {
	taken := make(map[string]bool, len(used))
	for _, z := range used {
		taken[z] = true
	}

	for i := range zoneLetters {
		if z := b.region + string(rune('a'+i)); !taken[z] {
			return z
		}
	}

	return b.region + "a"
}

func (b *InMemoryBackend) configStatusLocked(r *firewallRecord, state string) map[string]PerObjectStatus {
	cfg := map[string]PerObjectStatus{}

	if p, ok := b.policies.Get(r.PolicyARN); ok {
		cfg[p.ARN] = PerObjectStatus{SyncStatus: state, UpdateToken: p.UpdateToken}

		for _, ref := range p.Policy.StatefulRuleGroupReferences {
			if g, found := b.ruleGroups.Get(ref.ResourceArn); found {
				cfg[g.ARN] = PerObjectStatus{SyncStatus: state, UpdateToken: g.UpdateToken}
			}
		}

		for _, ref := range p.Policy.StatelessRuleGroupReferences {
			if g, found := b.ruleGroups.Get(ref.ResourceArn); found {
				cfg[g.ARN] = PerObjectStatus{SyncStatus: state, UpdateToken: g.UpdateToken}
			}
		}
	}

	return cfg
}

func (r *firewallRecord) statusAt(now time.Time) (string, string, string) {
	switch tgw := r.tgwStatusAt(now); {
	case !r.DeleteUntil.IsZero():
		return statusDeleting, syncPending, statusDeleting
	case tgw == tgwPendingAcceptance || tgw == tgwCreating || tgw == tgwRejecting:
		return statusProvisioning, syncPending, attachmentCreating
	case isTGWFailure(tgw):
		return statusFailed, syncPending, attachmentFailed
	case now.Before(r.ProvisionUntil):
		return statusProvisioning, syncPending, attachmentCreating
	case now.Before(r.SyncUntil):
		return statusReady, syncPending, attachmentReady
	default:
		return statusReady, syncInSync, attachmentReady
	}
}

func (r *firewallRecord) expired(now time.Time) bool {
	return !r.DeleteUntil.IsZero() && !now.Before(r.DeleteUntil)
}

func (b *InMemoryBackend) firewallViewLocked(r *firewallRecord, now time.Time) (Firewall, FirewallStatus) {
	status, sync, attachment := r.statusAt(now)

	fw := Firewall{
		FirewallArn:                      r.ARN,
		FirewallID:                       r.ID,
		FirewallName:                     r.Name,
		FirewallPolicyArn:                r.PolicyARN,
		Description:                      r.Description,
		VpcID:                            r.VpcID,
		SubnetMappings:                   slices.Clone(r.SubnetMappings),
		DeleteProtection:                 r.DeleteProtection,
		SubnetChangeProtection:           r.SubnetProtection,
		FirewallPolicyChangeProtection:   r.PolicyProtection,
		EncryptionConfiguration:          normalizeEncryption(r.Encryption),
		AvailabilityZoneChangeProtection: r.ZoneProtection,
		AvailabilityZoneMappings:         zoneMappings(r.AZMappings),
		TransitGatewayID:                 r.TGWID,
		TransitGatewayOwnerAccountID:     r.TGWOwner,
		ProxySettings:                    r.ProxySettings,
		EnabledAnalysisTypes:             cloneStrings(r.AnalysisTypes),
		Tags:                             tagList(r.Tags),
	}

	syncStates := make(map[string]SyncState, len(r.Zones))

	for _, m := range r.SubnetMappings {
		syncStates[r.Zones[m.SubnetID]] = SyncState{
			Attachment: &Attachment{
				EndpointID: r.Endpoints[m.SubnetID],
				Status:     attachment,
				SubnetID:   m.SubnetID,
			},
			Config: b.configStatusLocked(r, sync),
		}
	}

	for _, az := range r.AZMappings {
		syncStates[az] = SyncState{
			Attachment: &Attachment{EndpointID: r.Endpoints[az], Status: attachment},
			Config:     b.configStatusLocked(r, sync),
		}
	}

	st := FirewallStatus{Status: status, ConfigurationSyncStateSummary: sync, SyncStates: syncStates}
	if r.TGWID != "" {
		st.TGWAttachmentSyncState = &TransitGatewayAttachmentSyncState{
			AttachmentID: r.TGWAttachmentID, TransitGatewayAttachmentStatus: r.tgwStatusAt(now),
		}
	}

	return fw, st
}

func (b *InMemoryBackend) sweepLocked(now time.Time) {
	for _, r := range b.firewalls.All() {
		if r.expired(now) {
			b.removeFirewallLocked(r.ARN)
		}
	}

	b.sweepProxiesLocked(now)
	b.sweepContainersLocked(now)

	for _, r := range b.vpcEndpoints.All() {
		if r.expired(now) {
			b.vpcEndpoints.Delete(r.ARN)
		}
	}
}

func (b *InMemoryBackend) deadline(now time.Time) time.Time { return now.Add(b.delay) }

// CreateFirewall creates a firewall in PROVISIONING state.
func (b *InMemoryBackend) CreateFirewall(_ context.Context, in *CreateFirewallInput) (*FirewallOutput, error) {
	if err := validateCreateFirewall(in); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFirewall")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	fwARN := b.arnFor(kindFirewall, in.FirewallName)
	if b.firewalls.Has(fwARN) {
		return nil, invalid("A firewall named %s already exists", in.FirewallName)
	}

	if !b.policies.Has(in.FirewallPolicyArn) {
		return nil, notFound("firewall policy", in.FirewallPolicyArn)
	}

	r := &firewallRecord{
		Name:             in.FirewallName,
		ID:               newResourceID(),
		ARN:              fwARN,
		PolicyARN:        in.FirewallPolicyArn,
		VpcID:            in.VpcID,
		Description:      in.Description,
		UpdateToken:      newToken(),
		Encryption:       normalizeEncryption(in.EncryptionConfiguration),
		Tags:             tagMap(in.Tags),
		AnalysisTypes:    cloneStrings(in.EnabledAnalysisTypes),
		DeleteProtection: in.DeleteProtection,
		SubnetProtection: in.SubnetChangeProtection,
		PolicyProtection: in.FirewallPolicyChangeProtection,
		ZoneProtection:   in.AvailabilityZoneChangeProtection,
		ProxySettings:    in.ProxySettings,
		Endpoints:        map[string]string{},
		Zones:            map[string]string{},
		ProvisionUntil:   b.deadline(now),
		SyncUntil:        b.deadline(now),
	}

	for _, m := range in.SubnetMappings {
		if m.IPAddressType == "" {
			m.IPAddressType = "IPV4"
		}

		r.SubnetMappings = append(r.SubnetMappings, m)
	}

	if in.TransitGatewayID != "" {
		if err := b.attachTransitGatewayLocked(r, in, now); err != nil {
			return nil, err
		}
	} else if err := b.resolveSubnetsLocked(r, r.VpcID, r.SubnetMappings); err != nil {
		return nil, err
	}

	b.firewalls.Put(r)

	fw, st := b.firewallViewLocked(r, now)

	return &FirewallOutput{Firewall: fw, FirewallStatus: st}, nil
}

// findFirewallLocked resolves a live firewall by ARN or name.
func (b *InMemoryBackend) findFirewallLocked(arnStr, name string, now time.Time) (*firewallRecord, error) {
	if err := targetNeeded(arnStr, name); err != nil {
		return nil, err
	}

	key := arnStr
	if key == "" {
		key = b.arnFor(kindFirewall, name)
	}

	r, ok := b.firewalls.Get(key)
	if !ok || r.expired(now) {
		return nil, notFound("firewall", strings.TrimPrefix(key, "arn:"))
	}

	return r, nil
}

// DescribeFirewall returns a firewall, its status and its update token.
func (b *InMemoryBackend) DescribeFirewall(
	_ context.Context,
	in *DescribeFirewallInput,
) (*DescribeFirewallOutput, error) {
	b.mu.RLock("DescribeFirewall")
	defer b.mu.RUnlock()

	now := time.Now()

	r, err := b.findFirewallLocked(in.FirewallArn, in.FirewallName, now)
	if err != nil {
		return nil, err
	}

	fw, st := b.firewallViewLocked(r, now)

	return &DescribeFirewallOutput{Firewall: fw, FirewallStatus: st, UpdateToken: r.UpdateToken}, nil
}

// DescribeFirewallMetadata returns summary information about a firewall.
func (b *InMemoryBackend) DescribeFirewallMetadata(
	_ context.Context, in *DescribeFirewallMetadataInput,
) (*DescribeFirewallMetadataOutput, error) {
	b.mu.RLock("DescribeFirewallMetadata")
	defer b.mu.RUnlock()

	now := time.Now()

	r, err := b.findFirewallLocked(in.FirewallArn, "", now)
	if err != nil {
		return nil, err
	}

	status, _, _ := r.statusAt(now)

	out := &DescribeFirewallMetadataOutput{
		Description:                r.Description,
		FirewallArn:                r.ARN,
		FirewallPolicyArn:          r.PolicyARN,
		Status:                     status,
		TransitGatewayAttachmentID: r.TGWAttachmentID,
	}

	if r.TGWID != "" {
		out.SupportedAvailabilityZones = make(map[string]AvailabilityZoneMetadata, len(r.AZMappings))
		for _, az := range r.AZMappings {
			out.SupportedAvailabilityZones[az] = AvailabilityZoneMetadata{IPAddressType: ipv4}
		}
	}

	return out, nil
}

// ListFirewalls lists firewalls, optionally filtered by VPC.
func (b *InMemoryBackend) ListFirewalls(_ context.Context, in *ListFirewallsInput) (*ListFirewallsOutput, error) {
	b.mu.RLock("ListFirewalls")
	defer b.mu.RUnlock()

	now := time.Now()
	all := make([]FirewallMetadata, 0, b.firewalls.Len())

	for _, r := range b.firewalls.All() {
		if r.expired(now) || (len(in.VpcIDs) > 0 && !slices.Contains(in.VpcIDs, r.VpcID)) {
			continue
		}

		all = append(all, FirewallMetadata{FirewallArn: r.ARN, FirewallName: r.Name})
	}

	slices.SortFunc(all, func(a, c FirewallMetadata) int { return strings.Compare(a.FirewallName, c.FirewallName) })

	pg, err := paginate(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListFirewallsOutput{Firewalls: pg.Data, NextToken: pg.Next}, nil
}

// DeleteFirewall starts deletion; the firewall disappears once the DELETING window passes.
func (b *InMemoryBackend) DeleteFirewall(_ context.Context, in *DeleteFirewallInput) (*FirewallOutput, error) {
	b.mu.Lock("DeleteFirewall")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	r, err := b.findFirewallLocked(in.FirewallArn, in.FirewallName, now)
	if err != nil {
		return nil, err
	}

	if r.DeleteProtection && r.DeleteUntil.IsZero() {
		return nil, invalidOp("The firewall %s has delete protection enabled", r.Name)
	}

	if r.DeleteUntil.IsZero() {
		r.DeleteUntil = b.deadline(now)
	}

	fw, st := b.firewallViewLocked(r, now)

	if b.delay <= 0 {
		b.removeFirewallLocked(r.ARN)
	}

	return &FirewallOutput{Firewall: fw, FirewallStatus: st}, nil
}

func isTGWFailure(s string) bool {
	switch s {
	case tgwRejected, tgwFailed, tgwError, tgwDeleting, tgwDeleted:
		return true
	default:
		return false
	}
}
