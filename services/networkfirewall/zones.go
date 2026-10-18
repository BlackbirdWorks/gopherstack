package networkfirewall

import (
	"context"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	tgwPendingAcceptance = "PENDING_ACCEPTANCE"
	tgwCreating          = "CREATING"
	tgwReady             = "READY"
	tgwRejecting         = "REJECTING"
	tgwRejected          = "REJECTED"
	tgwDeleting          = "DELETING"
	tgwDeleted           = "DELETED"
	tgwFailed            = "FAILED"
	tgwError             = "ERROR"
)

// TransitGatewayResolver looks up the owner account of an EC2 transit gateway.
type TransitGatewayResolver interface {
	TransitGatewayOwner(id string) (ownerAccountID string, ok bool)
}

// TransitGatewayAttachmentSyncState is the transit gateway attachment status of a firewall.
type TransitGatewayAttachmentSyncState struct {
	AttachmentID                   string `json:"AttachmentId,omitempty"`
	StatusMessage                  string `json:"StatusMessage,omitempty"`
	TransitGatewayAttachmentStatus string `json:"TransitGatewayAttachmentStatus,omitempty"`
}

// AssociateAvailabilityZonesInput is the AssociateAvailabilityZones request.
type AssociateAvailabilityZonesInput struct {
	firewallTarget
	AvailabilityZoneMappings []AvailabilityZoneMapping `json:"AvailabilityZoneMappings"`
}

// DisassociateAvailabilityZonesInput is the DisassociateAvailabilityZones request.
type DisassociateAvailabilityZonesInput = AssociateAvailabilityZonesInput

// AvailabilityZonesOutput is the Associate/DisassociateAvailabilityZones response.
type AvailabilityZonesOutput struct {
	FirewallArn              string                    `json:"FirewallArn"`
	FirewallName             string                    `json:"FirewallName"`
	UpdateToken              string                    `json:"UpdateToken"`
	AvailabilityZoneMappings []AvailabilityZoneMapping `json:"AvailabilityZoneMappings"`
}

// UpdateAvailabilityZoneChangeProtectionInput is the UpdateAvailabilityZoneChangeProtection request.
type UpdateAvailabilityZoneChangeProtectionInput struct {
	firewallTarget
	AvailabilityZoneChangeProtection bool `json:"AvailabilityZoneChangeProtection"`
}

// UpdateAvailabilityZoneChangeProtectionOutput is the UpdateAvailabilityZoneChangeProtection response.
type UpdateAvailabilityZoneChangeProtectionOutput struct {
	FirewallArn                      string `json:"FirewallArn"`
	FirewallName                     string `json:"FirewallName"`
	UpdateToken                      string `json:"UpdateToken"`
	AvailabilityZoneChangeProtection bool   `json:"AvailabilityZoneChangeProtection"`
}

// TransitGatewayAttachmentInput is the Accept/Reject/DeleteNetworkFirewallTransitGatewayAttachment request.
type TransitGatewayAttachmentInput struct {
	TransitGatewayAttachmentID string `json:"TransitGatewayAttachmentId"`
}

// TransitGatewayAttachmentOutput is the Accept/Reject/DeleteNetworkFirewallTransitGatewayAttachment response.
type TransitGatewayAttachmentOutput struct {
	TransitGatewayAttachmentID     string `json:"TransitGatewayAttachmentId"`
	TransitGatewayAttachmentStatus string `json:"TransitGatewayAttachmentStatus"`
}

// UpdateProxySettingsInput is the UpdateProxySettings request.
type UpdateProxySettingsInput struct {
	ProxySettings *ProxySettings `json:"ProxySettings"`
	firewallTarget
}

// UpdateProxySettingsOutput is the UpdateProxySettings response.
type UpdateProxySettingsOutput struct {
	ProxySettings *ProxySettings `json:"ProxySettings,omitempty"`
	FirewallArn   string         `json:"FirewallArn"`
	FirewallName  string         `json:"FirewallName"`
	UpdateToken   string         `json:"UpdateToken"`
}

// UpdateFirewallAnalysisSettingsInput is the UpdateFirewallAnalysisSettings request.
type UpdateFirewallAnalysisSettingsInput struct {
	firewallTarget
	EnabledAnalysisTypes []string `json:"EnabledAnalysisTypes"`
}

// UpdateFirewallAnalysisSettingsOutput is the UpdateFirewallAnalysisSettings response.
type UpdateFirewallAnalysisSettingsOutput struct {
	FirewallArn          string   `json:"FirewallArn"`
	FirewallName         string   `json:"FirewallName"`
	UpdateToken          string   `json:"UpdateToken"`
	EnabledAnalysisTypes []string `json:"EnabledAnalysisTypes"`
}

func zoneMappings(zones []string) []AvailabilityZoneMapping {
	if len(zones) == 0 {
		return nil
	}

	out := make([]AvailabilityZoneMapping, len(zones))
	for i, z := range zones {
		out[i] = AvailabilityZoneMapping{AvailabilityZone: z}
	}

	return out
}

func validateZoneMappings(m []AvailabilityZoneMapping) error {
	if len(m) == 0 {
		return invalid("AvailabilityZoneMappings is required")
	}

	seen := make(map[string]bool, len(m))

	for _, z := range m {
		if z.AvailabilityZone == "" {
			return invalid("AvailabilityZoneMapping.AvailabilityZone is required")
		}

		if seen[z.AvailabilityZone] {
			return invalid("Availability Zone %s is specified more than once", z.AvailabilityZone)
		}

		seen[z.AvailabilityZone] = true
	}

	return nil
}

func validateAnalysisTypes(types []string) error {
	for _, t := range types {
		if t != analysisTLSSNI && t != analysisHTTPHost {
			return invalid("EnabledAnalysisTypes value %q is not valid", t)
		}
	}

	return nil
}

func validateProxySettings(p *ProxySettings) error {
	if p == nil {
		return nil
	}

	seen := map[int32]bool{}

	for _, l := range p.ListenerProperties {
		if l.Port == nil || *l.Port < 1 || *l.Port > maxPort {
			return invalid("ListenerProperty.Port must be between 1 and %d", maxPort)
		}

		if l.Type != listenerHTTP && l.Type != listenerHTTPS {
			return invalid("ListenerProperty.Type %q is not valid", l.Type)
		}

		if seen[*l.Port] {
			return invalid("Listener port %d is specified more than once", *l.Port)
		}

		seen[*l.Port] = true
	}

	return nil
}

// SetTransitGatewayResolver wires EC2 transit gateway lookups; nil keeps standalone behaviour.
func (b *InMemoryBackend) SetTransitGatewayResolver(r TransitGatewayResolver) {
	b.mu.Lock("SetTransitGatewayResolver")
	defer b.mu.Unlock()

	b.tgws = r
}

func (r *firewallRecord) tgwStatusAt(now time.Time) string {
	if r.TGWID == "" || now.Before(r.TGWUntil) {
		return r.TGWStatus
	}

	switch r.TGWStatus {
	case tgwCreating:
		return tgwReady
	case tgwRejecting:
		return tgwRejected
	case tgwDeleting:
		return tgwDeleted
	default:
		return r.TGWStatus
	}
}

// attachTransitGatewayLocked records the AZ endpoints and the transit gateway attachment of a new firewall.
func (b *InMemoryBackend) attachTransitGatewayLocked(r *firewallRecord, in *CreateFirewallInput, now time.Time) error {
	owner := b.accountID

	if b.tgws != nil {
		o, ok := b.tgws.TransitGatewayOwner(in.TransitGatewayID)
		if !ok {
			return invalid("Transit gateway %s does not exist", in.TransitGatewayID)
		}

		owner = o
	}

	r.TGWID = in.TransitGatewayID
	r.TGWOwner = owner
	r.TGWAttachmentID = awsid.New("tgw-attach-")
	r.TGWStatus, r.TGWUntil = tgwCreating, b.deadline(now)

	if owner != b.accountID {
		r.TGWStatus = tgwPendingAcceptance
	}

	b.addZonesLocked(r, in.AvailabilityZoneMappings)

	return nil
}

func (b *InMemoryBackend) addZonesLocked(r *firewallRecord, add []AvailabilityZoneMapping) {
	for _, z := range add {
		r.AZMappings = append(r.AZMappings, z.AvailabilityZone)
		r.Zones[z.AvailabilityZone] = z.AvailabilityZone
		r.Endpoints[z.AvailabilityZone] = awsid.New("vpce-")
	}
}

func requireTGWFirewall(r *firewallRecord) error {
	if r.TGWID == "" {
		return invalid("The firewall %s is not attached to a transit gateway", r.Name)
	}

	return nil
}

// AssociateAvailabilityZones adds Availability Zones to a transit gateway firewall.
func (b *InMemoryBackend) AssociateAvailabilityZones(
	_ context.Context, in *AssociateAvailabilityZonesInput,
) (*AvailabilityZonesOutput, error) {
	if err := validateZoneMappings(in.AvailabilityZoneMappings); err != nil {
		return nil, err
	}

	return mutateFirewall(b, "AssociateAvailabilityZones", in.firewallTarget,
		func(r *firewallRecord, now time.Time) error {
			if err := requireTGWFirewall(r); err != nil {
				return err
			}

			if r.ZoneProtection {
				return invalidOp("The firewall %s has Availability Zone change protection enabled", r.Name)
			}

			for _, z := range in.AvailabilityZoneMappings {
				if slices.Contains(r.AZMappings, z.AvailabilityZone) {
					return invalid("Availability Zone %s is already associated with the firewall", z.AvailabilityZone)
				}
			}

			b.addZonesLocked(r, in.AvailabilityZoneMappings)
			r.ProvisionUntil = b.deadline(now)
			r.SyncUntil = r.ProvisionUntil

			return nil
		}, zonesOutput)
}

// DisassociateAvailabilityZones removes Availability Zones; at least one must remain.
func (b *InMemoryBackend) DisassociateAvailabilityZones(
	_ context.Context, in *DisassociateAvailabilityZonesInput,
) (*AvailabilityZonesOutput, error) {
	if err := validateZoneMappings(in.AvailabilityZoneMappings); err != nil {
		return nil, err
	}

	return mutateFirewall(b, "DisassociateAvailabilityZones", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			if err := requireTGWFirewall(r); err != nil {
				return err
			}

			if r.ZoneProtection {
				return invalidOp("The firewall %s has Availability Zone change protection enabled", r.Name)
			}

			for _, z := range in.AvailabilityZoneMappings {
				if !slices.Contains(r.AZMappings, z.AvailabilityZone) {
					return invalid("Availability Zone %s is not associated with the firewall", z.AvailabilityZone)
				}
			}

			kept := slices.DeleteFunc(slices.Clone(r.AZMappings), func(az string) bool {
				return slices.ContainsFunc(in.AvailabilityZoneMappings, func(m AvailabilityZoneMapping) bool {
					return m.AvailabilityZone == az
				})
			})
			if len(kept) == 0 {
				return invalid("A firewall must keep at least one Availability Zone")
			}

			for _, z := range in.AvailabilityZoneMappings {
				delete(r.Zones, z.AvailabilityZone)
				delete(r.Endpoints, z.AvailabilityZone)
			}

			r.AZMappings = kept

			return nil
		}, zonesOutput)
}

func zonesOutput(r *firewallRecord) *AvailabilityZonesOutput {
	return &AvailabilityZonesOutput{
		FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
		AvailabilityZoneMappings: zoneMappings(r.AZMappings),
	}
}

// UpdateAvailabilityZoneChangeProtection toggles Availability Zone change protection.
func (b *InMemoryBackend) UpdateAvailabilityZoneChangeProtection(
	_ context.Context, in *UpdateAvailabilityZoneChangeProtectionInput,
) (*UpdateAvailabilityZoneChangeProtectionOutput, error) {
	return mutateFirewall(b, "UpdateAvailabilityZoneChangeProtection", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			r.ZoneProtection = in.AvailabilityZoneChangeProtection

			return nil
		},
		func(r *firewallRecord) *UpdateAvailabilityZoneChangeProtectionOutput {
			return &UpdateAvailabilityZoneChangeProtectionOutput{
				FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
				AvailabilityZoneChangeProtection: r.ZoneProtection,
			}
		})
}

// UpdateProxySettings replaces the proxy listeners of a firewall.
func (b *InMemoryBackend) UpdateProxySettings(
	_ context.Context, in *UpdateProxySettingsInput,
) (*UpdateProxySettingsOutput, error) {
	if in.ProxySettings == nil {
		return nil, invalid("ProxySettings is required")
	}

	if err := validateProxySettings(in.ProxySettings); err != nil {
		return nil, err
	}

	return mutateFirewall(b, "UpdateProxySettings", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			r.ProxySettings = in.ProxySettings

			return nil
		},
		func(r *firewallRecord) *UpdateProxySettingsOutput {
			return &UpdateProxySettingsOutput{
				ProxySettings: r.ProxySettings, FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
			}
		})
}

// UpdateFirewallAnalysisSettings sets the analysis types a firewall collects.
func (b *InMemoryBackend) UpdateFirewallAnalysisSettings(
	_ context.Context, in *UpdateFirewallAnalysisSettingsInput,
) (*UpdateFirewallAnalysisSettingsOutput, error) {
	if err := validateAnalysisTypes(in.EnabledAnalysisTypes); err != nil {
		return nil, err
	}

	return mutateFirewall(b, "UpdateFirewallAnalysisSettings", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			r.AnalysisTypes = cloneStrings(in.EnabledAnalysisTypes)

			return nil
		},
		func(r *firewallRecord) *UpdateFirewallAnalysisSettingsOutput {
			return &UpdateFirewallAnalysisSettingsOutput{
				FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
				EnabledAnalysisTypes: cloneStrings(r.AnalysisTypes),
			}
		})
}

func (b *InMemoryBackend) tgwFirewallLocked(id string, now time.Time) (*firewallRecord, error) {
	if id == "" {
		return nil, invalid("TransitGatewayAttachmentId is required")
	}

	for _, r := range b.firewalls.All() {
		if r.TGWAttachmentID == id && !r.expired(now) {
			return r, nil
		}
	}

	return nil, notFound("transit gateway attachment", id)
}

// moveTGWAttachment applies one attachment transition when the current status is in from.
func (b *InMemoryBackend) moveTGWAttachment(
	op string, id string, from []string, to string,
) (*TransitGatewayAttachmentOutput, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	r, err := b.tgwFirewallLocked(id, now)
	if err != nil {
		return nil, err
	}

	cur := r.tgwStatusAt(now)
	if !slices.Contains(from, cur) {
		return nil, invalidOp("The transit gateway attachment %s is %s and cannot be changed this way", id, cur)
	}

	r.TGWStatus, r.TGWUntil = to, b.deadline(now)

	if to == tgwCreating {
		r.ProvisionUntil = r.TGWUntil
		r.SyncUntil = r.TGWUntil
	}

	r.UpdateToken = newToken()

	return &TransitGatewayAttachmentOutput{
		TransitGatewayAttachmentID: id, TransitGatewayAttachmentStatus: r.tgwStatusAt(now),
	}, nil
}

// AcceptNetworkFirewallTransitGatewayAttachment accepts a pending attachment.
func (b *InMemoryBackend) AcceptNetworkFirewallTransitGatewayAttachment(
	_ context.Context, in *TransitGatewayAttachmentInput,
) (*TransitGatewayAttachmentOutput, error) {
	return b.moveTGWAttachment("AcceptNetworkFirewallTransitGatewayAttachment",
		in.TransitGatewayAttachmentID, []string{tgwPendingAcceptance}, tgwCreating)
}

// RejectNetworkFirewallTransitGatewayAttachment rejects a pending attachment.
func (b *InMemoryBackend) RejectNetworkFirewallTransitGatewayAttachment(
	_ context.Context, in *TransitGatewayAttachmentInput,
) (*TransitGatewayAttachmentOutput, error) {
	return b.moveTGWAttachment("RejectNetworkFirewallTransitGatewayAttachment",
		in.TransitGatewayAttachmentID, []string{tgwPendingAcceptance}, tgwRejecting)
}

// DeleteNetworkFirewallTransitGatewayAttachment deletes a settled attachment.
func (b *InMemoryBackend) DeleteNetworkFirewallTransitGatewayAttachment(
	_ context.Context, in *TransitGatewayAttachmentInput,
) (*TransitGatewayAttachmentOutput, error) {
	return b.moveTGWAttachment("DeleteNetworkFirewallTransitGatewayAttachment",
		in.TransitGatewayAttachmentID, []string{tgwReady, tgwRejected, tgwFailed, tgwError}, tgwDeleting)
}
