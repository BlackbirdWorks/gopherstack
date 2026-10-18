package networkfirewall

import (
	"context"
	"slices"
	"time"
)

// firewallTarget identifies a firewall for update calls.
type firewallTarget struct {
	FirewallArn  string `json:"FirewallArn"`
	FirewallName string `json:"FirewallName"`
	UpdateToken  string `json:"UpdateToken"`
}

// UpdateFirewallDescriptionInput is the UpdateFirewallDescription request.
type UpdateFirewallDescriptionInput struct {
	firewallTarget
	Description string `json:"Description"`
}

// UpdateFirewallDescriptionOutput is the UpdateFirewallDescription response.
type UpdateFirewallDescriptionOutput struct {
	Description  string `json:"Description"`
	FirewallArn  string `json:"FirewallArn"`
	FirewallName string `json:"FirewallName"`
	UpdateToken  string `json:"UpdateToken"`
}

// UpdateFirewallDeleteProtectionInput is the UpdateFirewallDeleteProtection request.
type UpdateFirewallDeleteProtectionInput struct {
	firewallTarget
	DeleteProtection bool `json:"DeleteProtection"`
}

// UpdateFirewallDeleteProtectionOutput is the UpdateFirewallDeleteProtection response.
type UpdateFirewallDeleteProtectionOutput struct {
	FirewallArn      string `json:"FirewallArn"`
	FirewallName     string `json:"FirewallName"`
	UpdateToken      string `json:"UpdateToken"`
	DeleteProtection bool   `json:"DeleteProtection"`
}

// UpdateSubnetChangeProtectionInput is the UpdateSubnetChangeProtection request.
type UpdateSubnetChangeProtectionInput struct {
	firewallTarget
	SubnetChangeProtection bool `json:"SubnetChangeProtection"`
}

// UpdateSubnetChangeProtectionOutput is the UpdateSubnetChangeProtection response.
type UpdateSubnetChangeProtectionOutput struct {
	FirewallArn            string `json:"FirewallArn"`
	FirewallName           string `json:"FirewallName"`
	UpdateToken            string `json:"UpdateToken"`
	SubnetChangeProtection bool   `json:"SubnetChangeProtection"`
}

// UpdateFirewallPolicyChangeProtectionInput is the UpdateFirewallPolicyChangeProtection request.
type UpdateFirewallPolicyChangeProtectionInput struct {
	firewallTarget
	FirewallPolicyChangeProtection bool `json:"FirewallPolicyChangeProtection"`
}

// UpdateFirewallPolicyChangeProtectionOutput is the UpdateFirewallPolicyChangeProtection response.
type UpdateFirewallPolicyChangeProtectionOutput struct {
	FirewallArn                    string `json:"FirewallArn"`
	FirewallName                   string `json:"FirewallName"`
	UpdateToken                    string `json:"UpdateToken"`
	FirewallPolicyChangeProtection bool   `json:"FirewallPolicyChangeProtection"`
}

// UpdateFirewallEncryptionConfigurationInput is the UpdateFirewallEncryptionConfiguration request.
type UpdateFirewallEncryptionConfigurationInput struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"EncryptionConfiguration"`
	firewallTarget
}

// UpdateFirewallEncryptionConfigurationOutput is the UpdateFirewallEncryptionConfiguration response.
type UpdateFirewallEncryptionConfigurationOutput struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"EncryptionConfiguration"`
	FirewallArn             string                   `json:"FirewallArn"`
	FirewallName            string                   `json:"FirewallName"`
	UpdateToken             string                   `json:"UpdateToken"`
}

// AssociateFirewallPolicyInput is the AssociateFirewallPolicy request.
type AssociateFirewallPolicyInput struct {
	firewallTarget
	FirewallPolicyArn string `json:"FirewallPolicyArn"`
}

// AssociateFirewallPolicyOutput is the AssociateFirewallPolicy response.
type AssociateFirewallPolicyOutput struct {
	FirewallArn       string `json:"FirewallArn"`
	FirewallName      string `json:"FirewallName"`
	FirewallPolicyArn string `json:"FirewallPolicyArn"`
	UpdateToken       string `json:"UpdateToken"`
}

// AssociateSubnetsInput is the AssociateSubnets request.
type AssociateSubnetsInput struct {
	firewallTarget
	SubnetMappings []SubnetMapping `json:"SubnetMappings"`
}

// DisassociateSubnetsInput is the DisassociateSubnets request.
type DisassociateSubnetsInput struct {
	firewallTarget
	SubnetIDs []string `json:"SubnetIds"`
}

// SubnetsOutput is the AssociateSubnets and DisassociateSubnets response.
type SubnetsOutput struct {
	FirewallArn    string          `json:"FirewallArn"`
	FirewallName   string          `json:"FirewallName"`
	UpdateToken    string          `json:"UpdateToken"`
	SubnetMappings []SubnetMapping `json:"SubnetMappings"`
}

// mutateFirewall applies apply to a live firewall under the write lock, rotates its
// update token and builds the response before the lock is released.
func mutateFirewall[T any](
	b *InMemoryBackend,
	op string,
	t firewallTarget,
	apply func(*firewallRecord, time.Time) error,
	build func(*firewallRecord) *T,
) (*T, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepLocked(now)

	r, err := b.findFirewallLocked(t.FirewallArn, t.FirewallName, now)
	if err != nil {
		return nil, err
	}

	if !r.DeleteUntil.IsZero() {
		return nil, invalidOp("The firewall %s is being deleted", r.Name)
	}

	if err = checkToken(r.UpdateToken, t.UpdateToken); err != nil {
		return nil, err
	}

	if err = apply(r, now); err != nil {
		return nil, err
	}

	r.UpdateToken = newToken()

	return build(r), nil
}

// UpdateFirewallDescription replaces a firewall's description.
func (b *InMemoryBackend) UpdateFirewallDescription(
	_ context.Context, in *UpdateFirewallDescriptionInput,
) (*UpdateFirewallDescriptionOutput, error) {
	if err := validateDescription(in.Description); err != nil {
		return nil, err
	}

	return mutateFirewall(b, "UpdateFirewallDescription", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			r.Description = in.Description

			return nil
		},
		func(r *firewallRecord) *UpdateFirewallDescriptionOutput {
			return &UpdateFirewallDescriptionOutput{
				Description: r.Description, FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
			}
		})
}

// UpdateFirewallDeleteProtection toggles delete protection.
func (b *InMemoryBackend) UpdateFirewallDeleteProtection(
	_ context.Context, in *UpdateFirewallDeleteProtectionInput,
) (*UpdateFirewallDeleteProtectionOutput, error) {
	return mutateFirewall(b, "UpdateFirewallDeleteProtection", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			r.DeleteProtection = in.DeleteProtection

			return nil
		},
		func(r *firewallRecord) *UpdateFirewallDeleteProtectionOutput {
			return &UpdateFirewallDeleteProtectionOutput{
				FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
				DeleteProtection: r.DeleteProtection,
			}
		})
}

// UpdateSubnetChangeProtection toggles subnet change protection.
func (b *InMemoryBackend) UpdateSubnetChangeProtection(
	_ context.Context, in *UpdateSubnetChangeProtectionInput,
) (*UpdateSubnetChangeProtectionOutput, error) {
	return mutateFirewall(b, "UpdateSubnetChangeProtection", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			r.SubnetProtection = in.SubnetChangeProtection

			return nil
		},
		func(r *firewallRecord) *UpdateSubnetChangeProtectionOutput {
			return &UpdateSubnetChangeProtectionOutput{
				FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
				SubnetChangeProtection: r.SubnetProtection,
			}
		})
}

// UpdateFirewallPolicyChangeProtection toggles policy change protection.
func (b *InMemoryBackend) UpdateFirewallPolicyChangeProtection(
	_ context.Context, in *UpdateFirewallPolicyChangeProtectionInput,
) (*UpdateFirewallPolicyChangeProtectionOutput, error) {
	return mutateFirewall(b, "UpdateFirewallPolicyChangeProtection", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			r.PolicyProtection = in.FirewallPolicyChangeProtection

			return nil
		},
		func(r *firewallRecord) *UpdateFirewallPolicyChangeProtectionOutput {
			return &UpdateFirewallPolicyChangeProtectionOutput{
				FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
				FirewallPolicyChangeProtection: r.PolicyProtection,
			}
		})
}

// UpdateFirewallEncryptionConfiguration replaces a firewall's encryption settings.
func (b *InMemoryBackend) UpdateFirewallEncryptionConfiguration(
	_ context.Context, in *UpdateFirewallEncryptionConfigurationInput,
) (*UpdateFirewallEncryptionConfigurationOutput, error) {
	if err := validateEncryption(in.EncryptionConfiguration); err != nil {
		return nil, err
	}

	return mutateFirewall(b, "UpdateFirewallEncryptionConfiguration", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			r.Encryption = normalizeEncryption(in.EncryptionConfiguration)

			return nil
		},
		func(r *firewallRecord) *UpdateFirewallEncryptionConfigurationOutput {
			return &UpdateFirewallEncryptionConfigurationOutput{
				EncryptionConfiguration: normalizeEncryption(r.Encryption),
				FirewallArn:             r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
			}
		})
}

// AssociateFirewallPolicy points a firewall at another policy.
func (b *InMemoryBackend) AssociateFirewallPolicy(
	_ context.Context, in *AssociateFirewallPolicyInput,
) (*AssociateFirewallPolicyOutput, error) {
	if in.FirewallPolicyArn == "" {
		return nil, invalid("FirewallPolicyArn is required")
	}

	return mutateFirewall(b, "AssociateFirewallPolicy", in.firewallTarget,
		func(r *firewallRecord, now time.Time) error {
			if r.PolicyProtection {
				return invalidOp("The firewall %s has policy change protection enabled", r.Name)
			}

			if !b.policies.Has(in.FirewallPolicyArn) {
				return notFound("firewall policy", in.FirewallPolicyArn)
			}

			r.PolicyARN = in.FirewallPolicyArn
			r.SyncUntil = b.deadline(now)

			return nil
		},
		func(r *firewallRecord) *AssociateFirewallPolicyOutput {
			return &AssociateFirewallPolicyOutput{
				FirewallArn: r.ARN, FirewallName: r.Name, FirewallPolicyArn: r.PolicyARN, UpdateToken: r.UpdateToken,
			}
		})
}

// AssociateSubnets adds subnets (one per Availability Zone) to a firewall.
func (b *InMemoryBackend) AssociateSubnets(_ context.Context, in *AssociateSubnetsInput) (*SubnetsOutput, error) {
	if len(in.SubnetMappings) == 0 {
		return nil, invalid("SubnetMappings is required")
	}

	if err := validateSubnetMappings(in.SubnetMappings); err != nil {
		return nil, err
	}

	return mutateFirewall(b, "AssociateSubnets", in.firewallTarget,
		func(r *firewallRecord, now time.Time) error {
			if r.TGWID != "" {
				return invalid("The firewall %s is attached to a transit gateway and has no subnets", r.Name)
			}

			if r.SubnetProtection {
				return invalidOp("The firewall %s has subnet change protection enabled", r.Name)
			}

			for _, m := range in.SubnetMappings {
				if _, exists := r.Zones[m.SubnetID]; exists {
					return invalid("Subnet %s is already associated with the firewall", m.SubnetID)
				}
			}

			if err := b.resolveSubnetsLocked(r, r.VpcID, in.SubnetMappings); err != nil {
				return err
			}

			for _, m := range in.SubnetMappings {
				if m.IPAddressType == "" {
					m.IPAddressType = ipv4
				}

				r.SubnetMappings = append(r.SubnetMappings, m)
			}

			r.ProvisionUntil = b.deadline(now)
			r.SyncUntil = r.ProvisionUntil

			return nil
		},
		subnetsOutput)
}

// DisassociateSubnets removes subnets from a firewall; at least one must remain.
func (b *InMemoryBackend) DisassociateSubnets(_ context.Context, in *DisassociateSubnetsInput) (*SubnetsOutput, error) {
	if len(in.SubnetIDs) == 0 {
		return nil, invalid("SubnetIds is required")
	}

	return mutateFirewall(b, "DisassociateSubnets", in.firewallTarget,
		func(r *firewallRecord, _ time.Time) error {
			if r.TGWID != "" {
				return invalid("The firewall %s is attached to a transit gateway and has no subnets", r.Name)
			}

			if r.SubnetProtection {
				return invalidOp("The firewall %s has subnet change protection enabled", r.Name)
			}

			for _, id := range in.SubnetIDs {
				if _, exists := r.Zones[id]; !exists {
					return invalid("Subnet %s is not associated with the firewall", id)
				}
			}

			kept := make([]SubnetMapping, 0, len(r.SubnetMappings))

			for _, m := range r.SubnetMappings {
				if !slices.Contains(in.SubnetIDs, m.SubnetID) {
					kept = append(kept, m)
				}
			}

			if len(kept) == 0 {
				return invalid("A firewall must keep at least one subnet")
			}

			for _, id := range in.SubnetIDs {
				delete(r.Zones, id)
				delete(r.Endpoints, id)
			}

			r.SubnetMappings = kept

			return nil
		},
		subnetsOutput)
}

func subnetsOutput(r *firewallRecord) *SubnetsOutput {
	return &SubnetsOutput{
		FirewallArn: r.ARN, FirewallName: r.Name, UpdateToken: r.UpdateToken,
		SubnetMappings: slices.Clone(r.SubnetMappings),
	}
}
