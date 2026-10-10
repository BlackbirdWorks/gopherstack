package sesv2

import (
	"fmt"
	"slices"
	"strings"
)

// ListManagementOptions mirrors types.ListManagementOptions.
type ListManagementOptions struct {
	ContactListName string `json:"ContactListName"`
	TopicName       string `json:"TopicName,omitempty"`
}

// SendOptions carries the SendEmail/SendBulkEmail members that reference other resources.
type SendOptions struct {
	ListManagement                 *ListManagementOptions `json:"listManagement,omitempty"`
	ConfigurationSetName           string                 `json:"configurationSetName,omitempty"`
	TenantName                     string                 `json:"tenantName,omitempty"`
	FeedbackForwardingEmailAddress string                 `json:"feedbackForwardingEmailAddress,omitempty"`

	EndpointID                                string `json:"-"`
	FromEmailAddressIdentityArn               string `json:"-"`
	FeedbackForwardingEmailAddressIdentityArn string `json:"-"`
}

// validateSendOptions checks that every referenced resource exists and, for a tenant, is associated with it.
func (b *InMemoryBackend) validateSendOptions(from, templateName string, o SendOptions) error {
	b.mu.RLock("validateSendOptions")
	defer b.mu.RUnlock()

	if o.ConfigurationSetName != "" {
		if _, ok := b.configurationSets.Get(o.ConfigurationSetName); !ok {
			return configSetMissing(o.ConfigurationSetName)
		}
	}

	if err := b.validateSendReferencesLocked(o); err != nil {
		return err
	}

	if lm := o.ListManagement; lm != nil {
		if err := b.validateListManagementLocked(lm); err != nil {
			return err
		}
	}

	if o.TenantName == "" {
		return nil
	}

	if _, ok := b.tenants[o.TenantName]; !ok {
		return fmt.Errorf("%w: Tenant %s not found", ErrNotFound, o.TenantName)
	}

	return b.checkTenantResourcesLocked(from, templateName, o)
}

func (b *InMemoryBackend) validateListManagementLocked(lm *ListManagementOptions) error {
	cl, ok := b.contactLists.Get(lm.ContactListName)
	if !ok {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, lm.ContactListName)
	}

	hasTopic := func(t Topic) bool { return t.TopicName == lm.TopicName }
	if lm.TopicName != "" && !slices.ContainsFunc(cl.Topics, hasTopic) {
		return fmt.Errorf("%w: topic %s not found in contact list %s", ErrNotFound, lm.TopicName, lm.ContactListName)
	}

	return nil
}

func (b *InMemoryBackend) checkTenantResourcesLocked(from, templateName string, o SendOptions) error {
	refs := []string{b.fromIdentityARNLocked(from)}
	if o.ConfigurationSetName != "" {
		refs = append(refs, b.configurationSetARN(o.ConfigurationSetName))
	}

	if templateName != "" {
		refs = append(refs, b.emailTemplateARN(templateName))
	}

	for _, ref := range refs {
		if ref != "" && !slices.Contains(b.resourceTenants[ref], o.TenantName) {
			return fmt.Errorf("%w: %s is not associated with tenant %s", ErrInvalidInput, ref, o.TenantName)
		}
	}

	return nil
}

// fromIdentityARNLocked returns the ARN of the identity that authorizes from (address first, then domain).
func (b *InMemoryBackend) fromIdentityARNLocked(from string) string {
	if _, ok := b.identities.Get(from); ok {
		return b.identityARN(from)
	}

	if _, domain, found := strings.CutLast(from, "@"); found {
		if _, ok := b.identities.Get(domain); ok {
			return b.identityARN(domain)
		}
	}

	return ""
}

// storedTemplateName returns the referenced stored template's name, or "" for inline content.
func storedTemplateName(t *bulkEmailTemplate) string {
	if t == nil || t.TemplateContent != nil {
		return ""
	}

	return t.TemplateName
}

// validateSendReferencesLocked checks the multi-region EndpointId and the
// sending-authorization identity ARNs. ARNs of other accounts are accepted:
// their policies live outside this single-account emulator.
func (b *InMemoryBackend) validateSendReferencesLocked(o SendOptions) error {
	if o.EndpointID != "" && !b.hasMultiRegionEndpointIDLocked(o.EndpointID) {
		return fmt.Errorf("%w: multi-region endpoint %s not found", ErrNotFound, o.EndpointID)
	}

	for _, identityARN := range []string{o.FromEmailAddressIdentityArn, o.FeedbackForwardingEmailAddressIdentityArn} {
		if identityARN == "" {
			continue
		}

		if err := b.checkIdentityARNLocked(identityARN); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) hasMultiRegionEndpointIDLocked(id string) bool {
	for _, ep := range b.multiRegionEndpoints {
		if mapString(ep, keyEndpointID) == id {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) checkIdentityARNLocked(identityARN string) error {
	const arnParts = 6

	parts := strings.SplitN(identityARN, ":", arnParts)
	name, isIdentity := "", false

	if len(parts) == arnParts && parts[0] == "arn" && parts[2] == "ses" {
		name, isIdentity = strings.CutPrefix(parts[5], "identity/")
	}

	if !isIdentity || name == "" {
		return fmt.Errorf("%w: %q is not an SES identity ARN", ErrInvalidInput, identityARN)
	}

	if parts[4] != b.accountID {
		return nil
	}

	if _, ok := b.identities.Get(name); !ok {
		return identityMissing(name)
	}

	return nil
}
