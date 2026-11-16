package azurearm

import (
	"errors"
	"maps"
	"strings"
)

func (p *ContainerRegistryProvider) syncAnonymousPull(rec *storedACR) error {
	on, _ := rec.extra["anonymousPullEnabled"].(bool)

	if err := p.dataPlane.SetAnonymousPull(rec.name, on); err != nil {
		return badACRRequest(err)
	}

	return nil
}

// syncNetwork pushes the effective firewall to the data plane; only Premium registries have one.
func (p *ContainerRegistryProvider) syncNetwork(rec *storedACR) error {
	var rules ContainerRegistryNetworkRules

	if rec.sku == acrSkuPremium {
		rules.PublicDisabled = strings.EqualFold(stringField(rec.extra, cosmosFieldPublicNetwork), acrStatusDisabled)

		set := mapOf(rec.extra["networkRuleSet"])
		rules.DefaultDeny = strings.EqualFold(stringField(set, acrFieldDefaultAction), "Deny")

		ipRules, _ := set[ehFieldIPRules].([]any)
		for _, r := range ipRules {
			if v := stringField(mapOf(r), fieldValue); v != "" {
				rules.Allow = append(rules.Allow, v)
			}
		}
	}

	if err := p.dataPlane.SetNetworkRules(rec.name, rules); err != nil {
		return badACRRequest(err)
	}

	return nil
}

var (
	errACRKeyOnCreate  = errors.New("a customer-managed key can only be enabled while creating a registry")
	errACRKeyPersist   = errors.New("encryption cannot be disabled once a customer-managed key is enabled")
	errACRKeyPremium   = errors.New("a customer-managed key requires the Premium SKU")
	errACRKeyIdentity  = errors.New("a customer-managed key needs a user-assigned managed identity")
	errACRKeyIdentifer = errors.New("encryption needs keyVaultProperties.keyIdentifier")
)

func acrBadOrNil(err error) error {
	if err == nil {
		return nil
	}

	return badACRRequest(err)
}

func acrEncryptionEnabled(extra map[string]any) bool {
	return strings.EqualFold(stringField(mapOf(extra["encryption"]), acrPropStatus), acrTokenEnabled)
}

func acrKeyIdentifier(extra map[string]any) string {
	return stringField(mapOf(mapOf(extra["encryption"])["keyVaultProperties"]), "keyIdentifier")
}

// syncEncryption validates the registry's customer-managed key (Learn: tutorial-customer-managed-keys) and tells
// the data plane which key to follow. wasEnabled is the state before this request, nil for a new registry.
func (p *ContainerRegistryProvider) syncEncryption(rec *storedACR, wasEnabled *bool) error {
	enabled := acrEncryptionEnabled(rec.extra)

	switch {
	case !enabled && wasEnabled != nil && *wasEnabled:
		return badACRRequest(errACRKeyPersist)
	case !enabled:
		return acrBadOrNil(p.dataPlane.SetEncryptionKey(rec.name, ""))
	case wasEnabled != nil && !*wasEnabled:
		return badACRRequest(errACRKeyOnCreate)
	case rec.sku != acrSkuPremium:
		return badACRRequest(errACRKeyPremium)
	case !strings.Contains(strings.ToLower(stringField(rec.identity, fieldType)), "userassigned") ||
		len(mapOf(rec.identity["userAssignedIdentities"])) == 0:
		return badACRRequest(errACRKeyIdentity)
	}

	keyID := acrKeyIdentifier(rec.extra)
	if keyID == "" {
		return badACRRequest(errACRKeyIdentifer)
	}

	if err := p.dataPlane.CheckEncryptionKey(keyID); err != nil {
		return badACRRequest(err)
	}

	return acrBadOrNil(p.dataPlane.SetEncryptionKey(rec.name, keyID))
}

// patchChild merges a webhook or replication update body into the stored child; omitted fields stay.
func (p *ContainerRegistryProvider) patchChild(
	id ResourceID,
	kind acrKind,
	body map[string]any,
) (map[string]any, error) {
	parent, ok := p.registry(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	if isACRDataChild(kind) {
		return p.patchDataChild(id, kind, body)
	}

	if isACRCacheKind(kind) {
		return p.patchCacheChild(id, kind, body)
	}

	rec, ok := p.child(id, kind)
	if !ok {
		return nil, acrNotFound(kind)
	}

	merged := maps.Clone(rec.props)
	maps.Copy(merged, mapOf(body[fieldProperties]))

	props, err := childProps(kind, id, parent, map[string]any{fieldProperties: merged})
	if err != nil {
		return nil, badACRRequest(err)
	}

	next := *rec
	next.props = props

	if v, has := body[fieldTags]; has {
		next.tags = stringTags(v)
	}

	if kind == acrKindWebhook {
		if err = p.dataPlane.SetWebhook(parent.name, webhookOf(&next)); err != nil {
			return nil, badACRRequest(err)
		}
	}

	*rec = next

	return p.childBody(id, rec), nil
}
