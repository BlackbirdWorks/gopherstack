package azurearm

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
)

const (
	ehDRType           = "disasterRecoveryConfigs"
	ehDRActionBreak    = "breakPairing"
	ehDRActionFailover = "failover"
	ehDRActionCheck    = "checkNameAvailability"
	ehRolePrimary      = "Primary"
	ehRoleSecondary    = "Secondary"
	ehRoleNotRepl      = "PrimaryNotReplicating"
	ehSKUStandard      = "Standard"
	ehSKUPremium       = "Premium"
	ehAliasMinLen      = 6
	ehAliasMaxLen      = 50
	ehCodeConflict     = "Conflict"
	ehReasonNone       = "None"
	ehReasonInvalid    = "InvalidName"
	ehReasonInUse      = "NameInUse"
)

var (
	ErrEventHubDRNotFound  = errors.New("azurearm: event hubs disaster recovery configuration not found")
	ErrEventHubAliasInUse  = errors.New("azurearm: event hubs alias is already in use")
	ErrEventHubDRInvalid   = errors.New("azurearm: invalid event hubs disaster recovery request")
	ehAliasRE              = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9-]*[A-Za-z0-9]$`)
	errEHSecondaryReadOnly = errors.New(
		"the namespace is the secondary of a geo-disaster recovery pairing and is read-only",
	)
	errEHDRNotSecondary = errors.New("fail over must be started on the secondary namespace of the pairing")
	errEHDRNotPrimary   = errors.New("the configuration is not the primary of a pairing")
)

type storedEHDR struct {
	alias         string
	namespace     string
	resourceGroup string
	partner       string
	partnerRG     string
	alternateName string
	role          string
}

func drKey(rg, ns, alias string) string { return ehKey(rg, ns, alias) }

func (p *EventHubProvider) drOf(id ResourceID) (*storedEHDR, error) {
	if err := p.requireNamespace(id); err != nil {
		return nil, err
	}

	c, ok := p.drConfigs[drKey(id.ResourceGroup, id.Names[0], id.LeafName())]
	if !ok {
		return nil, ErrEventHubDRNotFound
	}

	return c, nil
}

func (p *EventHubProvider) secondaryNamespace(rg, ns string) bool {
	for _, c := range p.drConfigs {
		if c.role == ehRoleSecondary && resourceGroupsEqual(c.resourceGroup, rg) && strings.EqualFold(c.namespace, ns) {
			return true
		}
	}

	return false
}

func (p *EventHubProvider) rejectSecondaryWrite(id ResourceID) error {
	switch classifyEventHubPath(id) {
	case ehKindHub, ehKindGroup, ehKindHubRule, ehKindNSRule, ehKindNetworkRuleSet:
		if p.secondaryNamespace(id.ResourceGroup, id.Names[0]) {
			return badEventHubRequest(errEHSecondaryReadOnly)
		}
	case ehKindUnsupported, ehKindNamespace, ehKindDR, ehKindDRRule:
	}

	return nil
}

func (p *EventHubProvider) putDR(id ResourceID, body map[string]any) (map[string]any, error) {
	if err := p.requireNamespace(id); err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	partnerID, _ := props["partnerNamespace"].(string)
	alt, _ := props["alternateName"].(string)
	alias := id.LeafName()

	partner, err := p.validPartner(id, alias, alt, partnerID)
	if err != nil {
		return nil, err
	}

	key := drKey(id.ResourceGroup, id.Names[0], alias)
	if existing, ok := p.drConfigs[key]; ok && existing.role == ehRoleSecondary {
		return nil, badEventHubRequest(errEHSecondaryReadOnly)
	}

	if err = p.aliasFree(id, alias); err != nil {
		return nil, err
	}

	p.dropPairing(id.ResourceGroup, id.Names[0], alias)

	p.drConfigs[key] = &storedEHDR{
		alias: alias, namespace: id.Names[0], resourceGroup: id.ResourceGroup, partner: partner.name,
		partnerRG: partner.resourceGroup, alternateName: alt, role: ehRolePrimary,
	}
	p.drConfigs[drKey(partner.resourceGroup, partner.name, alias)] = &storedEHDR{
		alias: alias, namespace: partner.name, resourceGroup: partner.resourceGroup, partner: id.Names[0],
		partnerRG: id.ResourceGroup, alternateName: alt, role: ehRoleSecondary,
	}

	if err = p.replicate(id.ResourceGroup, id.Names[0]); err != nil {
		return nil, err
	}

	return p.drBody(id, p.drConfigs[key]), nil
}

func (p *EventHubProvider) validPartner(id ResourceID, alias, alt, partnerID string) (*storedEHNamespace, error) {
	if len(alias) < ehAliasMinLen || len(alias) > ehAliasMaxLen || !ehAliasRE.MatchString(alias) {
		return nil, badEventHubRequest(fmt.Errorf("%w: the alias %q is not a valid name", ErrEventHubDRInvalid, alias))
	}

	if strings.EqualFold(alias, id.Names[0]) && alt == "" {
		return nil, badEventHubRequest(
			fmt.Errorf(
				"%w: alternateName is required when the alias and the namespace name are the same",
				ErrEventHubDRInvalid,
			),
		)
	}

	pid, err := ParseGenericResourcePath(partnerID)
	if err != nil || len(pid.Names) == 0 || !strings.EqualFold(strings.Join(pid.Types, "/"), ehNamespacesType) ||
		!strings.EqualFold(pid.Namespace, namespaceMicrosoftEventHub) {
		return nil, badEventHubRequest(
			fmt.Errorf("%w: partnerNamespace must be the ARM ID of an Event Hubs namespace", ErrEventHubDRInvalid),
		)
	}

	partner, ok := p.namespaces[ehKey(pid.ResourceGroup, pid.Names[0])]
	if !ok {
		return nil, badEventHubRequest(
			fmt.Errorf("%w: the partner namespace %q was not found", ErrEventHubDRInvalid, pid.Names[0]),
		)
	}

	self := p.namespaces[ehKey(id.ResourceGroup, id.Names[0])]
	if err = p.pairable(self, partner); err != nil {
		return nil, badEventHubRequest(err)
	}

	return partner, nil
}

func ehSKUName(ns *storedEHNamespace) string {
	if n, _ := ns.sku[fieldName].(string); n != "" {
		return n
	}

	return ehSKUStandard
}

// pairable applies the Learn rules: Standard or Premium tiers (Standard pairs with Standard, Premium with Premium),
// different regions, and a partner that is not already paired.
func (p *EventHubProvider) pairable(self, partner *storedEHNamespace) error {
	switch {
	case strings.EqualFold(self.name, partner.name) && resourceGroupsEqual(self.resourceGroup, partner.resourceGroup):
		return fmt.Errorf("%w: a namespace cannot be paired with itself", ErrEventHubDRInvalid)
	case !strings.EqualFold(ehSKUName(self), ehSKUName(partner)) ||
		(!strings.EqualFold(ehSKUName(self), ehSKUStandard) && !strings.EqualFold(ehSKUName(self), ehSKUPremium)):
		return fmt.Errorf(
			"%w: geo-disaster recovery pairs Standard with Standard or Premium with Premium namespaces",
			ErrEventHubDRInvalid,
		)
	case strings.EqualFold(self.location, partner.location):
		return fmt.Errorf(
			"%w: the secondary namespace must be in a different region than the primary",
			ErrEventHubDRInvalid,
		)
	case p.pairedNamespace(partner, self):
		return fmt.Errorf(
			"%w: the partner namespace is already part of a geo-disaster recovery pairing",
			ErrEventHubDRInvalid,
		)
	}

	return nil
}

// pairedNamespace reports whether ns has a config whose partner is not other.
func (p *EventHubProvider) pairedNamespace(ns, other *storedEHNamespace) bool {
	for _, c := range p.drConfigs {
		if c.role == ehRoleNotRepl || !resourceGroupsEqual(c.resourceGroup, ns.resourceGroup) ||
			!strings.EqualFold(c.namespace, ns.name) {
			continue
		}

		if !strings.EqualFold(c.partner, other.name) || !resourceGroupsEqual(c.partnerRG, other.resourceGroup) {
			return true
		}
	}

	return false
}

func (p *EventHubProvider) aliasFree(id ResourceID, alias string) error {
	for _, c := range p.drConfigs {
		if strings.EqualFold(c.alias, alias) &&
			(!resourceGroupsEqual(c.resourceGroup, id.ResourceGroup) || !strings.EqualFold(c.namespace, id.Names[0])) &&
			(!resourceGroupsEqual(c.partnerRG, id.ResourceGroup) || !strings.EqualFold(c.partner, id.Names[0])) {
			return ErrEventHubAliasInUse
		}
	}

	for _, ns := range p.namespaces {
		if strings.EqualFold(ns.name, alias) && !strings.EqualFold(ns.name, id.Names[0]) {
			return ErrEventHubAliasInUse
		}
	}

	return nil
}

// dropPairing removes both configs of an alias that involve the namespace.
func (p *EventHubProvider) dropPairing(rg, ns, alias string) {
	c, ok := p.drConfigs[drKey(rg, ns, alias)]
	if !ok {
		return
	}

	delete(p.drConfigs, drKey(rg, ns, alias))
	delete(p.drConfigs, drKey(c.partnerRG, c.partner, alias))
}

func (p *EventHubProvider) drBody(id ResourceID, c *storedEHDR) map[string]any {
	ns := p.namespaces[ehKey(c.resourceGroup, c.namespace)]
	partnerID := ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: c.partnerRG, Namespace: namespaceMicrosoftEventHub,
		Types: []string{ehNamespacesType}, Names: []string{c.partner},
	}
	props := map[string]any{
		fieldProvisioningState: provisioningStateSucceeded, "partnerNamespace": partnerID.ARMID(),
		"role": c.role, "pendingReplicationOperationsCount": 0,
	}

	if c.alternateName != "" {
		props["alternateName"] = c.alternateName
	}

	out := map[string]any{
		"id": id.ARMID(), fieldName: c.alias, fieldType: ehTypeName(id), fieldProperties: props,
	}
	if ns != nil {
		out[fieldLocation] = ns.location
	}

	return out
}

func (p *EventHubProvider) deleteDR(id ResourceID) error {
	if _, err := p.drOf(id); err != nil {
		return err
	}

	p.dropPairing(id.ResourceGroup, id.Names[0], id.LeafName())

	return nil
}

func (p *EventHubProvider) listDR(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, c := range p.drConfigs {
		if !resourceGroupsEqual(c.resourceGroup, id.ResourceGroup) || !strings.EqualFold(c.namespace, id.Names[0]) {
			continue
		}

		cid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: c.resourceGroup, Namespace: namespaceMicrosoftEventHub,
			Types: []string{ehNamespacesType, ehDRType}, Names: []string{c.namespace, c.alias},
		}
		out = append(out, p.drBody(cid, c))
	}

	return out
}

// BreakPairing stops replication: the primary's config becomes PrimaryNotReplicating and the secondary's is removed.
func (p *EventHubProvider) BreakPairing(id ResourceID) error {
	p.mu.Lock("BreakPairing")
	defer p.mu.Unlock()

	c, err := p.drOf(id)
	if err != nil {
		return err
	}

	if c.role != ehRolePrimary {
		return badEventHubRequest(errEHDRNotPrimary)
	}

	delete(p.drConfigs, drKey(c.partnerRG, c.partner, c.alias))
	c.role = ehRoleNotRepl

	return nil
}

// FailOver makes the secondary the active namespace: its config becomes PrimaryNotReplicating and the pairing,
// with the former primary's config, is removed.
func (p *EventHubProvider) FailOver(id ResourceID) error {
	p.mu.Lock("FailOver")
	defer p.mu.Unlock()

	c, err := p.drOf(id)
	if err != nil {
		return err
	}

	if c.role != ehRoleSecondary {
		return badEventHubRequest(errEHDRNotSecondary)
	}

	delete(p.drConfigs, drKey(c.partnerRG, c.partner, c.alias))
	c.role = ehRoleNotRepl

	return nil
}

// CheckDRNameAvailability reports whether name can be used as an alias.
func (p *EventHubProvider) CheckDRNameAvailability(id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.RLock("CheckDRNameAvailability")
	defer p.mu.RUnlock()

	if err := p.requireNamespace(id); err != nil {
		return nil, err
	}

	name, _ := body[fieldName].(string)
	if name == "" {
		return nil, badEventHubRequest(fmt.Errorf("%w: name is required", ErrEventHubDRInvalid))
	}

	switch {
	case len(name) < ehAliasMinLen || len(name) > ehAliasMaxLen || !ehAliasRE.MatchString(name):
		return map[string]any{
			acFieldAvailable: false, acFieldReason: ehReasonInvalid,
			fieldMessage: fmt.Sprintf("The name %q is not a valid alias name.", name),
		}, nil
	case p.nameTaken(name):
		return map[string]any{
			acFieldAvailable: false, acFieldReason: ehReasonInUse,
			fieldMessage: fmt.Sprintf("The name %q is already in use.", name),
		}, nil
	}

	return map[string]any{acFieldAvailable: true, acFieldReason: ehReasonNone, fieldMessage: ""}, nil
}

func (p *EventHubProvider) nameTaken(name string) bool {
	for _, ns := range p.namespaces {
		if strings.EqualFold(ns.name, name) {
			return true
		}
	}

	for _, c := range p.drConfigs {
		if strings.EqualFold(c.alias, name) {
			return true
		}
	}

	return false
}

// replicate copies the primary's event hubs and consumer groups to the secondary and removes the ones the primary
// no longer has. Callers hold p.mu.
func (p *EventHubProvider) replicate(rg, ns string) error {
	var cfg *storedEHDR

	for _, c := range p.drConfigs {
		if c.role == ehRolePrimary && resourceGroupsEqual(c.resourceGroup, rg) && strings.EqualFold(c.namespace, ns) {
			cfg = c
		}
	}

	if cfg == nil {
		return nil
	}

	if err := p.syncHubs(cfg); err != nil {
		return err
	}

	return p.syncGroups(cfg)
}

func (p *EventHubProvider) syncHubs(cfg *storedEHDR) error {
	for _, h := range p.hubsOf(cfg.resourceGroup, cfg.namespace) {
		info, ok := p.dataPlane.HubInfo(cfg.namespace, h.name)
		if !ok {
			continue
		}

		if _, exists := p.hubs[ehKey(cfg.partnerRG, cfg.partner, h.name)]; exists {
			continue
		}

		info2, err := p.dataPlane.CreateHub(cfg.partner, h.name, len(info.PartitionIDs), info.Retention)
		if err != nil {
			return badEventHubRequest(err)
		}

		_ = info2
		p.hubs[ehKey(cfg.partnerRG, cfg.partner, h.name)] = &storedEHHub{
			name: h.name, namespace: cfg.partner, resourceGroup: cfg.partnerRG, extra: h.extra,
		}
	}

	for _, h := range p.hubsOf(cfg.partnerRG, cfg.partner) {
		if _, ok := p.hubs[ehKey(cfg.resourceGroup, cfg.namespace, h.name)]; !ok {
			if err := p.deleteHub(cfg.partnerRG, cfg.partner, h.name); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *EventHubProvider) syncGroups(cfg *storedEHDR) error {
	for _, g := range p.groupsOf(cfg.resourceGroup, cfg.namespace) {
		key := ehKey(cfg.partnerRG, cfg.partner, g.hub, g.name)
		if _, exists := p.groups[key]; exists {
			continue
		}

		if err := p.dataPlane.CreateConsumerGroup(cfg.partner, g.hub, g.name); err != nil {
			return badEventHubRequest(err)
		}

		p.groups[key] = &storedEHGroup{
			name: g.name, hub: g.hub, namespace: cfg.partner, resourceGroup: cfg.partnerRG,
			userMetadata: g.userMetadata,
		}
	}

	for _, g := range p.groupsOf(cfg.partnerRG, cfg.partner) {
		if _, ok := p.groups[ehKey(cfg.resourceGroup, cfg.namespace, g.hub, g.name)]; !ok {
			delete(p.groups, ehKey(cfg.partnerRG, cfg.partner, g.hub, g.name))
			_ = p.dataPlane.DeleteConsumerGroup(cfg.partner, g.hub, g.name)
		}
	}

	return nil
}

func (p *EventHubProvider) hubsOf(rg, ns string) []*storedEHHub {
	var out []*storedEHHub

	for _, h := range p.hubs {
		if resourceGroupsEqual(h.resourceGroup, rg) && strings.EqualFold(h.namespace, ns) {
			out = append(out, h)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })

	return out
}

func (p *EventHubProvider) groupsOf(rg, ns string) []*storedEHGroup {
	var out []*storedEHGroup

	for _, g := range p.groups {
		if resourceGroupsEqual(g.resourceGroup, rg) && strings.EqualFold(g.namespace, ns) {
			out = append(out, g)
		}
	}

	return out
}

// dropNamespaceConfigs removes the configs of a deleted namespace; a surviving primary stops replicating.
func (p *EventHubProvider) dropNamespaceConfigs(rg, ns string) {
	for _, c := range p.drConfigs {
		if !resourceGroupsEqual(c.resourceGroup, rg) || !strings.EqualFold(c.namespace, ns) {
			continue
		}

		delete(p.drConfigs, drKey(c.resourceGroup, c.namespace, c.alias))

		if mirror, ok := p.drConfigs[drKey(c.partnerRG, c.partner, c.alias)]; ok {
			switch mirror.role {
			case ehRolePrimary:
				mirror.role = ehRoleNotRepl
			case ehRoleSecondary:
				delete(p.drConfigs, drKey(c.partnerRG, c.partner, c.alias))
			}
		}
	}
}

func eventHubDRErrorDetails(err error) (errorEntry, bool) {
	switch {
	case errors.Is(err, ErrEventHubDRNotFound):
		return notFoundEntry("The disaster recovery configuration was not found."), true
	case errors.Is(err, ErrEventHubAliasInUse):
		return errorEntry{ehCodeConflict, "The alias is already in use.", http.StatusConflict}, true
	}

	return errorEntry{}, false
}

func (p *EventHubProvider) drOfAlias(id ResourceID) error {
	if err := p.requireNamespace(id); err != nil {
		return err
	}

	if _, ok := p.drConfigs[drKey(id.ResourceGroup, id.Names[0], id.Names[1])]; !ok {
		return ErrEventHubDRNotFound
	}

	return nil
}

func (p *EventHubProvider) drRuleBodyFor(id ResourceID) (map[string]any, error) {
	if err := p.drOfAlias(id); err != nil {
		return nil, err
	}

	r, ok := p.dataPlane.GetRule(id.Names[0], "", id.LeafName())
	if !ok {
		return nil, ErrEventHubRuleNotFound
	}

	return ruleBody(id, r), nil
}

func (p *EventHubProvider) replicated(id ResourceID, f func() (map[string]any, error)) (map[string]any, error) {
	out, err := f()
	if err != nil {
		return nil, err
	}

	return out, p.replicate(id.ResourceGroup, id.Names[0])
}

func (p *EventHubProvider) replicatedErr(id ResourceID, f func() error) error {
	if err := f(); err != nil {
		return err
	}

	return p.replicate(id.ResourceGroup, id.Names[0])
}

// listRules lists the namespace-scoped (or hub-scoped) authorization rules of a namespace, hub or alias.
func (p *EventHubProvider) listRules(id ResourceID) []map[string]any {
	if len(id.Names) == 0 || p.requireNamespace(id) != nil {
		return nil
	}

	hub := ""
	if classifyEventHubPath(id) == ehKindHubRule && len(id.Names) > 1 {
		hub = id.Names[1]
	}

	entries, err := p.dataPlane.ListRules(id.Names[0])
	if err != nil {
		return nil
	}

	var out []map[string]any

	for _, e := range entries {
		if !strings.EqualFold(e.Hub, hub) {
			continue
		}

		out = append(out, ruleBody(childRuleID(id, e.Name), e.EventHubRule))
	}

	return out
}

func childRuleID(parent ResourceID, name string) ResourceID {
	out := parent
	out.Names = append(append([]string{}, parent.Names...), name)

	return out
}

func (p *EventHubProvider) requireListParent(id ResourceID) error {
	if len(id.Names) == 0 {
		return nil
	}

	if err := p.requireNamespace(id); err != nil {
		return err
	}

	switch classifyEventHubPath(id) {
	case ehKindDRRule:
		return p.drOfAlias(id)
	case ehKindGroup, ehKindHubRule:
		if len(id.Names) > 1 {
			_, err := p.requireHub(id)

			return err
		}
	case ehKindUnsupported, ehKindNamespace, ehKindHub, ehKindNSRule, ehKindNetworkRuleSet, ehKindDR:
	}

	return nil
}
