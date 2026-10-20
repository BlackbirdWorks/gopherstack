package azurearm

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	ehPropPartitionCount = "partitionCount"
	ehPropRetentionDays  = "messageRetentionInDays"
	ehPropRetentionDesc  = "retentionDescription"
	ehPropPartitionIDs   = "partitionIds"
	ehPropCreatedAt      = "createdAt"
	ehPropUpdatedAt      = "updatedAt"
	ehPropStatus         = "status"
	ehPropRights         = "rights"
	ehStatusActive       = "Active"
	ehFieldSKU           = "sku"
	ehFieldIPRules       = "ipRules"
	ehFieldVNetRules     = "virtualNetworkRules"
	ehCleanupDelete      = "Delete"
)

func ehTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func badEventHubRequest(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidRequestBody, err)
}

func (p *EventHubProvider) putNamespace(
	ctx context.Context,
	id ResourceID,
	body map[string]any,
) (map[string]any, error) {
	name := id.LeafName()
	key := ehKey(id.ResourceGroup, name)
	existing, existed := p.namespaces[key]

	if err := p.dataPlane.EnsureNamespace(name); err != nil {
		return nil, badEventHubRequest(err)
	}

	ns := &storedEHNamespace{
		name: name, resourceGroup: id.ResourceGroup, location: DefaultLocation, createdAt: time.Now().UTC(),
		tags: stringTags(body[fieldTags]), extra: ehExtra(body),
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		ns.location = loc
	} else if existed {
		ns.location = existing.location
	}

	if existed {
		ns.createdAt, ns.host = existing.createdAt, existing.host
	}

	if h := RequestHostFromContext(ctx); h != "" {
		ns.host = h
	}

	if sku, ok := body[ehFieldSKU].(map[string]any); ok {
		ns.sku = sku
	} else if existed {
		ns.sku = existing.sku
	}

	p.namespaces[key] = ns

	return p.namespaceBody(id, ns), nil
}

func (p *EventHubProvider) namespaceBody(id ResourceID, ns *storedEHNamespace) map[string]any {
	sku := ns.sku
	if len(sku) == 0 {
		sku = map[string]any{fieldName: skuTierStandard, cosmosFieldTier: skuTierStandard, "capacity": 1}
	}

	return map[string]any{
		"id": id.ARMID(), fieldName: ns.name, fieldType: namespaceMicrosoftEventHub + "/" + ehNamespacesType,
		fieldLocation: ns.location, fieldTags: tagsOrEmpty(ns.tags), ehFieldSKU: sku,
		fieldProperties: ehMerge(map[string]any{
			"isAutoInflateEnabled":   false,
			"maximumThroughputUnits": 0,
			"kafkaEnabled":           false,
			"zoneRedundant":          false,
			"disableLocalAuth":       false,
			"minimumTlsVersion":      "1.2",
			cosmosFieldPublicNetwork: kvPublicNetworkEnabled,
		}, ns.extra, map[string]any{
			fieldProvisioningState: provisioningStateSucceeded,
			ehPropStatus:           ehStatusActive,
			"serviceBusEndpoint":   p.restEndpoint(ns),
			"metricId":             id.SubscriptionID + ":" + strings.ToLower(ns.name),
			ehPropCreatedAt:        ehTime(ns.createdAt),
			ehPropUpdatedAt:        ehTime(ns.createdAt),
		}),
	}
}

func (p *EventHubProvider) listNamespaces(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, ns := range p.namespaces {
		if id.ResourceGroup != "" && !resourceGroupsEqual(ns.resourceGroup, id.ResourceGroup) {
			continue
		}

		nsID := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: ns.resourceGroup, Namespace: namespaceMicrosoftEventHub,
			Types: []string{ehNamespacesType}, Names: []string{ns.name},
		}
		out = append(out, p.namespaceBody(nsID, ns))
	}

	return out
}

func retentionOf(props map[string]any) time.Duration {
	if desc, ok := props[ehPropRetentionDesc].(map[string]any); ok {
		if hours, hok := desc["retentionTimeInHours"].(float64); hok && hours > 0 {
			return time.Duration(hours) * time.Hour
		}
	}

	if days, ok := props[ehPropRetentionDays].(float64); ok && days > 0 {
		return time.Duration(days) * ehHoursPerDay * time.Hour
	}

	return 0
}

func (p *EventHubProvider) putHub(id ResourceID, body map[string]any) (map[string]any, error) {
	if err := p.requireNamespace(id); err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	count, _ := props[ehPropPartitionCount].(float64)

	info, err := p.dataPlane.CreateHub(id.Names[0], id.LeafName(), int(count), retentionOf(props))
	if err != nil {
		return nil, badEventHubRequest(err)
	}

	h := &storedEHHub{
		name: id.LeafName(), namespace: id.Names[0], resourceGroup: id.ResourceGroup,
		extra: ehExtra(body, ehPropPartitionCount, ehPropRetentionDays, ehPropRetentionDesc, ehPropPartitionIDs,
			ehPropCreatedAt, ehPropUpdatedAt),
	}
	p.hubs[ehKey(id.ResourceGroup, id.Names[0], id.LeafName())] = h

	return hubBody(id, h, info), nil
}

func hubBody(id ResourceID, h *storedEHHub, info EventHubInfo) map[string]any {
	hours := int(info.Retention / time.Hour)
	days := int(math.Ceil(float64(hours) / ehHoursPerDay))

	return map[string]any{
		"id": id.ARMID(), fieldName: h.name, fieldType: ehTypeName(id), fieldLocation: "",
		fieldProperties: ehMerge(map[string]any{ehPropStatus: ehStatusActive}, h.extra, map[string]any{
			fieldProvisioningState: provisioningStateSucceeded,
			ehPropPartitionIDs:     info.PartitionIDs,
			ehPropPartitionCount:   len(info.PartitionIDs),
			ehPropRetentionDays:    max(days, 1),
			ehPropRetentionDesc: map[string]any{
				"cleanupPolicy": ehCleanupDelete, "retentionTimeInHours": hours,
			},
			ehPropCreatedAt: ehTime(info.CreatedAt),
			ehPropUpdatedAt: ehTime(info.CreatedAt),
		}),
	}
}

func (p *EventHubProvider) hubBodyFor(id ResourceID) (map[string]any, error) {
	h, err := p.requireHub(id)
	if err != nil {
		return nil, err
	}

	info, ok := p.dataPlane.HubInfo(id.Names[0], h.name)
	if !ok {
		return nil, ErrEventHubNotFound
	}

	return hubBody(id, h, info), nil
}

func (p *EventHubProvider) listHubs(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, h := range p.hubs {
		if id.ResourceGroup != "" && !resourceGroupsEqual(h.resourceGroup, id.ResourceGroup) {
			continue
		}

		if len(id.Names) > 0 && !strings.EqualFold(h.namespace, id.Names[0]) {
			continue
		}

		info, ok := p.dataPlane.HubInfo(h.namespace, h.name)
		if !ok {
			continue
		}

		hID := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: h.resourceGroup, Namespace: namespaceMicrosoftEventHub,
			Types: []string{ehNamespacesType, ehHubsType}, Names: []string{h.namespace, h.name},
		}
		out = append(out, hubBody(hID, h, info))
	}

	return out
}

func (p *EventHubProvider) deleteHub(rg, ns, name string) error {
	key := ehKey(rg, ns, name)
	if _, ok := p.hubs[key]; !ok {
		return ErrEventHubNotFound
	}

	for k, g := range p.groups {
		if resourceGroupsEqual(g.resourceGroup, rg) && strings.EqualFold(g.namespace, ns) &&
			strings.EqualFold(g.hub, name) {
			delete(p.groups, k)
		}
	}

	delete(p.hubs, key)

	return p.dataPlane.DeleteHub(ns, name)
}

func (p *EventHubProvider) putGroup(id ResourceID, body map[string]any) (map[string]any, error) {
	if _, err := p.requireHub(id); err != nil {
		return nil, err
	}

	if err := p.dataPlane.CreateConsumerGroup(id.Names[0], id.Names[1], id.LeafName()); err != nil {
		return nil, badEventHubRequest(err)
	}

	props, _ := body[fieldProperties].(map[string]any)
	meta, _ := props["userMetadata"].(string)

	g := &storedEHGroup{
		name:          id.LeafName(),
		hub:           id.Names[1],
		namespace:     id.Names[0],
		resourceGroup: id.ResourceGroup,
		userMetadata:  meta,
	}
	p.groups[ehKey(id.ResourceGroup, id.Names[0], id.Names[1], id.LeafName())] = g

	return p.groupBody(id, g), nil
}

func (p *EventHubProvider) groupBody(id ResourceID, g *storedEHGroup) map[string]any {
	created := time.Now().UTC()
	if info, ok := p.dataPlane.HubInfo(g.namespace, g.hub); ok {
		created = info.CreatedAt
	}

	props := map[string]any{
		fieldProvisioningState: provisioningStateSucceeded,
		ehPropCreatedAt:        ehTime(created),
		ehPropUpdatedAt:        ehTime(created),
	}
	if g.userMetadata != "" {
		props["userMetadata"] = g.userMetadata
	}

	return map[string]any{
		"id": id.ARMID(), fieldName: g.name, fieldType: ehTypeName(id), fieldLocation: "", fieldProperties: props,
	}
}

func (p *EventHubProvider) groupBodyFor(id ResourceID) (map[string]any, error) {
	if _, err := p.requireHub(id); err != nil {
		return nil, err
	}

	g, ok := p.groups[ehKey(id.ResourceGroup, id.Names[0], id.Names[1], id.LeafName())]
	if !ok {
		return nil, ErrEventHubConsumerGroupNotFound
	}

	return p.groupBody(id, g), nil
}

func (p *EventHubProvider) deleteGroup(id ResourceID) error {
	key := ehKey(id.ResourceGroup, id.Names[0], id.Names[1], id.LeafName())
	if _, ok := p.groups[key]; !ok {
		return ErrEventHubConsumerGroupNotFound
	}

	delete(p.groups, key)

	return p.dataPlane.DeleteConsumerGroup(id.Names[0], id.Names[1], id.LeafName())
}

func (p *EventHubProvider) listGroups(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, g := range p.groups {
		if id.ResourceGroup != "" && !resourceGroupsEqual(g.resourceGroup, id.ResourceGroup) {
			continue
		}

		if len(id.Names) > 1 &&
			(!strings.EqualFold(g.namespace, id.Names[0]) || !strings.EqualFold(g.hub, id.Names[1])) {
			continue
		}

		gID := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: g.resourceGroup, Namespace: namespaceMicrosoftEventHub,
			Types: []string{ehNamespacesType, ehHubsType, ehGroupsType}, Names: []string{g.namespace, g.hub, g.name},
		}
		out = append(out, p.groupBody(gID, g))
	}

	return out
}

// ruleScope returns the hub a rule id is scoped to ("" for namespace rules)
// after checking the parents exist.
func (p *EventHubProvider) ruleScope(id ResourceID) (string, error) {
	if classifyEventHubPath(id) == ehKindHubRule {
		h, err := p.requireHub(id)
		if err != nil {
			return "", err
		}

		return h.name, nil
	}

	return "", p.requireNamespace(id)
}

func (p *EventHubProvider) putRule(id ResourceID, body map[string]any) (map[string]any, error) {
	hub, err := p.ruleScope(id)
	if err != nil {
		return nil, err
	}

	rule, err := p.dataPlane.PutRule(id.Names[0], hub, id.LeafName(), ehRights(body))
	if err != nil {
		return nil, badEventHubRequest(err)
	}

	return ruleBody(id, rule), nil
}

func ruleBody(id ResourceID, r EventHubRule) map[string]any {
	return map[string]any{
		"id": id.ARMID(), fieldName: id.LeafName(), fieldType: ehTypeName(id), fieldLocation: "",
		fieldProperties: map[string]any{ehPropRights: r.Rights},
	}
}

func (p *EventHubProvider) ruleBodyFor(id ResourceID) (map[string]any, error) {
	hub, err := p.ruleScope(id)
	if err != nil {
		return nil, err
	}

	r, ok := p.dataPlane.GetRule(id.Names[0], hub, id.LeafName())
	if !ok {
		return nil, ErrEventHubRuleNotFound
	}

	return ruleBody(id, r), nil
}

func (p *EventHubProvider) deleteRule(id ResourceID) error {
	hub, err := p.ruleScope(id)
	if err != nil {
		return err
	}

	return p.dataPlane.DeleteRule(id.Names[0], hub, id.LeafName())
}

func (p *EventHubProvider) putNetworkRuleSet(id ResourceID, body map[string]any) (map[string]any, error) {
	if err := p.requireNamespace(id); err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	p.networkRuleSets[ehKey(id.ResourceGroup, id.Names[0])] = props

	return networkRuleSetBody(id, props), nil
}

func networkRuleSetBody(id ResourceID, props map[string]any) map[string]any {
	str := func(k, def string) string {
		if v, ok := props[k].(string); ok && v != "" {
			return v
		}

		return def
	}
	list := func(k string) []any {
		if v, ok := props[k].([]any); ok {
			return v
		}

		return []any{}
	}
	trusted, _ := props["trustedServiceAccessEnabled"].(bool)

	return map[string]any{
		"id": id.ARMID(), fieldName: "default", fieldType: ehTypeName(id), fieldLocation: "",
		fieldProperties: map[string]any{
			"defaultAction":               str("defaultAction", "Allow"),
			cosmosFieldPublicNetwork:      str(cosmosFieldPublicNetwork, kvPublicNetworkEnabled),
			"trustedServiceAccessEnabled": trusted,
			ehFieldIPRules:                list(ehFieldIPRules),
			ehFieldVNetRules:              list(ehFieldVNetRules),
		},
	}
}

// ListKeys implements ResourceProvider for .../authorizationRules/{rule}/listKeys.
func (p *EventHubProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	kind := classifyEventHubPath(id)
	if kind != ehKindNSRule && kind != ehKindHubRule {
		return nil, errUnsupportedEventHubType(id)
	}

	p.mu.RLock("ListKeys")
	defer p.mu.RUnlock()

	hub, err := p.ruleScope(id)
	if err != nil {
		return nil, err
	}

	r, ok := p.dataPlane.GetRule(id.Names[0], hub, id.LeafName())
	if !ok {
		return nil, ErrEventHubRuleNotFound
	}

	ns := p.namespaces[ehKey(id.ResourceGroup, id.Names[0])]

	return map[string]any{
		"primaryConnectionString":   p.connectionString(ns, id.LeafName(), r.PrimaryKey, hub),
		"secondaryConnectionString": p.connectionString(ns, id.LeafName(), r.SecondaryKey, hub),
		"primaryKey":                r.PrimaryKey,
		"secondaryKey":              r.SecondaryKey,
		fieldKeyName:                id.LeafName(),
	}, nil
}

func (p *EventHubProvider) connectionString(ns *storedEHNamespace, rule, key, hub string) string {
	cs := fmt.Sprintf("Endpoint=sb://%s/;SharedAccessKeyName=%s;SharedAccessKey=%s", p.amqpAddr(ns), rule, key)
	if hub != "" {
		cs += ";EntityPath=" + hub
	}

	return cs + ";UseDevelopmentEmulator=true"
}
