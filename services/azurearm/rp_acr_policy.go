package azurearm

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	acrActionListUsages = "listUsages"
	acrPolicyExport     = "exportPolicy"
	acrFieldPolicies    = "policies"
	acrUnitBytes        = "Bytes"
	acrUnitCount        = "Count"
	acrBytesPerGiB      = int64(1) << 30
)

var (
	errACRExportPremium = errors.New("exportPolicy can only be disabled on a Premium registry")
	errACRExportPublic  = errors.New("publicNetworkAccess must be Disabled to disable exportPolicy")
)

// acrMergePolicies keeps the policies a PATCH does not mention, as ARM merges nested objects.
func acrMergePolicies(prev, next map[string]any) {
	merged := maps.Clone(mapOf(prev[acrFieldPolicies]))
	if merged == nil {
		merged = map[string]any{}
	}

	for name, v := range mapOf(next[acrFieldPolicies]) {
		cur := maps.Clone(mapOf(merged[name]))
		if cur == nil {
			cur = map[string]any{}
		}

		maps.Copy(cur, mapOf(v))
		merged[name] = cur
	}

	if len(merged) > 0 {
		next[acrFieldPolicies] = merged
	}
}

// syncPolicies tells the data plane whether artifacts may be exported; disabling needs a Premium registry with
// public network access disabled (Learn: data-loss-prevention).
func (p *ContainerRegistryProvider) syncPolicies(rec *storedACR) error {
	policy := mapOf(mapOf(rec.extra[acrFieldPolicies])[acrPolicyExport])
	disabled := strings.EqualFold(stringField(policy, acrPropStatus), acrStatusDisabled)

	switch {
	case disabled && rec.sku != acrSkuPremium:
		return badACRRequest(errACRExportPremium)
	case disabled && !strings.EqualFold(stringField(rec.extra, cosmosFieldPublicNetwork), acrStatusDisabled):
		return badACRRequest(errACRExportPublic)
	}

	return acrBadOrNil(p.dataPlane.SetExportAllowed(rec.name, !disabled))
}

// acrSKULimits are the included storage in GiB and the webhook cap of each SKU (Learn container-registry-skus).
func acrSKULimits(sku string) (int64, int64) {
	switch sku {
	case acrSkuPremium:
		return acrPremiumGiB, acrPremiumWebhooks
	case aciSKUStandard:
		return acrStandardGiB, acrStandardWebhooks
	}

	return acrBasicGiB, acrBasicWebhooks
}

const (
	acrPremiumGiB       = 500
	acrStandardGiB      = 100
	acrBasicGiB         = 10
	acrPremiumWebhooks  = 500
	acrStandardWebhooks = 10
	acrBasicWebhooks    = 2
	acrFieldCurrent     = "currentValue"
	acrFieldLimit       = "limit"
	acrFieldUnit        = "unit"
	acrReplicaLimit     = 100
	acrIPRuleLimit      = 200
	acrVNetLimit        = 100
)

func acrUsage(name string, limit, current int64, unit string) map[string]any {
	return map[string]any{fieldName: name, acrFieldLimit: limit, acrFieldCurrent: current, acrFieldUnit: unit}
}

// ListUsages answers GET .../registries/{name}/listUsages: stored bytes and the counts of webhooks and, on
// Premium, replicas, IP rules and virtual network rules, with the SKU limits of Learn container-registry-skus.
func (p *ContainerRegistryProvider) ListUsages(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("ListUsages")
	rec, ok := p.registry(id.ResourceGroup, id.LeafName())

	if !ok {
		p.mu.RUnlock()

		return nil, ErrACRRegistryNotFound
	}

	sku, name := rec.sku, rec.name
	rules := mapOf(rec.extra["networkRuleSet"])
	ipRules, _ := rules[ehFieldIPRules].([]any)
	vnetRules, _ := rules["virtualNetworkRules"].([]any)

	var webhooks, replicas int64

	for _, child := range p.res {
		if !strings.EqualFold(child.registry, rec.name) ||
			!resourceGroupsEqual(child.resourceGroup, rec.resourceGroup) {
			continue
		}

		switch child.kind {
		case acrKindWebhook:
			webhooks++
		case acrKindReplication:
			replicas++
		case acrKindUnsupported, acrKindRegistry, acrKindScopeMap, acrKindToken, acrKindCacheRule,
			acrKindCredentialSet:
		}
	}

	p.mu.RUnlock()

	used, err := p.dataPlane.UsedBytes(name)
	if err != nil {
		return nil, err
	}

	gib, hooks := acrSKULimits(sku)
	usages := []any{
		acrUsage("Size", gib*acrBytesPerGiB, used, acrUnitBytes),
		acrUsage("Webhooks", hooks, webhooks, acrUnitCount),
	}

	if sku == acrSkuPremium {
		usages = append(usages,
			acrUsage("Geo-replications", acrReplicaLimit, replicas+1, acrUnitCount),
			acrUsage("IPRules", acrIPRuleLimit, int64(len(ipRules)), acrUnitCount),
			acrUsage("VNetRules", acrVNetLimit, int64(len(vnetRules)), acrUnitCount),
		)
	}

	return map[string]any{fieldValue: usages}, nil
}

func (h *Handler) handleACRListUsages(c *echo.Context, segs []string) error {
	p, err := h.acrProviderOrNotFound(c)
	if p == nil {
		return err
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.ListUsages(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}
