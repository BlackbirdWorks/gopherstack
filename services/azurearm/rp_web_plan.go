package azurearm

import (
	"maps"
	"strings"
	"time"
)

func webPlanSKU(body map[string]any, fallback map[string]any) map[string]any {
	if sku, ok := body["sku"].(map[string]any); ok && len(sku) > 0 {
		return sku
	}

	if fallback != nil {
		return fallback
	}

	return map[string]any{
		fieldName: webPlanSKUDefault, "tier": "Dynamic", "size": webPlanSKUDefault, "family": "Y", "capacity": 0,
	}
}

func (p *WebProvider) putPlan(id ResourceID, body map[string]any) (map[string]any, error) {
	if !validWebName(id.LeafName()) {
		return nil, badWebRequest(errWebPlanName)
	}

	key := planKey(id.ResourceGroup, id.LeafName())
	existing, exists := p.plans[key]

	props, _ := body[fieldProperties].(map[string]any)
	extra := maps.Clone(props)

	if extra == nil {
		extra = map[string]any{}
	}

	for _, k := range []string{fieldProvisioningState, "status", "numberOfSites", "name", "resourceGroup", "subscription"} {
		delete(extra, k)
	}

	kind, _ := body[webFieldKind].(string)

	rec := &storedWebPlan{
		Name: id.LeafName(), ResourceGroup: id.ResourceGroup, Location: DefaultLocation, Kind: kind,
		Created: time.Now().UTC(), Tags: stringTags(body[fieldTags]), Extra: extra,
	}

	var fallback map[string]any

	if exists {
		rec.Created, rec.Location, fallback = existing.Created, existing.Location, existing.SKU
		if kind == "" {
			rec.Kind = existing.Kind
		}
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.Location = loc
	}

	rec.SKU = webPlanSKU(body, fallback)
	p.plans[key] = rec

	return p.planBody(id, rec), nil
}

func (p *WebProvider) planSiteCount(id ResourceID) int {
	n := 0

	for _, s := range p.sites {
		farm, _ := s.Extra[webFieldServerFarmID].(string)
		if strings.EqualFold(farm, id.ARMID()) {
			n++
		}
	}

	return n
}

func (p *WebProvider) planBody(id ResourceID, pl *storedWebPlan) map[string]any {
	tier, _ := pl.SKU["tier"].(string)
	reserved, _ := pl.Extra[webFieldReserved].(bool)

	props := map[string]any{
		fieldName: pl.Name, "status": "Ready", "subscription": id.SubscriptionID, "geoRegion": pl.Location,
		"numberOfSites": p.planSiteCount(id), "maximumNumberOfWorkers": 200, "numberOfWorkers": 1, "isSpot": false,
		webFieldReserved: reserved || strings.Contains(strings.ToLower(pl.Kind), "linux"), "isXenon": false, "hyperV": false,
		"resourceGroup": pl.ResourceGroup, "perSiteScaling": false, "elasticScaleEnabled": strings.EqualFold(tier, "ElasticPremium"),
		"zoneRedundant": false, "targetWorkerCount": 0, "targetWorkerSizeId": 0,
		"workerTierName": nil, fieldProvisioningState: provisioningStateSucceeded,
		"webSpace": pl.ResourceGroup + "-" + pl.Location,
	}

	out := map[string]any{
		"id": id.ARMID(), fieldName: pl.Name, fieldType: namespaceMicrosoftWeb + "/" + webPlansType,
		fieldLocation: pl.Location, fieldTags: tagsOrEmpty(pl.Tags), "sku": pl.SKU,
		fieldProperties: ehMerge(props, pl.Extra, nil),
	}

	if pl.Kind != "" {
		out[webFieldKind] = pl.Kind
	}

	return out
}
