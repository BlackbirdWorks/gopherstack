package azurearm

import (
	"context"
	"maps"
	"sort"
	"strings"
	"time"
)

func acEnvelope(id ResourceID, rec *storedAC, typ string, props map[string]any) map[string]any {
	return map[string]any{
		"id": id.ARMID(), fieldName: rec.name, fieldType: typ, fieldProperties: props,
	}
}

func (p *AppConfigProvider) storeBody(id ResourceID, rec *storedAC) map[string]any {
	softDelete := acSoftDeleteDays
	if rec.sku == acSkuFree {
		softDelete = 0
	}

	props := ehMerge(map[string]any{
		acPropLocalAuth: false, "softDeleteRetentionInDays": softDelete, acPropPurgeProtection: false,
	}, rec.extra, map[string]any{
		fieldProvisioningState: provisioningStateSucceeded,
		acPropCreationDate:     rec.createdAt.UTC().Format(time.RFC3339),
		acPropEndpoint:         p.endpointFor(rec.name, rec.host),
	})

	out := acEnvelope(id, rec, namespaceMicrosoftAppConfig+"/"+acStoresType, props)
	out[fieldLocation], out[fieldTags] = rec.location, tagsOrEmpty(rec.tags)
	out[ehFieldSKU] = map[string]any{fieldName: rec.sku}

	if rec.identity != nil {
		ident := maps.Clone(rec.identity)
		if t, _ := ident[fieldType].(string); strings.Contains(strings.ToLower(t), "systemassigned") {
			ident["principalId"], ident["tenantId"] = p.principalID(id), DefaultTenantID
		}

		out[acFieldIdentity] = ident
	}

	return out
}

func (p *AppConfigProvider) replicaBody(id ResourceID, rec *storedAC) map[string]any {
	props := ehMerge(map[string]any{}, rec.extra, map[string]any{
		fieldProvisioningState: provisioningStateSucceeded,
		acPropEndpoint:         p.endpointFor(rec.store+"-"+rec.name, rec.host),
	})

	out := acEnvelope(id, rec, namespaceMicrosoftAppConfig+"/"+acStoresType+"/"+acReplicasType, props)
	out[fieldLocation] = rec.location

	return out
}

// ListReplicas lists the replicas of the store addressed by storeID.
func (p *AppConfigProvider) ListReplicas(_ context.Context, storeID ResourceID) ([]map[string]any, error) {
	p.mu.RLock("ListReplicas")
	defer p.mu.RUnlock()

	if _, ok := p.store(storeID.ResourceGroup, storeID.LeafName()); !ok {
		return nil, ErrAppConfigStoreNotFound
	}

	out := []map[string]any{}

	for _, rec := range p.res {
		if rec.kind != acKindReplica || !strings.EqualFold(rec.store, storeID.LeafName()) ||
			!resourceGroupsEqual(rec.resourceGroup, storeID.ResourceGroup) {
			continue
		}

		rid := storeID
		rid.Types, rid.Names = []string{acStoresType, acReplicasType}, []string{storeID.LeafName(), rec.name}
		out = append(out, p.replicaBody(rid, rec))
	}

	sortByName(out)

	return out, nil
}

func (p *AppConfigProvider) keyBody(storeName string, k AppConfigKey, host string) map[string]any {
	conn := "Endpoint=" + p.endpointFor(storeName, host) + ";Id=" + k.ID + ";Secret=" + k.Value

	return map[string]any{
		"id": k.ID, fieldName: k.Name, fieldValue: k.Value, "connectionString": conn,
		"lastModified": k.LastModified.UTC().Format(time.RFC3339), "readOnly": k.ReadOnly,
	}
}

// ListKeys implements ResourceProvider for .../configurationStores/{name}/listKeys.
func (p *AppConfigProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("ListKeys")
	defer p.mu.RUnlock()

	rec, keys, err := p.keysOf(id)
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, p.keyBody(rec.name, k, rec.host))
	}

	return map[string]any{fieldValue: out}, nil
}

func (p *AppConfigProvider) keysOf(id ResourceID) (*storedAC, []AppConfigKey, error) {
	if classifyAppConfigPath(id) != acKindStore {
		return nil, nil, errUnsupportedAppConfigType(id)
	}

	rec, ok := p.store(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, nil, ErrAppConfigStoreNotFound
	}

	keys, err := p.dataPlane.StoreKeys(rec.name)

	return rec, keys, err
}

// RegenerateKey replaces the access key with the given id and returns it.
func (p *AppConfigProvider) RegenerateKey(_ context.Context, id ResourceID, keyID string) (map[string]any, error) {
	if keyID == "" {
		return nil, badAppConfigRequest(ErrInvalidRequestBody)
	}

	p.mu.RLock("RegenerateKey")
	defer p.mu.RUnlock()

	rec, ok := p.store(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrAppConfigStoreNotFound
	}

	k, err := p.dataPlane.RegenerateStoreKey(rec.name, keyID)
	if err != nil {
		return nil, badAppConfigRequest(err)
	}

	return p.keyBody(rec.name, k, rec.host), nil
}

func sortByName(list []map[string]any) {
	sort.Slice(list, func(i, j int) bool { return nameField(list[i]) < nameField(list[j]) })
}
