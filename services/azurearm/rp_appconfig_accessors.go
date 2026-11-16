package azurearm

import "strings"

// AppConfigStoreSettings are the ARM-held settings of a configuration store that the data plane honours.
type AppConfigStoreSettings struct {
	SKU                 string
	PublicNetworkAccess string
}

// StoreSettings returns the SKU and public network access of the named configuration store.
func (p *AppConfigProvider) StoreSettings(name string) (AppConfigStoreSettings, bool) {
	p.mu.RLock("StoreSettings")
	defer p.mu.RUnlock()

	for _, rec := range p.res {
		if rec.kind != acKindStore || !strings.EqualFold(rec.name, name) {
			continue
		}

		access, _ := rec.extra["publicNetworkAccess"].(string)

		return AppConfigStoreSettings{SKU: rec.sku, PublicNetworkAccess: access}, true
	}

	return AppConfigStoreSettings{}, false
}

// ReplicaParent resolves a replica endpoint label ("<store>-<replica>") to the store it replicates.
func (p *AppConfigProvider) ReplicaParent(label string) (string, bool) {
	p.mu.RLock("ReplicaParent")
	defer p.mu.RUnlock()

	for _, rec := range p.res {
		if rec.kind == acKindReplica && strings.EqualFold(rec.store+"-"+rec.name, label) {
			return rec.store, true
		}
	}

	return "", false
}
