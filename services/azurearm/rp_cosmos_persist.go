package azurearm

import (
	"maps"
	"strings"
)

type cosmosAccountRecord struct {
	Properties    map[string]any    `json:"properties,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
	Identity      any               `json:"identity,omitempty"`
	Keys          cosmosKeys        `json:"keys"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Subscription  string            `json:"subscription"`
	Host          string            `json:"host,omitempty"`
	Kind          string            `json:"kind"`
	InstanceID    string            `json:"instanceId"`
}

// cosmosSnapshot is the Microsoft.DocumentDB provider's persisted ARM-side state. Databases and containers
// persist with the Cosmos DB data plane.
type cosmosSnapshot struct {
	Accounts   map[string]cosmosAccountRecord   `json:"accounts,omitempty"`
	DBOwner    map[string]string                `json:"dbOwner,omitempty"`
	Throughput map[string]cosmosThroughput      `json:"throughput,omitempty"`
	Extras     map[string]cosmosContainerExtras `json:"extras,omitempty"`
}

func (a *storedCosmosAccount) record() cosmosAccountRecord {
	return cosmosAccountRecord{
		Properties: a.properties, Tags: a.tags, Identity: a.identity, Keys: a.keys, Name: a.name,
		ResourceGroup: a.resourceGroup, Location: a.location, Subscription: a.subscription, Host: a.host,
		Kind: a.kind, InstanceID: a.instanceID,
	}
}

func (r cosmosAccountRecord) stored() *storedCosmosAccount {
	props := r.Properties
	if props == nil {
		props = map[string]any{}
	}

	return &storedCosmosAccount{
		properties: props, tags: r.Tags, identity: r.Identity, keys: r.Keys, name: r.Name,
		resourceGroup: r.ResourceGroup, location: r.Location, subscription: r.Subscription, host: r.Host,
		kind: r.Kind, instanceID: r.InstanceID,
	}
}

func (p *CosmosProvider) snapshot() *cosmosSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &cosmosSnapshot{
		Accounts:   make(map[string]cosmosAccountRecord, len(p.accounts)),
		DBOwner:    make(map[string]string, len(p.dbOwner)),
		Throughput: make(map[string]cosmosThroughput, len(p.throughput)),
		Extras:     make(map[string]cosmosContainerExtras, len(p.extras)),
	}

	for k, a := range p.accounts {
		snap.Accounts[k] = a.record()
	}

	maps.Copy(snap.DBOwner, p.dbOwner)

	maps.Copy(snap.Throughput, p.throughput)

	maps.Copy(snap.Extras, p.extras)

	return snap
}

// restore replaces the provider's state; nil clears it. Restored keys are re-authorized with the data plane.
func (p *CosmosProvider) restore(snap *cosmosSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.accounts = make(map[string]*storedCosmosAccount)
	p.dbOwner = make(map[string]string)
	p.throughput = make(map[string]cosmosThroughput)
	p.extras = make(map[string]cosmosContainerExtras)

	if snap == nil {
		return
	}

	for k, r := range snap.Accounts {
		a := r.stored()
		p.accounts[strings.ToLower(k)] = a
		p.authorizeKeys(a.keys)
	}

	maps.Copy(p.dbOwner, snap.DBOwner)

	maps.Copy(p.throughput, snap.Throughput)

	maps.Copy(p.extras, snap.Extras)
}
