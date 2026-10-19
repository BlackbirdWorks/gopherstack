package azurearm

import (
	"strings"
	"time"
)

type keyVaultRecord struct {
	Properties    map[string]any    `json:"properties,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Host          string            `json:"host,omitempty"`
	Subscription  string            `json:"subscription"`
}

type deletedKeyVaultRecord struct {
	DeletionDate       time.Time      `json:"deletionDate"`
	ScheduledPurgeDate time.Time      `json:"scheduledPurgeDate"`
	Vault              keyVaultRecord `json:"vault"`
}

// keyVaultSnapshot is the Microsoft.KeyVault provider's persisted ARM-side state.
type keyVaultSnapshot struct {
	Vaults  map[string]keyVaultRecord        `json:"vaults,omitempty"`
	Deleted map[string]deletedKeyVaultRecord `json:"deleted,omitempty"`
}

func recordOf(v *storedKeyVault) keyVaultRecord {
	return keyVaultRecord{
		Properties: v.properties, Tags: v.tags, Name: v.name, ResourceGroup: v.resourceGroup,
		Location: v.location, Host: v.host, Subscription: v.subscription,
	}
}

func (r keyVaultRecord) stored() storedKeyVault {
	props := r.Properties
	if props == nil {
		props = map[string]any{}
	}

	return storedKeyVault{
		properties: props, tags: r.Tags, name: r.Name, resourceGroup: r.ResourceGroup,
		location: r.Location, host: r.Host, subscription: r.Subscription,
	}
}

func (p *KeyVaultProvider) snapshot() *keyVaultSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &keyVaultSnapshot{
		Vaults:  make(map[string]keyVaultRecord, len(p.vaults)),
		Deleted: make(map[string]deletedKeyVaultRecord, len(p.deleted)),
	}

	for k, v := range p.vaults {
		snap.Vaults[k] = recordOf(v)
	}

	for k, d := range p.deleted {
		snap.Deleted[k] = deletedKeyVaultRecord{
			DeletionDate: d.deletionDate, ScheduledPurgeDate: d.scheduledPurge, Vault: recordOf(&d.vault),
		}
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *KeyVaultProvider) restore(snap *keyVaultSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.vaults = make(map[string]*storedKeyVault)
	p.deleted = make(map[string]*deletedKeyVault)

	if snap == nil {
		return
	}

	for k, r := range snap.Vaults {
		v := r.stored()
		p.vaults[strings.ToLower(k)] = &v
	}

	for k, d := range snap.Deleted {
		p.deleted[strings.ToLower(k)] = &deletedKeyVault{
			deletionDate: d.DeletionDate, scheduledPurge: d.ScheduledPurgeDate, vault: d.Vault.stored(),
		}
	}
}
