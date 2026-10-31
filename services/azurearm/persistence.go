package azurearm

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// armSnapshotVersion identifies the shape of armSnapshot. Must be bumped
// whenever a change to ResourceGroup/Resource would make an older snapshot
// unsafe to decode as the current shape; Restore discards (rather than
// partially decodes) any mismatch, mirroring services/cosmosdb.
const armSnapshotVersion = 1

// armSnapshot is the top-level on-disk shape for the ARM backend. It stores
// ONLY ARM-side metadata (resource groups, generic resources, provider
// registration state) -- never a second copy of any data-plane service's own
// state (AZURE.md section 10.9). Storage accounts (owned by rp_storage.go's
// StorageProvider, not InMemoryBackend) are NOT YET included in
// Handler.Snapshot/Restore below -- an ARM-created storage account does not
// currently survive a snapshot/restore cycle; see services/azurearm/PARITY.md's
// persist=partial note on StorageAccountCRUD for this known gap.
type armSnapshot struct {
	ResourceGroups      map[string]*ResourceGroup  `json:"resourceGroups"`
	Resources           map[string]*Resource       `json:"resources"`
	RegisteredProviders map[string]map[string]bool `json:"registeredProviders"`
	KeyVault            *keyVaultSnapshot          `json:"keyVault,omitempty"`
	Cosmos              *cosmosSnapshot            `json:"cosmos,omitempty"`
	EventHub            *eventHubSnapshot          `json:"eventHub,omitempty"`
	EventGrid           *eventGridSnapshot         `json:"eventGrid,omitempty"`
	AppConfig           *appConfigSnapshot         `json:"appConfig,omitempty"`
	ContainerRegistry   *containerRegistrySnapshot `json:"containerRegistry,omitempty"`
	Redis               *redisSnapshot             `json:"redis,omitempty"`
	SQL                 *sqlSnapshot               `json:"sql,omitempty"`
	PostgresFlex        *flexSnapshot              `json:"postgresFlex,omitempty"`
	MySQLFlex           *flexSnapshot              `json:"mysqlFlex,omitempty"`
	Web                 *webSnapshot               `json:"web,omitempty"`
	ContainerInstance   *containerInstanceSnapshot `json:"containerInstance,omitempty"`
	Network             *networkSnapshot           `json:"network,omitempty"`
	ManagedIdentity     *managedIdentitySnapshot   `json:"managedIdentity,omitempty"`
	Compute             *computeSnapshot           `json:"compute,omitempty"`
	ContainerApps       *containerAppsSnapshot     `json:"containerApps,omitempty"`
	ContainerService    *containerServiceSnapshot  `json:"containerService,omitempty"`
	APIManagement       *apimSnapshot              `json:"apiManagement,omitempty"`
	Authorization       *authorizationSnapshot     `json:"authorization,omitempty"`
	Version             int                        `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	return persistence.MarshalSnapshot(ctx, "azurearm", b.snapshotState(nil, nil, nil, nil, nil, nil))
}

func (b *InMemoryBackend) snapshotState(
	kv *keyVaultSnapshot,
	cosmos *cosmosSnapshot,
	eh *eventHubSnapshot,
	eg *eventGridSnapshot,
	ac *appConfigSnapshot,
	cr *containerRegistrySnapshot,
) armSnapshot {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return armSnapshot{
		KeyVault:            kv,
		Cosmos:              cosmos,
		EventHub:            eh,
		EventGrid:           eg,
		AppConfig:           ac,
		ContainerRegistry:   cr,
		Version:             armSnapshotVersion,
		ResourceGroups:      b.resourceGroups,
		Resources:           b.resources,
		RegisteredProviders: b.registeredProviders,
	}
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable. Restore must be idempotent against the data
// plane (AZURE.md section 10.9): this method only ever restores ARM's own
// metadata, never re-creates or otherwise touches any data-plane service's
// state.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	_, err := b.restore(ctx, data)

	return err
}

func (b *InMemoryBackend) restore(ctx context.Context, data []byte) (armSnapshot, error) {
	var snap armSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azurearm", data, &snap); err != nil {
		return snap, err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != armSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"azurearm: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", armSnapshotVersion)

		b.resourceGroups = make(map[string]*ResourceGroup)
		b.resources = make(map[string]*Resource)
		b.byType.Store(nil)
		b.registeredProviders = make(map[string]map[string]bool)

		return armSnapshot{}, nil
	}

	if snap.ResourceGroups == nil {
		snap.ResourceGroups = make(map[string]*ResourceGroup)
	}

	if snap.Resources == nil {
		snap.Resources = make(map[string]*Resource)
	}

	if snap.RegisteredProviders == nil {
		snap.RegisteredProviders = make(map[string]map[string]bool)
	}

	if err := validateSnapshotResourceGroups(snap.ResourceGroups); err != nil {
		return snap, err
	}

	if err := validateSnapshotResources(snap.Resources); err != nil {
		return snap, err
	}

	b.resourceGroups = snap.ResourceGroups
	b.resources = snap.Resources
	b.byType.Store(nil)
	b.registeredProviders = snap.RegisteredProviders

	return snap, nil
}

// validateSnapshotResourceGroups rejects a snapshot whose "resourceGroups"
// map holds a JSON null entry, which decodes to a nil pointer that would
// panic on first dereference if stored as-is (same class of bug
// services/cosmosdb's persistence.go guards against).
func validateSnapshotResourceGroups(groups map[string]*ResourceGroup) error {
	for name, g := range groups {
		if g == nil {
			return fmt.Errorf("%w: %q", ErrSnapshotResourceGroupNull, name)
		}
	}

	return nil
}

func validateSnapshotResources(resources map[string]*Resource) error {
	for key, r := range resources {
		if r == nil {
			return fmt.Errorf("%w: %q", ErrSnapshotResourceNull, key)
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable: backend state plus the
// Microsoft.KeyVault provider's ARM-side vaults and deleted vaults.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	var kv *keyVaultSnapshot
	if p, ok := h.keyVaultProvider(); ok {
		kv = p.snapshot()
	}

	var cosmos *cosmosSnapshot
	if p, ok := h.cosmosProvider(); ok {
		cosmos = p.snapshot()
	}

	var eh *eventHubSnapshot
	if p, ok := h.eventHubProvider(); ok {
		eh = p.snapshot()
	}

	var eg *eventGridSnapshot
	if p, ok := h.eventGridProvider(); ok {
		eg = p.snapshot()
	}

	var ac *appConfigSnapshot
	if p, ok := h.appConfigProvider(); ok {
		ac = p.snapshot()
	}

	var cr *containerRegistrySnapshot
	if p, ok := h.containerRegistryProvider(); ok {
		cr = p.snapshot()
	}

	st := h.Backend.snapshotState(kv, cosmos, eh, eg, ac, cr)
	if p, ok := h.redisProvider(); ok {
		st.Redis = p.snapshot()
	}

	if p, ok := h.sqlProvider(); ok {
		st.SQL = p.snapshot()
	}

	if p, ok := h.postgresFlexProvider(); ok {
		st.PostgresFlex = p.snapshot()
	}

	if p, ok := h.mysqlFlexProvider(); ok {
		st.MySQLFlex = p.snapshot()
	}

	if p, ok := h.webProvider(); ok {
		st.Web = p.snapshot()
	}

	if p, ok := h.containerInstanceProvider(); ok {
		st.ContainerInstance = p.snapshot()
	}

	h.snapshotInfra(&st)

	return persistence.MarshalSnapshot(ctx, "azurearm", st)
}

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	snap, err := h.Backend.restore(ctx, data)
	if err != nil {
		return fmt.Errorf("azurearm: restore snapshot: %w", err)
	}

	if p, ok := h.keyVaultProvider(); ok {
		p.restore(snap.KeyVault)
	}

	if p, ok := h.cosmosProvider(); ok {
		p.restore(snap.Cosmos)
	}

	if p, ok := h.eventHubProvider(); ok {
		p.restore(snap.EventHub)
	}

	if p, ok := h.eventGridProvider(); ok {
		p.restore(snap.EventGrid)
	}

	if p, ok := h.appConfigProvider(); ok {
		p.restore(snap.AppConfig)
	}

	if p, ok := h.containerRegistryProvider(); ok {
		p.restore(snap.ContainerRegistry)
	}

	if p, ok := h.redisProvider(); ok {
		p.restore(snap.Redis)
	}

	if p, ok := h.sqlProvider(); ok {
		p.restore(snap.SQL)
	}

	if p, ok := h.postgresFlexProvider(); ok {
		p.restore(snap.PostgresFlex)
	}

	if p, ok := h.mysqlFlexProvider(); ok {
		p.restore(snap.MySQLFlex)
	}

	if p, ok := h.webProvider(); ok {
		p.restore(snap.Web)
	}

	if p, ok := h.containerInstanceProvider(); ok {
		p.restore(snap.ContainerInstance)
	}

	h.restoreInfra(snap)

	return nil
}

// restoreInfra restores the network, identity, compute and container apps providers.
func (h *Handler) restoreInfra(snap armSnapshot) {
	if p, ok := h.networkProvider(); ok {
		p.restore(snap.Network)
	}

	if p, ok := h.managedIdentityProvider(); ok {
		p.restore(snap.ManagedIdentity)
	}

	if p, ok := h.computeProvider(); ok {
		p.restore(snap.Compute)
	}

	if p, ok := h.containerAppsProvider(); ok {
		p.restore(snap.ContainerApps)
	}

	if p, ok := h.containerServiceProvider(); ok {
		p.restore(snap.ContainerService)
	}

	if p, ok := h.apimProvider(); ok {
		p.restore(snap.APIManagement)
	}

	if p, ok := h.authorizationProvider(); ok {
		p.restore(snap.Authorization)
	}
}

// snapshotInfra adds the network, identity, compute, container apps and container service providers.
func (h *Handler) snapshotInfra(st *armSnapshot) {
	if p, ok := h.networkProvider(); ok {
		st.Network = p.snapshot()
	}

	if p, ok := h.managedIdentityProvider(); ok {
		st.ManagedIdentity = p.snapshot()
	}

	if p, ok := h.computeProvider(); ok {
		st.Compute = p.snapshot()
	}

	if p, ok := h.containerAppsProvider(); ok {
		st.ContainerApps = p.snapshot()
	}

	if p, ok := h.containerServiceProvider(); ok {
		st.ContainerService = p.snapshot()
	}

	if p, ok := h.apimProvider(); ok {
		st.APIManagement = p.snapshot()
	}

	if p, ok := h.authorizationProvider(); ok {
		st.Authorization = p.snapshot()
	}
}
