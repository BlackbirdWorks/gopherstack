package azurearm

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftApp = "Microsoft.App"
	acaEnvType            = "managedEnvironments"
	acaStorageType        = "storages"
	acaAppType            = "containerApps"
	acaRevisionType       = "revisions"
	acaReplicaType        = "replicas"
	acaJobType            = "jobs"
	acaExecutionType      = "executions"
	acaDomainSuffix       = "azurecontainerapps.io"
	acaEnvDepth           = 1
	acaChildDepth         = 2
	acaReplicaDepth       = 3
)

func acaAPIVersions() []string { return []string{"2025-07-01"} }

type acaKind int

const (
	acaKindOther acaKind = iota
	acaKindEnv
	acaKindStorage
	acaKindApp
	acaKindRevision
	acaKindReplica
	acaKindJob
	acaKindExecution
)

// acaTarget is what a ResourceID addresses: its kind, its names and whether it lists a collection.
type acaTarget struct {
	names   []string
	kind    acaKind
	listing bool
}

func acaTargetOf(id ResourceID) acaTarget {
	t := acaTarget{names: id.Names, listing: len(id.Names) < len(id.Types)}

	types := make([]string, 0, len(id.Types))

	for _, s := range id.Types {
		types = append(types, strings.ToLower(s))
	}

	switch {
	case acaTypesAre(types, acaEnvType):
		t.kind = acaKindEnv
	case acaTypesAre(types, acaEnvType, acaStorageType):
		t.kind = acaKindStorage
	case acaTypesAre(types, acaAppType):
		t.kind = acaKindApp
	case acaTypesAre(types, acaAppType, acaRevisionType):
		t.kind = acaKindRevision
	case acaTypesAre(types, acaAppType, acaRevisionType, acaReplicaType):
		t.kind = acaKindReplica
	case acaTypesAre(types, acaJobType):
		t.kind = acaKindJob
	case acaTypesAre(types, acaJobType, acaExecutionType):
		t.kind = acaKindExecution
	}

	return t
}

func acaTypesAre(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}

	for i, w := range want {
		if got[i] != strings.ToLower(w) {
			return false
		}
	}

	return true
}

func acaKey(rg, name string) string { return strings.ToLower(rg) + "/" + strings.ToLower(name) }

func acaRegion(loc string) string { return strings.ToLower(strings.ReplaceAll(loc, " ", "")) }

// KeyVaultSecrets reads secret values for container app secrets that reference Key Vault.
type KeyVaultSecrets interface {
	SecretValue(vault, name, version string) (string, error)
}

// ContainerAppsProvider implements ResourceProvider for Microsoft.App. ARM keeps the configuration; revisions run as
// replicas in the container engine shared with Container Instances.
type ContainerAppsProvider struct {
	now     func() time.Time
	vault   KeyVaultSecrets
	backend *InMemoryBackend
	net     *NetworkProvider
	ids     *identityService
	mu      *lockmetrics.RWMutex
	opMu    *lockmetrics.RWMutex
	envs    map[string]*storedACAEnv
	apps    map[string]*storedACAApp
	jobs    map[string]*storedACAJob
	hosts   map[string]acaHostTarget
	rt      *acaRuntime
}

// NewContainerAppsProvider creates a provider that runs replicas in engine. backend serves the Microsoft.App types
// this provider does not model; network and ids may be nil.
func NewContainerAppsProvider(
	engine *ACIEngine, backend *InMemoryBackend, network *NetworkProvider, ids *identityService,
) *ContainerAppsProvider {
	return &ContainerAppsProvider{
		now: time.Now, backend: backend, net: network, ids: ids,
		mu: lockmetrics.New("azurearm.containerapps"), opMu: lockmetrics.New("azurearm.containerapps.ops"),
		envs: map[string]*storedACAEnv{}, apps: map[string]*storedACAApp{}, jobs: map[string]*storedACAJob{},
		hosts: map[string]acaHostTarget{}, rt: newACARuntime(engine),
	}
}

var _ ResourceProvider = (*ContainerAppsProvider)(nil)

// Namespace implements ResourceProvider.
func (p *ContainerAppsProvider) Namespace() string { return namespaceMicrosoftApp }

// ResourceTypes implements ResourceProvider.
func (p *ContainerAppsProvider) ResourceTypes() []ResourceTypeDef {
	v := acaAPIVersions()

	return []ResourceTypeDef{
		{Type: acaEnvType, APIVersions: v, HasChildren: true},
		{Type: acaAppType, APIVersions: v, HasChildren: true},
		{Type: acaJobType, APIVersions: v, HasChildren: true},
	}
}

// SetKeyVaultSecrets wires the Key Vault used to resolve keyVaultUrl secrets.
func (p *ContainerAppsProvider) SetKeyVaultSecrets(v KeyVaultSecrets) {
	p.mu.Lock("SetKeyVaultSecrets")
	defer p.mu.Unlock()

	p.vault = v
}

// SetClock replaces the clock that drives scale-to-zero; for tests.
func (p *ContainerAppsProvider) SetClock(c ACAClock) { p.rt.setClock(c) }

// SetTrafficPicker replaces the source of the 0..n-1 draw that splits traffic between revisions; for tests.
func (p *ContainerAppsProvider) SetTrafficPicker(fn func(n int) int) { p.rt.setPick(fn) }

// ListKeys implements ResourceProvider.
func (p *ContainerAppsProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Put implements ResourceProvider.
func (p *ContainerAppsProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	t := acaTargetOf(id)

	switch {
	case t.listing:
		return nil, ErrResourceNotFound
	case t.kind == acaKindEnv:
		return p.putEnv(id, body)
	case t.kind == acaKindStorage:
		return p.putStorage(id, body)
	case t.kind == acaKindApp:
		return p.putApp(ctx, id, body)
	case t.kind == acaKindJob:
		return p.putJob(id, body)
	case t.kind == acaKindOther:
		out, _, err := p.backend.putGenericResource(id, body)

		return out, err
	}

	return nil, acaBad("%s cannot be created or updated directly.", id.LeafType())
}

// Get implements ResourceProvider.
func (p *ContainerAppsProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	t := acaTargetOf(id)

	if t.kind == acaKindOther {
		return p.backend.getGenericResource(id)
	}

	if t.listing {
		return nil, ErrResourceNotFound
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	return p.getLocked(id, t)
}

func (p *ContainerAppsProvider) getLocked(id ResourceID, t acaTarget) (map[string]any, error) {
	switch t.kind {
	case acaKindEnv:
		if e, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]; ok {
			return p.envBody(id, e), nil
		}
	case acaKindStorage:
		return p.storageGet(id, t)
	case acaKindApp:
		if a, ok := p.apps[acaKey(id.ResourceGroup, t.names[0])]; ok {
			return p.appBody(id, a), nil
		}
	case acaKindRevision, acaKindReplica:
		return p.revisionGet(id, t)
	case acaKindJob:
		if j, ok := p.jobs[acaKey(id.ResourceGroup, t.names[0])]; ok {
			return p.jobBody(id, j), nil
		}
	case acaKindExecution:
		return p.executionGet(id, t)
	case acaKindOther:
	}

	return nil, ErrResourceNotFound
}

// Delete implements ResourceProvider.
func (p *ContainerAppsProvider) Delete(_ context.Context, id ResourceID) error {
	t := acaTargetOf(id)

	switch {
	case t.listing:
		return ErrResourceNotFound
	case t.kind == acaKindEnv:
		return p.deleteEnv(id, t.names[0])
	case t.kind == acaKindStorage:
		return p.deleteStorage(id, t)
	case t.kind == acaKindApp:
		return p.deleteApp(id, t.names[0])
	case t.kind == acaKindJob:
		return p.deleteJob(id, t.names[0])
	case t.kind == acaKindOther:
		return p.backend.deleteGenericResource(id)
	}

	return acaBad("%s cannot be deleted directly.", id.LeafType())
}

// List implements ResourceProvider.
func (p *ContainerAppsProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	t := acaTargetOf(id)

	if t.kind == acaKindOther {
		return p.backend.listGenericResources(id), nil
	}

	p.mu.RLock("List")
	defer p.mu.RUnlock()

	switch t.kind {
	case acaKindEnv:
		return p.listEnvs(id), nil
	case acaKindApp:
		return p.listApps(id), nil
	case acaKindJob:
		return p.listJobs(id), nil
	case acaKindStorage:
		return p.listStorages(id)
	case acaKindRevision, acaKindReplica:
		return p.listRevisions(id, t)
	case acaKindExecution:
		return p.listExecutions(id, t)
	case acaKindOther:
	}

	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *ContainerAppsProvider) Reset() {
	p.opMu.Lock("Reset")
	defer p.opMu.Unlock()

	p.mu.Lock("Reset")
	p.envs, p.apps, p.jobs = map[string]*storedACAEnv{}, map[string]*storedACAApp{}, map[string]*storedACAJob{}
	p.hosts = map[string]acaHostTarget{}
	p.mu.Unlock()

	p.rt.closeAll()
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *ContainerAppsProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.opMu.Lock("DeleteResourcesInGroup")
	defer p.opMu.Unlock()

	p.mu.Lock("DeleteResourcesInGroup")

	var revKeys, jobKeys []string

	envKeys := map[string]bool{}

	for k, e := range p.envs {
		if resourceGroupsEqual(e.ResourceGroup, resourceGroup) {
			envKeys[strings.ToLower(e.armID())] = true

			delete(p.envs, k)
		}
	}

	for k, a := range p.apps {
		if resourceGroupsEqual(a.ResourceGroup, resourceGroup) || envKeys[strings.ToLower(a.EnvironmentID)] {
			revKeys = append(revKeys, p.revisionKeys(a)...)

			delete(p.apps, k)
		}
	}

	for k, j := range p.jobs {
		if resourceGroupsEqual(j.ResourceGroup, resourceGroup) || envKeys[strings.ToLower(j.EnvironmentID)] {
			jobKeys = append(jobKeys, p.executionKeys(j)...)

			delete(p.jobs, k)
		}
	}

	p.rebuildHostsLocked()
	p.mu.Unlock()

	p.rt.closeRevisions(revKeys)
	p.rt.removeKeys(jobKeys)
}

// Close stops every replica.
func (p *ContainerAppsProvider) Close() { p.rt.closeAll() }

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func acaResourceID(sub, rg string, types []string, names []string) ResourceID {
	return ResourceID{
		SubscriptionID: sub,
		ResourceGroup:  rg,
		Namespace:      namespaceMicrosoftApp,
		Types:          types,
		Names:          names,
	}
}

func acaEtag(t time.Time) string { return fmt.Sprintf("\"%x\"", t.UnixNano()) }

// etagOf implements etagSource for the top-level types.
func (p *ContainerAppsProvider) etagOf(id ResourceID) (string, bool) {
	t := acaTargetOf(id)
	if t.listing || len(t.names) == 0 {
		return "", false
	}

	p.mu.RLock("etagOf")
	defer p.mu.RUnlock()

	key := acaKey(id.ResourceGroup, t.names[0])

	switch t.kind {
	case acaKindEnv:
		if e, ok := p.envs[key]; ok {
			return acaEtag(e.Modified), true
		}
	case acaKindApp:
		if a, ok := p.apps[key]; ok {
			return acaEtag(a.Modified), true
		}
	case acaKindJob:
		if j, ok := p.jobs[key]; ok {
			return acaEtag(j.Modified), true
		}
	case acaKindOther, acaKindStorage, acaKindRevision, acaKindReplica, acaKindExecution:
	}

	return "", false
}
