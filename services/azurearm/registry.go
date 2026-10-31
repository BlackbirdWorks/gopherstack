package azurearm

import (
	"context"
	"sort"
	"strings"
)

// ResourceProvider is the small interface a resource-provider namespace
// (Microsoft.Storage today; Microsoft.ServiceBus/Microsoft.DocumentDB in
// M8/M9, Microsoft.KeyVault/Microsoft.AppConfiguration in M11/M12) plugs
// into the registry with. Deliberately narrow (AZURE.md section 10.6) so
// each future milestone adds one file and one registry entry.
//
// Put/Get/Delete/ListKeys all return the full ARM wire response body (or nil
// for Delete's 204 case) -- the ResourceProvider owns its own response
// shape entirely, including the common id/name/type/location/tags/
// properties.provisioningState envelope (Resource.Body / a hand-built
// equivalent), rather than the registry re-wrapping it.
type ResourceProvider interface {
	// Namespace returns the ARM provider namespace this implementation
	// serves, e.g. "Microsoft.Storage".
	Namespace() string
	// ResourceTypes returns the resource types this provider serves, for
	// the /providers/{ns} registration-status response.
	ResourceTypes() []ResourceTypeDef
	// Put creates or updates the resource identified by id from body,
	// returning the full wire response body.
	Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error)
	// Get returns the full wire response body for id.
	Get(ctx context.Context, id ResourceID) (map[string]any, error)
	// Delete removes the resource identified by id.
	Delete(ctx context.Context, id ResourceID) error
	// List returns every resource of id's LeafType, scoped to id's
	// ResourceGroup if set, else the whole subscription.
	List(ctx context.Context, id ResourceID) ([]map[string]any, error)
	// ListKeys returns the POST .../listKeys response body for id.
	ListKeys(ctx context.Context, id ResourceID) (map[string]any, error)
	// Reset clears all of this provider's own state, called by
	// Handler.Reset via Registry.ResetAll (the /_gopherstack/reset
	// endpoint) -- without this, a dedicated provider's resources survive
	// a reset that's supposed to wipe every service's state.
	Reset()
	// DeleteResourcesInGroup deletes every resource this provider owns in
	// resourceGroup, called by Registry.DeleteResourcesInGroup when a
	// resource group is deleted -- without this, a dedicated provider's
	// resources in that group are orphaned (InMemoryBackend's own generic
	// resources are cascaded directly by DeleteResourceGroup, but a
	// dedicated provider's internal state is opaque to InMemoryBackend).
	DeleteResourcesInGroup(ctx context.Context, resourceGroup string)
}

// Registry dispatches generic-resource operations to the ResourceProvider
// registered for a namespace, falling back to InMemoryBackend's own
// generic (metadata-only) resource storage for any namespace with no
// dedicated provider -- so PUT/GET/DELETE of an arbitrary
// Microsoft.SomeFutureThing/whatever resource still round-trips instead of
// 404ing, per AZURE.md section 10.1's generic-resource-plane requirement.
type Registry struct {
	backend   *InMemoryBackend
	providers map[string]ResourceProvider
}

// NewRegistry creates a Registry backed by backend, with no providers
// registered yet (use Register).
func NewRegistry(backend *InMemoryBackend) *Registry {
	return &Registry{backend: backend, providers: make(map[string]ResourceProvider)}
}

// Register adds a ResourceProvider to the registry, keyed by its
// lowercased Namespace() -- ARM namespaces are matched case-insensitively
// (AZURE.md section 10.1: "Microsoft.storage" and "microsoft.STORAGE" must
// both reach the same dedicated provider, not fall through to the generic
// pass-through).
func (r *Registry) Register(p ResourceProvider) {
	r.providers[strings.ToLower(p.Namespace())] = p

	if np, ok := p.(*NetworkProvider); ok {
		np.SetLookup(r.getRaw)
	}
}

// Providers returns the namespaces of every registered dedicated
// ResourceProvider, sorted, for the provider-list endpoint.
func (r *Registry) Providers() []string {
	out := make([]string, 0, len(r.providers))
	for _, p := range r.providers {
		out = append(out, p.Namespace())
	}

	sort.Strings(out)

	return out
}

// providerFor returns the dedicated ResourceProvider for id.Namespace, or
// nil if none is registered (generic pass-through applies).
func (r *Registry) providerFor(id ResourceID) ResourceProvider {
	return r.providers[strings.ToLower(id.Namespace)]
}

// ProviderNamed returns the dedicated ResourceProvider registered for ns
// (matched case-insensitively, AZURE.md section 10.1), and whether one was
// found. Used by the /providers/{ns} and /providers/{ns}/register handlers,
// which take ns directly from the URL path rather than a parsed ResourceID.
func (r *Registry) ProviderNamed(ns string) (ResourceProvider, bool) {
	p, ok := r.providers[strings.ToLower(ns)]

	return p, ok
}

// SetServiceBusEntities gives the registered Microsoft.ServiceBus provider
// (if any) a real ServiceBusEntities adapter, replacing its no-op default --
// called post-construction by cli.go's wireAzureARMResourceProviders once
// services/azureservicebus's own Handler exists, mirroring M8's
// wireAzureStorageVHost pattern (this can't be wired at azurearm.Provider.Init
// time the way StorageAccountsProvider is, since it needs a live reference to
// a sibling service's already-constructed runtime handler). A no-op if
// Microsoft.ServiceBus isn't registered or isn't a *ServiceBusProvider.
func (r *Registry) SetServiceBusEntities(e ServiceBusEntities) {
	p, ok := r.ProviderNamed(namespaceMicrosoftServiceBus)
	if !ok {
		return
	}

	if sb, sbOk := p.(*ServiceBusProvider); sbOk {
		sb.dataPlane = e
	}
}

// SetEventHubs gives the registered Microsoft.EventHub provider (if any) a
// real data-plane adapter, replacing its no-op default.
func (r *Registry) SetEventHubs(e EventHubs) {
	p, ok := r.ProviderNamed(namespaceMicrosoftEventHub)
	if !ok {
		return
	}

	if eh, ehOk := p.(*EventHubProvider); ehOk {
		eh.dataPlane = e
	}
}

// SetEventGrid gives the registered Microsoft.EventGrid provider (if any) a real
// data-plane adapter, replacing its no-op default.
func (r *Registry) SetEventGrid(e EventGrid) {
	p, ok := r.ProviderNamed(namespaceMicrosoftEventGrid)
	if !ok {
		return
	}

	if eg, egOk := p.(*EventGridProvider); egOk {
		eg.dataPlane = e
	}
}

// SetAppConfig gives the registered Microsoft.AppConfiguration provider (if any) a real
// data-plane adapter, replacing its no-op default.
func (r *Registry) SetAppConfig(a AppConfig) {
	p, ok := r.ProviderNamed(namespaceMicrosoftAppConfig)
	if !ok {
		return
	}

	if ac, acOk := p.(*AppConfigProvider); acOk {
		ac.dataPlane = a
	}
}

// SetContainerRegistries gives the registered Microsoft.ContainerRegistry provider (if any) a real data-plane
// adapter, replacing its no-op default.
func (r *Registry) SetContainerRegistries(c ContainerRegistries) {
	p, ok := r.ProviderNamed(namespaceMicrosoftContainerRegistry)
	if !ok {
		return
	}

	if cp, cpOk := p.(*ContainerRegistryProvider); cpOk {
		cp.dataPlane = c
	}
}

// SetFunctionApps gives the registered Microsoft.Web provider (if any) a real Azure Functions data-plane adapter,
// replacing its no-op default.
func (r *Registry) SetFunctionApps(f FunctionApps) {
	p, ok := r.ProviderNamed(namespaceMicrosoftWeb)
	if !ok {
		return
	}

	if wp, wpOk := p.(*WebProvider); wpOk {
		wp.dataPlane = f
	}
}

// SetACIFileShares gives the registered Microsoft.ContainerInstance provider (if any) the Azure Files seam that
// azureFile volumes use.
func (r *Registry) SetACIFileShares(s ACIFileShares) {
	p, ok := r.ProviderNamed(namespaceMicrosoftContainerInstance)
	if !ok {
		return
	}

	if cp, cpOk := p.(*ContainerInstanceProvider); cpOk {
		cp.engine.SetFileShares(s)
	}
}

// SetKeyVaultSecrets gives the registered Microsoft.App provider (if any) the Key Vault reader that resolves
// keyVaultUrl secrets.
func (r *Registry) SetKeyVaultSecrets(v KeyVaultSecrets) {
	p, ok := r.ProviderNamed(namespaceMicrosoftApp)
	if !ok {
		return
	}

	if cp, cpOk := p.(*ContainerAppsProvider); cpOk {
		cp.SetKeyVaultSecrets(v)
	}
}

// CloseAll stops the workloads of every provider that runs any.
func (r *Registry) CloseAll() {
	for _, p := range r.providers {
		if c, ok := p.(interface{ Close() }); ok {
			c.Close()
		}
	}
}

// SetKeyVaults gives the registered Microsoft.KeyVault provider (if any) a
// real KeyVaults adapter, replacing its no-op default.
func (r *Registry) SetKeyVaults(k KeyVaults) {
	p, ok := r.ProviderNamed(namespaceMicrosoftKeyVault)
	if !ok {
		return
	}

	if kv, kvOk := p.(*KeyVaultProvider); kvOk {
		kv.dataPlane = k
	}
}

// SetCosmosResources gives the registered Microsoft.DocumentDB provider (if any) a real data-plane adapter,
// replacing its no-op default.
func (r *Registry) SetCosmosResources(c CosmosResources) {
	p, ok := r.ProviderNamed(namespaceMicrosoftDocumentDB)
	if !ok {
		return
	}

	if cp, cpOk := p.(*CosmosProvider); cpOk {
		cp.dataPlane = c
	}
}

// Put creates or updates the resource identified by id.
func (r *Registry) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, bool, error) {
	if np := r.peRoute(id); np != nil {
		out, err := np.targetPut(ctx, id, body, r.parentCheck(id))

		return out, false, err
	}

	if p := r.providerFor(id); p != nil {
		_, getErr := p.Get(ctx, id)
		created := getErr != nil

		respBody, err := p.Put(ctx, id, body)

		return respBody, created, err
	}

	return r.backend.putGenericResource(id, body)
}

// Get returns the resource identified by id.
func (r *Registry) Get(ctx context.Context, id ResourceID) (map[string]any, error) {
	if np := r.peRoute(id); np != nil {
		return np.targetGet(ctx, id, r.parentCheck(id))
	}

	if p := r.providerFor(id); p != nil {
		body, err := p.Get(ctx, id)
		if err != nil || len(id.Types) != 1 || strings.EqualFold(id.Namespace, namespaceMicrosoftNetwork) {
			return body, err
		}

		return r.enrichTarget(body), nil
	}

	return r.backend.getGenericResource(id)
}

// Delete removes the resource identified by id.
func (r *Registry) Delete(ctx context.Context, id ResourceID) error {
	if np := r.peRoute(id); np != nil {
		return np.targetDelete(ctx, id, r.parentCheck(id))
	}

	if p := r.providerFor(id); p != nil {
		return p.Delete(ctx, id)
	}

	return r.backend.deleteGenericResource(id)
}

// List returns every resource of id's LeafType, scoped per id.ResourceGroup.
func (r *Registry) List(ctx context.Context, id ResourceID) ([]map[string]any, error) {
	if np := r.peRoute(id); np != nil {
		return np.targetList(ctx, id, r.parentCheck(id))
	}

	if p := r.providerFor(id); p != nil {
		list, err := p.List(ctx, id)
		if err != nil || len(id.Types) != 1 || strings.EqualFold(id.Namespace, namespaceMicrosoftNetwork) {
			return list, err
		}

		out := make([]map[string]any, len(list))
		for i, b := range list {
			out[i] = r.enrichTarget(b)
		}

		return out, nil
	}

	return r.backend.listGenericResources(id), nil
}

// ListKeys returns the listKeys response body for id.
func (r *Registry) ListKeys(ctx context.Context, id ResourceID) (map[string]any, error) {
	if p := r.providerFor(id); p != nil {
		return p.ListKeys(ctx, id)
	}

	return nil, ErrResourceNotFound
}

// ResetAll clears every registered dedicated provider's own state. Called
// by Handler.Reset alongside InMemoryBackend.Reset -- the generic pass-through
// resources and dedicated-provider resources (e.g. StorageProvider.accounts)
// are two separate stores, and the /_gopherstack/reset endpoint must clear
// both.
func (r *Registry) ResetAll() {
	for _, p := range r.providers {
		p.Reset()
	}
}

// DeleteResourcesInGroup cascades a resource-group delete into every
// registered dedicated provider, so a provider's own resources in that
// group (e.g. StorageProvider's accounts) don't survive as orphans reachable
// by their own resource ID after the owning group is gone.
func (r *Registry) DeleteResourcesInGroup(ctx context.Context, resourceGroup string) {
	for _, p := range r.providers {
		p.DeleteResourcesInGroup(ctx, resourceGroup)
	}
}

// --- InMemoryBackend's generic (metadata-only) resource storage ---

// putGenericResource creates or updates a generic resource from body,
// merging body's top-level "tags" and "properties" fields (any other
// top-level field, e.g. "sku"/"kind", is preserved verbatim if the resource
// already exists via body carrying them again -- ARM PUT is a full replace
// semantically, matching this milestone's scope note that PATCH-merge
// beyond this is deferred).
func (b *InMemoryBackend) putGenericResource(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	b.mu.Lock("putGenericResource")
	defer b.mu.Unlock()

	key := id.storeKey()
	_, existed := b.resources[key]

	location, _ := body["location"].(string)
	if location == "" {
		if existing, ok := b.resources[key]; ok {
			location = existing.Location
		}
	}

	props, _ := body["properties"].(map[string]any)

	res := &Resource{
		ID:         id,
		Location:   location,
		Tags:       stringTags(body["tags"]),
		Properties: props,
	}

	if sku, ok := body["sku"].(map[string]any); ok {
		res.SKU = sku
	}

	if kind, ok := body["kind"].(string); ok {
		res.Kind = kind
	}

	b.resources[key] = res
	b.byType.Store(nil)

	return res.Body(), !existed, nil
}

func (b *InMemoryBackend) getGenericResource(id ResourceID) (map[string]any, error) {
	b.mu.RLock("getGenericResource")
	defer b.mu.RUnlock()

	res, ok := b.resources[id.storeKey()]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return res.Body(), nil
}

func (b *InMemoryBackend) deleteGenericResource(id ResourceID) error {
	b.mu.Lock("deleteGenericResource")
	defer b.mu.Unlock()

	key := id.storeKey()
	if _, ok := b.resources[key]; !ok {
		return ErrResourceNotFound
	}

	delete(b.resources, key)
	b.byType.Store(nil)

	return nil
}

func (b *InMemoryBackend) listGenericResources(id ResourceID) []map[string]any {
	b.mu.RLock("listGenericResources")

	bucket := b.typeIndex()[typeIndexKey(id.SubscriptionID, id.Namespace, id.LeafType())]
	rgKey := resourceGroupKey(id.ResourceGroup)

	type entry struct {
		res *Resource
		id  string
	}

	entries := make([]entry, 0, len(bucket))

	for _, res := range bucket {
		if id.ResourceGroup != "" && resourceGroupKey(res.ID.ResourceGroup) != rgKey {
			continue
		}

		entries = append(entries, entry{res: res, id: res.ID.ARMID()})
	}

	b.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })

	var out []map[string]any
	if len(entries) > 0 {
		out = make([]map[string]any, 0, len(entries))
	}

	for _, e := range entries {
		out = append(out, e.res.Body())
	}

	return out
}

// nameField safely reads the "name" string field from a wire response body
// map, returning "" if absent or not a string -- avoids an unchecked type
// assertion in sort comparators.
func nameField(body map[string]any) string {
	s, _ := body[fieldName].(string)

	return s
}

// parentCheck returns a probe that the target resource of a private endpoint child path exists.
func (r *Registry) parentCheck(id ResourceID) func(context.Context) error {
	parent := ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: id.Namespace,
		Types: id.Types[:1], Names: id.Names[:1],
	}

	return func(ctx context.Context) error {
		_, err := r.Get(ctx, parent)

		return err
	}
}

// getRaw is Get without the private endpoint connection enrichment; the network provider calls it under its lock.
func (r *Registry) getRaw(ctx context.Context, id ResourceID) (map[string]any, error) {
	if p := r.providerFor(id); p != nil {
		return p.Get(ctx, id)
	}

	return r.backend.getGenericResource(id)
}
