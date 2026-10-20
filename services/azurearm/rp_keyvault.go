package azurearm

import (
	"context"
	"fmt"
	"maps"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftKeyVault = "Microsoft.KeyVault"
	kvVaultsType               = "vaults"
	kvAccessPoliciesType       = "accessPolicies"
	kvDefaultRetentionDays     = 90
	kvPolicyOpAdd              = "add"
	kvPolicyOpRemove           = "remove"
	kvPolicyOpReplace          = "replace"
	fieldAccessPolicies        = "accessPolicies"
	kvFieldTenantID            = "tenantId"
	kvPublicNetworkEnabled     = "Enabled"
	kvDefaultHost              = "localhost"
	kvFieldCreateMode          = "createMode"
	kvCreateModeRecover        = "recover"
	kvDeletedVaultsType        = "deletedVaults"
)

func keyVaultAPIVersions() []string { return []string{"2023-07-01", "2022-07-01"} }

// KeyVaultEndpointConfig configures how the KeyVault resource provider
// advertises properties.vaultUri.
type KeyVaultEndpointConfig struct {
	Override string
	Port     int
}

type storedKeyVault struct {
	properties    map[string]any
	tags          map[string]string
	name          string
	resourceGroup string
	location      string
	host          string
	subscription  string
}

// KeyVaultProvider implements ResourceProvider for Microsoft.KeyVault/vaults.
// Vault lifecycle is delegated to the data plane through the KeyVaults seam.
type KeyVaultProvider struct {
	dataPlane KeyVaults
	mu        *lockmetrics.RWMutex
	vaults    map[string]*storedKeyVault
	deleted   map[string]*deletedKeyVault
	now       func() time.Time
	cfg       KeyVaultEndpointConfig
}

// NewKeyVaultProvider creates a KeyVaultProvider; dataPlane may be nil.
func NewKeyVaultProvider(cfg KeyVaultEndpointConfig, dataPlane KeyVaults) *KeyVaultProvider {
	if dataPlane == nil {
		dataPlane = noopKeyVaults{}
	}

	if cfg.Port == 0 {
		cfg.Port = DefaultKeyVaultPort
	}

	return &KeyVaultProvider{
		mu:        lockmetrics.New("azurearm.keyvaultprovider"),
		vaults:    make(map[string]*storedKeyVault),
		deleted:   make(map[string]*deletedKeyVault),
		now:       time.Now,
		cfg:       cfg,
		dataPlane: dataPlane,
	}
}

var _ ResourceProvider = (*KeyVaultProvider)(nil)

// Namespace implements ResourceProvider.
func (p *KeyVaultProvider) Namespace() string { return namespaceMicrosoftKeyVault }

// ResourceTypes implements ResourceProvider.
func (p *KeyVaultProvider) ResourceTypes() []ResourceTypeDef {
	v := keyVaultAPIVersions()

	return []ResourceTypeDef{
		{Type: kvVaultsType, APIVersions: v, HasChildren: true},
		{Type: kvVaultsType + "/" + kvAccessPoliciesType, APIVersions: v, HasChildren: false},
	}
}

func (p *KeyVaultProvider) isVault(id ResourceID) bool {
	return len(id.Types) == 1 && strings.EqualFold(id.Types[0], kvVaultsType)
}

func (p *KeyVaultProvider) isAccessPolicy(id ResourceID) bool {
	return len(id.Types) == 2 && strings.EqualFold(id.Types[0], kvVaultsType) &&
		strings.EqualFold(id.Types[1], kvAccessPoliciesType)
}

func (p *KeyVaultProvider) owned(rg, name string) (*storedKeyVault, bool) {
	v, ok := p.vaults[strings.ToLower(name)]
	if !ok || !resourceGroupsEqual(v.resourceGroup, rg) {
		return nil, false
	}

	return v, true
}

func intProp(props map[string]any, key string, def int) int {
	switch n := props[key].(type) {
	case float64:
		return int(n)
	case int:
		return n
	}

	return def
}

// Put implements ResourceProvider.
func (p *KeyVaultProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	if p.isAccessPolicy(id) {
		return p.putAccessPolicies(id, body)
	}

	if !p.isVault(id) {
		return nil, fmt.Errorf(
			"%w: unsupported Microsoft.KeyVault resource type %q",
			ErrResourceNotFound,
			id.ResourceType(),
		)
	}

	p.mu.Lock("Put")
	res, err := p.putLocked(ctx, id, body)
	p.mu.Unlock()

	if err != nil {
		return nil, err
	}

	name := id.LeafName()
	if res.recovered {
		p.dataPlane.RecoverVault(name)
	}

	props := res.props
	retention := intProp(props, "softDeleteRetentionInDays", kvDefaultRetentionDays)
	protect, _ := props["enablePurgeProtection"].(bool)
	p.dataPlane.CreateVault(name, retention, protect)

	return res.out, nil
}

type kvPutResult struct {
	out       map[string]any
	props     map[string]any
	recovered bool
}

// putLocked stores the vault; the caller holds p.mu. recovered reports that a
// soft-deleted vault was restored.
func (p *KeyVaultProvider) putLocked(ctx context.Context, id ResourceID, body map[string]any) (kvPutResult, error) {
	name := id.LeafName()
	key := strings.ToLower(name)
	existing, existed := p.vaults[key]

	if existed && !resourceGroupsEqual(existing.resourceGroup, id.ResourceGroup) {
		return kvPutResult{}, errVaultExistsInOtherResourceGroup
	}

	props, _ := body[fieldProperties].(map[string]any)
	if props == nil {
		props = map[string]any{}
	}

	mode, _ := props[kvFieldCreateMode].(string)
	delete(props, kvFieldCreateMode)

	gone, softDeleted := p.deleted[key]

	switch {
	case softDeleted && !strings.EqualFold(mode, kvCreateModeRecover):
		return kvPutResult{}, errVaultSoftDeleted
	case !softDeleted && !existed && strings.EqualFold(mode, kvCreateModeRecover):
		return kvPutResult{}, ErrKeyVaultNotFound
	}

	location := DefaultLocation

	switch loc, _ := body[fieldLocation].(string); {
	case loc != "":
		location = loc
	case existed:
		location = existing.location
	case softDeleted:
		location = gone.vault.location
	}

	host := RequestHostFromContext(ctx)
	if host == "" && existed {
		host = existing.host
	}

	v := &storedKeyVault{
		name: name, resourceGroup: id.ResourceGroup, location: location, subscription: id.SubscriptionID,
		tags: stringTags(body[fieldTags]), properties: props, host: host,
	}
	p.vaults[key] = v
	delete(p.deleted, key)

	return kvPutResult{out: p.buildBody(id, v), props: props, recovered: softDeleted}, nil
}

func (p *KeyVaultProvider) putAccessPolicies(id ResourceID, body map[string]any) (map[string]any, error) {
	op := strings.ToLower(id.LeafName())
	if op != kvPolicyOpAdd && op != kvPolicyOpRemove && op != kvPolicyOpReplace {
		return nil, fmt.Errorf("%w: access policy operation must be add, remove or replace", ErrInvalidRequestBody)
	}

	p.mu.Lock("PutAccessPolicies")
	defer p.mu.Unlock()

	v, ok := p.owned(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrKeyVaultNotFound
	}

	props, _ := body[fieldProperties].(map[string]any)
	in, _ := props[fieldAccessPolicies].([]any)
	cur, _ := v.properties[fieldAccessPolicies].([]any)

	switch op {
	case kvPolicyOpReplace:
		cur = in
	case kvPolicyOpAdd:
		cur = mergePolicies(cur, in)
	case kvPolicyOpRemove:
		cur = removePolicies(cur, in)
	}

	v.properties[fieldAccessPolicies] = cur

	return map[string]any{
		"id":          id.ARMID(),
		fieldName:     id.LeafName(),
		fieldType:     namespaceMicrosoftKeyVault + "/" + kvVaultsType + "/" + kvAccessPoliciesType,
		fieldLocation: v.location,
		fieldProperties: map[string]any{
			fieldAccessPolicies: orEmptyList(cur),
		},
	}, nil
}

func policyKey(p any) string {
	m, _ := p.(map[string]any)
	tenant, _ := m[kvFieldTenantID].(string)
	object, _ := m["objectId"].(string)
	app, _ := m["applicationId"].(string)

	return strings.ToLower(tenant + "|" + object + "|" + app)
}

func mergePolicies(cur, in []any) []any {
	out := append([]any(nil), cur...)

	for _, n := range in {
		replaced := false

		for i, c := range out {
			if policyKey(c) == policyKey(n) {
				out[i] = n
				replaced = true

				break
			}
		}

		if !replaced {
			out = append(out, n)
		}
	}

	return out
}

func removePolicies(cur, in []any) []any {
	drop := map[string]bool{}
	for _, n := range in {
		drop[policyKey(n)] = true
	}

	out := make([]any, 0, len(cur))

	for _, c := range cur {
		if !drop[policyKey(c)] {
			out = append(out, c)
		}
	}

	return out
}

func orEmptyList(l []any) []any {
	if l == nil {
		return []any{}
	}

	return l
}

// Get implements ResourceProvider.
func (p *KeyVaultProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	if !p.isVault(id) {
		return nil, fmt.Errorf(
			"%w: unsupported Microsoft.KeyVault resource type %q",
			ErrResourceNotFound,
			id.ResourceType(),
		)
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	v, ok := p.owned(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrKeyVaultNotFound
	}

	return p.buildBody(id, v), nil
}

// Delete implements ResourceProvider.
func (p *KeyVaultProvider) Delete(_ context.Context, id ResourceID) error {
	if !p.isVault(id) {
		return fmt.Errorf("%w: unsupported Microsoft.KeyVault resource type %q", ErrResourceNotFound, id.ResourceType())
	}

	p.mu.Lock("Delete")

	v, ok := p.owned(id.ResourceGroup, id.LeafName())
	if !ok {
		p.mu.Unlock()

		return ErrKeyVaultNotFound
	}

	soft := p.retireLocked(v)
	p.mu.Unlock()

	p.releaseDataPlane(v.name, soft)

	return nil
}

// List implements ResourceProvider.
func (p *KeyVaultProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	if strings.EqualFold(id.LeafType(), kvDeletedVaultsType) {
		return p.listDeleted(id.SubscriptionID), nil
	}

	vaults := p.inGroup(id.ResourceGroup)
	out := make([]map[string]any, 0, len(vaults))

	for _, v := range vaults {
		vid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: v.resourceGroup,
			Namespace: namespaceMicrosoftKeyVault, Types: []string{kvVaultsType}, Names: []string{v.name},
		}
		out = append(out, p.buildBody(vid, v))
	}

	return out, nil
}

// inGroup returns the vaults in resourceGroup (every vault when empty), sorted by name.
func (p *KeyVaultProvider) inGroup(resourceGroup string) []*storedKeyVault {
	p.mu.RLock("inGroup")
	defer p.mu.RUnlock()

	var found []*storedKeyVault

	for _, v := range p.vaults {
		if resourceGroup == "" || resourceGroupsEqual(v.resourceGroup, resourceGroup) {
			found = append(found, v)
		}
	}

	sort.Slice(found, func(i, j int) bool { return found[i].name < found[j].name })

	return found
}

// ListKeys implements ResourceProvider; vaults have no listKeys action.
func (p *KeyVaultProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *KeyVaultProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	p.vaults = make(map[string]*storedKeyVault)
	p.deleted = make(map[string]*deletedKeyVault)
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *KeyVaultProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")

	type retired struct {
		name string
		soft bool
	}

	var gone []retired

	for _, v := range p.vaults {
		if resourceGroupsEqual(v.resourceGroup, resourceGroup) {
			gone = append(gone, retired{v.name, p.retireLocked(v)})
		}
	}

	p.mu.Unlock()

	for _, g := range gone {
		p.releaseDataPlane(g.name, g.soft)
	}
}

func advertiseVaultURI(override, host string, port int, name string) string {
	hostAndPort := override
	if hostAndPort == "" {
		if host == "" {
			host = kvDefaultHost
		}

		hostAndPort = net.JoinHostPort(host, strconv.Itoa(port))
	}

	return "https://" + name + "." + hostAndPort + "/"
}

func (p *KeyVaultProvider) buildBody(id ResourceID, v *storedKeyVault) map[string]any {
	props := map[string]any{
		"sku":                          map[string]any{"family": "A", fieldName: "standard"},
		kvFieldTenantID:                DefaultTenantID,
		fieldAccessPolicies:            []any{},
		"enabledForDeployment":         false,
		"enabledForDiskEncryption":     false,
		"enabledForTemplateDeployment": false,
		"enableSoftDelete":             true,
		"softDeleteRetentionInDays":    kvDefaultRetentionDays,
		"enableRbacAuthorization":      false,
		"publicNetworkAccess":          kvPublicNetworkEnabled,
	}

	maps.Copy(props, v.properties)

	props[fieldAccessPolicies] = orEmptyList(asList(props[fieldAccessPolicies]))
	props["vaultUri"] = advertiseVaultURI(p.cfg.Override, v.host, p.cfg.Port, v.name)
	props[fieldProvisioningState] = provisioningStateSucceeded

	return map[string]any{
		"id":            id.ARMID(),
		fieldName:       v.name,
		fieldType:       namespaceMicrosoftKeyVault + "/" + kvVaultsType,
		fieldLocation:   v.location,
		fieldTags:       tagsOrEmpty(v.tags),
		fieldProperties: props,
	}
}

func asList(v any) []any {
	l, _ := v.([]any)

	return l
}
