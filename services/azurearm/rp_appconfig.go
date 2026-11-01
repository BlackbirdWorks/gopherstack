package azurearm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftAppConfig = "Microsoft.AppConfiguration"
	acStoresType                = "configurationStores"
	acKeyValuesType             = "keyValues"
	acReplicasType              = "replicas"
	acDeletedType               = "deletedConfigurationStores"
	acDefaultHost               = "localhost"
	acHostPrefix                = "azconfig."
	acSkuFree                   = "free"
	acSoftDeleteDays            = 7
	acCreateModeRecover         = "Recover"
	acPropEndpoint              = "endpoint"
	acPropCreationDate          = "creationDate"
	acPropCreateMode            = "createMode"
	acPropPurgeProtection       = "enablePurgeProtection"
	acFieldIdentity             = "identity"
	acFieldKey                  = "key"
	acFieldLabel                = "label"
	acFieldReason               = "reason"
	acFieldAvailable            = "nameAvailable"
	acPropLocalAuth             = "disableLocalAuth"
)

func appConfigAPIVersions() []string { return []string{"2024-05-01", "2023-03-01"} }

// AppConfigEndpointConfig configures the endpoints the provider advertises.
type AppConfigEndpointConfig struct {
	Override string
	Port     int
}

type acKind int

const (
	acKindUnsupported acKind = iota
	acKindStore
	acKindReplica
	acKindKeyValue
)

type storedAC struct {
	tags          map[string]string
	extra         map[string]any
	identity      map[string]any
	createdAt     time.Time
	name          string
	resourceGroup string
	location      string
	host          string
	sku           string
	store         string
	kind          acKind
}

type deletedAC struct {
	deletedAt      time.Time
	tags           map[string]string
	name           string
	location       string
	subscriptionID string
	storeID        string
	purgeProtected bool
}

// AppConfigProvider implements ResourceProvider for Microsoft.AppConfiguration. ARM keeps
// location, tags, SKU and unmodelled properties; stores, access keys and key-values live in
// the data plane.
type AppConfigProvider struct {
	dataPlane AppConfig
	mu        *lockmetrics.RWMutex
	res       map[string]*storedAC
	deleted   map[string]*deletedAC
	cfg       AppConfigEndpointConfig
}

// NewAppConfigProvider creates an AppConfigProvider; dataPlane may be nil.
func NewAppConfigProvider(cfg AppConfigEndpointConfig, dataPlane AppConfig) *AppConfigProvider {
	if dataPlane == nil {
		dataPlane = noopAppConfig{}
	}

	if cfg.Port == 0 {
		cfg.Port = DefaultAppConfigPort
	}

	return &AppConfigProvider{
		mu: lockmetrics.New("azurearm.appconfigprovider"), res: map[string]*storedAC{},
		deleted: map[string]*deletedAC{}, cfg: cfg, dataPlane: dataPlane,
	}
}

var _ ResourceProvider = (*AppConfigProvider)(nil)

// Namespace implements ResourceProvider.
func (p *AppConfigProvider) Namespace() string { return namespaceMicrosoftAppConfig }

// ResourceTypes implements ResourceProvider.
func (p *AppConfigProvider) ResourceTypes() []ResourceTypeDef {
	v := appConfigAPIVersions()

	return []ResourceTypeDef{
		{Type: acStoresType, APIVersions: v, HasChildren: true},
		{Type: acStoresType + "/" + acKeyValuesType, APIVersions: v, HasChildren: false},
		{Type: acStoresType + "/" + acReplicasType, APIVersions: v, HasChildren: false},
		{Type: "locations/" + acDeletedType, APIVersions: v, HasChildren: false},
	}
}

func classifyAppConfigPath(id ResourceID) acKind {
	switch strings.ToLower(strings.Join(id.Types, "/")) {
	case "configurationstores":
		return acKindStore
	case "configurationstores/replicas":
		return acKindReplica
	case "configurationstores/keyvalues":
		return acKindKeyValue
	}

	return acKindUnsupported
}

func errUnsupportedAppConfigType(id ResourceID) error {
	return fmt.Errorf(
		"%w: unsupported Microsoft.AppConfiguration resource type %q",
		ErrResourceNotFound,
		id.ResourceType(),
	)
}

func acKey(rg string, kind acKind, names ...string) string {
	return ehKey(rg, append([]string{strconv.Itoa(int(kind))}, names...)...)
}

func (p *AppConfigProvider) store(rg, name string) (*storedAC, bool) {
	r, ok := p.res[acKey(rg, acKindStore, name)]

	return r, ok
}

func (p *AppConfigProvider) requireStore(id ResourceID) error {
	if _, ok := p.store(id.ResourceGroup, id.Names[0]); !ok {
		return ErrAppConfigStoreNotFound
	}

	return nil
}

func badAppConfigRequest(err error) error {
	if errors.Is(err, ErrAppConfigKeyLocked) || errors.Is(err, ErrAppConfigUnavailable) {
		return err
	}

	return fmt.Errorf("%w: %w", ErrInvalidRequestBody, err)
}

// Put implements ResourceProvider.
func (p *AppConfigProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	kind := classifyAppConfigPath(id)

	p.mu.Lock("Put")
	defer p.mu.Unlock()

	switch kind {
	case acKindStore:
		return p.putStore(ctx, id, body)
	case acKindReplica:
		return p.putReplica(ctx, id, body)
	case acKindKeyValue:
		return p.putKeyValue(id, body)
	case acKindUnsupported:
	}

	return nil, errUnsupportedAppConfigType(id)
}

func (p *AppConfigProvider) nameTakenElsewhere(id ResourceID) bool {
	for _, r := range p.res {
		if r.kind == acKindStore && strings.EqualFold(r.name, id.LeafName()) &&
			!resourceGroupsEqual(r.resourceGroup, id.ResourceGroup) {
			return true
		}
	}

	return false
}

func (p *AppConfigProvider) checkSoftDeleted(id ResourceID, body map[string]any, location string) error {
	key := deletedKey(location, id.LeafName())

	if _, ok := p.deleted[key]; !ok {
		return nil
	}

	props, _ := body[fieldProperties].(map[string]any)
	if mode, _ := props[acPropCreateMode].(string); strings.EqualFold(mode, acCreateModeRecover) {
		delete(p.deleted, key)

		return nil
	}

	return ErrAppConfigSoftDeleted
}

func deletedKey(location, name string) string {
	return strings.ToLower(location) + "|" + strings.ToLower(name)
}

func (p *AppConfigProvider) putStore(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	if p.nameTakenElsewhere(id) {
		return nil, ErrAppConfigNameTaken
	}

	rec := p.newStoreRecord(ctx, id, body)
	if _, exists := p.store(id.ResourceGroup, rec.name); !exists {
		if err := p.checkSoftDeleted(id, body, rec.location); err != nil {
			return nil, err
		}
	}

	created, err := p.dataPlane.PutStore(rec.name)
	if err != nil {
		return nil, badAppConfigRequest(err)
	}

	rec.createdAt = created
	p.res[acKey(id.ResourceGroup, acKindStore, rec.name)] = rec

	return p.storeBody(id, rec), nil
}

func (p *AppConfigProvider) newStoreRecord(ctx context.Context, id ResourceID, body map[string]any) *storedAC {
	rec := &storedAC{
		name: id.LeafName(), resourceGroup: id.ResourceGroup, location: DefaultLocation, kind: acKindStore,
		tags: stringTags(body[fieldTags]), extra: dropEmptyObjects(
			ehExtra(body, acPropEndpoint, acPropCreationDate, acPropCreateMode),
		),
		sku: acSkuOf(body), identity: acIdentityOf(body),
	}

	if existing, ok := p.store(id.ResourceGroup, rec.name); ok {
		rec.location, rec.host = existing.location, existing.host
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.location = loc
	}

	if h := RequestHostFromContext(ctx); h != "" {
		rec.host = h
	}

	return rec
}

// dropEmptyObjects removes properties whose value is an empty object, or an object holding
// only empty objects, which the provider would otherwise read back as an empty nested block
// and report as drift.
func dropEmptyObjects(props map[string]any) map[string]any {
	for k, v := range props {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}

		if dropEmptyObjects(m); len(m) == 0 {
			delete(props, k)
		}
	}

	return props
}

func acSkuOf(body map[string]any) string {
	sku, _ := body[ehFieldSKU].(map[string]any)
	name, _ := sku[fieldName].(string)

	if name == "" {
		return "standard"
	}

	return strings.ToLower(name)
}

func acIdentityOf(body map[string]any) map[string]any {
	ident, _ := body[acFieldIdentity].(map[string]any)
	if len(ident) == 0 {
		return nil
	}

	out := map[string]any{}
	maps.Copy(out, ident)

	return out
}

func (p *AppConfigProvider) putReplica(
	ctx context.Context,
	id ResourceID,
	body map[string]any,
) (map[string]any, error) {
	if err := p.requireStore(id); err != nil {
		return nil, err
	}

	rec := &storedAC{
		name: id.LeafName(), resourceGroup: id.ResourceGroup, location: DefaultLocation, kind: acKindReplica,
		store: id.Names[0], createdAt: time.Now().UTC(), extra: ehExtra(body, acPropEndpoint),
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.location = loc
	}

	if existing, ok := p.res[acKey(id.ResourceGroup, acKindReplica, id.Names...)]; ok {
		rec.createdAt = existing.createdAt
	}

	rec.host = RequestHostFromContext(ctx)
	p.res[acKey(id.ResourceGroup, acKindReplica, id.Names...)] = rec

	return p.replicaBody(id, rec), nil
}

// Get implements ResourceProvider.
func (p *AppConfigProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	switch classifyAppConfigPath(id) {
	case acKindStore:
		rec, ok := p.store(id.ResourceGroup, id.LeafName())
		if !ok {
			return nil, ErrAppConfigStoreNotFound
		}

		return p.storeBody(id, rec), nil
	case acKindReplica:
		rec, ok := p.res[acKey(id.ResourceGroup, acKindReplica, id.Names...)]
		if !ok {
			return nil, ErrAppConfigReplicaNotFound
		}

		return p.replicaBody(id, rec), nil
	case acKindKeyValue:
		return p.getKeyValue(id)
	case acKindUnsupported:
	}

	return nil, errUnsupportedAppConfigType(id)
}

// Delete implements ResourceProvider.
func (p *AppConfigProvider) Delete(_ context.Context, id ResourceID) error {
	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	switch classifyAppConfigPath(id) {
	case acKindStore:
		return p.deleteStore(id.ResourceGroup, id.LeafName(), id.SubscriptionID)
	case acKindReplica:
		key := acKey(id.ResourceGroup, acKindReplica, id.Names...)
		if _, ok := p.res[key]; !ok {
			return ErrAppConfigReplicaNotFound
		}

		delete(p.res, key)

		return nil
	case acKindKeyValue:
		return p.deleteKeyValue(id)
	case acKindUnsupported:
	}

	return errUnsupportedAppConfigType(id)
}

func (p *AppConfigProvider) deleteStore(rg, name, sub string) error {
	rec, ok := p.store(rg, name)
	if !ok {
		return ErrAppConfigStoreNotFound
	}

	for k, r := range p.res {
		if r.kind == acKindReplica && r.store != "" && strings.EqualFold(r.store, name) &&
			resourceGroupsEqual(r.resourceGroup, rg) {
			delete(p.res, k)
		}
	}

	delete(p.res, acKey(rg, acKindStore, name))

	purge, _ := rec.extra[acPropPurgeProtection].(bool)
	p.deleted[deletedKey(rec.location, rec.name)] = &deletedAC{
		name: rec.name, location: rec.location, subscriptionID: sub, tags: rec.tags, deletedAt: time.Now().UTC(),
		storeID: ResourceID{
			SubscriptionID: sub, ResourceGroup: rg, Namespace: namespaceMicrosoftAppConfig,
			Types: []string{acStoresType}, Names: []string{rec.name},
		}.ARMID(),
		purgeProtected: purge,
	}

	return p.dataPlane.DeleteStore(rec.name)
}

// List implements ResourceProvider; only stores are listable here.
func (p *AppConfigProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	var out []map[string]any

	if classifyAppConfigPath(id) == acKindStore {
		for _, rec := range p.res {
			if rec.kind != acKindStore ||
				(id.ResourceGroup != "" && !resourceGroupsEqual(rec.resourceGroup, id.ResourceGroup)) {
				continue
			}

			rid := ResourceID{
				SubscriptionID: id.SubscriptionID, ResourceGroup: rec.resourceGroup,
				Namespace: namespaceMicrosoftAppConfig, Types: id.Types, Names: []string{rec.name},
			}
			out = append(out, p.storeBody(rid, rec))
		}
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

// Reset implements ResourceProvider.
func (p *AppConfigProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	p.res, p.deleted = map[string]*storedAC{}, map[string]*deletedAC{}
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *AppConfigProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for _, rec := range p.res {
		if rec.kind == acKindStore && resourceGroupsEqual(rec.resourceGroup, resourceGroup) {
			_ = p.deleteStore(rec.resourceGroup, rec.name, "")
		}
	}
}

// providerErrorDetails looks err up in every dedicated resource provider's error table.
func providerErrorDetails(err error) (errorEntry, bool) {
	for _, lookup := range []func(error) (errorEntry, bool){
		cosmosErrorDetails, eventHubErrorDetails, eventGridErrorDetails,
		appConfigErrorDetails, acrErrorDetails, redisErrorDetails, sqlErrorDetails, flexErrorDetails, webErrorDetails,
		monitorErrorDetails, aciErrorDetails, netErrorDetails, apimErrorDetails,
	} {
		if e, ok := lookup(err); ok {
			return e, true
		}
	}

	return errorEntry{}, false
}

func appConfigErrorDetails(err error) (errorEntry, bool) {
	table := []struct {
		err   error
		entry errorEntry
	}{
		{ErrAppConfigStoreNotFound, notFoundEntry("The configuration store was not found.")},
		{ErrAppConfigKeyValueNotFound, notFoundEntry("The key-value was not found.")},
		{ErrAppConfigReplicaNotFound, notFoundEntry("The replica was not found.")},
		{ErrAppConfigDeletedNotFound, notFoundEntry("The deleted configuration store was not found.")},
		{ErrAppConfigKeyLocked, errorEntry{"Conflict", "The key-value is read-only.", http.StatusConflict}},
		{
			ErrAppConfigNameTaken,
			errorEntry{"NameUnavailable", "The store name is already in use.", http.StatusConflict},
		},
		{ErrAppConfigSoftDeleted, errorEntry{
			"NameUnavailable", "The store name is in use by a soft-deleted store; recover or purge it first.",
			http.StatusConflict,
		}},
		{ErrAppConfigPurgeProtected, errorEntry{
			"ActionNotAllowed", "The store cannot be purged because purge protection is enabled.", http.StatusForbidden,
		}},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	return errorEntry{}, false
}

func notFoundEntry(msg string) errorEntry {
	return errorEntry{armErrorCodeResourceNotFound, msg, http.StatusNotFound}
}

func (p *AppConfigProvider) endpointFor(name, host string) string {
	hostAndPort := p.cfg.Override
	if hostAndPort == "" {
		if host == "" {
			host = acDefaultHost
		}

		hostAndPort = net.JoinHostPort(acHostPrefix+host, strconv.Itoa(p.cfg.Port))
	}

	return "https://" + name + "." + hostAndPort
}

func (p *AppConfigProvider) principalID(id ResourceID) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.ToLower(id.ARMID()))).String()
}
