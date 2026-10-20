package azurearm

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"maps"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftDocumentDB = "Microsoft.DocumentDB"
	cosmosAccountsType           = "databaseAccounts"
	cosmosSQLDatabasesType       = "sqlDatabases"
	cosmosContainersType         = "containers"
	cosmosThroughputType         = "throughputSettings"
	cosmosDefaultKind            = "GlobalDocumentDB"
	cosmosKeyBytes               = 64
	cosmosNameMin                = 3
	cosmosNameMax                = 44
	cosmosDefaultHost            = "localhost"
	cosmosFieldKind              = "kind"
	cosmosFieldIdentity          = "identity"
	cosmosFieldLocations         = "locations"
	cosmosFieldConsistency       = "consistencyPolicy"
	cosmosFieldBackup            = "backupPolicy"
	cosmosFieldLocationName      = "locationName"
	cosmosFieldFailoverPriority  = "failoverPriority"
	cosmosFieldOfferType         = "databaseAccountOfferType"
	cosmosFieldType              = "type"
	cosmosFieldDocEndpoint       = "documentEndpoint"
	cosmosFieldPublicNetwork     = "publicNetworkAccess"
	cosmosFieldTier              = "tier"
	cosmosBackupIntervalMinutes  = 240
	cosmosBackupRetentionHours   = 8
)

var cosmosNameRe = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

func cosmosAPIVersions() []string { return []string{"2024-11-15", "2024-05-15"} }

// CosmosEndpointConfig configures how the DocumentDB provider advertises documentEndpoint.
type CosmosEndpointConfig struct {
	Override string
}

type cosmosKeys struct {
	Primary           string `json:"primary"`
	Secondary         string `json:"secondary"`
	PrimaryReadonly   string `json:"primaryReadonly"`
	SecondaryReadonly string `json:"secondaryReadonly"`
}

func (k *cosmosKeys) all() []string {
	return []string{k.Primary, k.Secondary, k.PrimaryReadonly, k.SecondaryReadonly}
}

type storedCosmosAccount struct {
	properties    map[string]any
	tags          map[string]string
	identity      any
	keys          cosmosKeys
	name          string
	resourceGroup string
	location      string
	subscription  string
	host          string
	kind          string
	instanceID    string
}

type cosmosThroughput struct {
	Throughput   int `json:"throughput,omitempty"`
	AutoscaleMax int `json:"autoscaleMax,omitempty"`
}

// CosmosProvider implements ResourceProvider for Microsoft.DocumentDB. Databases and containers live in the
// Cosmos DB data plane behind the CosmosResources seam; this provider keeps account metadata, ownership and
// throughput settings.
type CosmosProvider struct {
	dataPlane  CosmosResources
	mu         *lockmetrics.RWMutex
	accounts   map[string]*storedCosmosAccount
	dbOwner    map[string]string
	throughput map[string]cosmosThroughput
	extras     map[string]cosmosContainerExtras
	cfg        CosmosEndpointConfig
}

// NewCosmosProvider creates a CosmosProvider; dataPlane may be nil.
func NewCosmosProvider(cfg CosmosEndpointConfig, dataPlane CosmosResources) *CosmosProvider {
	if dataPlane == nil {
		dataPlane = noopCosmosResources{}
	}

	return &CosmosProvider{
		dataPlane:  dataPlane,
		mu:         lockmetrics.New("azurearm.cosmosprovider"),
		accounts:   make(map[string]*storedCosmosAccount),
		dbOwner:    make(map[string]string),
		throughput: make(map[string]cosmosThroughput),
		extras:     make(map[string]cosmosContainerExtras),
		cfg:        cfg,
	}
}

var _ ResourceProvider = (*CosmosProvider)(nil)

// Namespace implements ResourceProvider.
func (p *CosmosProvider) Namespace() string { return namespaceMicrosoftDocumentDB }

// ResourceTypes implements ResourceProvider.
func (p *CosmosProvider) ResourceTypes() []ResourceTypeDef {
	v := cosmosAPIVersions()
	acct := cosmosAccountsType

	return []ResourceTypeDef{
		{Type: acct, APIVersions: v, HasChildren: true},
		{Type: acct + "/" + cosmosSQLDatabasesType, APIVersions: v, HasChildren: true},
		{Type: acct + "/" + cosmosSQLDatabasesType + "/" + cosmosContainersType, APIVersions: v},
		{Type: acct + "/" + cosmosSQLDatabasesType + "/" + cosmosThroughputType, APIVersions: v},
		{
			Type:        acct + "/" + cosmosSQLDatabasesType + "/" + cosmosContainersType + "/" + cosmosThroughputType,
			APIVersions: v,
		},
	}
}

func newCosmosKey() string {
	b := make([]byte, cosmosKeyBytes)
	_, _ = rand.Read(b)

	return base64.StdEncoding.EncodeToString(b)
}

func (p *CosmosProvider) newKeys() cosmosKeys {
	primary := p.dataPlane.MasterKey()
	if primary == "" {
		primary = newCosmosKey()
	}

	return cosmosKeys{
		Primary: primary, Secondary: newCosmosKey(),
		PrimaryReadonly: newCosmosKey(), SecondaryReadonly: newCosmosKey(),
	}
}

// authorizeKeys makes the data plane accept every key the account hands out.
func (p *CosmosProvider) authorizeKeys(k cosmosKeys) {
	for _, key := range k.all() {
		p.dataPlane.AcceptKey(key)
	}
}

func (p *CosmosProvider) owned(rg, name string) (*storedCosmosAccount, bool) {
	a, ok := p.accounts[strings.ToLower(name)]
	if !ok || !resourceGroupsEqual(a.resourceGroup, rg) {
		return nil, false
	}

	return a, true
}

func (p *CosmosProvider) isAccount(id ResourceID) bool {
	return len(id.Types) == 1 && strings.EqualFold(id.Types[0], cosmosAccountsType)
}

func unsupportedCosmosType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.DocumentDB resource type %q", ErrResourceNotFound, id.ResourceType())
}

// Put implements ResourceProvider.
func (p *CosmosProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	if !p.isAccount(id) {
		return p.putChild(id, body)
	}

	if err := validateCosmosAccountName(id.LeafName()); err != nil {
		return nil, err
	}

	p.mu.Lock("Put")
	defer p.mu.Unlock()

	key := strings.ToLower(id.LeafName())

	existing, existed := p.accounts[key]
	if existed && !resourceGroupsEqual(existing.resourceGroup, id.ResourceGroup) {
		return nil, errCosmosAccountOtherGroup
	}

	a := p.buildAccount(ctx, id, body, existing)
	p.accounts[key] = a

	if !existed {
		p.authorizeKeys(a.keys)
	}

	return p.accountBody(ctx, id, a), nil
}

func validateCosmosAccountName(name string) error {
	if len(name) < cosmosNameMin || len(name) > cosmosNameMax || !cosmosNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: account name must be 3-44 lowercase letters, digits and hyphens", ErrCosmosBadRequest)
	}

	return nil
}

func (p *CosmosProvider) buildAccount(
	ctx context.Context, id ResourceID, body map[string]any, existing *storedCosmosAccount,
) *storedCosmosAccount {
	props, _ := body[fieldProperties].(map[string]any)
	if props == nil {
		props = map[string]any{}
	}

	a := &storedCosmosAccount{
		name: id.LeafName(), resourceGroup: id.ResourceGroup, subscription: id.SubscriptionID,
		properties: props, tags: stringTags(body[fieldTags]), identity: body[cosmosFieldIdentity],
		kind: cosmosDefaultKind, location: DefaultLocation, host: RequestHostFromContext(ctx),
	}

	if kind, _ := body[cosmosFieldKind].(string); kind != "" {
		a.kind = kind
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		a.location = loc
	}

	if existing == nil {
		a.keys = p.newKeys()
		a.instanceID = uuid.NewString()

		return a
	}

	a.keys, a.instanceID = existing.keys, existing.instanceID
	if a.host == "" {
		a.host = existing.host
	}

	return a
}

// Get implements ResourceProvider.
func (p *CosmosProvider) Get(ctx context.Context, id ResourceID) (map[string]any, error) {
	if !p.isAccount(id) {
		return p.getChild(id)
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	a, ok := p.owned(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrCosmosAccountNotFound
	}

	return p.accountBody(ctx, id, a), nil
}

// Patch merges tags, identity and properties into an existing account.
func (p *CosmosProvider) Patch(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("Patch")
	defer p.mu.Unlock()

	a, ok := p.owned(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrCosmosAccountNotFound
	}

	if _, has := body[fieldTags]; has {
		a.tags = stringTags(body[fieldTags])
	}

	if ident, has := body[cosmosFieldIdentity]; has {
		a.identity = ident
	}

	if props, _ := body[fieldProperties].(map[string]any); props != nil {
		maps.Copy(a.properties, props)
	}

	return p.accountBody(ctx, id, a), nil
}

// Delete implements ResourceProvider.
func (p *CosmosProvider) Delete(_ context.Context, id ResourceID) error {
	if !p.isAccount(id) {
		return p.deleteChild(id)
	}

	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	a, ok := p.owned(id.ResourceGroup, id.LeafName())
	if !ok {
		return ErrCosmosAccountNotFound
	}

	p.dropAccountLocked(a)

	return nil
}

// dropAccountLocked removes a and the databases and keys it owns; the caller holds p.mu.
func (p *CosmosProvider) dropAccountLocked(a *storedCosmosAccount) {
	acct := strings.ToLower(a.name)

	for db, owner := range p.dbOwner {
		if owner == acct {
			_ = p.dataPlane.DeleteDatabase(db)
			delete(p.dbOwner, db)
		}
	}

	prefix := acct + "/"

	for k := range p.throughput {
		if strings.HasPrefix(k, prefix) {
			delete(p.throughput, k)
		}
	}

	for k := range p.extras {
		if strings.HasPrefix(k, prefix) {
			delete(p.extras, k)
		}
	}

	for _, key := range a.keys.all() {
		p.dataPlane.RevokeKey(key)
	}

	delete(p.accounts, acct)
}

// List implements ResourceProvider; id's Names holds the parent chain for child lists.
func (p *CosmosProvider) List(ctx context.Context, id ResourceID) ([]map[string]any, error) {
	if len(id.Types) > 1 {
		return p.listChildren(id)
	}

	p.mu.RLock("List")
	defer p.mu.RUnlock()

	found := make([]*storedCosmosAccount, 0, len(p.accounts))

	for _, a := range p.accounts {
		if id.ResourceGroup == "" || resourceGroupsEqual(a.resourceGroup, id.ResourceGroup) {
			found = append(found, a)
		}
	}

	sort.Slice(found, func(i, j int) bool { return found[i].name < found[j].name })

	out := make([]map[string]any, 0, len(found))

	for _, a := range found {
		aid := ResourceID{
			SubscriptionID: a.subscription, ResourceGroup: a.resourceGroup,
			Namespace: namespaceMicrosoftDocumentDB, Types: []string{cosmosAccountsType}, Names: []string{a.name},
		}
		out = append(out, p.accountBody(ctx, aid, a))
	}

	return out, nil
}

// Reset implements ResourceProvider.
func (p *CosmosProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	for _, a := range p.accounts {
		p.dropAccountLocked(a)
	}

	p.accounts = make(map[string]*storedCosmosAccount)
	p.dbOwner = make(map[string]string)
	p.throughput = make(map[string]cosmosThroughput)
	p.extras = make(map[string]cosmosContainerExtras)
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *CosmosProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for _, a := range p.accounts {
		if resourceGroupsEqual(a.resourceGroup, resourceGroup) {
			p.dropAccountLocked(a)
		}
	}
}

func (p *CosmosProvider) documentEndpoint(host string) string {
	port, tls := p.dataPlane.Endpoint()

	scheme := "http"
	if tls {
		scheme = "https"
	}

	hostAndPort := p.cfg.Override
	if hostAndPort == "" {
		if host == "" {
			host = cosmosDefaultHost
		}

		hostAndPort = net.JoinHostPort(host, strconv.Itoa(port))
	}

	return scheme + "://" + hostAndPort + "/"
}

func (p *CosmosProvider) accountBody(ctx context.Context, id ResourceID, a *storedCosmosAccount) map[string]any {
	host := RequestHostFromContext(ctx)
	if host == "" {
		host = a.host
	}

	props := cosmosAccountDefaults(a)
	maps.Copy(props, a.properties)

	endpoint := p.documentEndpoint(host)
	props[cosmosFieldDocEndpoint] = endpoint
	props[cosmosFieldConsistency] = fillConsistency(props[cosmosFieldConsistency])
	props[cosmosFieldBackup] = fillBackupPolicy(props[cosmosFieldBackup])
	applyCosmosLocations(props, a, endpoint)
	props[fieldProvisioningState] = provisioningStateSucceeded
	props["instanceId"] = a.instanceID

	out := map[string]any{
		"id":            id.ARMID(),
		fieldName:       a.name,
		fieldType:       namespaceMicrosoftDocumentDB + "/" + cosmosAccountsType,
		fieldLocation:   a.location,
		fieldTags:       tagsOrEmpty(a.tags),
		cosmosFieldKind: a.kind,
		fieldProperties: props,
	}

	if a.identity != nil {
		out[cosmosFieldIdentity] = a.identity
	}

	return out
}

func cosmosAccountDefaults(*storedCosmosAccount) map[string]any {
	return map[string]any{
		cosmosFieldOfferType:                 "Standard",
		cosmosFieldPublicNetwork:             kvPublicNetworkEnabled,
		"enableAutomaticFailover":            false,
		"enableMultipleWriteLocations":       false,
		"isVirtualNetworkFilterEnabled":      false,
		"virtualNetworkRules":                []any{},
		"ipRules":                            []any{},
		"capabilities":                       []any{},
		"cors":                               []any{},
		"enableFreeTier":                     false,
		"enableAnalyticalStorage":            false,
		"disableKeyBasedMetadataWriteAccess": false,
		"disableLocalAuth":                   false,
		"defaultIdentity":                    "FirstPartyIdentity",
		"networkAclBypass":                   "None",
		"networkAclBypassResourceIds":        []any{},
		"minimalTlsVersion":                  "Tls12",
		"enablePartitionMerge":               false,
		"enableBurstCapacity":                false,
		"capacity":                           map[string]any{"totalThroughputLimit": -1},
		"configurationOverrides":             map[string]any{},
		"privateEndpointConnections":         []any{},
	}
}

func fillConsistency(in any) map[string]any {
	out := map[string]any{"defaultConsistencyLevel": "Session"}
	if m, ok := in.(map[string]any); ok {
		maps.Copy(out, m)
	}

	if _, ok := out["maxIntervalInSeconds"]; !ok {
		out["maxIntervalInSeconds"] = 5
	}

	if _, ok := out["maxStalenessPrefix"]; !ok {
		out["maxStalenessPrefix"] = 100
	}

	return out
}

func fillBackupPolicy(in any) map[string]any {
	out := map[string]any{cosmosFieldType: "Periodic"}
	if m, ok := in.(map[string]any); ok {
		maps.Copy(out, m)
	}

	if strings.EqualFold(fmt.Sprint(out[cosmosFieldType]), "Continuous") {
		if _, ok := out["continuousModeProperties"]; !ok {
			out["continuousModeProperties"] = map[string]any{cosmosFieldTier: "Continuous30Days"}
		}

		return out
	}

	periodic := map[string]any{
		"backupIntervalInMinutes":        cosmosBackupIntervalMinutes,
		"backupRetentionIntervalInHours": cosmosBackupRetentionHours,
		"backupStorageRedundancy":        "Geo",
	}
	if m, ok := out["periodicModeProperties"].(map[string]any); ok {
		maps.Copy(periodic, m)
	}

	out["periodicModeProperties"] = periodic

	return out
}

type cosmosLocation struct {
	name     string
	priority int
	zone     bool
}

func requestedCosmosLocations(props map[string]any, a *storedCosmosAccount) []cosmosLocation {
	var locs []cosmosLocation

	list, _ := props[cosmosFieldLocations].([]any)
	for i, raw := range list {
		m, _ := raw.(map[string]any)
		name, _ := m[cosmosFieldLocationName].(string)

		if name == "" {
			continue
		}

		prio := i
		if f, ok := m[cosmosFieldFailoverPriority].(float64); ok {
			prio = int(f)
		}

		zone, _ := m["isZoneRedundant"].(bool)
		locs = append(locs, cosmosLocation{name: name, priority: prio, zone: zone})
	}

	if len(locs) == 0 {
		locs = []cosmosLocation{{name: a.location}}
	}

	sort.SliceStable(locs, func(i, j int) bool { return locs[i].priority < locs[j].priority })

	return locs
}

func cosmosLocationID(account, location string) string {
	return account + "-" + strings.ToLower(strings.ReplaceAll(location, " ", ""))
}

func applyCosmosLocations(props map[string]any, a *storedCosmosAccount, endpoint string) {
	locs := requestedCosmosLocations(props, a)
	full := make([]any, 0, len(locs))
	policies := make([]any, 0, len(locs))

	for _, l := range locs {
		lid := cosmosLocationID(a.name, l.name)
		full = append(full, map[string]any{
			"id": lid, cosmosFieldLocationName: l.name, cosmosFieldDocEndpoint: endpoint,
			fieldProvisioningState: provisioningStateSucceeded, cosmosFieldFailoverPriority: l.priority,
			"isZoneRedundant": l.zone,
		})
		policies = append(policies, map[string]any{
			"id": lid, cosmosFieldLocationName: l.name, cosmosFieldFailoverPriority: l.priority,
		})
	}

	props[cosmosFieldLocations] = full
	props["readLocations"] = full
	props["writeLocations"] = full[:1]

	if multi, _ := props["enableMultipleWriteLocations"].(bool); multi {
		props["writeLocations"] = full
	}

	props["failoverPolicies"] = policies
}
