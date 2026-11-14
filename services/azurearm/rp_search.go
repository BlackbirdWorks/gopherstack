package azurearm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	srchOldAPIVersion        = "2022-09-01"
	srchPreviewAPIVersion    = "2024-06-01-preview"
	srchFieldAvailable       = "nameAvailable"
	namespaceMicrosoftSearch = "Microsoft.Search"
	srchServicesType         = "searchServices"
	srchHostInfix            = ".search."
	srchDefaultHost          = "localhost"
	srchSKUFree              = "free"
	srchSKUBasic             = "basic"
	srchSKUStandard3         = "standard3"
	srchHostingDefault       = "default"
	srchHostingHighDensity   = "highDensity"
	srchAccessEnabled        = "enabled"
	srchAuthAPIKeyOnly       = "apiKeyOnly"
	srchAuthAADOrAPIKey      = "aadOrApiKey"
	srchAADFailureMode       = "http401WithBearerChallenge"
	srchMaxSearchUnits       = 36
	srchMaxReplicas          = 12
	srchBasicMaxReplicas     = 3
	srchHDMaxPartitions      = 3
	srchMinNameLen           = 2
	srchMaxNameLen           = 60
	srchFieldAuthOptions     = "authOptions"
	srchFieldDisableLocal    = "disableLocalAuth"
	srchFieldSemantic        = "semanticSearch"
	srchFieldCMK             = "encryptionWithCmk"
	srchFieldReplicas        = "replicaCount"
	srchFieldPartitions      = "partitionCount"
	srchFieldHosting         = "hostingMode"
	srchFieldPublicAccess    = "publicNetworkAccess"
	srchFieldEndpoint        = "endpoint"
	srchFieldStatus          = "status"
	srchFieldIdentity        = "identity"
	srchSKUStandard          = "standard"
	srchBypassNone           = "None"
	srchFieldPEC             = "privateEndpointConnections"
	srchReasonInvalid        = "Invalid"
	srchReasonExists         = "AlreadyExists"
	srchCodeConflict         = "Conflict"
	srchCodeBadRequest       = "BadRequest"
	srchCodeUnavailable      = "ServiceUnavailable"
)

var srchValidSKUs = []string{ //nolint:gochecknoglobals // read-only SKU list
	srchSKUFree, srchSKUBasic, srchSKUStandard, "standard2", srchSKUStandard3,
	"storage_optimized_l1", "storage_optimized_l2",
}

var srchNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9]([a-z0-9]|-[a-z0-9])*$`)

func searchAPIVersions() []string {
	return []string{"2025-05-01", srchPreviewAPIVersion, "2023-11-01", srchOldAPIVersion}
}

// SearchEndpointConfig configures the endpoints the provider advertises.
type SearchEndpointConfig struct {
	Override string
	Port     int
}

type storedSearch struct {
	createdAt     time.Time
	tags          map[string]string
	extra         map[string]any
	links         map[string]*storedSearchLink
	authOptions   map[string]any
	identity      map[string]any
	name          string
	resourceGroup string
	location      string
	host          string
	sku           string
	hostingMode   string
	publicAccess  string
	replicas      int
	partitions    int
	disableLocal  bool
}

// SearchProvider implements ResourceProvider for Microsoft.Search. ARM keeps location, tags, SKU,
// capacity and unmodelled properties; services and keys live in the data plane.
type SearchProvider struct {
	dataPlane SearchPlane
	mu        *lockmetrics.RWMutex
	res       map[string]*storedSearch
	cfg       SearchEndpointConfig
}

// NewSearchProvider creates a SearchProvider; dataPlane may be nil.
func NewSearchProvider(cfg SearchEndpointConfig, dataPlane SearchPlane) *SearchProvider {
	if dataPlane == nil {
		dataPlane = noopSearchPlane{}
	}

	if cfg.Port == 0 {
		cfg.Port = DefaultSearchPort
	}

	return &SearchProvider{
		mu: lockmetrics.New("azurearm.searchprovider"), res: map[string]*storedSearch{}, cfg: cfg,
		dataPlane: dataPlane,
	}
}

var _ ResourceProvider = (*SearchProvider)(nil)

// Namespace implements ResourceProvider.
func (p *SearchProvider) Namespace() string { return namespaceMicrosoftSearch }

// ResourceTypes implements ResourceProvider.
func (p *SearchProvider) ResourceTypes() []ResourceTypeDef {
	return []ResourceTypeDef{{Type: srchServicesType, APIVersions: searchAPIVersions(), HasChildren: false}}
}

func errUnsupportedSearchType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.Search resource type %q", ErrResourceNotFound, id.ResourceType())
}

func srchKey(rg, name string) string { return ehKey(rg, name) }

func srchValidName(name string) bool {
	return len(name) >= srchMinNameLen && len(name) <= srchMaxNameLen && srchNameRE.MatchString(name)
}

func badSearchRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrSearchInvalidConfig, fmt.Sprintf(format, args...))
}

func srchInt(v any, def int) (int, bool) {
	switch n := v.(type) {
	case nil:
		return def, true
	case float64:
		return int(n), n == float64(int(n))
	case int:
		return n, true
	}

	return 0, false
}

// validateCapacity enforces the documented replica/partition limits per SKU.
func validateCapacity(sku, hosting string, replicas, partitions int) error {
	if hosting == srchHostingHighDensity && sku != srchSKUStandard3 {
		return badSearchRequest("hostingMode highDensity requires the standard3 SKU")
	}

	if replicas < 1 || replicas > srchMaxReplicas {
		return badSearchRequest("replicaCount must be between 1 and %d", srchMaxReplicas)
	}

	switch sku {
	case srchSKUFree:
		if replicas != 1 || partitions != 1 {
			return badSearchRequest("the free SKU supports exactly 1 replica and 1 partition")
		}

		return nil
	case srchSKUBasic:
		if replicas > srchBasicMaxReplicas || partitions != 1 {
			return badSearchRequest("the basic SKU supports up to %d replicas and 1 partition", srchBasicMaxReplicas)
		}

		return nil
	}

	valid := []int{1, 2, 3, 4, 6, 12}
	if hosting == srchHostingHighDensity {
		valid = []int{1, 2, srchHDMaxPartitions}
	}

	if !slices.Contains(valid, partitions) {
		return badSearchRequest("partitionCount must be one of %v for this SKU and hosting mode", valid)
	}

	if replicas*partitions > srchMaxSearchUnits {
		return badSearchRequest("replicaCount x partitionCount must not exceed %d search units", srchMaxSearchUnits)
	}

	return nil
}

func srchAuthOf(props map[string]any) (map[string]any, bool, error) {
	opts, present := props[srchFieldAuthOptions].(map[string]any)
	if !present || len(opts) == 0 {
		return nil, false, nil
	}

	_, keyOnly := opts[srchAuthAPIKeyOnly]
	aad, hasAAD := opts[srchAuthAADOrAPIKey].(map[string]any)

	switch {
	case keyOnly && hasAAD:
		return nil, false, badSearchRequest("authOptions must set exactly one of apiKeyOnly and aadOrApiKey")
	case keyOnly:
		return map[string]any{srchAuthAPIKeyOnly: map[string]any{}}, true, nil
	case hasAAD:
		mode, _ := aad["aadAuthFailureMode"].(string)
		if mode == "" {
			mode = srchAADFailureMode
		}

		if mode != srchAADFailureMode && mode != "http403" {
			return nil, false, badSearchRequest("unsupported aadAuthFailureMode %q", mode)
		}

		return map[string]any{srchAuthAADOrAPIKey: map[string]any{"aadAuthFailureMode": mode}}, true, nil
	}

	return nil, false, badSearchRequest("authOptions must set apiKeyOnly or aadOrApiKey")
}

func (r *storedSearch) planeConfig() SearchServiceConfig {
	cfg := SearchServiceConfig{
		SKU: r.sku, HostingMode: r.hostingMode, PublicNetworkAccess: r.publicAccess, AuthMode: srchAuthAPIKeyOnly,
		ReplicaCount: r.replicas, PartitionCount: r.partitions, DisableLocalAuth: r.disableLocal,
	}

	if aad, ok := r.authOptions[srchAuthAADOrAPIKey].(map[string]any); ok {
		cfg.AuthMode = srchAuthAADOrAPIKey
		cfg.AADFailureMode, _ = aad["aadAuthFailureMode"].(string)
	}

	return cfg
}

func (p *SearchProvider) nameTakenElsewhere(id ResourceID) bool {
	for _, r := range p.res {
		if strings.EqualFold(r.name, id.LeafName()) && !resourceGroupsEqual(r.resourceGroup, id.ResourceGroup) {
			return true
		}
	}

	return false
}

// Put implements ResourceProvider.
func (p *SearchProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	if !strings.EqualFold(strings.Join(id.Types, "/"), srchServicesType) {
		return nil, errUnsupportedSearchType(id)
	}

	return p.write(ctx, id, body, false)
}

// Patch merges a partial update (the Update operation) into an existing service.
func (p *SearchProvider) Patch(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	return p.write(ctx, id, body, true)
}

func (p *SearchProvider) write(
	ctx context.Context,
	id ResourceID,
	body map[string]any,
	patch bool,
) (map[string]any, error) {
	p.mu.Lock("Put")
	defer p.mu.Unlock()

	name := id.LeafName()
	if !srchValidName(name) {
		return nil, badSearchRequest("the service name %q is invalid", name)
	}

	existing := p.res[srchKey(id.ResourceGroup, name)]
	if existing == nil && patch {
		return nil, ErrSearchServiceNotFound
	}

	if existing == nil && p.nameTakenElsewhere(id) {
		return nil, ErrSearchNameTaken
	}

	rec, err := p.buildRecord(id, body, existing)
	if err == nil && rec.host == "" {
		rec.host = RequestHostFromContext(ctx)
	}

	if err != nil {
		return nil, err
	}

	created, err := p.dataPlane.PutService(rec.name, rec.planeConfig())
	if err != nil {
		return nil, err
	}

	rec.createdAt = created
	p.res[srchKey(id.ResourceGroup, name)] = rec

	return p.body(id, rec), nil
}

func (p *SearchProvider) buildRecord(id ResourceID, body map[string]any, old *storedSearch) (*storedSearch, error) {
	rec := &storedSearch{
		name: id.LeafName(), resourceGroup: id.ResourceGroup, location: DefaultLocation, sku: srchSKUStandard,
		hostingMode: srchHostingDefault, publicAccess: srchAccessEnabled, replicas: 1, partitions: 1,
		tags: map[string]string{},
	}

	if old != nil {
		cp := *old
		rec = &cp
		rec.extra = maps.Clone(old.extra)
	}

	if err := rec.apply(body, old != nil); err != nil {
		return nil, err
	}

	return rec, validateCapacity(rec.sku, rec.hostingMode, rec.replicas, rec.partitions)
}

func (r *storedSearch) apply(body map[string]any, update bool) error {
	if loc, _ := body[fieldLocation].(string); loc != "" && !update {
		r.location = loc
	}

	if t, ok := body[fieldTags]; ok {
		r.tags = stringTags(t)
	}

	if ident, ok := body[srchFieldIdentity].(map[string]any); ok {
		r.identity = ident
		if len(ident) == 0 {
			r.identity = nil
		}
	}

	if sku, ok := body[ehFieldSKU].(map[string]any); ok {
		name, _ := sku[fieldName].(string)

		name = strings.ToLower(name)
		if !slices.Contains(srchValidSKUs, name) {
			return badSearchRequest("unsupported sku %q", name)
		}

		if update && name != r.sku {
			return ErrSearchSKUImmutable
		}

		r.sku = name
	}

	props, _ := body[fieldProperties].(map[string]any)
	if props == nil {
		return nil
	}

	return r.applyProps(props)
}

func (r *storedSearch) applyProps(props map[string]any) error {
	var ok bool

	if r.replicas, ok = srchInt(props[srchFieldReplicas], r.replicas); !ok {
		return badSearchRequest("replicaCount must be an integer")
	}

	if r.partitions, ok = srchInt(props[srchFieldPartitions], r.partitions); !ok {
		return badSearchRequest("partitionCount must be an integer")
	}

	if err := r.applyModes(props); err != nil {
		return err
	}

	if err := validateSearchEnums(props); err != nil {
		return err
	}

	if v, has := props[srchFieldDisableLocal].(bool); has {
		r.disableLocal = v
	}

	auth, set, err := srchAuthOf(props)
	if err != nil {
		return err
	}

	if set {
		r.authOptions = auth
	}

	if r.disableLocal && r.authOptions != nil {
		return badSearchRequest("authOptions cannot be set while disableLocalAuth is true")
	}

	r.extra = dropEmptyObjects(ehMerge(map[string]any{}, r.extra, ehExtra(map[string]any{fieldProperties: props},
		srchFieldReplicas, srchFieldPartitions, srchFieldHosting, srchFieldPublicAccess, srchFieldDisableLocal,
		srchFieldAuthOptions, srchFieldEndpoint, srchFieldStatus, "statusDetails", "privateEndpointConnections",
		"sharedPrivateLinkResources", "upgradeAvailable", "serviceUpgradedAt", "eTag")))

	return nil
}

func (p *SearchProvider) endpointFor(name, host string) string {
	hostAndPort := p.cfg.Override
	if hostAndPort == "" {
		if host == "" {
			host = srchDefaultHost
		}

		port := p.cfg.Port
		if pp := p.dataPlane.Port(); pp > 0 {
			port = pp
		}

		hostAndPort = "search." + net.JoinHostPort(host, strconv.Itoa(port))
	}

	return "https://" + name + "." + hostAndPort
}

func (p *SearchProvider) body(id ResourceID, rec *storedSearch) map[string]any {
	props := ehMerge(map[string]any{
		srchFieldDisableLocal: rec.disableLocal,
		"eTag":                "W/\"" + rec.createdAt.UTC().Format("20060102T150405.000Z") + "\"",
		"networkRuleSet":      map[string]any{"ipRules": []any{}, "bypass": srchBypassNone},
	}, rec.extra, map[string]any{
		srchFieldReplicas: rec.replicas, srchFieldPartitions: rec.partitions, srchFieldHosting: rec.hostingMode,
		srchFieldPublicAccess: rec.publicAccess, srchFieldStatus: "running", "statusDetails": "",
		fieldProvisioningState: "succeeded", srchFieldEndpoint: p.endpointFor(rec.name, rec.host),
		srchFieldPEC: []any{}, "sharedPrivateLinkResources": p.linkBodies(id, rec),
	})

	if rec.authOptions != nil {
		props[srchFieldAuthOptions] = rec.authOptions
	}

	props[srchFieldCMK] = map[string]any{
		"enforcement": firstNonEmpty(
			strOf(mapOf(rec.extra[srchFieldCMK]), "enforcement"),
			"Unspecified",
		),
		"encryptionComplianceStatus": "Compliant",
	}

	out := map[string]any{
		"id": id.ARMID(), fieldName: rec.name, fieldType: namespaceMicrosoftSearch + "/" + srchServicesType,
		fieldLocation: rec.location, fieldTags: tagsOrEmpty(rec.tags), fieldProperties: props,
		ehFieldSKU: map[string]any{fieldName: rec.sku},
	}

	if rec.identity != nil {
		ident := maps.Clone(rec.identity)
		if t, _ := ident[fieldType].(string); strings.Contains(strings.ToLower(t), "systemassigned") {
			ident["principalId"] = uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.ToLower(id.ARMID()))).String()
			ident["tenantId"] = DefaultTenantID
		}

		out[srchFieldIdentity] = ident
	}

	return out
}

// Get implements ResourceProvider.
func (p *SearchProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	if !strings.EqualFold(strings.Join(id.Types, "/"), srchServicesType) {
		return nil, errUnsupportedSearchType(id)
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	rec, ok := p.res[srchKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, ErrSearchServiceNotFound
	}

	return p.body(id, rec), nil
}

// Delete implements ResourceProvider.
func (p *SearchProvider) Delete(_ context.Context, id ResourceID) error {
	if !strings.EqualFold(strings.Join(id.Types, "/"), srchServicesType) {
		return errUnsupportedSearchType(id)
	}

	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	return p.deleteLocked(id.ResourceGroup, id.LeafName())
}

func (p *SearchProvider) deleteLocked(rg, name string) error {
	if _, ok := p.res[srchKey(rg, name)]; !ok {
		return ErrSearchServiceNotFound
	}

	delete(p.res, srchKey(rg, name))

	return p.dataPlane.DeleteService(name)
}

// List implements ResourceProvider.
func (p *SearchProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	out := []map[string]any{}

	for _, rec := range p.res {
		if id.ResourceGroup != "" && !resourceGroupsEqual(rec.resourceGroup, id.ResourceGroup) {
			continue
		}

		rid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: rec.resourceGroup,
			Namespace: namespaceMicrosoftSearch, Types: []string{srchServicesType}, Names: []string{rec.name},
		}
		out = append(out, p.body(rid, rec))
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

// Reset implements ResourceProvider.
func (p *SearchProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	p.res = map[string]*storedSearch{}
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *SearchProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for _, rec := range p.res {
		if resourceGroupsEqual(rec.resourceGroup, resourceGroup) {
			_ = p.deleteLocked(rec.resourceGroup, rec.name)
		}
	}
}

func (p *SearchProvider) service(id ResourceID) (*storedSearch, error) {
	rec, ok := p.res[srchKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, ErrSearchServiceNotFound
	}

	return rec, nil
}

// CheckName reports whether name is a valid, unused search service name.
func (p *SearchProvider) CheckName(name string) map[string]any {
	p.mu.RLock("CheckName")
	defer p.mu.RUnlock()

	if !srchValidName(name) {
		return map[string]any{
			srchFieldAvailable: false, acFieldReason: srchReasonInvalid,
			fieldMessage: "Search service names must only contain lowercase letters, digits or dashes, " +
				"cannot use dash as the first two or last one characters, cannot contain consecutive dashes, " +
				"and must be between 2 and 60 characters in length.",
		}
	}

	for _, r := range p.res {
		if strings.EqualFold(r.name, name) {
			return map[string]any{
				srchFieldAvailable: false, acFieldReason: srchReasonExists,
				fieldMessage: "A search service with the name '" + name + "' already exists.",
			}
		}
	}

	return map[string]any{srchFieldAvailable: true}
}

func searchErrorDetails(err error) (errorEntry, bool) {
	table := []struct {
		err   error
		entry errorEntry
	}{
		{ErrSearchServiceNotFound, notFoundEntry("The search service was not found.")},
		{ErrSearchQueryKeyNotFound, notFoundEntry("The query key was not found.")},
		{ErrSearchLinkNotFound, notFoundEntry("The shared private link resource was not found.")},
		{
			ErrSearchNameTaken,
			errorEntry{"NameAlreadyExists", "The search service name is already in use.", http.StatusConflict},
		},
		{
			ErrSearchQueryKeyNameTaken,
			errorEntry{srchCodeConflict, "A query key with this name already exists.", http.StatusConflict},
		},
		{
			ErrSearchSKUImmutable,
			errorEntry{
				srchCodeBadRequest,
				"The SKU of an existing search service cannot be changed.",
				http.StatusBadRequest,
			},
		},
		{
			ErrSearchUnavailable,
			errorEntry{srchCodeUnavailable, "The search data plane is not available.", http.StatusServiceUnavailable},
		},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	if errors.Is(err, ErrSearchInvalidConfig) {
		return errorEntry{
			"BadRequest",
			strings.TrimPrefix(err.Error(), ErrSearchInvalidConfig.Error()+": "),
			http.StatusBadRequest,
		}, true
	}

	return errorEntry{}, false
}

// validateSearchEnums checks semanticSearch and encryptionWithCmk.enforcement against the armsearch enums.
func validateSearchEnums(props map[string]any) error {
	if v, has := props[srchFieldSemantic]; has {
		if s, _ := v.(string); !containsFold([]string{"disabled", "free", srchSKUStandard}, s) || s == "" {
			return badSearchRequest("semanticSearch %v is not valid; allowed values are disabled, free and standard", v)
		}
	}

	if enc := mapOf(props[srchFieldCMK]); enc != nil {
		if e := strOf(
			enc,
			"enforcement",
		); e != "" &&
			!slices.Contains([]string{apimDisabled, flexEnabled, "Unspecified"}, e) {
			return badSearchRequest("encryptionWithCmk.enforcement %q is not valid; allowed values are Disabled, "+
				"Enabled and Unspecified", e)
		}
	}

	return nil
}

func (r *storedSearch) applyModes(props map[string]any) error {
	if v, has := props[srchFieldHosting].(string); has {
		if v != srchHostingDefault && v != srchHostingHighDensity {
			return badSearchRequest("unsupported hostingMode %q", v)
		}

		r.hostingMode = v
	}

	if v, has := props[srchFieldPublicAccess].(string); has {
		v = strings.ToLower(v)
		if v != srchAccessEnabled && v != "disabled" && v != "securedbyperimeter" {
			return badSearchRequest("unsupported publicNetworkAccess %q", v)
		}

		r.publicAccess = v
		if v == "securedbyperimeter" {
			r.publicAccess = "securedByPerimeter"
		}
	}

	return nil
}
