package azurearm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftContainerRegistry = "Microsoft.ContainerRegistry"
	acrRegistriesType                   = "registries"
	acrReplicationsType                 = "replications"
	acrWebhooksType                     = "webhooks"
	acrDefaultHost                      = "localhost"
	acrHostPrefix                       = "azurecr."
	acrPropAdmin                        = "adminUserEnabled"
	acrPropServiceURI                   = "serviceUri"
	acrPropActions                      = "actions"
	acrPropStatus                       = "status"
	acrPropScope                        = "scope"
	acrWebhookEnabled                   = "enabled"
	acrSkuPremium                       = "Premium"
	acrFieldSKU                         = "sku"
)

var acrNameRe = regexp.MustCompile(`^[a-zA-Z0-9]{5,50}$`)

func containerRegistryAPIVersions() []string {
	return []string{"2023-11-01-preview", "2023-07-01", "2023-06-01-preview", "2022-12-01"}
}

// ContainerRegistryEndpointConfig configures the login server the provider advertises.
type ContainerRegistryEndpointConfig struct {
	Override string
	Port     int
}

type acrKind int

const (
	acrKindUnsupported acrKind = iota
	acrKindRegistry
	acrKindReplication
	acrKindWebhook
)

type storedACR struct {
	tags          map[string]string
	extra         map[string]any
	props         map[string]any
	identity      map[string]any
	createdAt     time.Time
	name          string
	resourceGroup string
	location      string
	host          string
	sku           string
	registry      string
	kind          acrKind
}

// ContainerRegistryProvider implements ResourceProvider for Microsoft.ContainerRegistry. ARM keeps location,
// tags, SKU, child resources and unmodelled properties; the registry, its admin flag and credentials live in
// the data plane.
type ContainerRegistryProvider struct {
	dataPlane ContainerRegistries
	mu        *lockmetrics.RWMutex
	res       map[string]*storedACR
	cfg       ContainerRegistryEndpointConfig
}

// NewContainerRegistryProvider creates a ContainerRegistryProvider; dataPlane may be nil.
func NewContainerRegistryProvider(
	cfg ContainerRegistryEndpointConfig, dataPlane ContainerRegistries,
) *ContainerRegistryProvider {
	if dataPlane == nil {
		dataPlane = noopContainerRegistries{}
	}

	if cfg.Port == 0 {
		cfg.Port = DefaultContainerRegistryPort
	}

	return &ContainerRegistryProvider{
		mu: lockmetrics.New("azurearm.containerregistryprovider"), res: map[string]*storedACR{}, cfg: cfg,
		dataPlane: dataPlane,
	}
}

var _ ResourceProvider = (*ContainerRegistryProvider)(nil)

// Namespace implements ResourceProvider.
func (p *ContainerRegistryProvider) Namespace() string { return namespaceMicrosoftContainerRegistry }

// ResourceTypes implements ResourceProvider.
func (p *ContainerRegistryProvider) ResourceTypes() []ResourceTypeDef {
	v := containerRegistryAPIVersions()

	return []ResourceTypeDef{
		{Type: acrRegistriesType, APIVersions: v, HasChildren: true},
		{Type: acrRegistriesType + "/" + acrReplicationsType, APIVersions: v, HasChildren: false},
		{Type: acrRegistriesType + "/" + acrWebhooksType, APIVersions: v, HasChildren: false},
	}
}

func classifyACRPath(id ResourceID) acrKind {
	switch strings.ToLower(strings.Join(id.Types, "/")) {
	case acrRegistriesType:
		return acrKindRegistry
	case acrRegistriesType + "/" + acrReplicationsType:
		return acrKindReplication
	case acrRegistriesType + "/" + acrWebhooksType:
		return acrKindWebhook
	}

	return acrKindUnsupported
}

func acrKey(rg string, kind acrKind, names ...string) string {
	return ehKey(rg, append([]string{strconv.Itoa(int(kind))}, names...)...)
}

func errUnsupportedACRType(id ResourceID) error {
	return fmt.Errorf(
		"%w: unsupported Microsoft.ContainerRegistry resource type %q", ErrResourceNotFound, id.ResourceType(),
	)
}

func acrTypeName(kind acrKind) string {
	base := namespaceMicrosoftContainerRegistry + "/" + acrRegistriesType

	switch kind {
	case acrKindReplication:
		return base + "/" + acrReplicationsType
	case acrKindWebhook:
		return base + "/" + acrWebhooksType
	case acrKindRegistry, acrKindUnsupported:
	}

	return base
}

func acrNotFound(kind acrKind) error {
	switch kind {
	case acrKindReplication:
		return ErrACRReplicationNotFound
	case acrKindWebhook:
		return ErrACRWebhookNotFound
	case acrKindRegistry, acrKindUnsupported:
	}

	return ErrACRRegistryNotFound
}

func (p *ContainerRegistryProvider) registry(rg, name string) (*storedACR, bool) {
	r, ok := p.res[acrKey(rg, acrKindRegistry, name)]

	return r, ok
}

func (p *ContainerRegistryProvider) child(id ResourceID, kind acrKind) (*storedACR, bool) {
	r, ok := p.res[acrKey(id.ResourceGroup, kind, id.Names[0], id.Names[1])]

	return r, ok
}

func badACRRequest(err error) error {
	if errors.Is(err, ErrACRUnavailable) {
		return err
	}

	return fmt.Errorf("%w: %w", ErrInvalidRequestBody, err)
}

// Put implements ResourceProvider.
func (p *ContainerRegistryProvider) Put(
	ctx context.Context, id ResourceID, body map[string]any,
) (map[string]any, error) {
	kind := classifyACRPath(id)
	if kind == acrKindUnsupported {
		return nil, errUnsupportedACRType(id)
	}

	p.mu.Lock("Put")
	defer p.mu.Unlock()

	if kind == acrKindRegistry {
		return p.putRegistry(ctx, id, body)
	}

	return p.putChild(ctx, id, kind, body)
}

func parseSKU(body map[string]any, fallback string) (string, error) {
	sku, ok := body[acrFieldSKU].(map[string]any)
	if !ok {
		if fallback != "" {
			return fallback, nil
		}

		return "", ErrACRInvalidSKU
	}

	name, _ := sku[fieldName].(string)
	for _, valid := range []string{"Basic", "Standard", acrSkuPremium} {
		if strings.EqualFold(name, valid) {
			return valid, nil
		}
	}

	return "", ErrACRInvalidSKU
}

func (p *ContainerRegistryProvider) nameTakenElsewhere(id ResourceID) bool {
	for _, r := range p.res {
		if r.kind == acrKindRegistry && strings.EqualFold(r.name, id.LeafName()) &&
			!resourceGroupsEqual(r.resourceGroup, id.ResourceGroup) {
			return true
		}
	}

	return false
}

func (p *ContainerRegistryProvider) putRegistry(
	ctx context.Context, id ResourceID, body map[string]any,
) (map[string]any, error) {
	existing, exists := p.registry(id.ResourceGroup, id.LeafName())

	fallback := ""
	if exists {
		fallback = existing.sku
	}

	sku, err := parseSKU(body, fallback)
	if err != nil {
		return nil, badACRRequest(err)
	}

	if p.nameTakenElsewhere(id) {
		return nil, ErrACRNameTaken
	}

	props, _ := body[fieldProperties].(map[string]any)
	admin, _ := props[acrPropAdmin].(bool)

	info, err := p.dataPlane.PutRegistry(id.LeafName(), admin)
	if err != nil {
		return nil, badACRRequest(err)
	}

	rec := &storedACR{
		name: id.LeafName(), resourceGroup: id.ResourceGroup, location: DefaultLocation, createdAt: info.CreatedAt,
		tags: stringTags(body[fieldTags]), extra: acrExtra(body), sku: sku, kind: acrKindRegistry,
		identity: mapOf(body["identity"]),
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.location = loc
	} else if exists {
		rec.location = existing.location
	}

	if h := RequestHostFromContext(ctx); h != "" {
		rec.host = h
	}

	p.res[acrKey(id.ResourceGroup, acrKindRegistry, rec.name)] = rec

	return p.registryBody(id, rec, info)
}

func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

// acrExtra keeps the request properties the emulator does not model, so they round-trip.
func acrExtra(body map[string]any) map[string]any {
	return ehExtra(body, acrPropAdmin, "loginServer", "creationDate", "status")
}

func (p *ContainerRegistryProvider) putChild(
	ctx context.Context, id ResourceID, kind acrKind, body map[string]any,
) (map[string]any, error) {
	parent, ok := p.registry(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	props, err := childProps(kind, id, parent, body)
	if err != nil {
		return nil, badACRRequest(err)
	}

	rec := &storedACR{
		name: id.LeafName(), resourceGroup: id.ResourceGroup, location: parent.location, createdAt: time.Now().UTC(),
		tags: stringTags(body[fieldTags]), props: props, kind: kind, registry: id.Names[0],
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.location = loc
	}

	if old, exists := p.child(id, kind); exists {
		rec.createdAt = old.createdAt
	}

	if h := RequestHostFromContext(ctx); h != "" {
		rec.host = h
	}

	if kind == acrKindWebhook {
		if err = p.dataPlane.SetWebhook(parent.name, webhookOf(rec)); err != nil {
			return nil, badACRRequest(err)
		}
	}

	p.res[acrKey(id.ResourceGroup, kind, id.Names[0], rec.name)] = rec

	return p.childBody(id, rec), nil
}

func webhookOf(rec *storedACR) ContainerRegistryWebhook {
	w := ContainerRegistryWebhook{Name: rec.name, Headers: map[string]string{}}
	w.ServiceURI, _ = rec.props[acrPropServiceURI].(string)
	w.Scope, _ = rec.props[acrPropScope].(string)
	w.Status, _ = rec.props[acrPropStatus].(string)

	actions, _ := rec.props[acrPropActions].([]any)
	for _, a := range actions {
		if s, ok := a.(string); ok {
			w.Actions = append(w.Actions, s)
		}
	}

	headers, _ := rec.props["customHeaders"].(map[string]any)
	for k, v := range headers {
		if s, ok := v.(string); ok {
			w.Headers[k] = s
		}
	}

	return w
}

var (
	errACRWebhookInvalid = errors.New("webhook needs a serviceUri and at least one action")
	errACRWebhookName    = errors.New("webhook name must be 5-50 alphanumeric characters")
)

func childProps(kind acrKind, id ResourceID, parent *storedACR, body map[string]any) (map[string]any, error) {
	props := ehMerge(map[string]any{}, mapOf(body[fieldProperties]), nil)
	delete(props, fieldProvisioningState)

	if kind == acrKindReplication {
		if parent.sku != acrSkuPremium {
			return nil, ErrACRPremiumRequired
		}

		return props, nil
	}

	if !acrNameRe.MatchString(id.LeafName()) {
		return nil, errACRWebhookName
	}

	uri, _ := props[acrPropServiceURI].(string)
	actions, _ := props[acrPropActions].([]any)

	if uri == "" || len(actions) == 0 {
		return nil, errACRWebhookInvalid
	}

	if _, ok := props[acrPropStatus]; !ok {
		props[acrPropStatus] = acrWebhookEnabled
	}

	return props, nil
}

// Patch updates tags, SKU, identity and properties of a registry in place (ARM PATCH).
func (p *ContainerRegistryProvider) Patch(
	_ context.Context,
	id ResourceID,
	body map[string]any,
) (map[string]any, error) {
	if classifyACRPath(id) != acrKindRegistry {
		return nil, errUnsupportedACRType(id)
	}

	p.mu.Lock("Patch")
	defer p.mu.Unlock()

	rec, ok := p.registry(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	info, ok := p.dataPlane.RegistryInfo(rec.name)
	if !ok {
		return nil, ErrACRRegistryNotFound
	}

	if _, has := body[acrFieldSKU]; has {
		sku, err := parseSKU(body, rec.sku)
		if err != nil {
			return nil, badACRRequest(err)
		}

		rec.sku = sku
	}

	if v, has := body[fieldTags]; has {
		rec.tags = stringTags(v)
	}

	if v := mapOf(body["identity"]); v != nil {
		rec.identity = v
	}

	if props := mapOf(body[fieldProperties]); props != nil {
		updated, err := p.patchProps(rec, info, props)
		if err != nil {
			return nil, err
		}

		info = updated
	}

	return p.registryBody(id, rec, info)
}

func (p *ContainerRegistryProvider) patchProps(
	rec *storedACR, info ContainerRegistryInfo, props map[string]any,
) (ContainerRegistryInfo, error) {
	maps.Copy(rec.extra, acrExtra(map[string]any{fieldProperties: props}))

	if admin, ok := props[acrPropAdmin].(bool); ok && admin != info.AdminEnabled {
		updated, err := p.dataPlane.PutRegistry(rec.name, admin)
		if err != nil {
			return info, badACRRequest(err)
		}

		return updated, nil
	}

	return info, nil
}

// Get implements ResourceProvider.
func (p *ContainerRegistryProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	kind := classifyACRPath(id)
	if kind == acrKindUnsupported {
		return nil, errUnsupportedACRType(id)
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	return p.bodyFor(id, kind)
}

func (p *ContainerRegistryProvider) bodyFor(id ResourceID, kind acrKind) (map[string]any, error) {
	if kind == acrKindRegistry {
		rec, ok := p.registry(id.ResourceGroup, id.LeafName())
		if !ok {
			return nil, ErrACRRegistryNotFound
		}

		info, found := p.dataPlane.RegistryInfo(rec.name)
		if !found {
			return nil, ErrACRRegistryNotFound
		}

		return p.registryBody(id, rec, info)
	}

	if _, ok := p.registry(id.ResourceGroup, id.Names[0]); !ok {
		return nil, ErrACRRegistryNotFound
	}

	rec, ok := p.child(id, kind)
	if !ok {
		return nil, acrNotFound(kind)
	}

	return p.childBody(id, rec), nil
}

// Delete implements ResourceProvider.
func (p *ContainerRegistryProvider) Delete(_ context.Context, id ResourceID) error {
	kind := classifyACRPath(id)
	if kind == acrKindUnsupported {
		return errUnsupportedACRType(id)
	}

	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	if kind == acrKindRegistry {
		return p.deleteRegistry(id.ResourceGroup, id.LeafName())
	}

	key := acrKey(id.ResourceGroup, kind, id.Names[0], id.LeafName())
	if _, ok := p.res[key]; !ok {
		return acrNotFound(kind)
	}

	if kind == acrKindWebhook {
		if err := p.dataPlane.DeleteWebhook(id.Names[0], id.LeafName()); err != nil {
			return err
		}
	}

	delete(p.res, key)

	return nil
}

func (p *ContainerRegistryProvider) deleteRegistry(rg, name string) error {
	if _, ok := p.registry(rg, name); !ok {
		return ErrACRRegistryNotFound
	}

	for k, r := range p.res {
		if r.kind != acrKindRegistry && resourceGroupsEqual(r.resourceGroup, rg) &&
			strings.EqualFold(r.registry, name) {
			delete(p.res, k)
		}
	}

	delete(p.res, acrKey(rg, acrKindRegistry, name))

	return p.dataPlane.DeleteRegistry(name)
}

// List implements ResourceProvider; registries are listable at subscription and group scope.
func (p *ContainerRegistryProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	if classifyACRPath(id) != acrKindRegistry {
		return nil, nil
	}

	p.mu.RLock("List")
	defer p.mu.RUnlock()

	var out []map[string]any

	for _, rec := range p.res {
		if rec.kind != acrKindRegistry ||
			(id.ResourceGroup != "" && !resourceGroupsEqual(rec.resourceGroup, id.ResourceGroup)) {
			continue
		}

		rid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: rec.resourceGroup,
			Namespace: namespaceMicrosoftContainerRegistry, Types: id.Types, Names: []string{rec.name},
		}

		if body, err := p.bodyFor(rid, acrKindRegistry); err == nil {
			out = append(out, body)
		}
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

// ListChildren lists a registry's replications or webhooks.
func (p *ContainerRegistryProvider) ListChildren(id ResourceID, kind acrKind) ([]map[string]any, error) {
	p.mu.RLock("ListChildren")
	defer p.mu.RUnlock()

	if _, ok := p.registry(id.ResourceGroup, id.Names[0]); !ok {
		return nil, ErrACRRegistryNotFound
	}

	out := []map[string]any{}

	for _, rec := range p.res {
		if rec.kind != kind || !resourceGroupsEqual(rec.resourceGroup, id.ResourceGroup) ||
			!strings.EqualFold(rec.registry, id.Names[0]) {
			continue
		}

		cid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: rec.resourceGroup, Namespace: id.Namespace,
			Types: []string{acrRegistriesType, childTypeSegment(kind)}, Names: []string{rec.registry, rec.name},
		}
		out = append(out, p.childBody(cid, rec))
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

func childTypeSegment(kind acrKind) string {
	if kind == acrKindReplication {
		return acrReplicationsType
	}

	return acrWebhooksType
}

// ListKeys is not applicable to registries; credentials come from ListCredentials.
func (p *ContainerRegistryProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	return nil, errUnsupportedACRType(id)
}

// Reset implements ResourceProvider.
func (p *ContainerRegistryProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	p.res = map[string]*storedACR{}
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *ContainerRegistryProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for _, rec := range p.res {
		if rec.kind == acrKindRegistry && resourceGroupsEqual(rec.resourceGroup, resourceGroup) {
			_ = p.deleteRegistry(rec.resourceGroup, rec.name)
		}
	}
}

func (p *ContainerRegistryProvider) loginServer(name, host string) string {
	hostAndPort := p.cfg.Override
	if hostAndPort == "" {
		if host == "" {
			host = acrDefaultHost
		}

		hostAndPort = net.JoinHostPort(acrHostPrefix+host, strconv.Itoa(p.cfg.Port))
	}

	return strings.ToLower(name) + "." + hostAndPort
}

func acrErrorDetails(err error) (errorEntry, bool) {
	table := []struct {
		err   error
		entry errorEntry
	}{
		{ErrACRRegistryNotFound, notFoundEntry("The container registry was not found.")},
		{ErrACRReplicationNotFound, notFoundEntry("The replication was not found.")},
		{ErrACRWebhookNotFound, notFoundEntry("The webhook was not found.")},
		{ErrACRNameTaken, errorEntry{"RegistryNameInUse", "The registry name is already in use.", http.StatusConflict}},
		{ErrACRAdminDisabled, errorEntry{"AdminUserDisabled", "The admin user is not enabled.", http.StatusBadRequest}},
		{ErrACRInvalidCredential, errorEntry{
			"InvalidParameter", "The credential name must be password or password2.", http.StatusBadRequest,
		}},
		{ErrACRUnavailable, errorEntry{
			"ServiceUnavailable", "The container registry data plane is not available.", http.StatusServiceUnavailable,
		}},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	return errorEntry{}, false
}

// CheckName answers POST .../checkNameAvailability.
func (p *ContainerRegistryProvider) CheckName(name string) map[string]any {
	if !acrNameRe.MatchString(name) {
		return map[string]any{
			acFieldAvailable: false, acFieldReason: "Invalid",
			fieldMessage: "The registry name must contain only alphanumeric characters and be 5 to 50 characters long.",
		}
	}

	p.mu.RLock("CheckName")
	defer p.mu.RUnlock()

	for _, r := range p.res {
		if r.kind == acrKindRegistry && strings.EqualFold(r.name, name) {
			return map[string]any{
				acFieldAvailable: false, acFieldReason: "AlreadyExists",
				fieldMessage: "The registry " + name + " is already in use.",
			}
		}
	}

	return map[string]any{"nameAvailable": true}
}
