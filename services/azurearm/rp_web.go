package azurearm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftWeb = "Microsoft.Web"
	webSitesType          = "sites"
	webPlansType          = "serverfarms"
	webFieldKind          = "kind"
	webFieldSiteConfig    = "siteConfig"
	webFieldServerFarmID  = "serverFarmId"
	webFieldLinuxFx       = "linuxFxVersion"
	webFieldAppSettings   = "appSettings"
	webFieldReserved      = "reserved"
	webKindFunctionApp    = "functionapp"
	webConfigWeb          = "web"
	webConfigAppSettings  = "appsettings"
	webStateRunning       = "Running"
	webStateStopped       = "Stopped"
	webPlanSKUDefault     = "Y1"
	webNameMin            = 2
	webNameMax            = 60
)

var webNameRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

func webAPIVersions() []string {
	return []string{"2024-04-01", "2023-12-01", "2023-01-01", "2022-09-01"}
}

type storedWebSite struct {
	Created       time.Time                 `json:"created"`
	Tags          map[string]string         `json:"tags,omitempty"`
	Identity      map[string]any            `json:"identity,omitempty"`
	Extra         map[string]any            `json:"extra,omitempty"`
	Configs       map[string]map[string]any `json:"configs,omitempty"`
	Name          string                    `json:"name"`
	ResourceGroup string                    `json:"resourceGroup"`
	Location      string                    `json:"location"`
	Kind          string                    `json:"kind"`
}

type storedWebPlan struct {
	Created       time.Time         `json:"created"`
	Tags          map[string]string `json:"tags,omitempty"`
	SKU           map[string]any    `json:"sku,omitempty"`
	Extra         map[string]any    `json:"extra,omitempty"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Kind          string            `json:"kind"`
}

// WebProvider implements ResourceProvider for Microsoft.Web function apps and their hosting plans. ARM keeps
// location, tags, plan links, and site configuration; the app itself (settings, keys, package, host) lives in
// the Azure Functions data plane.
type WebProvider struct {
	dataPlane FunctionApps
	mu        *lockmetrics.RWMutex
	sites     map[string]*storedWebSite
	plans     map[string]*storedWebPlan
}

// NewWebProvider creates a WebProvider; dataPlane may be nil.
func NewWebProvider(dataPlane FunctionApps) *WebProvider {
	if dataPlane == nil {
		dataPlane = noopFunctionApps{}
	}

	return &WebProvider{
		mu: lockmetrics.New("azurearm.webprovider"), dataPlane: dataPlane,
		sites: map[string]*storedWebSite{}, plans: map[string]*storedWebPlan{},
	}
}

var _ ResourceProvider = (*WebProvider)(nil)

// Namespace implements ResourceProvider.
func (p *WebProvider) Namespace() string { return namespaceMicrosoftWeb }

// ResourceTypes implements ResourceProvider.
func (p *WebProvider) ResourceTypes() []ResourceTypeDef {
	v := webAPIVersions()

	return []ResourceTypeDef{
		{Type: webSitesType, APIVersions: v, HasChildren: true},
		{Type: webPlansType, APIVersions: v, HasChildren: false},
		{Type: webSitesType + "/config", APIVersions: v, HasChildren: false},
		{Type: webSitesType + "/functions", APIVersions: v, HasChildren: false},
		{Type: webSitesType + "/basicPublishingCredentialsPolicies", APIVersions: v, HasChildren: false},
	}
}

func planKey(rg, name string) string { return strings.ToLower(rg) + "/" + strings.ToLower(name) }

func webSiteKey(name string) string { return strings.ToLower(name) }

func errUnsupportedWebType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.Web resource type %q", ErrResourceNotFound, id.ResourceType())
}

func webKind(id ResourceID) string {
	if len(id.Types) != 1 {
		return ""
	}

	switch strings.ToLower(id.Types[0]) {
	case webSitesType:
		return webSitesType
	case webPlansType:
		return webPlansType
	}

	return ""
}

func badWebRequest(err error) error {
	if errors.Is(err, ErrWebFunctionsUnavailable) || errors.Is(err, ErrWebNameTaken) ||
		errors.Is(err, ErrWebUnsupportedKind) || errors.Is(err, ErrWebPlanNotFound) {
		return err
	}

	return fmt.Errorf("%w: %w", ErrInvalidRequestBody, err)
}

func validWebName(name string) bool {
	return len(name) >= webNameMin && len(name) <= webNameMax && webNameRe.MatchString(name)
}

func (p *WebProvider) site(rg, name string) (*storedWebSite, bool) {
	s, ok := p.sites[webSiteKey(name)]
	if !ok || !resourceGroupsEqual(s.ResourceGroup, rg) {
		return nil, false
	}

	return s, true
}

// Put implements ResourceProvider.
func (p *WebProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("Put")
	defer p.mu.Unlock()

	switch webKind(id) {
	case webSitesType:
		return p.putSite(ctx, id, body)
	case webPlansType:
		return p.putPlan(id, body)
	}

	return nil, errUnsupportedWebType(id)
}

func isFunctionAppKind(kind string) bool {
	return strings.Contains(strings.ToLower(kind), webKindFunctionApp)
}

func (p *WebProvider) planExists(serverFarmID string) bool {
	if serverFarmID == "" {
		return true
	}

	pid, err := ParseGenericResourcePath(serverFarmID)
	if err != nil || !strings.EqualFold(pid.Namespace, namespaceMicrosoftWeb) || webKind(pid) != webPlansType {
		return false
	}

	_, ok := p.plans[planKey(pid.ResourceGroup, pid.LeafName())]

	return ok
}

func (p *WebProvider) putSite(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	name := id.LeafName()
	existing, exists := p.site(id.ResourceGroup, name)

	kind, _ := body[webFieldKind].(string)
	if kind == "" && exists {
		kind = existing.Kind
	}

	if !isFunctionAppKind(kind) {
		return nil, ErrWebUnsupportedKind
	}

	if !validWebName(name) {
		return nil, badWebRequest(fmt.Errorf("site name %q must be %d-%d letters, digits or hyphens", name, webNameMin, webNameMax))
	}

	if other, taken := p.sites[webSiteKey(name)]; taken && !resourceGroupsEqual(other.ResourceGroup, id.ResourceGroup) {
		return nil, ErrWebNameTaken
	}

	props, _ := body[fieldProperties].(map[string]any)

	farm, _ := props[webFieldServerFarmID].(string)
	if !p.planExists(farm) {
		return nil, ErrWebPlanNotFound
	}

	rec := &storedWebSite{
		Name: name, ResourceGroup: id.ResourceGroup, Location: DefaultLocation, Kind: kind, Created: time.Now().UTC(),
		Tags: stringTags(body[fieldTags]), Identity: mapOf(body["identity"]), Configs: map[string]map[string]any{},
		Extra: webSiteExtra(props),
	}

	if exists {
		rec.Created, rec.Location, rec.Configs = existing.Created, existing.Location, existing.Configs
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.Location = loc
	}

	siteCfg := mapOf(props[webFieldSiteConfig])
	cfgWeb := maps.Clone(rec.Configs[webConfigWeb])

	if cfgWeb == nil {
		cfgWeb = map[string]any{}
	}

	for k, v := range siteCfg {
		if k != webFieldAppSettings {
			cfgWeb[k] = v
		}
	}

	rec.Configs[webConfigWeb] = cfgWeb

	linuxFx, _ := cfgWeb[webFieldLinuxFx].(string)

	info, err := p.dataPlane.PutApp(name, FunctionAppConfig{Kind: kind, LinuxFx: linuxFx})
	if err != nil {
		return nil, badWebRequest(err)
	}

	if raw, ok := siteCfg[webFieldAppSettings].([]any); ok {
		if err = p.dataPlane.SetSettings(name, nameValueSettings(raw)); err != nil {
			return nil, badWebRequest(err)
		}
	}

	p.sites[webSiteKey(name)] = rec

	return p.siteBody(ctx, id, rec, info), nil
}

// webSiteExtra keeps the site properties the emulator does not model, so they round-trip.
func webSiteExtra(props map[string]any) map[string]any {
	out := maps.Clone(props)
	if out == nil {
		out = map[string]any{}
	}

	for _, k := range []string{
		webFieldSiteConfig, fieldProvisioningState, "state", "hostNames", "defaultHostName", "enabledHostNames",
		"hostNameSslStates", "repositorySiteName", "usageState", "availabilityState", "name", "lastModifiedTimeUtc",
		"outboundIpAddresses", "possibleOutboundIpAddresses", "resourceGroup",
	} {
		delete(out, k)
	}

	return out
}

func nameValueSettings(raw []any) map[string]string {
	out := make(map[string]string, len(raw))

	for _, e := range raw {
		m, _ := e.(map[string]any)
		n, _ := m[fieldName].(string)

		if n == "" {
			continue
		}

		v, _ := m[fieldValue].(string)
		out[n] = v
	}

	return out
}

func (p *WebProvider) siteBody(_ context.Context, id ResourceID, s *storedWebSite, info FunctionAppInfo) map[string]any {
	state := webStateRunning
	if info.Stopped {
		state = webStateStopped
	}

	cfg := webConfigDefaults(s)
	siteCfg := map[string]any{
		"numberOfWorkers": cfg["numberOfWorkers"], webFieldLinuxFx: cfg[webFieldLinuxFx], "alwaysOn": cfg["alwaysOn"],
		"acrUseManagedIdentityCreds": false, "http20Enabled": cfg["http20Enabled"], "functionAppScaleLimit": 0,
		"minimumElasticInstanceCount": 0,
	}

	props := map[string]any{
		"name": s.Name, "state": state, "hostNames": []string{info.HostName}, "defaultHostName": info.HostName,
		"enabledHostNames": []string{info.HostName, info.SCMHost}, "repositorySiteName": s.Name,
		"usageState": "Normal", "enabled": true, "availabilityState": "Normal",
		"hostNameSslStates": []any{
			map[string]any{fieldName: info.HostName, "sslState": "Disabled", "hostType": "Standard"},
			map[string]any{fieldName: info.SCMHost, "sslState": "Disabled", "hostType": "Repository"},
		},
		webFieldReserved: strings.Contains(strings.ToLower(s.Kind), "linux"), "isXenon": false, "hyperV": false,
		"clientAffinityEnabled": false, "clientCertEnabled": false, "httpsOnly": false, "scmSiteAlsoStopped": false,
		"containerSize": 1536, "dailyMemoryTimeQuota": 0, "outboundIpAddresses": "127.0.0.1",
		"possibleOutboundIpAddresses": "127.0.0.1", "resourceGroup": s.ResourceGroup, webFieldSiteConfig: siteCfg,
		"lastModifiedTimeUtc": info.Created.UTC().Format(time.RFC3339), fieldProvisioningState: provisioningStateSucceeded,
	}

	out := map[string]any{
		"id": id.ARMID(), fieldName: s.Name, fieldType: namespaceMicrosoftWeb + "/" + webSitesType, webFieldKind: s.Kind,
		fieldLocation: s.Location, fieldTags: tagsOrEmpty(s.Tags), fieldProperties: ehMerge(props, s.Extra, nil),
	}

	if s.Identity != nil {
		out["identity"] = s.Identity
	}

	return out
}

// Get implements ResourceProvider.
func (p *WebProvider) Get(ctx context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	switch webKind(id) {
	case webSitesType:
		return p.getSite(ctx, id)
	case webPlansType:
		pl, ok := p.plans[planKey(id.ResourceGroup, id.LeafName())]
		if !ok {
			return nil, ErrWebPlanNotFound
		}

		return p.planBody(id, pl), nil
	}

	return nil, errUnsupportedWebType(id)
}

func (p *WebProvider) getSite(ctx context.Context, id ResourceID) (map[string]any, error) {
	s, ok := p.site(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrWebSiteNotFound
	}

	info, found := p.dataPlane.AppInfo(s.Name)
	if !found {
		return nil, ErrWebSiteNotFound
	}

	return p.siteBody(ctx, id, s, info), nil
}

// Delete implements ResourceProvider.
func (p *WebProvider) Delete(_ context.Context, id ResourceID) error {
	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	switch webKind(id) {
	case webSitesType:
		s, ok := p.site(id.ResourceGroup, id.LeafName())
		if !ok {
			return ErrWebSiteNotFound
		}

		if err := p.dataPlane.DeleteApp(s.Name); err != nil {
			return err
		}

		delete(p.sites, webSiteKey(s.Name))

		return nil
	case webPlansType:
		key := planKey(id.ResourceGroup, id.LeafName())
		if _, ok := p.plans[key]; !ok {
			return ErrWebPlanNotFound
		}

		if p.planSiteCount(id) > 0 {
			return ErrWebPlanInUse
		}

		delete(p.plans, key)

		return nil
	}

	return errUnsupportedWebType(id)
}

// List implements ResourceProvider.
func (p *WebProvider) List(ctx context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	var out []map[string]any

	switch strings.ToLower(id.LeafType()) {
	case webSitesType:
		for _, s := range p.sites {
			if id.ResourceGroup != "" && !resourceGroupsEqual(s.ResourceGroup, id.ResourceGroup) {
				continue
			}

			sid := ResourceID{
				SubscriptionID: id.SubscriptionID, ResourceGroup: s.ResourceGroup, Namespace: namespaceMicrosoftWeb,
				Types: []string{webSitesType}, Names: []string{s.Name},
			}

			if body, err := p.getSite(ctx, sid); err == nil {
				out = append(out, body)
			}
		}
	case webPlansType:
		for _, pl := range p.plans {
			if id.ResourceGroup != "" && !resourceGroupsEqual(pl.ResourceGroup, id.ResourceGroup) {
				continue
			}

			pid := ResourceID{
				SubscriptionID: id.SubscriptionID, ResourceGroup: pl.ResourceGroup, Namespace: namespaceMicrosoftWeb,
				Types: []string{webPlansType}, Names: []string{pl.Name},
			}
			out = append(out, p.planBody(pid, pl))
		}
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

// ListKeys implements ResourceProvider: POST .../sites/{name}/host/default/listkeys is routed here by type.
func (p *WebProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	return nil, errUnsupportedWebType(id)
}

// Reset implements ResourceProvider.
func (p *WebProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	p.sites, p.plans = map[string]*storedWebSite{}, map[string]*storedWebPlan{}
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *WebProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for k, s := range p.sites {
		if resourceGroupsEqual(s.ResourceGroup, resourceGroup) {
			_ = p.dataPlane.DeleteApp(s.Name)

			delete(p.sites, k)
		}
	}

	for k, pl := range p.plans {
		if resourceGroupsEqual(pl.ResourceGroup, resourceGroup) {
			delete(p.plans, k)
		}
	}
}

func webErrorDetails(err error) (errorEntry, bool) {
	table := []struct {
		err   error
		entry errorEntry
	}{
		{ErrWebSiteNotFound, notFoundEntry("The web app was not found.")},
		{ErrWebFunctionNotFound, notFoundEntry("The function was not found.")},
		{ErrWebKeyNotFound, notFoundEntry("The key was not found.")},
		{ErrWebConfigNotFound, notFoundEntry("The site configuration was not found.")},
		{ErrWebPlanNotFound, errorEntry{
			"BadRequest", "The server farm referenced by the site does not exist.", http.StatusBadRequest,
		}},
		{ErrWebNameTaken, errorEntry{"Conflict", "A site with the given name already exists.", http.StatusConflict}},
		{ErrWebPlanInUse, errorEntry{
			"Conflict", "Cannot delete the server farm because it still has sites.", http.StatusConflict,
		}},
		{ErrWebUnsupportedKind, errorEntry{
			"BadRequest", "Only function apps (kind containing 'functionapp') are supported.", http.StatusBadRequest,
		}},
		{ErrWebInvalidKeyType, errorEntry{
			"BadRequest", "The key type must be functionKeys or systemKeys.", http.StatusBadRequest,
		}},
		{ErrWebFunctionsUnavailable, errorEntry{
			"ServiceUnavailable", "The Azure Functions data plane is not available.", http.StatusServiceUnavailable,
		}},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	return errorEntry{}, false
}
