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
	webKindApp            = "app"
	webKindLinux          = "linux"
	webFieldAppCmd        = "appCommandLine"
	webFieldHealthCheck   = "healthCheckPath"
	webConfigWeb          = "web"
	webConfigAppSettings  = "appsettings"
	webCodeBadRequest     = "BadRequest"
	webReasonInvalid      = "Invalid"
	webNone               = "None"
	webAllow              = "Allow"
	webDescription        = "description"
	webPlanFamily         = "family"
	webFieldRG            = "resourceGroup"
	webFieldWorkers       = "numberOfWorkers"
	webContainerSize      = 1536
	webLogsLimitMB        = 35
	webMaxPriority        = 2147483647
	webStateRunning       = "Running"
	webStateStopped       = "Stopped"
	webPlanSKUDefault     = "Y1"
	webMaxWorkers         = 200
	webNameMin            = 2
	webNameMax            = 60
)

var webNameRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

func webAPIVersions() []string {
	return []string{"2024-04-01", "2023-12-01", "2023-01-01", "2022-09-01"}
}

type storedWebSite struct {
	Created       time.Time                 `json:"created"`
	Pending       *pendingSwap              `json:"pendingSwap,omitempty"`
	Extra         map[string]any            `json:"extra,omitempty"`
	Configs       map[string]map[string]any `json:"configs,omitempty"`
	Slots         map[string]*storedWebSite `json:"slots,omitempty"`
	Tags          map[string]string         `json:"tags,omitempty"`
	LastSwap      *swapStatus               `json:"lastSwap,omitempty"`
	Name          string                    `json:"name"`
	Slot          string                    `json:"slot,omitempty"`
	ResourceGroup string                    `json:"resourceGroup"`
	Location      string                    `json:"location"`
	Kind          string                    `json:"kind"`
	Identity      map[string]any            `json:"identity,omitempty"`
	Managed       resourceIdentity          `json:"managedIdentity"`
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
	ids       *identityService
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
		{Type: webSitesType + "/slots", APIVersions: v, HasChildren: true},
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
	if len(id.Types) == 2 && strings.EqualFold(id.Types[0], webSitesType) &&
		strings.EqualFold(id.Types[1], webSlotsType) {
		return webSlotsType
	}

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
		errors.Is(err, ErrWebUnsupportedKind) || errors.Is(err, ErrWebPlanNotFound) ||
		errors.Is(err, ErrWebPlanNotLinux) || errors.Is(err, ErrWebNotFunctionApp) ||
		errors.Is(err, ErrWebInvalidSiteConfig) || errors.Is(err, ErrWebSlotName) {
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
	case webSlotsType:
		return p.putSlot(ctx, id, body)
	case webPlansType:
		return p.putPlan(id, body)
	}

	return nil, errUnsupportedWebType(id)
}

func isFunctionAppKind(kind string) bool {
	return strings.Contains(strings.ToLower(kind), webKindFunctionApp)
}

func kindHasToken(kind, token string) bool {
	for t := range strings.SplitSeq(strings.ToLower(kind), ",") {
		if strings.TrimSpace(t) == token {
			return true
		}
	}

	return false
}

func isLinuxWebAppKind(kind string) bool {
	return kindHasToken(kind, webKindApp) && kindHasToken(kind, webKindLinux)
}

func isLinuxKind(kind string) bool { return strings.Contains(strings.ToLower(kind), webKindLinux) }

// planReserved reports whether the plan hosts Linux workers (properties.reserved or a linux kind).
func (p *WebProvider) planReserved(serverFarmID string) bool {
	pid, err := ParseGenericResourcePath(serverFarmID)
	if err != nil {
		return false
	}

	pl, ok := p.plans[planKey(pid.ResourceGroup, pid.LeafName())]
	if !ok {
		return false
	}

	reserved, _ := pl.Extra[webFieldReserved].(bool)

	return reserved || isLinuxKind(pl.Kind)
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

func (p *WebProvider) checkSitePlan(kind, farm string) error {
	if !isFunctionAppKind(kind) && farm == "" {
		return badWebRequest(fmt.Errorf("%w: %w", ErrWebInvalidSiteConfig, errWebPlanRequired))
	}

	if farm != "" && isLinuxKind(kind) && !p.planReserved(farm) {
		return ErrWebPlanNotLinux
	}

	return nil
}

func (p *WebProvider) checkSiteWritable(id ResourceID, name string, existing *storedWebSite) error {
	if other, taken := p.sites[webSiteKey(name)]; taken && !resourceGroupsEqual(other.ResourceGroup, id.ResourceGroup) {
		return ErrWebNameTaken
	}

	if p.slotLabelTaken(name, "") {
		return ErrWebNameTaken
	}

	if existing != nil && existing.Pending != nil {
		return ErrWebSwapPending
	}

	return nil
}

// siteIdentity applies a request's identity block on top of the identity of the existing site (nil for a new one).
func (p *WebProvider) siteIdentity(body map[string]any, existing *storedWebSite) (resourceIdentity, error) {
	var prev resourceIdentity
	if existing != nil {
		prev = existing.Managed
	}

	_, present := body[fieldIdentity]

	return p.ids.apply(body[fieldIdentity], present, prev)
}

func (p *WebProvider) putSite(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	name := id.LeafName()
	existing, exists := p.site(id.ResourceGroup, name)

	kind, _ := body[webFieldKind].(string)
	if kind == "" && exists {
		kind = existing.Kind
	}

	if !isFunctionAppKind(kind) && !isLinuxWebAppKind(kind) {
		return nil, ErrWebUnsupportedKind
	}

	if !validWebName(name) {
		return nil, badWebRequest(errWebSiteName)
	}

	if err := p.checkSiteWritable(id, name, existing); err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)

	farm, _ := props[webFieldServerFarmID].(string)
	if !p.planExists(farm) {
		return nil, ErrWebPlanNotFound
	}

	if err := p.checkSitePlan(kind, farm); err != nil {
		return nil, err
	}

	ident, err := p.siteIdentity(body, existing)
	if err != nil {
		return nil, err
	}

	rec := &storedWebSite{
		Name: name, ResourceGroup: id.ResourceGroup, Location: DefaultLocation, Kind: kind, Created: time.Now().UTC(),
		Tags: stringTags(body[fieldTags]), Managed: ident, Configs: map[string]map[string]any{},
		Extra: webSiteExtra(props),
	}

	if exists {
		rec.Created, rec.Location, rec.Configs = existing.Created, existing.Location, existing.Configs
		rec.Slots, rec.LastSwap = existing.Slots, existing.LastSwap
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.Location = loc
	}

	siteCfg := mapOf(props[webFieldSiteConfig])
	cfgWeb := mergeWebConfig(rec.Configs[webConfigWeb], siteCfg)
	rec.Configs[webConfigWeb] = cfgWeb

	info, err := p.dataPlane.PutApp(name, siteAppConfig(kind, cfgWeb))
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

func siteAppConfig(kind string, cfg map[string]any) FunctionAppConfig {
	fx, _ := cfg[webFieldLinuxFx].(string)
	cmd, _ := cfg[webFieldAppCmd].(string)
	health, _ := cfg[webFieldHealthCheck].(string)

	return FunctionAppConfig{Kind: kind, LinuxFx: fx, AppCommandLine: cmd, HealthCheckPath: health}
}

// mergeWebConfig overlays the request's siteConfig (minus appSettings, which the data plane owns) on cur.
func mergeWebConfig(cur, siteCfg map[string]any) map[string]any {
	out := maps.Clone(cur)
	if out == nil {
		out = map[string]any{}
	}

	for k, v := range siteCfg {
		if k != webFieldAppSettings {
			out[k] = v
		}
	}

	return out
}

// webSiteExtra keeps the site properties the emulator does not model, so they round-trip.
func webSiteExtra(props map[string]any) map[string]any {
	out := maps.Clone(props)
	if out == nil {
		out = map[string]any{}
	}

	for _, k := range []string{
		webFieldSiteConfig, fieldProvisioningState, sqlPropState, "hostNames", "defaultHostName", "enabledHostNames",
		"hostNameSslStates", "repositorySiteName", "usageState", "availabilityState", fieldName, "lastModifiedTimeUtc",
		"outboundIpAddresses", "possibleOutboundIpAddresses", webFieldRG, "cloningInfo", "slotSwapStatus",
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

func (p *WebProvider) siteBody(
	_ context.Context,
	id ResourceID,
	s *storedWebSite,
	info FunctionAppInfo,
) map[string]any {
	resName, resType := s.Name, namespaceMicrosoftWeb+"/"+webSitesType
	if s.Slot != "" {
		resName, resType = s.Name+"/"+s.Slot, resType+"/"+webSlotsType
	}

	state := webStateRunning
	if info.Stopped {
		state = webStateStopped
	}

	cfg := webConfigDefaults(s)
	siteCfg := map[string]any{
		webFieldWorkers: cfg[webFieldWorkers], webFieldLinuxFx: cfg[webFieldLinuxFx], "alwaysOn": cfg["alwaysOn"],
		"acrUseManagedIdentityCreds": false, "http20Enabled": cfg["http20Enabled"],
	}

	if isFunctionAppKind(s.Kind) {
		siteCfg["functionAppScaleLimit"], siteCfg["minimumElasticInstanceCount"] = 0, 0
	} else {
		siteCfg[webFieldAppCmd], siteCfg[webFieldHealthCheck] = cfg[webFieldAppCmd], cfg[webFieldHealthCheck]
	}

	availability := "Normal"
	if info.Failure != "" {
		availability = "Limited"
	}

	props := map[string]any{
		fieldName:            s.Name,
		sqlPropState:         state,
		"hostNames":          []string{info.HostName},
		"defaultHostName":    info.HostName,
		"enabledHostNames":   []string{info.HostName, info.SCMHost},
		"repositorySiteName": s.Name,
		"usageState":         "Normal",
		acrWebhookEnabled:    true,
		"availabilityState":  availability,
		"hostNameSslStates": []any{
			map[string]any{fieldName: info.HostName, "sslState": redisPublicDisabled, "hostType": skuTierStandard},
			map[string]any{fieldName: info.SCMHost, "sslState": redisPublicDisabled, "hostType": "Repository"},
		},
		webFieldReserved:              strings.Contains(strings.ToLower(s.Kind), "linux"),
		"isXenon":                     false,
		"hyperV":                      false,
		"clientAffinityEnabled":       false,
		"clientCertEnabled":           false,
		"httpsOnly":                   false,
		"scmSiteAlsoStopped":          false,
		"containerSize":               webContainerSize,
		"dailyMemoryTimeQuota":        0,
		"outboundIpAddresses":         "127.0.0.1",
		"possibleOutboundIpAddresses": "127.0.0.1",
		webFieldRG:                    s.ResourceGroup,
		webFieldSiteConfig:            siteCfg,
		"lastModifiedTimeUtc": info.Created.UTC().
			Format(time.RFC3339),
		fieldProvisioningState: provisioningStateSucceeded,
	}

	if s.LastSwap != nil {
		props["slotSwapStatus"] = map[string]any{
			"timestampUtc": s.LastSwap.Time.UTC().Format(time.RFC3339), "sourceSlotName": s.LastSwap.Source,
			"destinationSlotName": s.LastSwap.Destination,
		}
	}

	if s.Pending != nil {
		props["targetSwapSlot"] = s.Pending.Target
	}

	out := map[string]any{
		"id":            id.ARMID(),
		fieldName:       resName,
		fieldType:       resType,
		webFieldKind:    s.Kind,
		fieldLocation:   s.Location,
		fieldTags:       tagsOrEmpty(s.Tags),
		fieldProperties: ehMerge(props, s.Extra, nil),
	}

	if ident := p.ids.body(s.Managed); ident != nil {
		out[fieldIdentity] = ident
	}

	return out
}

// Get implements ResourceProvider.
func (p *WebProvider) Get(ctx context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	switch webKind(id) {
	case webSitesType, webSlotsType:
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
	s, ok := p.record(id)
	if !ok {
		return nil, ErrWebSiteNotFound
	}

	info, found := p.dataPlane.AppInfo(s.dpName())
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
	case webSlotsType:
		return p.deleteSlot(id)
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
	case webSlotsType:
		return p.listSlots(ctx, id), nil
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
	if errors.Is(err, ErrWebInvalidSiteConfig) {
		return errorEntry{webCodeBadRequest, err.Error(), http.StatusBadRequest}, true
	}

	table := append(webSlotErrorDetails(), []struct {
		err   error
		entry errorEntry
	}{
		{ErrWebSiteNotFound, notFoundEntry("The web app was not found.")},
		{ErrWebFunctionNotFound, notFoundEntry("The function was not found.")},
		{ErrWebKeyNotFound, notFoundEntry("The key was not found.")},
		{ErrWebConfigNotFound, notFoundEntry("The site configuration was not found.")},
		{ErrWebPlanNotFound, errorEntry{
			webCodeBadRequest, "The server farm referenced by the site does not exist.", http.StatusBadRequest,
		}},
		{
			ErrWebNameTaken,
			errorEntry{redisCodeConflict, "A site with the given name already exists.", http.StatusConflict},
		},
		{ErrWebPlanNotLinux, errorEntry{
			webCodeBadRequest, "A Linux app must be hosted on a Linux (reserved) App Service plan.",
			http.StatusBadRequest,
		}},
		{ErrWebNotFunctionApp, errorEntry{
			webCodeBadRequest, "This operation is only valid for function apps.", http.StatusBadRequest,
		}},
		{ErrWebPlanInUse, errorEntry{
			redisCodeConflict, "Cannot delete the server farm because it still has sites.", http.StatusConflict,
		}},
		{
			ErrWebUnsupportedKind,
			errorEntry{
				webCodeBadRequest,
				"Only Linux web apps (kind app,linux) and function apps (kind containing 'functionapp') are supported.",
				http.StatusBadRequest,
			},
		},
		{ErrWebInvalidKeyType, errorEntry{
			webCodeBadRequest, "The key type must be functionKeys or systemKeys.", http.StatusBadRequest,
		}},
		{ErrWebFunctionsUnavailable, errorEntry{
			redisCodeUnavailable, "The Azure Functions data plane is not available.", http.StatusServiceUnavailable,
		}},
	}...)

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	return errorEntry{}, false
}
