package azurearm

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	webSlotsType       = "slots"
	webSlotProduction  = "production"
	webSlotConfigNames = "slotConfigNames"
	webFieldTargetSlot = "targetSlot"
	webFieldCloning    = "cloningInfo"
	webFieldCloneSrc   = "sourceWebAppId"
	webFieldCloneOver  = "appSettingsOverrides"
	webSlotSiteMax     = 40
	webSlotNameMax     = 19
	webSlotCombinedMax = 59
	webTypeSlotsDiff   = "Microsoft.Web/sites/slotsdiffs"
	webTierStandard    = "standard"
	webLimitElastic    = 2
	webLimitStandard   = 5
	webLimitPremium    = 20
	webFieldLevel      = "level"

	webDiffGeneral = "General"
	webDiffSetting = "AppSetting"
	webDiffConnStr = "ConnectionString"
)

var webSlotNameRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

// pendingSwap marks a slot whose configuration was prepared for a swap with preview.
type pendingSwap struct {
	Settings    map[string]string `json:"settings"`
	ConnStrings map[string]any    `json:"connStrings,omitempty"`
	Target      string            `json:"target"`
}

type swapStatus struct {
	Time        time.Time `json:"time"`
	Source      string    `json:"source"`
	Destination string    `json:"destination"`
}

// swappedWebConfig are the siteConfig keys that follow the content across a swap: language framework versions,
// platform bitness, WebSockets and handler mappings. appCommandLine travels with linuxFxVersion because the
// startup command only makes sense for the image it was written for.
func swappedWebConfig() []string {
	return []string{
		webFieldLinuxFx, webFieldAppCmd, "netFrameworkVersion", "phpVersion", "pythonVersion", "nodeVersion",
		"javaVersion", "javaContainer", "javaContainerVersion", "use32BitWorkerProcess", "webSocketsEnabled",
		"handlerMappings",
	}
}

func (s *storedWebSite) dpName() string {
	if s.Slot == "" {
		return s.Name
	}

	return s.Name + "/" + s.Slot
}

func (s *storedWebSite) publishUser() string {
	if s.Slot == "" {
		return "$" + s.Name
	}

	return "$" + s.Name + "__" + s.Slot
}

func (s *storedWebSite) slotSuffix() string {
	if s.Slot == "" {
		return ""
	}

	return "-" + s.Slot
}

func isProductionSlot(name string) bool {
	return name == "" || strings.EqualFold(name, webSlotProduction)
}

// record resolves a site or slot id to its stored record.
func (p *WebProvider) record(id ResourceID) (*storedWebSite, bool) {
	if len(id.Names) == webSlotRestSegs {
		parent, ok := p.site(id.ResourceGroup, id.Names[0])
		if !ok {
			return nil, false
		}

		sl, found := parent.Slots[strings.ToLower(id.Names[1])]

		return sl, found
	}

	return p.site(id.ResourceGroup, id.LeafName())
}

// slotRecord returns the production record (slot "" or "production") or the named slot of parent.
func slotRecord(parent *storedWebSite, slot string) (*storedWebSite, bool) {
	if isProductionSlot(slot) {
		return parent, true
	}

	sl, ok := parent.Slots[strings.ToLower(slot)]

	return sl, ok
}

func slotLabel(app, slot string) string { return strings.ToLower(app + "-" + slot) }

// slotLabelTaken reports whether <label>.<domain> already belongs to some site or slot other than the given slot.
func (p *WebProvider) slotLabelTaken(label, exceptApp string) bool {
	label = strings.ToLower(label)

	for _, s := range p.sites {
		if strings.EqualFold(s.Name, exceptApp) {
			continue
		}

		for _, sl := range s.Slots {
			if slotLabel(s.Name, sl.Slot) == label {
				return true
			}
		}
	}

	return false
}

func tierFromSKUName(name string) string {
	name = strings.ToLower(name)

	switch {
	case strings.HasPrefix(name, "f"):
		return "free"
	case strings.HasPrefix(name, "d"):
		return "shared"
	case strings.HasPrefix(name, "b"):
		return "basic"
	case strings.HasPrefix(name, "s"):
		return webTierStandard
	case strings.HasPrefix(name, "p"), strings.HasPrefix(name, "i"):
		return "premium"
	}

	return ""
}

// slotLimit is the number of non-production slots a site may have: App Service limits for dedicated tiers
// (Standard 5, Premium and Isolated 20) and the Azure Functions limits for Consumption (2 slots including
// production), Premium (3) and Flex Consumption (none).
func slotLimit(kind string, sku map[string]any) int {
	tier, _ := sku[cosmosFieldTier].(string)
	name, _ := sku["name"].(string)

	tier = strings.ToLower(tier)
	if tier == "" {
		tier = tierFromSKUName(name)
	}

	if isFunctionAppKind(kind) {
		switch {
		case tier == "dynamic" || strings.EqualFold(name, webPlanSKUDefault):
			return 1
		case tier == "elasticpremium" || strings.HasPrefix(strings.ToLower(name), "ep"):
			return webLimitElastic
		case tier == "flexconsumption" || strings.EqualFold(name, "FC1"):
			return 0
		}
	}

	switch {
	case tier == webTierStandard:
		return webLimitStandard
	case strings.HasPrefix(tier, "premium"), strings.HasPrefix(tier, "isolated"):
		return webLimitPremium
	}

	return 0
}

func (p *WebProvider) siteSKU(s *storedWebSite) (map[string]any, bool) {
	farm, _ := s.Extra[webFieldServerFarmID].(string)
	if farm == "" {
		return nil, false
	}

	pid, err := ParseGenericResourcePath(farm)
	if err != nil {
		return nil, false
	}

	pl, ok := p.plans[planKey(pid.ResourceGroup, pid.LeafName())]
	if !ok {
		return nil, false
	}

	return pl.SKU, true
}

func (p *WebProvider) checkSlotName(app, slot string) error {
	switch {
	case isProductionSlot(slot):
		return badWebRequest(fmt.Errorf("%w: %q is the production slot", ErrWebSlotName, slot))
	case !webSlotNameRe.MatchString(slot):
		return badWebRequest(fmt.Errorf("%w: letters, digits and hyphens only", ErrWebSlotName))
	case len(app) > webSlotSiteMax || len(slot) > webSlotNameMax || len(app)+len(slot) >= webSlotCombinedMax:
		return badWebRequest(fmt.Errorf("%w: site name up to %d, slot name up to %d and fewer than %d together",
			ErrWebSlotName, webSlotSiteMax, webSlotNameMax, webSlotCombinedMax))
	}

	return nil
}

func (p *WebProvider) putSlot(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	parent, ok := p.site(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrWebSiteNotFound
	}

	slot := id.Names[1]
	existing, exists := parent.Slots[strings.ToLower(slot)]
	props, _ := body[fieldProperties].(map[string]any)

	if err := p.checkSlotWritable(parent, slot, existing, props); err != nil {
		return nil, err
	}

	ident, err := p.siteIdentity(body, existing)
	if err != nil {
		return nil, err
	}

	rec := &storedWebSite{
		Name: parent.Name, Slot: slot, ResourceGroup: parent.ResourceGroup, Location: parent.Location,
		Kind: parent.Kind, Created: time.Now().UTC(), Tags: stringTags(body[fieldTags]),
		Managed: ident, Configs: map[string]map[string]any{}, Extra: webSiteExtra(props),
	}
	rec.Extra[webFieldServerFarmID] = parent.Extra[webFieldServerFarmID]

	if exists {
		rec.Created, rec.Location, rec.Configs, rec.LastSwap = existing.Created, existing.Location, existing.Configs,
			existing.LastSwap
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.Location = loc
	}

	if kind, _ := body[webFieldKind].(string); kind != "" && isFunctionAppKind(kind) == isFunctionAppKind(parent.Kind) {
		rec.Kind = kind
	}

	settings, err := p.applySlotProps(parent, rec, props, !exists)
	if err != nil {
		return nil, err
	}

	info, err := p.dataPlane.PutApp(rec.dpName(), siteAppConfig(rec.Kind, rec.Configs[webConfigWeb]))
	if err != nil {
		return nil, badWebRequest(err)
	}

	if settings != nil {
		if err = p.dataPlane.SetSettings(rec.dpName(), settings); err != nil {
			return nil, badWebRequest(err)
		}
	}

	if parent.Slots == nil {
		parent.Slots = map[string]*storedWebSite{}
	}

	parent.Slots[strings.ToLower(slot)] = rec

	return p.siteBody(ctx, id, rec, info), nil
}

// checkSlotWritable validates a slot PUT: name, pending swap, hostname, plan and the tier's slot limit.
func (p *WebProvider) checkSlotWritable(
	parent *storedWebSite, slot string, existing *storedWebSite, props map[string]any,
) error {
	if err := p.checkSlotName(parent.Name, slot); err != nil {
		return err
	}

	if existing != nil && existing.Pending != nil {
		return ErrWebSwapPending
	}

	label := slotLabel(parent.Name, slot)
	if _, taken := p.sites[label]; taken || p.slotLabelTaken(label, parent.Name) {
		return ErrWebNameTaken
	}

	if farm, _ := props[webFieldServerFarmID].(string); farm != "" {
		if parentFarm, _ := parent.Extra[webFieldServerFarmID].(string); !strings.EqualFold(farm, parentFarm) {
			return badWebRequest(fmt.Errorf("%w: a slot runs in its site's App Service plan", ErrWebInvalidSiteConfig))
		}
	}

	if existing != nil {
		return nil
	}

	sku, _ := p.siteSKU(parent)
	if sku == nil && isFunctionAppKind(parent.Kind) {
		sku = map[string]any{cosmosFieldTier: "Dynamic"}
	}

	if limit := slotLimit(parent.Kind, sku); len(parent.Slots) >= limit {
		return fmt.Errorf("%w: %d", ErrWebSlotLimit, limit)
	}

	return nil
}

// applySlotProps merges the request's siteConfig into rec's web configuration and, when creating, clones another
// app or slot named by cloningInfo.sourceWebAppId. It returns the app settings to store, or nil to leave them.
func (p *WebProvider) applySlotProps(
	parent, rec *storedWebSite,
	props map[string]any,
	create bool,
) (map[string]string, error) {
	var settings map[string]string

	if clone := mapOf(props[webFieldCloning]); create && clone[webFieldCloneSrc] != nil {
		src, err := p.cloneSource(clone)
		if err != nil {
			return nil, err
		}

		for k, v := range src.Configs {
			rec.Configs[k] = cloneAnyMap(v)
		}

		if settings, err = p.dataPlane.Settings(src.dpName()); err != nil {
			return nil, badWebRequest(err)
		}

		for k, v := range mapOf(clone[webFieldCloneOver]) {
			if str, isStr := v.(string); isStr {
				settings[k] = str
			}
		}
	}

	siteCfg := mapOf(props[webFieldSiteConfig])
	rec.Configs[webConfigWeb] = mergeWebConfig(rec.Configs[webConfigWeb], siteCfg)

	if fx, _ := rec.Configs[webConfigWeb][webFieldLinuxFx].(string); fx == "" && isFunctionAppKind(rec.Kind) {
		if parentFx, found := parent.Configs[webConfigWeb][webFieldLinuxFx]; found {
			rec.Configs[webConfigWeb][webFieldLinuxFx] = parentFx
		}
	}

	if raw, ok := siteCfg[webFieldAppSettings].([]any); ok {
		if settings == nil {
			settings = map[string]string{}
		}

		maps.Copy(settings, nameValueSettings(raw))
	}

	return settings, nil
}

func (p *WebProvider) cloneSource(clone map[string]any) (*storedWebSite, error) {
	raw, _ := clone[webFieldCloneSrc].(string)

	sid, err := ParseGenericResourcePath(raw)
	if err != nil || webKind(sid) != webSitesType && webKind(sid) != webSlotsType {
		return nil, badWebRequest(fmt.Errorf("%w: cloningInfo.sourceWebAppId must name a site or slot",
			ErrWebInvalidSiteConfig))
	}

	src, ok := p.record(sid)
	if !ok {
		return nil, ErrWebSiteNotFound
	}

	return src, nil
}

func (p *WebProvider) deleteSlot(id ResourceID) error {
	parent, ok := p.site(id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrWebSiteNotFound
	}

	key := strings.ToLower(id.Names[1])

	sl, ok := parent.Slots[key]
	if !ok {
		return ErrWebSiteNotFound
	}

	if err := p.dataPlane.DeleteApp(sl.dpName()); err != nil {
		return err
	}

	delete(parent.Slots, key)

	return nil
}

// ListSlots serves GET .../sites/{name}/slots.
func (p *WebProvider) ListSlots(ctx context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("ListSlots")
	defer p.mu.RUnlock()

	if _, ok := p.site(id.ResourceGroup, id.Names[0]); !ok {
		return nil, ErrWebSiteNotFound
	}

	return p.listSlots(ctx, id), nil
}

func (p *WebProvider) listSlots(ctx context.Context, id ResourceID) []map[string]any {
	var out []map[string]any

	parent, ok := p.site(id.ResourceGroup, id.Names[0])
	if !ok {
		return out
	}

	for _, sl := range parent.Slots {
		sid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: parent.ResourceGroup, Namespace: namespaceMicrosoftWeb,
			Types: []string{webSitesType, webSlotsType}, Names: []string{parent.Name, sl.Slot},
		}

		if body, err := p.getSite(ctx, sid); err == nil {
			out = append(out, body)
		}
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out
}

// stickyNames reads a slotConfigNames list from the site's configuration.
func stickyNames(parent *storedWebSite, key string) map[string]bool {
	out := map[string]bool{}

	list, _ := parent.Configs[webSlotConfigNames][key].([]any)
	for _, v := range list {
		if n, ok := v.(string); ok {
			out[strings.ToLower(n)] = true
		}
	}

	return out
}

// implicitStickySetting reports app settings that never swap, whatever slotConfigNames says.
func implicitStickySetting(name string) bool {
	up := strings.ToUpper(name)

	return strings.HasSuffix(up, "_EXTENSION_VERSION") || up == "WEBSITE_HTTPLOGGING_RETENTION_DAYS" ||
		up == "DIAGNOSTICS_AZUREBLOBRETENTIONDAYS"
}

func settingSticky(sticky map[string]bool, name string) bool {
	return sticky[strings.ToLower(name)] || implicitStickySetting(name)
}

// slotSides is a swap's resolved endpoints.
type slotSides struct {
	parent      *storedWebSite
	source      *storedWebSite
	target      *storedWebSite
	appSticky   map[string]bool
	connSticky  map[string]bool
	sourceLabel string
	targetLabel string
}

func (p *WebProvider) swapSides(id ResourceID, body map[string]any) (*slotSides, error) {
	rec, ok := p.record(id)
	if !ok {
		return nil, ErrWebSiteNotFound
	}

	parent, _ := p.site(id.ResourceGroup, id.Names[0])

	targetName, _ := body[webFieldTargetSlot].(string)
	if targetName == "" {
		return nil, badWebRequest(fmt.Errorf("%w: targetSlot is required", ErrWebInvalidSiteConfig))
	}

	target, ok := slotRecord(parent, targetName)
	if !ok {
		return nil, ErrWebSiteNotFound
	}

	if rec == target {
		return nil, badWebRequest(fmt.Errorf("%w: source and target slots are the same", ErrWebInvalidSiteConfig))
	}

	label := func(s *storedWebSite) string {
		if s.Slot == "" {
			return webSlotProduction
		}

		return s.Slot
	}

	return &slotSides{
		parent: parent, source: rec, target: target, sourceLabel: label(rec), targetLabel: label(target),
		appSticky: stickyNames(parent, "appSettingNames"), connSticky: stickyNames(parent, "connectionStringNames"),
	}, nil
}

func connStringsOf(s *storedWebSite) map[string]any {
	return cloneAnyMap(s.Configs[webConfigConn])
}

// prepareSwap is swap with preview's first phase: the target's slot-specific settings are applied to the source,
// which restarts and warms up; on failure the source is reverted.
func (p *WebProvider) prepareSwap(ctx context.Context, sd *slotSides) error {
	if sd.target.Pending != nil {
		return ErrWebSwapPending
	}

	if sd.source.Pending != nil {
		if sd.source.Pending.Target == sd.targetLabel {
			return nil
		}

		return ErrWebSwapPending
	}

	orig, err := p.dataPlane.Settings(sd.source.dpName())
	if err != nil {
		return badWebRequest(err)
	}

	tgt, err := p.dataPlane.Settings(sd.target.dpName())
	if err != nil {
		return badWebRequest(err)
	}

	sd.source.Pending = &pendingSwap{
		Target: sd.targetLabel, Settings: maps.Clone(orig), ConnStrings: connStringsOf(sd.source),
	}

	next := mixStrings(orig, tgt, sd.appSticky, true)
	sd.source.Configs[webConfigConn] = mixAny(
		sd.source.Pending.ConnStrings, sd.target.Configs[webConfigConn], sd.connSticky, true)

	if err = p.dataPlane.SetSettings(sd.source.dpName(), next); err == nil {
		err = p.dataPlane.WarmUp(ctx, sd.source.dpName())
	}

	if err != nil {
		_ = p.revertSource(sd.source)

		return fmt.Errorf("%w: %w", ErrWebSwapFailed, err)
	}

	return nil
}

// revertSource restores the source slot's own settings and clears its pending swap.
func (p *WebProvider) revertSource(src *storedWebSite) error {
	pend := src.Pending
	if pend == nil {
		return nil
	}

	src.Pending = nil
	src.Configs[webConfigConn] = pend.ConnStrings

	if len(pend.ConnStrings) == 0 {
		delete(src.Configs, webConfigConn)
	}

	return p.dataPlane.SetSettings(src.dpName(), pend.Settings)
}

// mixStrings builds a slot's settings from own and other: sticky names always come from the slot that owns them,
// the rest from the other slot. With applyOnly the slot keeps its non-sticky values and only sticky ones come from
// other.
func mixStrings(own, other map[string]string, sticky map[string]bool, applyOnly bool) map[string]string {
	out := map[string]string{}

	for k, v := range own {
		if settingSticky(sticky, k) != applyOnly {
			out[k] = v
		}
	}

	for k, v := range other {
		if settingSticky(sticky, k) == applyOnly {
			out[k] = v
		}
	}

	return out
}

func mixAny(own, other map[string]any, sticky map[string]bool, applyOnly bool) map[string]any {
	out := map[string]any{}

	for k, v := range own {
		if sticky[strings.ToLower(k)] != applyOnly {
			out[k] = v
		}
	}

	for k, v := range other {
		if sticky[strings.ToLower(k)] == applyOnly {
			out[k] = v
		}
	}

	return out
}

// swapState is one slot's swappable state before a swap.
type swapState struct {
	settings map[string]string
	conn     map[string]any
	web      map[string]any
}

func (p *WebProvider) stateOf(s *storedWebSite, settings map[string]string, conn map[string]any) swapState {
	return swapState{settings: settings, conn: conn, web: cloneAnyMap(s.Configs[webConfigWeb])}
}

// swappedWeb returns own's web configuration with the swappable keys taken from other.
func swappedWeb(own, other map[string]any) map[string]any {
	out := cloneAnyMap(own)

	for _, k := range swappedWebConfig() {
		if v, ok := other[k]; ok {
			out[k] = v
		} else {
			delete(out, k)
		}
	}

	return out
}

// completeSwap is the second phase: content and non-slot-specific configuration trade places, each slot keeps its
// own slot-specific settings, and both restart.
func (p *WebProvider) completeSwap(sd *slotSides) error {
	src, tgt := sd.source, sd.target

	srcSet := src.Pending.Settings

	tgtSet, err := p.dataPlane.Settings(tgt.dpName())
	if err != nil {
		return badWebRequest(err)
	}

	srcState := p.stateOf(src, srcSet, src.Pending.ConnStrings)
	tgtState := p.stateOf(tgt, tgtSet, connStringsOf(tgt))
	srcWeb, tgtWeb := swappedWeb(srcState.web, tgtState.web), swappedWeb(tgtState.web, srcState.web)

	steps := []struct {
		rec *storedWebSite
		cfg map[string]any
		old map[string]any
	}{{tgt, tgtWeb, tgtState.web}, {src, srcWeb, srcState.web}}

	for i, st := range steps {
		if _, err = p.dataPlane.PutApp(st.rec.dpName(), siteAppConfig(st.rec.Kind, st.cfg)); err != nil {
			for _, done := range steps[:i] {
				_, _ = p.dataPlane.PutApp(done.rec.dpName(), siteAppConfig(done.rec.Kind, done.old))
			}

			_ = p.revertSource(src)

			return badWebRequest(err)
		}
	}

	src.Configs[webConfigWeb], tgt.Configs[webConfigWeb] = srcWeb, tgtWeb

	newSrc := mixStrings(srcSet, tgtSet, sd.appSticky, false)
	newTgt := mixStrings(tgtSet, srcSet, sd.appSticky, false)
	srcConn := mixAny(srcState.conn, tgtState.conn, sd.connSticky, false)
	tgtConn := mixAny(tgtState.conn, srcState.conn, sd.connSticky, false)

	setConn(src, srcConn)
	setConn(tgt, tgtConn)

	src.Pending = nil

	for _, step := range []struct {
		rec *storedWebSite
		set map[string]string
	}{{src, newSrc}, {tgt, newTgt}} {
		if err = p.dataPlane.SetSettings(step.rec.dpName(), step.set); err != nil {
			return badWebRequest(err)
		}
	}

	if err = p.dataPlane.SwapContent(src.dpName(), tgt.dpName()); err != nil {
		return badWebRequest(err)
	}

	now := time.Now().UTC()
	status := &swapStatus{Time: now, Source: sd.sourceLabel, Destination: sd.targetLabel}
	src.LastSwap, tgt.LastSwap = status, status

	return nil
}

// setConn stores a connection strings configuration, dropping it when empty.
func setConn(s *storedWebSite, conn map[string]any) {
	if len(conn) == 0 {
		delete(s.Configs, webConfigConn)

		return
	}

	s.Configs[webConfigConn] = conn
}

// SwapSlots serves POST .../slotsswap: a full swap, or the completion of a swap with preview.
func (p *WebProvider) SwapSlots(ctx context.Context, id ResourceID, body map[string]any) error {
	p.mu.Lock("SwapSlots")
	defer p.mu.Unlock()

	sd, err := p.swapSides(id, body)
	if err != nil {
		return err
	}

	if err = p.prepareSwap(ctx, sd); err != nil {
		return err
	}

	return p.completeSwap(sd)
}

// ApplySlotConfig serves POST .../applySlotConfig: the first phase of a swap with preview.
func (p *WebProvider) ApplySlotConfig(ctx context.Context, id ResourceID, body map[string]any) error {
	p.mu.Lock("ApplySlotConfig")
	defer p.mu.Unlock()

	sd, err := p.swapSides(id, body)
	if err != nil {
		return err
	}

	return p.prepareSwap(ctx, sd)
}

// ResetSlotConfig serves POST .../resetSlotConfig: it cancels a swap with preview.
func (p *WebProvider) ResetSlotConfig(_ context.Context, id ResourceID) error {
	p.mu.Lock("ResetSlotConfig")
	defer p.mu.Unlock()

	rec, ok := p.record(id)
	if !ok {
		return ErrWebSiteNotFound
	}

	if err := p.revertSource(rec); err != nil {
		return badWebRequest(err)
	}

	return nil
}

func (p *WebProvider) diffEntry(
	id ResourceID, kind, name string, cur, tgt *string,
) map[string]any {
	str := func(v *string) any {
		if v == nil {
			return nil
		}

		return *v
	}

	return map[string]any{
		"id": id.ARMID() + "/slotsdiffs/" + name, fieldName: name, fieldType: webTypeSlotsDiff,
		fieldProperties: map[string]any{
			"settingType": kind, "settingName": name, "valueInCurrentSlot": str(cur), "valueInTargetSlot": str(tgt),
			webFieldLevel: "Information", "diffRule": "Swap",
			"description": "The setting changes in the current slot when the slots are swapped.",
		},
	}
}

// SlotDifferences serves POST .../slotsdiffs: the swapped settings whose values differ between the slots.
func (p *WebProvider) SlotDifferences(_ context.Context, id ResourceID, body map[string]any) ([]map[string]any, error) {
	p.mu.RLock("SlotDifferences")
	defer p.mu.RUnlock()

	sd, err := p.swapSides(id, body)
	if err != nil {
		return nil, err
	}

	cur, err := p.dataPlane.Settings(sd.source.dpName())
	if err != nil {
		return nil, badWebRequest(err)
	}

	tgt, err := p.dataPlane.Settings(sd.target.dpName())
	if err != nil {
		return nil, badWebRequest(err)
	}

	out := []map[string]any{}

	for _, k := range sortedUnion(cur, tgt) {
		cv, cOK := cur[k]
		tv, tOK := tgt[k]

		if settingSticky(sd.appSticky, k) || cOK == tOK && cv == tv {
			continue
		}

		out = append(out, p.diffEntry(id, webDiffSetting, k, optStr(cv, cOK), optStr(tv, tOK)))
	}

	curConn, tgtConn := sd.source.Configs[webConfigConn], sd.target.Configs[webConfigConn]

	for _, k := range sortedUnion(curConn, tgtConn) {
		if sd.connSticky[strings.ToLower(k)] || fmt.Sprint(curConn[k]) == fmt.Sprint(tgtConn[k]) {
			continue
		}

		out = append(out, p.diffEntry(id, webDiffConnStr, k, connValue(curConn, k), connValue(tgtConn, k)))
	}

	curWeb, tgtWeb := sd.source.Configs[webConfigWeb], sd.target.Configs[webConfigWeb]

	for _, k := range swappedWebConfig() {
		if fmt.Sprint(curWeb[k]) != fmt.Sprint(tgtWeb[k]) {
			out = append(out, p.diffEntry(id, webDiffGeneral, k, anyStr(curWeb, k), anyStr(tgtWeb, k)))
		}
	}

	return out, nil
}

func optStr(v string, ok bool) *string {
	if !ok {
		return nil
	}

	return &v
}

func anyStr(m map[string]any, k string) *string {
	v, ok := m[k]
	if !ok || v == nil {
		return nil
	}

	s := fmt.Sprint(v)

	return &s
}

func connValue(m map[string]any, k string) *string {
	entry, ok := m[k].(map[string]any)
	if !ok {
		return nil
	}

	return anyStr(entry, fieldValue)
}

func sortedUnion[V any](a, b map[string]V) []string {
	seen := map[string]bool{}

	var out []string

	for _, m := range []map[string]V{a, b} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}

	sort.Strings(out)

	return out
}

func webSlotErrorDetails() []struct {
	err   error
	entry errorEntry
} {
	return []struct {
		err   error
		entry errorEntry
	}{
		{ErrWebSwapPending, errorEntry{
			redisCodeConflict,
			"The slot cannot be changed because its configuration settings have been prepared for swap.",
			http.StatusConflict,
		}},
		{ErrWebSlotLimit, errorEntry{
			redisCodeConflict, "The App Service plan tier does not allow more deployment slots.", http.StatusConflict,
		}},
		{ErrWebSlotName, errorEntry{webCodeBadRequest, "Invalid deployment slot name.", http.StatusBadRequest}},
		{ErrWebSwapFailed, errorEntry{
			webCodeBadRequest, "The slot swap failed and the source slot was restored.", http.StatusBadRequest,
		}},
	}
}
