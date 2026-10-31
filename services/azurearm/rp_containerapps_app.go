package azurearm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	acaSuffixLen        = 7
	acaDefaultInactive  = 100
	acaMaxInactive      = 100
	acaCodeRevisionName = "ContainerAppRevisionNameConflict"
)

type storedACARevision struct {
	Created    time.Time      `json:"created"`
	LastActive time.Time      `json:"lastActive"`
	Template   map[string]any `json:"template"`
	Name       string         `json:"name"`
	Suffix     string         `json:"suffix"`
	Active     bool           `json:"active"`
}

type storedACAApp struct {
	Created         time.Time            `json:"created"`
	Modified        time.Time            `json:"modified"`
	Tags            map[string]string    `json:"tags,omitempty"`
	Config          map[string]any       `json:"config"`
	Sub             string               `json:"sub"`
	Name            string               `json:"name"`
	ResourceGroup   string               `json:"resourceGroup"`
	Location        string               `json:"location"`
	Kind            string               `json:"kind,omitempty"`
	ManagedBy       string               `json:"managedBy,omitempty"`
	EnvironmentID   string               `json:"environmentId"`
	WorkloadProfile string               `json:"workloadProfile,omitempty"`
	Identity        resourceIdentity     `json:"identity"`
	Revisions       []*storedACARevision `json:"revisions"`
	Stopped         bool                 `json:"stopped,omitempty"`
}

func (a *storedACAApp) latest() *storedACARevision {
	if len(a.Revisions) == 0 {
		return nil
	}

	return a.Revisions[len(a.Revisions)-1]
}

func (a *storedACAApp) revision(name string) *storedACARevision {
	for _, r := range a.Revisions {
		if strings.EqualFold(r.Name, name) {
			return r
		}
	}

	return nil
}

func (a *storedACAApp) ingress() map[string]any { return mapOf(a.Config[fieldIngress]) }

func (a *storedACAApp) mode() string {
	m, _ := acaModeOf(a.Config)

	return m
}

func acaRevName(app, suffix string) string { return app + "--" + suffix }

func (p *ContainerAppsProvider) envByID(id string) *storedACAEnv {
	for _, e := range p.envs {
		if strings.EqualFold(e.armID(), id) {
			return e
		}
	}

	return nil
}

func acaTemplateKey(t map[string]any) string {
	c := acaClone(t)
	delete(c, fieldRevSuffix)

	b, _ := json.Marshal(c)

	return string(b)
}

func acaNormalizeTemplate(t map[string]any) {
	for _, key := range []string{fieldContainers, fieldInitContainer} {
		for _, e := range acaList(t[key]) {
			c := mapOf(e)
			if mapOf(c[fieldResources]) == nil {
				c[fieldResources] = map[string]any{"cpu": acaDefaultCPU, "memory": "1Gi"}
			}
		}
	}

	s := mapOf(t[fieldScale])
	if s == nil {
		s = map[string]any{}
		t[fieldScale] = s
	}

	if _, has := s[fieldMinReplicas]; !has {
		s[fieldMinReplicas] = float64(0)
	}

	if _, has := s[fieldMaxReplicas]; !has {
		s[fieldMaxReplicas] = float64(acaDefaultMax)
	}
}

type acaAppReq struct {
	cfg      map[string]any
	tmpl     map[string]any
	tags     map[string]string
	env      *storedACAEnv
	location string
	kind     string
	managed  string
	profile  string
	suffix   string
	revName  string
	ident    resourceIdentity
	create   bool
}

func (p *ContainerAppsProvider) putApp(
	ctx context.Context,
	id ResourceID,
	body map[string]any,
) (map[string]any, error) {
	p.opMu.Lock("putApp")
	defer p.opMu.Unlock()

	name := id.LeafName()
	if !acaNameValid(name, acaMinNameLen, acaMaxAppNameLen) {
		return nil, acaBad("Container app name '%s' must be %d-%d lowercase alphanumerics or '-', start with a "+
			"letter, end alphanumeric and contain no '--'.", name, acaMinNameLen, acaMaxAppNameLen)
	}

	key := acaKey(id.ResourceGroup, name)

	p.mu.Lock("putApp")

	prev := p.apps[key]

	req, err := p.parseApp(id, body, prev)
	if err != nil {
		p.mu.Unlock()

		return nil, err
	}

	rec := p.commitApp(id, prev, req)
	p.apps[key] = rec
	p.rebuildHostsLocked()
	p.mu.Unlock()

	p.reconcile(ctx, key)

	p.mu.RLock("putApp")
	defer p.mu.RUnlock()

	return p.appBody(id, rec), nil
}

func (p *ContainerAppsProvider) parseApp(id ResourceID, body map[string]any, prev *storedACAApp) (acaAppReq, error) {
	props := mapOf(body[fieldProperties])
	if props == nil {
		return acaAppReq{}, acaBad("properties are required.")
	}

	req := acaAppReq{
		cfg: acaClone(mapOf(props[fieldConfiguration])), tmpl: acaClone(mapOf(props[fieldTemplate])),
		tags: stringTags(body[fieldTags]), location: strOf(body, fieldLocation), kind: strOf(body, acaFieldKind),
		managed: strOf(body, "managedBy"), profile: strOf(props, fieldWorkloadProf),
	}

	if req.cfg == nil {
		req.cfg = map[string]any{}
	}

	if req.location == "" {
		req.location = DefaultLocation
	}

	var err error

	if req.env, err = p.appEnv(id, props, prev); err != nil {
		return req, err
	}

	var prevIdent resourceIdentity
	if prev != nil {
		prevIdent = prev.Identity
	}

	_, hasIdent := body[fieldIdentity]
	if req.ident, err = p.ids.apply(body[fieldIdentity], hasIdent, prevIdent); err != nil {
		return req, err
	}

	if err = p.checkProfile(req); err != nil {
		return req, err
	}

	if err = p.checkAppConfig(req); err != nil {
		return req, err
	}

	return p.planAppRevision(id.LeafName(), prev, req)
}

func (p *ContainerAppsProvider) appEnv(id ResourceID, props map[string]any, prev *storedACAApp) (*storedACAEnv, error) {
	ref, other := strOf(props, fieldManagedEnvID), strOf(props, fieldEnvironmentID)

	if ref == "" {
		ref = other
	}

	if ref == "" || (other != "" && !strings.EqualFold(other, ref)) {
		return nil, acaBad("properties.environmentId (or managedEnvironmentId) is required.")
	}

	env := p.envByID(ref)
	if env == nil || !strings.EqualFold(env.Sub, id.SubscriptionID) {
		return nil, netErr(http.StatusBadRequest, acaCodeEnvMissing, "Managed environment '%s' was not found.", ref)
	}

	if prev != nil && !strings.EqualFold(prev.EnvironmentID, env.armID()) {
		return nil, acaBad("The managed environment of an existing container app cannot be changed.")
	}

	return env, nil
}

func (p *ContainerAppsProvider) checkProfile(req acaAppReq) error {
	if req.profile == "" || strings.EqualFold(req.profile, acaConsumption) {
		return nil
	}

	for _, w := range acaList(req.env.Props["workloadProfiles"]) {
		if strings.EqualFold(strOf(mapOf(w), fieldName), req.profile) {
			return nil
		}
	}

	return acaBad("Workload profile '%s' does not exist in managed environment '%s'.", req.profile, req.env.Name)
}

func acaDedicated(profile string) bool {
	return profile != "" && !strings.EqualFold(profile, acaConsumption)
}

// checkAppConfig validates configuration and template; it does not touch revisions.
func (p *ContainerAppsProvider) checkAppConfig(req acaAppReq) error {
	if _, err := acaModeOf(req.cfg); err != nil {
		return err
	}

	secrets, err := acaCheckSecrets(acaList(req.cfg[fieldSecrets]), req.ident)
	if err != nil {
		return err
	}

	if err = acaCheckRegistries(acaList(req.cfg[fieldRegistries]), secrets, req.ident); err != nil {
		return err
	}

	if err = acaCheckDapr(mapOf(req.cfg["dapr"])); err != nil {
		return err
	}

	if err = acaCheckIngress(mapOf(req.cfg[fieldIngress])); err != nil {
		return err
	}

	if n, has := acaNum(req.cfg, "maxInactiveRevisions"); has && (n < 0 || n > acaMaxInactive) {
		return acaBad("configuration.maxInactiveRevisions must be between 0 and %d.", acaMaxInactive)
	}

	return acaCheckTemplate(req.tmpl, secrets, req.env, req.profile)
}

func acaStorageSet(env *storedACAEnv) map[string]bool {
	out := map[string]bool{}
	for k := range env.Storages {
		out[k] = true
	}

	return out
}

func acaCheckTemplate(t map[string]any, secrets map[string]bool, env *storedACAEnv, profile string) error {
	if t == nil || len(acaList(t[fieldContainers])) == 0 {
		return acaBad("properties.template.containers must contain at least one container.")
	}

	vols, err := acaCheckVolumes(acaList(t[fieldVolumes]), acaStorageSet(env))
	if err != nil {
		return err
	}

	if err = acaCheckContainers(t, secrets, vols); err != nil {
		return err
	}

	if err = acaCheckSizing(acaList(t[fieldContainers]), acaDedicated(profile), !env.hasProfiles()); err != nil {
		return err
	}

	if suffix := strOf(t, fieldRevSuffix); suffix != "" && !acaNameValid(suffix, 1, acaMaxSuffixLen) {
		return acaBad("revisionSuffix '%s' must be lowercase alphanumerics or '-', start with a letter, end "+
			"alphanumeric, contain no '--' and be at most %d characters.", suffix, acaMaxSuffixLen)
	}

	return acaCheckScale(mapOf(t[fieldScale]))
}

// planAppRevision decides whether the request creates a revision and validates traffic against the resulting names.
func (p *ContainerAppsProvider) planAppRevision(name string, prev *storedACAApp, req acaAppReq) (acaAppReq, error) {
	acaNormalizeTemplate(req.tmpl)

	req.suffix = strOf(req.tmpl, fieldRevSuffix)

	latest := (*storedACARevision)(nil)
	if prev != nil {
		latest = prev.latest()
	}

	names := map[string]bool{}

	if prev != nil {
		for _, r := range prev.Revisions {
			names[strings.ToLower(r.Name)] = true
		}
	}

	switch {
	case latest == nil:
		req.create = true
	case acaTemplateKey(req.tmpl) == acaTemplateKey(latest.Template) &&
		(req.suffix == "" || req.suffix == latest.Suffix):
		req.revName = latest.Name
	default:
		req.create = true
	}

	if req.create {
		if req.suffix == "" {
			req.suffix = acaFreshSuffix(name, names)
		} else if names[strings.ToLower(acaRevName(name, req.suffix))] {
			return req, netErr(http.StatusBadRequest, acaCodeRevisionName,
				"Revision '%s' already exists; a different revisionSuffix is required for a changed template.",
				acaRevName(name, req.suffix))
		}

		req.revName = acaRevName(name, req.suffix)
		names[strings.ToLower(req.revName)] = true
	}

	return req, acaCheckTraffic(req.cfg, names, prev)
}

func acaFreshSuffix(app string, taken map[string]bool) string {
	for {
		s := awsid.NewN("", acaSuffixLen)
		if !taken[strings.ToLower(acaRevName(app, s))] {
			return s
		}
	}
}

type acaTrafficEntry struct {
	revision string
	label    string
	weight   int
	latest   bool
}

func acaTrafficOf(cfg map[string]any) []acaTrafficEntry {
	list := acaList(mapOf(cfg[fieldIngress])[fieldTraffic])
	out := make([]acaTrafficEntry, 0, len(list))

	for _, e := range list {
		m := mapOf(e)
		out = append(out, acaTrafficEntry{
			revision: strOf(m, fieldRevisionName), label: strOf(m, fieldLabel), weight: acaWeightOf(m),
			latest: acaBool(m, fieldLatestRev),
		})
	}

	return out
}

func acaCheckTraffic(cfg map[string]any, revisions map[string]bool, prev *storedACAApp) error {
	mode, _ := acaModeOf(cfg)
	entries := acaTrafficOf(cfg)

	if len(entries) == 0 {
		return nil
	}

	known := map[string]bool{}

	if prev != nil {
		for _, e := range acaTrafficOf(prev.Config) {
			known[e.label] = true
		}
	}

	total, labels := 0, map[string]bool{}

	for _, e := range entries {
		if e.weight < 0 || e.weight > acaPercent {
			return acaBad("Traffic weights must be between 0 and 100.")
		}

		if err := acaCheckTrafficEntry(e, mode, revisions); err != nil {
			return err
		}

		if err := acaCheckTrafficLabel(e, labels); err != nil {
			return err
		}

		total += e.weight
	}

	for _, e := range entries {
		if e.revision == "" && !e.latest && !labels[e.label] && !known[e.label] {
			return acaBad("Traffic label '%s' is not assigned to any revision.", e.label)
		}
	}

	if total != acaPercent {
		return acaBad("The sum of the traffic weights must be 100, but it is %d.", total)
	}

	return nil
}

func acaCheckTrafficLabel(e acaTrafficEntry, labels map[string]bool) error {
	if e.label == "" {
		return nil
	}

	if err := acaCheckLabel(e.label); err != nil {
		return err
	}

	if labels[e.label] && (e.revision != "" || e.latest) {
		return acaBad("Label '%s' is assigned to more than one revision.", e.label)
	}

	labels[e.label] = true

	return nil
}

const acaPercent = 100

func acaCheckTrafficEntry(e acaTrafficEntry, mode string, revisions map[string]bool) error {
	switch {
	case e.revision != "" && e.latest:
		return acaBad("A traffic entry cannot set both revisionName and latestRevision.")
	case e.revision == "" && !e.latest && e.label == "":
		return acaBad("A traffic entry needs a revisionName, latestRevision or a label.")
	case e.revision != "" && !revisions[strings.ToLower(e.revision)]:
		return acaBad("Revision '%s' referenced by the traffic configuration was not found.", e.revision)
	case mode == acaModeSingle && (e.revision != "" || e.label != ""):
		return acaBad(
			"Traffic can only target named revisions or labels when activeRevisionsMode is Multiple or Labels.",
		)
	}

	return nil
}

// commitApp applies req to the stored app (creating it when prev is nil) and returns the record.
func (p *ContainerAppsProvider) commitApp(id ResourceID, prev *storedACAApp, req acaAppReq) *storedACAApp {
	now := p.now().UTC()
	rec := prev

	if rec == nil {
		rec = &storedACAApp{Created: now, Sub: id.SubscriptionID, Name: id.LeafName(), ResourceGroup: id.ResourceGroup}
	}

	rec.Tags, rec.Config, rec.Identity, rec.Modified = req.tags, req.cfg, req.ident, now
	rec.Location, rec.Kind, rec.ManagedBy = req.location, req.kind, req.managed
	rec.EnvironmentID, rec.WorkloadProfile = req.env.armID(), req.profile

	if prev != nil {
		rec.Location = prev.Location
	}

	if req.create {
		rec.Revisions = append(rec.Revisions, &storedACARevision{
			Name: req.revName, Suffix: req.suffix, Created: now, LastActive: now, Active: true, Template: req.tmpl,
		})
	} else if l := rec.latest(); l != nil {
		l.Template = req.tmpl
	}

	return rec
}

func (p *ContainerAppsProvider) appBody(id ResourceID, a *storedACAApp) map[string]any {
	env := p.envByID(a.EnvironmentID)
	ready := p.readyRevision(a)

	props := map[string]any{
		fieldProvisioningState: provisioningStateSucceeded, "runningStatus": acaRunningStatus(a),
		"environmentId": a.EnvironmentID, "managedEnvironmentId": a.EnvironmentID,
		acaFieldOutboundIPs: []string{acaStaticIP}, "customDomainVerificationId": acaVerificationID(a.Sub),
		fieldConfiguration: p.configBody(a, env),
	}

	if a.WorkloadProfile != "" {
		props[fieldWorkloadProf] = a.WorkloadProfile
	}

	if l := a.latest(); l != nil {
		props["latestRevisionName"] = l.Name
		props[fieldTemplate] = acaTemplateOut(l.Template)

		if ready != "" {
			props["latestReadyRevisionName"] = ready
		}

		if a.ingress() != nil && env != nil {
			props["latestRevisionFqdn"] = acaHostFor(l.Name, env.DefaultDomain, acaBool(a.ingress(), "external"))
		}
	}

	aid := acaResourceID(id.SubscriptionID, a.ResourceGroup, []string{acaAppType}, []string{a.Name})
	out := map[string]any{
		"id": aid.ARMID(), fieldName: a.Name, fieldType: namespaceMicrosoftApp + "/" + acaAppType,
		fieldLocation: a.Location, fieldTags: tagsOrEmpty(a.Tags), fieldProperties: props,
		acaFieldSystemData: acaSystemData(a.Created, a.Modified),
	}

	if a.Kind != "" {
		out[acaFieldKind] = a.Kind
	}

	if a.ManagedBy != "" {
		out["managedBy"] = a.ManagedBy
	}

	if ident := p.ids.body(a.Identity); ident != nil {
		out[fieldIdentity] = ident
	}

	return out
}

func acaRunningStatus(a *storedACAApp) string {
	if a.Stopped {
		return aciStateStopped
	}

	return aciStateRunning
}

func acaHostFor(label, domain string, external bool) string {
	if external {
		return label + "." + domain
	}

	return label + ".internal." + domain
}

func acaTemplateOut(t map[string]any) map[string]any {
	out := acaClone(t)
	if _, has := out[fieldRevSuffix]; !has {
		out[fieldRevSuffix] = ""
	}

	return out
}

func (p *ContainerAppsProvider) configBody(a *storedACAApp, env *storedACAEnv) map[string]any {
	cfg := acaClone(a.Config)

	if _, has := cfg["activeRevisionsMode"]; !has {
		cfg["activeRevisionsMode"] = acaModeSingle
	}

	if _, has := cfg["maxInactiveRevisions"]; !has {
		cfg["maxInactiveRevisions"] = float64(acaDefaultInactive)
	}

	secrets := acaPublicSecrets(acaList(cfg[fieldSecrets]))

	if len(secrets) > 0 {
		cfg[fieldSecrets] = secrets
	} else {
		delete(cfg, fieldSecrets)
	}

	if in := mapOf(cfg[fieldIngress]); in != nil {
		p.ingressBody(a, env, in)
	}

	return cfg
}

func (p *ContainerAppsProvider) ingressBody(a *storedACAApp, env *storedACAEnv, in map[string]any) {
	if env != nil {
		in["fqdn"] = acaHostFor(a.Name, env.DefaultDomain, acaBool(in, "external"))
	}

	if _, has := in["external"]; !has {
		in["external"] = false
	}

	if _, has := in["transport"]; !has {
		in["transport"] = "auto"
	}

	if len(acaList(in[fieldTraffic])) == 0 {
		in[fieldTraffic] = []any{map[string]any{fieldLatestRev: true, fieldWeight: float64(acaPercent)}}
	}
}

func (p *ContainerAppsProvider) listApps(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, k := range sortedKeys(p.apps) {
		if a := p.apps[k]; id.ResourceGroup == "" || resourceGroupsEqual(a.ResourceGroup, id.ResourceGroup) {
			out = append(out, p.appBody(id, a))
		}
	}

	return out
}

func (p *ContainerAppsProvider) deleteApp(id ResourceID, name string) error {
	p.opMu.Lock("deleteApp")
	defer p.opMu.Unlock()

	key := acaKey(id.ResourceGroup, name)

	p.mu.Lock("deleteApp")

	a, ok := p.apps[key]
	if !ok {
		p.mu.Unlock()

		return ErrResourceNotFound
	}

	revKeys := p.revisionKeys(a)

	delete(p.apps, key)
	p.rebuildHostsLocked()
	p.mu.Unlock()

	p.rt.closeRevisions(revKeys)

	return nil
}

func (p *ContainerAppsProvider) patchApp(id ResourceID, patch map[string]any) (map[string]any, error) {
	p.mu.RLock("patchApp")
	a, ok := p.apps[acaKey(id.ResourceGroup, id.LeafName())]

	var cur map[string]any

	if ok {
		props := map[string]any{
			fieldEnvironmentID: a.EnvironmentID, fieldConfiguration: acaClone(a.Config),
			fieldTemplate: acaClone(a.latest().Template),
		}
		if a.WorkloadProfile != "" {
			props[fieldWorkloadProf] = a.WorkloadProfile
		}

		cur = map[string]any{
			fieldLocation: a.Location, fieldTags: acaTagsAny(a.Tags), fieldProperties: props, acaFieldKind: a.Kind,
			"managedBy": a.ManagedBy,
		}

		if ident := p.ids.body(a.Identity); ident != nil {
			cur[fieldIdentity] = ident
		}
	}
	p.mu.RUnlock()

	if !ok {
		return nil, ErrResourceNotFound
	}

	acaMergePatch(cur, patch)

	return p.putApp(context.Background(), id, cur)
}

func (p *ContainerAppsProvider) listSecrets(id ResourceID) (map[string]any, error) {
	p.mu.RLock("listSecrets")
	defer p.mu.RUnlock()

	a, ok := p.apps[acaKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return map[string]any{fieldValue: acaSecretList(a.Config)}, nil
}

func acaSecretList(cfg map[string]any) []any {
	list := acaList(cfg[fieldSecrets])
	out := make([]any, 0, len(list))

	for _, e := range list {
		s := mapOf(e)
		item := map[string]any{fieldName: s[fieldName]}

		for _, k := range []string{fieldValue, fieldKeyVaultURL, fieldIdentitySet} {
			if v, has := s[k]; has {
				item[k] = v
			}
		}

		out = append(out, item)
	}

	return out
}

// pruneInactive drops the oldest inactive revisions beyond the app's maxInactiveRevisions.
func (a *storedACAApp) pruneInactive() []string {
	limit := acaDefaultInactive
	if n, has := acaNum(a.Config, "maxInactiveRevisions"); has {
		limit = int(n)
	}

	var inactive []int

	for i, r := range a.Revisions {
		if !r.Active {
			inactive = append(inactive, i)
		}
	}

	if len(inactive) <= limit {
		return nil
	}

	drop := map[int]bool{}

	var names []string

	for _, i := range inactive[:len(inactive)-limit] {
		drop[i] = true

		names = append(names, a.Revisions[i].Name)
	}

	kept := a.Revisions[:0:0]

	for i, r := range a.Revisions {
		if !drop[i] {
			kept = append(kept, r)
		}
	}

	a.Revisions = kept

	return names
}

func (p *ContainerAppsProvider) revisionKeys(a *storedACAApp) []string {
	keys := make([]string, 0, len(a.Revisions))
	for _, r := range a.Revisions {
		keys = append(keys, acaRevKey(a, r.Name))
	}

	return keys
}

func acaRevKey(a *storedACAApp, rev string) string {
	return "~aca/" + strings.ToLower(a.ResourceGroup) + "/" + strings.ToLower(rev)
}

func acaAtoi(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("parse integer %q: %w", s, err)
	}

	return n, nil
}

// acaPublicSecrets is the secrets array as GET returns it: names and Key Vault references, never values.
func acaPublicSecrets(list []any) []any {
	out := make([]any, 0, len(list))

	for _, e := range list {
		s := mapOf(e)
		item := map[string]any{fieldName: s[fieldName]}

		if kv := strOf(s, fieldKeyVaultURL); kv != "" {
			item[fieldKeyVaultURL], item[fieldIdentitySet] = kv, s[fieldIdentitySet]
		}

		out = append(out, item)
	}

	return out
}
