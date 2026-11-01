package azurearm

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	acaStaticIP        = "127.0.0.1"
	acaDomainHashLen   = 6
	acaSubnetConsumeMx = 23
	acaSubnetProfileMx = 27
	acaCodeEnvMissing  = "ManagedEnvironmentNotFound"
	acaCodeEnvInUse    = "ManagedEnvironmentHasContainerApps"
)

type storedACAStorage struct {
	Props map[string]any `json:"props"`
	Name  string         `json:"name"`
}

type storedACAEnv struct {
	Created       time.Time                    `json:"created"`
	Modified      time.Time                    `json:"modified"`
	Tags          map[string]string            `json:"tags,omitempty"`
	Props         map[string]any               `json:"props"`
	Storages      map[string]*storedACAStorage `json:"storages,omitempty"`
	Sub           string                       `json:"sub"`
	Name          string                       `json:"name"`
	ResourceGroup string                       `json:"resourceGroup"`
	Location      string                       `json:"location"`
	Kind          string                       `json:"kind,omitempty"`
	DefaultDomain string                       `json:"defaultDomain"`
	Identity      resourceIdentity             `json:"identity"`
}

func (e *storedACAEnv) armID() string {
	return acaResourceID(e.Sub, e.ResourceGroup, []string{acaEnvType}, []string{e.Name}).ARMID()
}

func (e *storedACAEnv) hasProfiles() bool { return len(acaList(e.Props["workloadProfiles"])) > 0 }

func acaDefaultDomain(sub, rg, name, location string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(sub + "/" + rg + "/" + name)))

	return name + "-" + hex.EncodeToString(sum[:])[:acaDomainHashLen] + "." + acaRegion(
		location,
	) + "." + acaDomainSuffix
}

func acaEnvReadOnly() []string {
	return []string{
		"provisioningState", "defaultDomain", "staticIp", "eventStreamEndpoint", "deploymentErrors",
		acaFieldPEC,
	}
}

func (p *ContainerAppsProvider) putEnv(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putEnv")
	defer p.opMu.Unlock()

	name := id.LeafName()
	if !acaNameValid(name, acaMinNameLen, acaMaxEnvNameLen) {
		return nil, acaBad("Managed environment name '%s' must be %d-%d lowercase alphanumerics or '-', start with a "+
			"letter, end alphanumeric and contain no '--'.", name, acaMinNameLen, acaMaxEnvNameLen)
	}

	props := acaClone(mapOf(body[fieldProperties]))
	if props == nil {
		props = map[string]any{}
	}

	for _, k := range acaEnvReadOnly() {
		delete(props, k)
	}

	if err := p.checkEnvProps(props); err != nil {
		return nil, err
	}

	_, hasIdentity := body[fieldIdentity]

	p.mu.Lock("putEnv")
	defer p.mu.Unlock()

	rec := &storedACAEnv{
		Sub: id.SubscriptionID, Name: name, ResourceGroup: id.ResourceGroup, Location: DefaultLocation,
		Created: p.now().UTC(), Props: props, Tags: stringTags(body[fieldTags]),
	}

	if loc := strOf(body, fieldLocation); loc != "" {
		rec.Location = loc
	}

	rec.Kind = strOf(body, acaFieldKind)

	var prevIdentity resourceIdentity

	if old, ok := p.envs[acaKey(id.ResourceGroup, name)]; ok {
		if err := acaEnvImmutable(old.Props, props); err != nil {
			return nil, err
		}

		rec.Created, rec.Storages, rec.Location = old.Created, old.Storages, old.Location
		prevIdentity = old.Identity

		if loc := strOf(body, fieldLocation); loc != "" && !strings.EqualFold(loc, old.Location) {
			return nil, acaBad("The location of managed environment '%s' cannot be changed.", name)
		}
	}

	ident, err := p.ids.apply(body[fieldIdentity], hasIdentity, prevIdentity)
	if err != nil {
		return nil, err
	}

	rec.Identity, rec.Modified = ident, p.now().UTC()
	rec.DefaultDomain = acaDefaultDomain(id.SubscriptionID, id.ResourceGroup, name, rec.Location)
	p.envs[acaKey(id.ResourceGroup, name)] = rec

	return p.envBody(id, rec), nil
}

func acaEnvImmutable(old, next map[string]any) error {
	ov, nv := mapOf(old["vnetConfiguration"]), mapOf(next["vnetConfiguration"])

	if !strings.EqualFold(strOf(ov, "infrastructureSubnetId"), strOf(nv, "infrastructureSubnetId")) ||
		acaBool(ov, "internal") != acaBool(nv, "internal") {
		return acaBad("The vnetConfiguration of an existing managed environment cannot be changed.")
	}

	if acaBool(old, "zoneRedundant") != acaBool(next, "zoneRedundant") {
		return acaBad("zoneRedundant of an existing managed environment cannot be changed.")
	}

	return nil
}

func (p *ContainerAppsProvider) checkEnvProps(props map[string]any) error {
	if err := acaCheckAppLogs(mapOf(props["appLogsConfiguration"])); err != nil {
		return err
	}

	if err := acaCheckProfiles(acaList(props["workloadProfiles"])); err != nil {
		return err
	}

	return p.checkVnet(mapOf(props["vnetConfiguration"]), len(acaList(props["workloadProfiles"])) > 0)
}

func acaCheckAppLogs(c map[string]any) error {
	if c == nil {
		return nil
	}

	switch strOf(c, "destination") {
	case "", "azure-monitor", "none":
		return nil
	case "log-analytics":
		la := mapOf(c["logAnalyticsConfiguration"])
		if strOf(la, "customerId") == "" || strOf(la, "sharedKey") == "" {
			return acaBad("appLogsConfiguration.logAnalyticsConfiguration needs customerId and sharedKey when the " +
				"destination is log-analytics.")
		}

		return nil
	}

	return acaBad("appLogsConfiguration.destination '%s' must be log-analytics, azure-monitor or none.",
		strOf(c, "destination"))
}

func acaCheckProfiles(list []any) error {
	names := map[string]bool{}

	for _, e := range list {
		w := mapOf(e)
		name, typ := strOf(w, fieldName), strOf(w, "workloadProfileType")

		if name == "" || typ == "" || names[strings.ToLower(name)] {
			return acaBad("Workload profiles need a unique name and a workloadProfileType.")
		}

		names[strings.ToLower(name)] = true

		minC, hasMin := acaNum(w, "minimumCount")
		maxC, hasMax := acaNum(w, "maximumCount")

		switch {
		case typ == acaConsumption && (hasMin || hasMax):
			return acaBad("Workload profile '%s' of type Consumption cannot set minimumCount or maximumCount.", name)
		case typ != acaConsumption && (!hasMin || !hasMax || minC < 0 || maxC < minC):
			return acaBad("Workload profile '%s' needs minimumCount <= maximumCount.", name)
		}
	}

	return nil
}

func (p *ContainerAppsProvider) checkVnet(v map[string]any, profiles bool) error {
	subnetID := strOf(v, "infrastructureSubnetId")
	if subnetID == "" {
		return nil
	}

	if p.net == nil {
		return nil
	}

	vnet, subnet, found := p.net.subnetByID(subnetID)
	if !found {
		if rid, err := ParseGenericResourcePath(subnetID); err == nil && len(rid.Names) > 0 {
			if _, exists := p.net.top(netVNets, rid.ResourceGroup, rid.Names[0]); exists {
				return acaBad("The subnet '%s' was not found in its virtual network.", subnetID)
			}
		}

		return nil
	}

	_ = vnet

	prefix := strOf(subnet.Props, "addressPrefix")
	if prefix == "" {
		return nil
	}

	_, bits, ok := strings.Cut(prefix, "/")
	if !ok {
		return nil
	}

	size, convErr := acaAtoi(bits)
	if convErr != nil {
		return nil //nolint:nilerr // an unparsable prefix is the network provider's to reject
	}

	need := acaSubnetConsumeMx
	if profiles {
		need = acaSubnetProfileMx
	}

	if size > need {
		return acaBad("The infrastructure subnet must be at least /%d for this environment type, but '%s' is /%d.",
			need, subnetID, size)
	}

	return nil
}

func (p *ContainerAppsProvider) envBody(id ResourceID, e *storedACAEnv) map[string]any {
	props := acaClone(e.Props)

	if la := mapOf(mapOf(props["appLogsConfiguration"])["logAnalyticsConfiguration"]); la != nil {
		delete(la, "sharedKey")
	}

	props[fieldProvisioningState] = provisioningStateSucceeded
	props["defaultDomain"] = e.DefaultDomain
	props["staticIp"] = acaStaticIP

	if _, has := props["zoneRedundant"]; !has {
		props["zoneRedundant"] = false
	}

	cd := mapOf(props["customDomainConfiguration"])
	if cd == nil {
		cd = map[string]any{}
		props["customDomainConfiguration"] = cd
	}

	cd["customDomainVerificationId"] = acaVerificationID(e.armID())

	if e.hasProfiles() && strOf(props, "infrastructureResourceGroup") == "" {
		props["infrastructureResourceGroup"] = "ME_" + e.Name + "_" + e.ResourceGroup + "_" + acaRegion(e.Location)
	}

	eid := acaResourceID(id.SubscriptionID, e.ResourceGroup, []string{acaEnvType}, []string{e.Name})
	out := map[string]any{
		"id": eid.ARMID(), fieldName: e.Name, fieldType: namespaceMicrosoftApp + "/" + acaEnvType,
		fieldLocation: e.Location, fieldTags: tagsOrEmpty(e.Tags), fieldProperties: props,
		acaFieldSystemData: acaSystemData(e.Created, e.Modified),
	}

	if e.Kind != "" {
		out[acaFieldKind] = e.Kind
	}

	if ident := p.ids.body(e.Identity); ident != nil {
		out[fieldIdentity] = ident
	}

	return out
}

func acaVerificationID(armID string) string {
	sum := sha256.Sum256([]byte("verify:" + strings.ToLower(armID)))

	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

func acaSystemData(created, modified time.Time) map[string]any {
	return map[string]any{
		"createdAt": created.Format(time.RFC3339Nano), "lastModifiedAt": modified.Format(time.RFC3339Nano),
		"createdByType": "Application", "lastModifiedByType": "Application",
	}
}

func (p *ContainerAppsProvider) listEnvs(id ResourceID) []map[string]any {
	var recs []*storedACAEnv

	for _, k := range sortedKeys(p.envs) {
		if e := p.envs[k]; id.ResourceGroup == "" || resourceGroupsEqual(e.ResourceGroup, id.ResourceGroup) {
			recs = append(recs, e)
		}
	}

	out := make([]map[string]any, 0, len(recs))

	for _, e := range recs {
		out = append(out, p.envBody(id, e))
	}

	return out
}

func (p *ContainerAppsProvider) deleteEnv(id ResourceID, name string) error {
	p.opMu.Lock("deleteEnv")
	defer p.opMu.Unlock()

	p.mu.Lock("deleteEnv")
	defer p.mu.Unlock()

	key := acaKey(id.ResourceGroup, name)

	e, ok := p.envs[key]
	if !ok {
		return ErrResourceNotFound
	}

	envID := strings.ToLower(e.armID())

	for _, a := range p.apps {
		if strings.ToLower(a.EnvironmentID) == envID {
			return netErr(http.StatusBadRequest, acaCodeEnvInUse,
				"Cannot delete managed environment '%s' because container app '%s' still uses it.", name, a.Name)
		}
	}

	for _, j := range p.jobs {
		if strings.ToLower(j.EnvironmentID) == envID {
			return netErr(http.StatusBadRequest, acaCodeEnvInUse,
				"Cannot delete managed environment '%s' because job '%s' still uses it.", name, j.Name)
		}
	}

	delete(p.envs, key)

	return nil
}

// PatchResource implements resourcePatcher for managed environments, container apps and jobs.
func (p *ContainerAppsProvider) PatchResource(id ResourceID, body map[string]any) (map[string]any, error) {
	t := acaTargetOf(id)
	if t.listing {
		return nil, ErrResourceNotFound
	}

	switch t.kind {
	case acaKindEnv:
		return p.patchEnv(id, body)
	case acaKindApp:
		return p.patchApp(id, body)
	case acaKindJob:
		return p.patchJob(id, body)
	case acaKindOther, acaKindStorage, acaKindRevision, acaKindReplica, acaKindExecution:
	}

	return nil, acaBad("%s does not support PATCH.", id.LeafType())
}

func acaMergePatch(dst, patch map[string]any) {
	for k, v := range patch {
		pm, isMap := v.(map[string]any)
		dm, dstMap := dst[k].(map[string]any)

		switch {
		case v == nil:
			delete(dst, k)
		case isMap && dstMap:
			acaMergePatch(dm, pm)
		default:
			dst[k] = v
		}
	}
}

func (p *ContainerAppsProvider) patchEnv(id ResourceID, patch map[string]any) (map[string]any, error) {
	p.mu.RLock("patchEnv")
	e, ok := p.envs[acaKey(id.ResourceGroup, id.LeafName())]

	var cur map[string]any
	if ok {
		cur = map[string]any{
			fieldLocation:   e.Location,
			fieldTags:       acaTagsAny(e.Tags),
			fieldProperties: acaClone(e.Props),
			acaFieldKind:    e.Kind,
		}
	}
	p.mu.RUnlock()

	if !ok {
		return nil, ErrResourceNotFound
	}

	acaMergePatch(cur, patch)

	return p.putEnv(id, cur)
}

func acaTagsAny(tags map[string]string) map[string]any {
	out := make(map[string]any, len(tags))
	for k, v := range tags {
		out[k] = v
	}

	return out
}

func (p *ContainerAppsProvider) putStorage(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putStorage")
	defer p.opMu.Unlock()

	t := acaTargetOf(id)
	props := acaClone(mapOf(body[fieldProperties]))

	if err := acaCheckStorage(t.names[1], props); err != nil {
		return nil, err
	}

	p.mu.Lock("putStorage")
	defer p.mu.Unlock()

	env, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	if env.Storages == nil {
		env.Storages = map[string]*storedACAStorage{}
	}

	env.Storages[strings.ToLower(t.names[1])] = &storedACAStorage{Name: t.names[1], Props: props}
	env.Modified = p.now().UTC()

	return p.storageBody(id, env.Storages[strings.ToLower(t.names[1])]), nil
}

func acaCheckStorage(name string, props map[string]any) error {
	if !acaNameValid(name, 1, acaMaxEnvNameLen) {
		return acaBad("Storage name '%s' is invalid.", name)
	}

	af, nfs := mapOf(props["azureFile"]), mapOf(props["nfsAzureFile"])

	switch {
	case (af == nil) == (nfs == nil):
		return acaBad("Storage '%s' must define exactly one of azureFile or nfsAzureFile.", name)
	case af != nil:
		mode := strOf(af, "accessMode")
		if strOf(af, "accountName") == "" || strOf(af, "accountKey") == "" || strOf(af, "shareName") == "" ||
			(mode != "ReadOnly" && mode != "ReadWrite") {
			return acaBad("azureFile storage needs accountName, accountKey, shareName and an accessMode of " +
				"ReadOnly or ReadWrite.")
		}
	default:
		if strOf(nfs, "server") == "" || strOf(nfs, "shareName") == "" {
			return acaBad("nfsAzureFile storage needs server and shareName.")
		}
	}

	return nil
}

func (p *ContainerAppsProvider) storageBody(id ResourceID, s *storedACAStorage) map[string]any {
	props := acaClone(s.Props)

	if af := mapOf(props["azureFile"]); af != nil {
		delete(af, "accountKey")
	}

	eid := acaResourceID(id.SubscriptionID, id.ResourceGroup, []string{acaEnvType, acaStorageType},
		[]string{id.Names[0], s.Name})

	return map[string]any{
		"id":               eid.ARMID(),
		fieldName:          s.Name,
		fieldType:          namespaceMicrosoftApp + "/" + acaEnvType + "/" + acaStorageType,
		fieldProperties:    props,
		acaFieldSystemData: map[string]any{},
	}
}

func (p *ContainerAppsProvider) storageGet(id ResourceID, t acaTarget) (map[string]any, error) {
	e, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	s, ok := e.Storages[strings.ToLower(t.names[1])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return p.storageBody(id, s), nil
}

func (p *ContainerAppsProvider) listStorages(id ResourceID) ([]map[string]any, error) {
	e, ok := p.envs[acaKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	keys := make([]string, 0, len(e.Storages))
	for k := range e.Storages {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]map[string]any, 0, len(keys))

	for _, k := range keys {
		out = append(out, p.storageBody(id, e.Storages[k]))
	}

	return out, nil
}

func (p *ContainerAppsProvider) deleteStorage(id ResourceID, t acaTarget) error {
	p.opMu.Lock("deleteStorage")
	defer p.opMu.Unlock()

	p.mu.Lock("deleteStorage")
	defer p.mu.Unlock()

	e, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return ErrResourceNotFound
	}

	key := strings.ToLower(t.names[1])
	if _, ok = e.Storages[key]; !ok {
		return ErrResourceNotFound
	}

	delete(e.Storages, key)

	return nil
}
