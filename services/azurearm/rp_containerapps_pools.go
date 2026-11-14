package azurearm

import (
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	acaPoolType    = "sessionPools"
	acaPoolMaxName = 128

	poolFieldContainer = "containerType"
	poolFieldMgmt      = "poolManagementType"
	poolFieldScale     = "scaleConfiguration"
	poolFieldDynamic   = "dynamicPoolConfiguration"
	poolFieldTemplate  = "customContainerTemplate"
	poolFieldNetwork   = "sessionNetworkConfiguration"
	poolFieldSecrets   = "secrets"
	poolFieldLife      = "lifecycleConfiguration"
	poolFieldEndpoint  = "poolManagementEndpoint"
	poolCustom         = "CustomContainer"
	poolDynamic        = "Dynamic"
	poolManual         = "Manual"
)

var acaPoolNameRe = regexp.MustCompile(`^[-\w._()]+$`)

//nolint:gochecknoglobals // static enums from the Container Apps spec
var (
	poolContainerTypes = []string{poolCustom, "PythonLTS", "NodeLTS", "Shell"}
	poolMgmtTypes      = []string{poolDynamic, poolManual}
	poolLifecycles     = []string{"Timed", "OnContainerExit"}
	poolEgress         = []string{"EgressEnabled", "EgressDisabled"}
)

type storedACAPool struct {
	Created       time.Time         `json:"created"`
	Modified      time.Time         `json:"modified"`
	Tags          map[string]string `json:"tags,omitempty"`
	Props         map[string]any    `json:"props"`
	Sub           string            `json:"sub"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Identity      resourceIdentity  `json:"identity"`
}

func acaCheckPoolTemplate(props map[string]any, kind string) error {
	tmpl := mapOf(props[poolFieldTemplate])

	if kind != poolCustom {
		if tmpl != nil {
			return acaBad("customContainerTemplate is only allowed when containerType is %s.", poolCustom)
		}

		return nil
	}

	if tmpl == nil {
		return acaBad("customContainerTemplate is required when containerType is %s.", poolCustom)
	}

	containers := acaList(tmpl["containers"])
	if len(containers) == 0 || strOf(mapOf(containers[0]), "image") == "" {
		return acaBad("customContainerTemplate.containers requires a container with an image.")
	}

	if _, set := acaNum(mapOf(tmpl["ingress"]), "targetPort"); !set {
		return acaBad("customContainerTemplate.ingress.targetPort is required.")
	}

	return nil
}

func acaCheckPoolScale(props map[string]any, mgmt string) error {
	if sc := mapOf(props[poolFieldScale]); sc != nil {
		if n, set := acaNum(sc, "maxConcurrentSessions"); set && n < 1 {
			return acaBad("scaleConfiguration.maxConcurrentSessions must be at least 1.")
		}

		if n, set := acaNum(sc, "readySessionInstances"); set && n < 0 {
			return acaBad("scaleConfiguration.readySessionInstances must not be negative.")
		}
	}

	dyn := mapOf(props[poolFieldDynamic])
	if dyn == nil {
		return nil
	}

	if mgmt != poolDynamic {
		return acaBad("dynamicPoolConfiguration is only allowed when poolManagementType is %s.", poolDynamic)
	}

	if lt := strOf(mapOf(dyn[poolFieldLife]), "lifecycleType"); lt != "" {
		if _, ok := canonicalIn(poolLifecycles, lt); !ok {
			return acaBad("lifecycleType '%s' is not valid; allowed values are Timed and OnContainerExit.", lt)
		}
	}

	return nil
}

func acaCheckPool(props map[string]any) error {
	kind, ok := canonicalIn(poolContainerTypes, strOf(props, poolFieldContainer))
	if !ok {
		return acaBad("properties.containerType must be one of %s.", strings.Join(poolContainerTypes, ", "))
	}

	props[poolFieldContainer] = kind

	mgmt, ok := canonicalIn(poolMgmtTypes, firstNonEmpty(strOf(props, poolFieldMgmt), poolDynamic))
	if !ok {
		return acaBad("properties.poolManagementType must be Dynamic or Manual.")
	}

	props[poolFieldMgmt] = mgmt

	if st := strOf(mapOf(props[poolFieldNetwork]), "status"); st != "" {
		if _, ok = canonicalIn(poolEgress, st); !ok {
			return acaBad("sessionNetworkConfiguration.status must be EgressEnabled or EgressDisabled.")
		}
	}

	if err := acaCheckPoolTemplate(props, kind); err != nil {
		return err
	}

	return acaCheckPoolScale(props, mgmt)
}

func (p *ContainerAppsProvider) putPool(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putPool")
	defer p.opMu.Unlock()

	name := id.LeafName()
	if len(name) > acaPoolMaxName || !acaPoolNameRe.MatchString(name) {
		return nil, acaBad("Session pool name '%s' is invalid.", name)
	}

	props := acaClone(mapOf(body[fieldProperties]))
	if props == nil {
		return nil, acaBad("properties are required.")
	}

	p.mu.Lock("putPool")
	defer p.mu.Unlock()

	key := acaKey(id.ResourceGroup, name)
	prev := p.pools[key]

	env := p.envByID(strOf(props, fieldEnvironmentID))
	if env == nil || !strings.EqualFold(env.Sub, id.SubscriptionID) {
		return nil, netErr(http.StatusBadRequest, acaCodeEnvMissing, "Managed environment '%s' was not found.",
			strOf(props, fieldEnvironmentID))
	}

	if prev != nil && !strings.EqualFold(strOf(prev.Props, fieldEnvironmentID), env.armID()) {
		return nil, acaBad("The managed environment of an existing session pool cannot be changed.")
	}

	props[fieldEnvironmentID] = env.armID()

	if err := acaCheckPool(props); err != nil {
		return nil, err
	}

	for _, k := range []string{fieldProvisioningState, poolFieldEndpoint, "nodeCount"} {
		delete(props, k)
	}

	rec, err := p.buildPool(id, prev, body, props)
	if err != nil {
		return nil, err
	}

	p.pools[key] = rec

	return p.poolBody(id, rec), nil
}

func (p *ContainerAppsProvider) buildPool(
	id ResourceID, prev *storedACAPool, body, props map[string]any,
) (*storedACAPool, error) {
	loc := acaRegion(strOf(body, fieldLocation))
	if prev != nil && loc == "" {
		loc = prev.Location
	}

	if loc == "" {
		return nil, netErr(
			http.StatusBadRequest,
			netCodeLocation,
			"The location property is required for this definition.",
		)
	}

	var prevIdent resourceIdentity
	if prev != nil {
		prevIdent = prev.Identity
	}

	_, hasIdent := body[fieldIdentity]

	ident, err := p.ids.apply(body[fieldIdentity], hasIdent, prevIdent)
	if err != nil {
		return nil, err
	}

	now := p.now().UTC()
	rec := &storedACAPool{
		Created: now, Modified: now, Tags: stringTags(body[fieldTags]), Props: props, Sub: id.SubscriptionID,
		Name: id.LeafName(), ResourceGroup: id.ResourceGroup, Location: loc, Identity: ident,
	}

	if prev != nil {
		rec.Created = prev.Created

		if prev.Location != loc {
			return nil, acaBad("The location of an existing session pool cannot be changed.")
		}
	}

	return rec, nil
}

func (p *ContainerAppsProvider) poolBody(id ResourceID, s *storedACAPool) map[string]any {
	props := acaClone(s.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded

	secrets := make([]any, 0, len(acaList(props[poolFieldSecrets])))
	for _, sec := range acaList(props[poolFieldSecrets]) {
		secrets = append(secrets, map[string]any{fieldName: strOf(mapOf(sec), fieldName)})
	}

	if len(secrets) > 0 {
		props[poolFieldSecrets] = secrets
	}

	if strOf(props, poolFieldMgmt) == poolManual {
		props["nodeCount"] = intOf(mapOf(props[poolFieldScale])["readySessionInstances"])
	}

	props[poolFieldEndpoint] = "https://" + s.Location + ".dynamicsessions.io/subscriptions/" + s.Sub +
		"/resourceGroups/" + s.ResourceGroup + "/sessionPools/" + s.Name

	pid := acaResourceID(id.SubscriptionID, s.ResourceGroup, []string{acaPoolType}, []string{s.Name})
	out := map[string]any{
		"id": pid.ARMID(), fieldName: s.Name, fieldType: namespaceMicrosoftApp + "/" + acaPoolType,
		fieldLocation: s.Location, fieldTags: tagsOrEmpty(s.Tags), fieldProperties: props,
		acaFieldSystemData: acaSystemData(s.Created, s.Modified),
	}

	if ident := p.ids.body(s.Identity); ident != nil {
		out[fieldIdentity] = ident
	}

	return out
}

func (p *ContainerAppsProvider) listPools(id ResourceID) []map[string]any {
	out := []map[string]any{}

	for _, k := range sortedKeys(p.pools) {
		if s := p.pools[k]; id.ResourceGroup == "" || resourceGroupsEqual(s.ResourceGroup, id.ResourceGroup) {
			out = append(out, p.poolBody(id, s))
		}
	}

	return out
}

func (p *ContainerAppsProvider) deletePool(id ResourceID) error {
	p.opMu.Lock("deletePool")
	defer p.opMu.Unlock()

	p.mu.Lock("deletePool")
	defer p.mu.Unlock()

	key := acaKey(id.ResourceGroup, id.LeafName())
	if _, ok := p.pools[key]; !ok {
		return ErrResourceNotFound
	}

	delete(p.pools, key)

	return nil
}

func (p *ContainerAppsProvider) patchPool(id ResourceID, patch map[string]any) (map[string]any, error) {
	p.mu.RLock("patchPool")
	s, ok := p.pools[acaKey(id.ResourceGroup, id.LeafName())]

	var cur map[string]any
	if ok {
		cur = map[string]any{
			fieldLocation: s.Location, fieldTags: acaTagsAny(s.Tags), fieldProperties: acaClone(s.Props),
		}

		if ident := p.ids.body(s.Identity); ident != nil {
			cur[fieldIdentity] = ident
		}
	}
	p.mu.RUnlock()

	if !ok {
		return nil, ErrResourceNotFound
	}

	acaMergePatch(cur, patch)

	return p.putPool(id, cur)
}
