package azurearm

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	aciProfilesType    = "containerGroupProfiles"
	aciRevisionsType   = "revisions"
	aciProfileSegs     = 8
	aciMaxRevisions    = 1000
	aciProfileMaxNames = 63
	aciRevisionDepth   = 2
)

var aciProfileNameRe = regexp.MustCompile(`^[a-zA-Z0-9]$|^[a-zA-Z0-9][-_a-zA-Z0-9]{0,61}[a-zA-Z0-9]$`)

// aciProfileExtra holds the profile properties a container group does not have.
type aciProfileExtra struct {
	UseKrypton          *bool            `json:"useKrypton,omitempty"`
	SecurityContext     *aciSecurityWire `json:"securityContext,omitempty"`
	ShutdownGracePeriod string           `json:"shutdownGracePeriod,omitempty"`
	TimeToLive          string           `json:"timeToLive,omitempty"`
}

type aciProfileRevision struct {
	Created time.Time       `json:"created"`
	Extra   aciProfileExtra `json:"extra"`
	Props   aciPropsWire    `json:"props"`
	Number  int             `json:"number"`
}

type aciRevisionKey struct {
	E aciProfileExtra
	P aciPropsWire
}

type storedACIProfile struct {
	Created       time.Time            `json:"created"`
	Tags          map[string]string    `json:"tags,omitempty"`
	Name          string               `json:"name"`
	Sub           string               `json:"sub,omitempty"`
	ResourceGroup string               `json:"resourceGroup"`
	Location      string               `json:"location"`
	Zones         []string             `json:"zones,omitempty"`
	Revisions     []aciProfileRevision `json:"revisions"`
}

func aciProfileID(id ResourceID) bool {
	return len(id.Types) >= 1 && strings.EqualFold(id.Types[0], aciProfilesType)
}

func (p *ContainerInstanceProvider) profile(rg, name string) (*storedACIProfile, bool) {
	g, ok := p.profiles[aciKey(rg, name)]

	return g, ok
}

// splitProfileProps removes the properties only a profile has and rejects the group-only ones, so the rest can be
// parsed as container group properties (azure-rest-api-specs ContainerGroupProfileProperties, 2025-09-01).
func splitProfileProps(raw any) (any, aciProfileExtra, error) {
	var extra aciProfileExtra

	props, ok := raw.(map[string]any)
	if !ok {
		return raw, extra, nil
	}

	rest := make(map[string]any, len(props))

	for k, v := range props {
		switch k {
		case "revision", "registeredRevisions":
		case "subnetIds", "dnsConfig":
			return nil, extra, aciUnsupported("property " + k + " on a container group profile")
		case "shutdownGracePeriod", "timeToLive", "useKrypton", "securityContext":
			buf, err := json.Marshal(map[string]any{k: v}, jsonx.V1())
			if err == nil {
				err = json.UnmarshalDecode(jsontext.NewDecoder(bytes.NewReader(buf)), &extra, jsonx.V1(),
					json.RejectUnknownMembers(true))
			}

			if err != nil {
				return nil, extra, aciBad("%s: %v", k, err)
			}
		default:
			rest[k] = v
		}
	}

	if err := extra.SecurityContext.validate("securityContext"); err != nil {
		return nil, extra, err
	}

	return rest, extra, nil
}

func (p *ContainerInstanceProvider) putProfile(id ResourceID, body map[string]any) (map[string]any, error) {
	name := id.LeafName()
	if len(name) > aciProfileMaxNames || !aciProfileNameRe.MatchString(name) {
		return nil, aciBad("container group profile name %q is invalid", name)
	}

	zones := strList(body["zones"])
	if err := normalizeACIZones(zones); err != nil {
		return nil, err
	}

	raw, extra, err := splitProfileProps(body[fieldProperties])
	if err != nil {
		return nil, err
	}

	props, err := parseACIProps(raw)
	if err != nil {
		return nil, err
	}

	rec := &storedACIProfile{
		Name: name, Sub: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Location: DefaultLocation,
		Created: time.Now().UTC(), Zones: zones, Tags: stringTags(body[fieldTags]),
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.Location = loc
	}

	p.mu.Lock("putProfile")
	defer p.mu.Unlock()

	if old, ok := p.profile(id.ResourceGroup, name); ok {
		rec.Created, rec.Revisions = old.Created, old.Revisions
	}

	next := aciProfileRevision{Number: 1, Created: rec.Created, Props: props, Extra: extra}
	if n := len(rec.Revisions); n > 0 {
		last := rec.Revisions[n-1]
		if aciRevisionEqual(last, next) {
			p.profiles[aciKey(id.ResourceGroup, name)] = rec

			return aciProfileBody(id, rec, n-1), nil
		}

		if n >= aciMaxRevisions {
			return nil, aciBad("a container group profile supports at most %d revisions", aciMaxRevisions)
		}

		next.Number, next.Created = last.Number+1, time.Now().UTC()
	}

	rec.Revisions = append(rec.Revisions, next)
	p.profiles[aciKey(id.ResourceGroup, name)] = rec

	return aciProfileBody(id, rec, len(rec.Revisions)-1), nil
}

func aciRevisionEqual(a, b aciProfileRevision) bool {
	x, errA := json.Marshal(aciRevisionKey{P: a.Props, E: a.Extra}, jsonx.V1())
	y, errB := json.Marshal(aciRevisionKey{P: b.Props, E: b.Extra}, jsonx.V1())

	return errA == nil && errB == nil && bytes.Equal(x, y)
}

func aciProfileBody(id ResourceID, rec *storedACIProfile, idx int) map[string]any {
	rev := rec.Revisions[idx]
	pp := rev.Props

	conts := make([]map[string]any, 0, len(pp.Containers))

	for _, c := range pp.Containers {
		conts = append(conts, withoutInstanceView(aciContainerBody(c, nil)))
	}

	registered := make([]int, 0, len(rec.Revisions))
	for _, r := range rec.Revisions {
		registered = append(registered, r.Number)
	}

	props := map[string]any{
		"containers": conts, "osType": pp.OSType, "restartPolicy": pp.RestartPolicy,
		"revision": rev.Number, "registeredRevisions": registered,
	}

	if len(pp.Volumes) > 0 {
		props["volumes"] = aciVolumesBody(pp.Volumes)
	}

	if len(pp.InitContainers) > 0 {
		inits := make([]map[string]any, 0, len(pp.InitContainers))

		for _, ic := range pp.InitContainers {
			inits = append(inits, withoutInstanceView(aciInitBody(ic, nil)))
		}

		props["initContainers"] = inits
	}

	addACIFeatureBody(props, pp)
	aciProfileOptional(props, rev)

	out := map[string]any{
		"id": id.ARMID(), fieldName: rec.Name, fieldType: namespaceMicrosoftContainerInstance + "/" + aciProfilesType,
		fieldLocation: rec.Location, fieldTags: tagsOrEmpty(rec.Tags), fieldProperties: props,
	}

	if len(rec.Zones) > 0 {
		out["zones"] = rec.Zones
	}

	return out
}

func aciProfileOptional(props map[string]any, rev aciProfileRevision) {
	pp, ex := rev.Props, rev.Extra

	if pp.Priority != "" {
		props["priority"] = pp.Priority
	}

	if pp.IPAddress != nil {
		ip := map[string]any{fieldType: pp.IPAddress.Type, "ports": pp.IPAddress.Ports}
		if pp.IPAddress.DNSNameLabel != "" {
			ip["dnsNameLabel"] = pp.IPAddress.DNSNameLabel
		}

		props["ipAddress"] = ip
	}

	if len(pp.ImageRegistryCredentials) > 0 {
		creds := make([]map[string]any, 0, len(pp.ImageRegistryCredentials))
		for _, c := range pp.ImageRegistryCredentials {
			creds = append(creds, map[string]any{aciKeyServer: c.Server, "username": c.Username})
		}

		props["imageRegistryCredentials"] = creds
	}

	if pp.Diagnostics != nil {
		props["diagnostics"] = aciDiagnosticsBody(pp.Diagnostics)
	}

	if ex.SecurityContext != nil {
		props["securityContext"] = ex.SecurityContext
	}

	if ex.ShutdownGracePeriod != "" {
		props["shutdownGracePeriod"] = ex.ShutdownGracePeriod
	}

	if ex.TimeToLive != "" {
		props["timeToLive"] = ex.TimeToLive
	}

	if ex.UseKrypton != nil {
		props["useKrypton"] = *ex.UseKrypton
	}
}

func (p *ContainerInstanceProvider) getProfile(id ResourceID) (map[string]any, error) {
	p.mu.RLock("getProfile")
	defer p.mu.RUnlock()

	rec, ok := p.profile(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrResourceNotFound
	}

	if len(id.Types) == 1 {
		return aciProfileBody(id, rec, len(rec.Revisions)-1), nil
	}

	if !strings.EqualFold(id.Types[1], aciRevisionsType) {
		return nil, ErrResourceNotFound
	}

	n, err := strconv.Atoi(id.LeafName())
	if err != nil {
		return nil, ErrResourceNotFound
	}

	for i, r := range rec.Revisions {
		if r.Number == n {
			return aciProfileBody(profileRootID(id), rec, i), nil
		}
	}

	return nil, ErrResourceNotFound
}

func profileRootID(id ResourceID) ResourceID {
	root := id
	root.Types, root.Names = id.Types[:1], id.Names[:1]

	return root
}

func (p *ContainerInstanceProvider) listProfiles(id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("listProfiles")
	defer p.mu.RUnlock()

	if len(id.Types) == aciRevisionDepth {
		if !strings.EqualFold(id.Types[1], aciRevisionsType) || len(id.Names) < 1 {
			return nil, ErrResourceNotFound
		}

		rec, ok := p.profile(id.ResourceGroup, id.Names[0])
		if !ok {
			return nil, ErrResourceNotFound
		}

		out := make([]map[string]any, 0, len(rec.Revisions))
		for i := range rec.Revisions {
			out = append(out, aciProfileBody(profileRootID(id), rec, i))
		}

		return out, nil
	}

	recs := make([]*storedACIProfile, 0, len(p.profiles))

	for _, g := range p.profiles {
		if id.ResourceGroup == "" || resourceGroupsEqual(g.ResourceGroup, id.ResourceGroup) {
			recs = append(recs, g)
		}
	}

	sort.Slice(recs, func(i, j int) bool {
		return aciKey(recs[i].ResourceGroup, recs[i].Name) < aciKey(recs[j].ResourceGroup, recs[j].Name)
	})

	out := make([]map[string]any, 0, len(recs))

	for _, g := range recs {
		gid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: g.ResourceGroup,
			Namespace: namespaceMicrosoftContainerInstance, Types: []string{aciProfilesType}, Names: []string{g.Name},
		}
		out = append(out, aciProfileBody(gid, g, len(g.Revisions)-1))
	}

	return out, nil
}

func (p *ContainerInstanceProvider) deleteProfile(id ResourceID) error {
	if len(id.Types) != 1 {
		return ErrResourceNotFound
	}

	key := aciKey(id.ResourceGroup, id.LeafName())

	p.mu.Lock("deleteProfile")
	defer p.mu.Unlock()

	if _, ok := p.profiles[key]; !ok {
		return ErrResourceNotFound
	}

	delete(p.profiles, key)

	return nil
}

func (p *ContainerInstanceProvider) patchProfile(id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("patchProfile")
	defer p.mu.Unlock()

	rec, ok := p.profile(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrResourceNotFound
	}

	if _, set := body[fieldTags]; set {
		rec.Tags = stringTags(body[fieldTags])
	}

	return aciProfileBody(id, rec, len(rec.Revisions)-1), nil
}

func isACIProfileScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, aciNsIdx, namespaceMicrosoftContainerInstance) && segAt(segs, aciTypeIdx, aciProfilesType)
}

func (h *Handler) handleACIProfilePatch(c *echo.Context, segs []string) error {
	p, ok := h.containerInstanceProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
		return p.patchProfile(id, body)
	})
}

func isACIProfileRevisions(segs []string, r *http.Request) bool {
	return r.Method == http.MethodGet && isACIProfileScope(segs, aciProfileSegs+1) &&
		strings.EqualFold(segs[aciProfileSegs], aciRevisionsType)
}

func (h *Handler) handleACIProfileRevisions(c *echo.Context, segs []string) error {
	p, ok := h.containerInstanceProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:aciProfileSegs]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	id.Types = append(id.Types, aciRevisionsType)

	return h.writeResult(c, func() (map[string]any, error) {
		list, lerr := p.listProfiles(id)

		return map[string]any{fieldValue: list}, lerr
	})
}

func withoutInstanceView(b map[string]any) map[string]any {
	if props, ok := b[fieldProperties].(map[string]any); ok {
		delete(props, "instanceView")
	}

	return b
}
