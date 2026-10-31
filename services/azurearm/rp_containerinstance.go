package azurearm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftContainerInstance = "Microsoft.ContainerInstance"
	aciGroupsType                       = "containerGroups"
	aciCodeInvalid                      = "InvalidRequestContent"
	aciLoopbackIP                       = "127.0.0.1"
	aciPrefixLen                        = len("azurearm: invalid container group: ")
	aciFQDNSuffix                       = "azurecontainer.io"
	aciKeyServer                        = "server"
)

func aciAPIVersions() []string { return []string{"2023-05-01"} }

type storedACIGroup struct {
	Created       time.Time         `json:"created"`
	Tags          map[string]string `json:"tags,omitempty"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Props         aciPropsWire      `json:"props"`
	Identity      resourceIdentity  `json:"identity"`
}

// ContainerInstanceProvider implements ResourceProvider for Microsoft.ContainerInstance container groups. ARM
// keeps the configuration; the ACIEngine runs the containers.
type ContainerInstanceProvider struct {
	engine *ACIEngine
	mu     *lockmetrics.RWMutex
	groups map[string]*storedACIGroup
	ids    *identityService
}

// NewContainerInstanceProvider creates a provider over engine.
func NewContainerInstanceProvider(engine *ACIEngine) *ContainerInstanceProvider {
	return &ContainerInstanceProvider{
		engine: engine, mu: lockmetrics.New("azurearm.aciprovider"), groups: map[string]*storedACIGroup{},
	}
}

var _ ResourceProvider = (*ContainerInstanceProvider)(nil)

// Namespace implements ResourceProvider.
func (p *ContainerInstanceProvider) Namespace() string { return namespaceMicrosoftContainerInstance }

// ResourceTypes implements ResourceProvider.
func (p *ContainerInstanceProvider) ResourceTypes() []ResourceTypeDef {
	return []ResourceTypeDef{{Type: aciGroupsType, APIVersions: aciAPIVersions(), HasChildren: true}}
}

// Engine returns the execution engine.
func (p *ContainerInstanceProvider) Engine() *ACIEngine { return p.engine }

func aciKey(rg, name string) string { return strings.ToLower(rg) + "/" + strings.ToLower(name) }

func (p *ContainerInstanceProvider) group(rg, name string) (*storedACIGroup, bool) {
	g, ok := p.groups[aciKey(rg, name)]

	return g, ok
}

func aciGroupID(id ResourceID) bool {
	return len(id.Types) == 1 && strings.EqualFold(id.Types[0], aciGroupsType)
}

// ListKeys implements ResourceProvider.
func (p *ContainerInstanceProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Put implements ResourceProvider. It answers once the containers have started or failed to.
func (p *ContainerInstanceProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	if !aciGroupID(id) {
		return nil, ErrResourceNotFound
	}

	name := id.LeafName()
	if len(name) > aciMaxNameLen || !aciNameRe.MatchString(name) {
		return nil, aciBad("container group name %q must be 1-63 letters, digits or hyphens", name)
	}

	if _, set := body["zones"]; set {
		return nil, aciUnsupported("zones")
	}

	props, err := parseACIProps(body[fieldProperties])
	if err != nil {
		return nil, err
	}

	rec := &storedACIGroup{
		Name: name, ResourceGroup: id.ResourceGroup, Location: DefaultLocation, Created: time.Now().UTC(),
		Tags: stringTags(body[fieldTags]), Props: props,
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.Location = loc
	}

	_, hasIdentity := body[fieldIdentity]

	p.mu.Lock("Put")

	var prevIdentity resourceIdentity

	if old, ok := p.group(id.ResourceGroup, name); ok {
		rec.Created, prevIdentity = old.Created, old.Identity
	}

	ident, identErr := p.ids.apply(body[fieldIdentity], hasIdentity, prevIdentity)
	if identErr != nil {
		p.mu.Unlock()

		return nil, identErr
	}

	rec.Identity = ident
	p.groups[aciKey(id.ResourceGroup, name)] = rec
	p.mu.Unlock()

	p.engine.Deploy(p.config(rec))

	return p.bodyOf(id, rec), nil
}

func (p *ContainerInstanceProvider) config(rec *storedACIGroup) aciConfig {
	return aciConfig{Key: aciKey(rec.ResourceGroup, rec.Name), Name: rec.Name, Props: rec.Props}
}

// Get implements ResourceProvider.
func (p *ContainerInstanceProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	if !aciGroupID(id) {
		return nil, ErrResourceNotFound
	}

	p.mu.RLock("Get")
	rec, ok := p.group(id.ResourceGroup, id.LeafName())
	p.mu.RUnlock()

	if !ok {
		return nil, ErrResourceNotFound
	}

	return p.bodyOf(id, rec), nil
}

// Delete implements ResourceProvider.
func (p *ContainerInstanceProvider) Delete(_ context.Context, id ResourceID) error {
	if !aciGroupID(id) {
		return ErrResourceNotFound
	}

	key := aciKey(id.ResourceGroup, id.LeafName())

	p.mu.Lock("Delete")
	_, ok := p.groups[key]
	delete(p.groups, key)
	p.mu.Unlock()

	if !ok {
		return ErrResourceNotFound
	}

	p.engine.Remove(key)

	return nil
}

// List implements ResourceProvider.
func (p *ContainerInstanceProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	recs := make([]*storedACIGroup, 0, len(p.groups))

	for _, g := range p.groups {
		if id.ResourceGroup == "" || resourceGroupsEqual(g.ResourceGroup, id.ResourceGroup) {
			recs = append(recs, g)
		}
	}
	p.mu.RUnlock()

	sort.Slice(recs, func(i, j int) bool {
		return aciKey(recs[i].ResourceGroup, recs[i].Name) < aciKey(recs[j].ResourceGroup, recs[j].Name)
	})

	out := make([]map[string]any, 0, len(recs))

	for _, g := range recs {
		gid := ResourceID{
			SubscriptionID: id.SubscriptionID,
			ResourceGroup:  g.ResourceGroup,
			Namespace:      namespaceMicrosoftContainerInstance,
			Types:          []string{aciGroupsType},
			Names:          []string{g.Name},
		}
		out = append(out, p.bodyOf(gid, g))
	}

	return out, nil
}

// Reset implements ResourceProvider.
func (p *ContainerInstanceProvider) Reset() {
	p.mu.Lock("Reset")
	p.groups = map[string]*storedACIGroup{}
	p.mu.Unlock()

	p.engine.RemoveUnprefixed(acaKeyPrefix)
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *ContainerInstanceProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")

	var keys []string

	for k, g := range p.groups {
		if resourceGroupsEqual(g.ResourceGroup, resourceGroup) {
			keys = append(keys, k)
			delete(p.groups, k)
		}
	}
	p.mu.Unlock()

	for _, k := range keys {
		p.engine.Remove(k)
	}
}

// Close stops every running group.
func (p *ContainerInstanceProvider) Close() { p.engine.Close() }

// Action runs start, stop or restart on a group.
func (p *ContainerInstanceProvider) Action(_ context.Context, id ResourceID, action string) error {
	p.mu.RLock("Action")
	rec, ok := p.group(id.ResourceGroup, id.LeafName())
	p.mu.RUnlock()

	if !ok {
		return ErrResourceNotFound
	}

	cfg := p.config(rec)

	switch action {
	case aciActionStop:
		p.engine.Stop(cfg.Key)
	case aciActionStart:
		if !p.engine.Active(cfg.Key) {
			p.engine.Deploy(cfg)
		}
	case aciActionRestart:
		p.engine.Deploy(cfg)
	}

	return nil
}

// Patch replaces the group's tags.
func (p *ContainerInstanceProvider) Patch(
	_ context.Context,
	id ResourceID,
	body map[string]any,
) (map[string]any, error) {
	p.mu.Lock("Patch")
	rec, ok := p.group(id.ResourceGroup, id.LeafName())

	if ok {
		if _, set := body[fieldTags]; set {
			rec.Tags = stringTags(body[fieldTags])
		}
	}
	p.mu.Unlock()

	if !ok {
		return nil, ErrResourceNotFound
	}

	return p.bodyOf(id, rec), nil
}

// Logs returns a container's logs.
func (p *ContainerInstanceProvider) Logs(
	ctx context.Context,
	id ResourceID,
	container string,
	tail int,
) (string, error) {
	p.mu.RLock("Logs")
	rec, ok := p.group(id.ResourceGroup, id.LeafName())
	p.mu.RUnlock()

	if !ok {
		return "", ErrResourceNotFound
	}

	return p.engine.Logs(ctx, aciKey(rec.ResourceGroup, rec.Name), container, tail)
}

func aciEventsBody(events []ACIEvent) []map[string]any {
	out := make([]map[string]any, 0, len(events))

	for _, ev := range events {
		out = append(out, map[string]any{
			"count":          ev.Count,
			"firstTimestamp": ev.First.Format(time.RFC3339Nano),
			"lastTimestamp": ev.Last.Format(
				time.RFC3339Nano,
			),
			fieldMessage: ev.Message,
			fieldName:    ev.Name,
			fieldType:    ev.Type,
		})
	}

	return out
}

func aciStateBody(s *ACIState) map[string]any {
	if s == nil {
		return nil
	}

	m := map[string]any{sqlPropState: s.State}
	if s.Detail != "" {
		m["detailStatus"] = s.Detail
	}

	if s.StartTime != nil {
		m["startTime"] = s.StartTime.Format(time.RFC3339Nano)
	}

	if s.FinishTime != nil {
		m["finishTime"] = s.FinishTime.Format(time.RFC3339Nano)
	}

	if s.ExitCode != nil {
		m["exitCode"] = *s.ExitCode
	}

	return m
}

func aciContainerBody(c aciContainerWire, view *ACIContainerView) map[string]any {
	pp := c.Properties
	props := map[string]any{"image": pp.Image}

	if len(pp.Command) > 0 {
		props["command"] = pp.Command
	}

	envs := make([]map[string]any, 0, len(pp.EnvironmentVariables))

	for _, e := range pp.EnvironmentVariables {
		m := map[string]any{fieldName: e.Name}
		if e.Value != nil {
			m[fieldValue] = *e.Value
		}

		envs = append(envs, m)
	}

	if len(envs) > 0 {
		props["environmentVariables"] = envs
	}

	props["resources"] = pp.Resources

	if len(pp.Ports) > 0 {
		props["ports"] = pp.Ports
	}

	if len(pp.VolumeMounts) > 0 {
		props["volumeMounts"] = pp.VolumeMounts
	}

	iv := map[string]any{"restartCount": 0}
	if view != nil {
		iv["restartCount"] = view.Restarts
		iv["currentState"] = aciStateBody(&view.Cur)
		iv["events"] = aciEventsBody(view.Events)

		if view.Prev != nil {
			iv["previousState"] = aciStateBody(view.Prev)
		}
	} else {
		iv["currentState"] = map[string]any{sqlPropState: aciStateTerminated, "detailStatus": aciStateStopped}
	}

	props["instanceView"] = iv

	return map[string]any{fieldName: c.Name, fieldProperties: props}
}

func aciVolumesBody(vols []aciVolumeWire) []map[string]any {
	out := make([]map[string]any, 0, len(vols))

	for _, v := range vols {
		m := map[string]any{fieldName: v.Name}

		switch {
		case v.Secret != nil:
			m["secret"] = map[string]string{}
		case v.AzureFile != nil:
			m["azureFile"] = map[string]any{
				"shareName": v.AzureFile.ShareName, "storageAccountName": v.AzureFile.StorageAccountName,
				"readOnly": v.AzureFile.ReadOnly,
			}
		default:
			m["emptyDir"] = map[string]any{}
		}

		out = append(out, m)
	}

	return out
}

func (p *ContainerInstanceProvider) bodyOf(id ResourceID, rec *storedACIGroup) map[string]any {
	view, live := p.engine.View(aciKey(rec.ResourceGroup, rec.Name))
	pp := rec.Props

	conts := make([]map[string]any, 0, len(pp.Containers))

	for i, c := range pp.Containers {
		var cv *ACIContainerView
		if live && i < len(view.Containers) {
			cv = &view.Containers[i]
		}

		conts = append(conts, aciContainerBody(c, cv))
	}

	state, prov := aciStateStopped, provisioningStateSucceeded
	events := []map[string]any{}

	if live {
		state, events = view.State, aciEventsBody(view.Events)

		if view.Failure != "" {
			prov = aciStateFailed
		}
	}

	props := map[string]any{
		fieldProvisioningState: prov, "containers": conts, "osType": pp.OSType, "restartPolicy": pp.RestartPolicy,
		"volumes":      aciVolumesBody(pp.Volumes),
		"instanceView": map[string]any{"events": events, sqlPropState: state},
	}

	if len(pp.Volumes) == 0 {
		delete(props, "volumes")
	}

	if pp.IPAddress != nil {
		props["ipAddress"] = p.ipBody(rec, live && view.State != aciStateStopped && view.State != aciStateFailed)
	}

	if len(pp.ImageRegistryCredentials) > 0 {
		creds := make([]map[string]any, 0, len(pp.ImageRegistryCredentials))
		for _, c := range pp.ImageRegistryCredentials {
			creds = append(creds, map[string]any{aciKeyServer: c.Server, "username": c.Username})
		}

		props["imageRegistryCredentials"] = creds
	}

	out := map[string]any{
		"id": id.ARMID(), fieldName: rec.Name, fieldType: namespaceMicrosoftContainerInstance + "/" + aciGroupsType,
		fieldLocation: rec.Location, fieldTags: tagsOrEmpty(rec.Tags), fieldProperties: props,
	}

	if ident := p.ids.body(rec.Identity); ident != nil {
		out[fieldIdentity] = ident
	}

	return out
}

func (p *ContainerInstanceProvider) ipBody(rec *storedACIGroup, allocated bool) map[string]any {
	ip := rec.Props.IPAddress
	m := map[string]any{fieldType: ip.Type, "ports": ip.Ports}

	if allocated {
		m["ip"] = aciLoopbackIP
	}

	if ip.DNSNameLabel != "" {
		m["dnsNameLabel"] = ip.DNSNameLabel
		m["fqdn"] = fmt.Sprintf("%s.%s.%s", ip.DNSNameLabel, rec.Location, aciFQDNSuffix)
	}

	return m
}

func aciErrorDetails(err error) (errorEntry, bool) {
	switch {
	case errors.Is(err, errACIInvalid):
		msg := err.Error()
		if len(msg) > aciPrefixLen && strings.HasPrefix(msg, "azurearm: invalid container group: ") {
			msg = msg[aciPrefixLen:]
		}

		return errorEntry{aciCodeInvalid, msg, http.StatusBadRequest}, true
	case errors.Is(err, ErrACIContainerNotFound):
		return errorEntry{"ContainerNotFound", "The container was not found in the group.", http.StatusNotFound}, true
	case errors.Is(err, ErrACINotRunning):
		return errorEntry{"ContainerGroupNotRunning", err.Error(), http.StatusBadRequest}, true
	case errors.Is(err, ErrACIGroupNotFound):
		return notFoundEntry("The container group was not found."), true
	}

	return errorEntry{}, false
}

func aciTailOf(raw string) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}

	return n
}
