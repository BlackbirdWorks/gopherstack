package gcpmonitoring

import (
	"sort"
	"strings"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxDisplayName = 512
	pathResMeta    = "resource.metadata."
	maxGroupDepth  = 20
)

func cloneGroup(g *monitoringpb.Group) *monitoringpb.Group {
	out, _ := proto.Clone(g).(*monitoringpb.Group)

	return out
}

// resourceEnv resolves group filter fields against a monitored resource and its metadata.
type resourceEnv struct {
	res *monitoredres.MonitoredResource
	md  *monitoredres.MonitoredResourceMetadata
}

func (e resourceEnv) values(path string) []string {
	switch {
	case path == pathResourceTyp:
		return []string{e.res.GetType()}
	case strings.HasPrefix(path, prefixResLbl):
		return one(e.res.GetLabels(), strings.TrimPrefix(path, prefixResLbl))
	case strings.HasPrefix(path, pathResMeta+"system_labels."):
		return systemLabel(e.md, strings.TrimPrefix(path, pathResMeta+"system_labels."))
	case strings.HasPrefix(path, pathResMeta+"user_labels."):
		return one(e.md.GetUserLabels(), strings.TrimPrefix(path, pathResMeta+"user_labels."))
	case strings.HasPrefix(path, pathResMeta+"tag."):
		return one(e.md.GetUserLabels(), strings.TrimPrefix(path, pathResMeta+"tag."))
	case path == pathResMeta+"name":
		return systemLabel(e.md, "name")
	}

	return nil
}

func validateGroupFilter(src string) error {
	f, err := parseFilter(src)
	if err != nil {
		return err
	}

	for field := range f.referenced() {
		if field != pathResourceTyp && !strings.HasPrefix(field, "resource.") {
			return errInvalid("Field group.filter had an invalid value of %q: "+
				"group filters can only reference resource.* fields.", field)
		}
	}

	return nil
}

func groupID(name string) string { return name[strings.LastIndex(name, "/")+1:] }

// groupResolverLocked returns a function listing the ids of the project's groups a series' resource belongs to.
func (b *InMemoryBackend) groupResolverLocked(project string) func(*seriesRec) []string {
	type compiled struct {
		f  *filter
		id string
	}

	var gs []compiled

	prefix := projectScope + project + "/" + collGroups + "/"

	for name, g := range b.groups {
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		if f, err := parseFilter(g.GetFilter()); err == nil {
			gs = append(gs, compiled{f: f, id: groupID(name)})
		}
	}

	return func(r *seriesRec) []string {
		var ids []string

		for _, g := range gs {
			if g.f.matches(resourceEnv{res: r.resource, md: r.metadata}) {
				ids = append(ids, g.id)
			}
		}

		return ids
	}
}

func (b *InMemoryBackend) checkParentLocked(project, name, parent string) error {
	if parent == "" {
		return nil
	}

	p, ok := b.groups[parent]
	if !ok || !strings.HasPrefix(parent, projectScope+project+"/") {
		return errNotFound("Parent group %s does not exist.", parent)
	}

	for depth := 0; p != nil; depth++ {
		if p.GetName() == name {
			return errInvalid("Field group.parent_name had an invalid value of %q: it would create a cycle.", parent)
		}

		if depth > maxGroupDepth {
			return errInvalid("Group hierarchy is too deep.")
		}

		p = b.groups[p.GetParentName()]
	}

	return nil
}

func validateGroup(g *monitoringpb.Group) error {
	if g == nil {
		return errInvalid("group is required.")
	}

	if g.GetDisplayName() == "" || len(g.GetDisplayName()) > maxDisplayName {
		return errInvalid("Field group.display_name had an invalid value: a display name of 1-%d characters "+
			"is required.", maxDisplayName)
	}

	if g.GetFilter() == "" {
		return errInvalid("Field group.filter had an invalid value: a filter is required.")
	}

	return validateGroupFilter(g.GetFilter())
}

// CreateGroup stores a group.
func (b *InMemoryBackend) CreateGroup(req *monitoringpb.CreateGroupRequest) (*monitoringpb.Group, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, err
	}

	if err = validateGroup(req.GetGroup()); err != nil {
		return nil, err
	}

	g := cloneGroup(req.GetGroup())
	g.Name = projectScope + project + "/" + collGroups + "/" + newNumericID()

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if err = b.checkParentLocked(project, g.GetName(), g.GetParentName()); err != nil {
		return nil, err
	}

	if req.GetValidateOnly() {
		return g, nil
	}

	b.groups[g.GetName()] = g

	return cloneGroup(g), nil
}

// GetGroup returns one group.
func (b *InMemoryBackend) GetGroup(name string) (*monitoringpb.Group, error) {
	if _, _, err := splitName(name, collGroups); err != nil {
		return nil, err
	}

	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, ok := b.groups[name]
	if !ok {
		return nil, errNotFound("Group %s does not exist.", name)
	}

	return cloneGroup(g), nil
}

func (b *InMemoryBackend) descendantsLocked(name string) []string {
	var out []string

	frontier := []string{name}

	for len(frontier) > 0 && len(out) < len(b.groups) {
		cur := frontier[0]
		frontier = frontier[1:]

		for n, g := range b.groups {
			if g.GetParentName() == cur {
				out = append(out, n)
				frontier = append(frontier, n)
			}
		}
	}

	return out
}

func (b *InMemoryBackend) ancestorsLocked(name string) []string {
	var out []string

	g := b.groups[name]
	for depth := 0; g != nil && g.GetParentName() != "" && depth <= maxGroupDepth; depth++ {
		out = append(out, g.GetParentName())
		g = b.groups[g.GetParentName()]
	}

	return out
}

func (b *InMemoryBackend) listGroupNamesLocked(req *monitoringpb.ListGroupsRequest, prefix string) ([]string, error) {
	var rel string

	switch f := req.GetFilter().(type) {
	case *monitoringpb.ListGroupsRequest_ChildrenOfGroup:
		rel = f.ChildrenOfGroup
	case *monitoringpb.ListGroupsRequest_AncestorsOfGroup:
		rel = f.AncestorsOfGroup
	case *monitoringpb.ListGroupsRequest_DescendantsOfGroup:
		rel = f.DescendantsOfGroup
	}

	if rel != "" {
		if _, ok := b.groups[rel]; !ok {
			return nil, errNotFound("Group %s does not exist.", rel)
		}
	}

	switch req.GetFilter().(type) {
	case *monitoringpb.ListGroupsRequest_AncestorsOfGroup:
		return b.ancestorsLocked(rel), nil
	case *monitoringpb.ListGroupsRequest_DescendantsOfGroup:
		return b.descendantsLocked(rel), nil
	}

	var names []string

	for n, g := range b.groups {
		if !strings.HasPrefix(n, prefix) {
			continue
		}

		if _, isChildren := req.GetFilter().(*monitoringpb.ListGroupsRequest_ChildrenOfGroup); isChildren &&
			g.GetParentName() != rel {
			continue
		}

		names = append(names, n)
	}

	return names, nil
}

// ListGroups pages the project's groups, optionally restricted to a group's children, ancestors or descendants.
func (b *InMemoryBackend) ListGroups(req *monitoringpb.ListGroupsRequest) ([]*monitoringpb.Group, string, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListGroups")
	names, err := b.listGroupNamesLocked(req, projectScope+project+"/")

	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	sort.Strings(names)

	all := make([]*monitoringpb.Group, 0, len(names))
	for _, n := range names {
		all = append(all, cloneGroup(b.groups[n]))
	}

	b.mu.RUnlock()

	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, nil
}

// UpdateGroup replaces a group's mutable fields.
func (b *InMemoryBackend) UpdateGroup(req *monitoringpb.UpdateGroupRequest) (*monitoringpb.Group, error) {
	in := req.GetGroup()
	if err := validateGroup(in); err != nil {
		return nil, err
	}

	project, _, err := splitName(in.GetName(), collGroups)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	if _, ok := b.groups[in.GetName()]; !ok {
		return nil, errNotFound("Group %s does not exist.", in.GetName())
	}

	if err = b.checkParentLocked(project, in.GetName(), in.GetParentName()); err != nil {
		return nil, err
	}

	g := cloneGroup(in)
	if !req.GetValidateOnly() {
		b.groups[g.GetName()] = g
	}

	return cloneGroup(g), nil
}

// DeleteGroup removes a group, and with recursive its descendants.
func (b *InMemoryBackend) DeleteGroup(req *monitoringpb.DeleteGroupRequest) error {
	if _, _, err := splitName(req.GetName(), collGroups); err != nil {
		return err
	}

	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, ok := b.groups[req.GetName()]; !ok {
		return errNotFound("Group %s does not exist.", req.GetName())
	}

	kids := b.descendantsLocked(req.GetName())
	if len(kids) > 0 && !req.GetRecursive() {
		return errPrecondition("Group %s has subgroups; set recursive to delete them as well.", req.GetName())
	}

	for _, k := range kids {
		delete(b.groups, k)
	}

	delete(b.groups, req.GetName())

	return nil
}

type resourceInfo struct {
	res *monitoredres.MonitoredResource
	md  *monitoredres.MonitoredResourceMetadata
}

// ListGroupMembers returns the distinct monitored resources that match the group's filter.
func (b *InMemoryBackend) ListGroupMembers(
	req *monitoringpb.ListGroupMembersRequest,
) ([]*monitoredres.MonitoredResource, string, int32, error) {
	project, _, err := splitName(req.GetName(), collGroups)
	if err != nil {
		return nil, "", 0, err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", 0, err
	}

	extra, err := parseFilter(req.GetFilter())
	if err != nil {
		return nil, "", 0, err
	}

	b.mu.RLock("ListGroupMembers")
	g, ok := b.groups[req.GetName()]

	if !ok {
		b.mu.RUnlock()

		return nil, "", 0, errNotFound("Group %s does not exist.", req.GetName())
	}

	members := b.matchingResourcesLocked(project, g, extra)
	b.mu.RUnlock()

	pg := page.New(members, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, int32(len(members)), nil //nolint:gosec // bounded by maxSeriesTotal
}

func (b *InMemoryBackend) matchingResourcesLocked(
	project string, g *monitoringpb.Group, extra *filter,
) []*monitoredres.MonitoredResource {
	gf, err := parseFilter(g.GetFilter())
	if err != nil {
		return nil
	}

	seen := map[string]resourceInfo{}

	for _, s := range b.series {
		if s.project != project {
			continue
		}

		e := resourceEnv{res: s.resource, md: s.metadata}
		if gf.matches(e) && extra.matches(e) {
			seen[seriesKey(project, nil, s.resource)] = resourceInfo{res: s.resource, md: s.metadata}
		}
	}

	keys := sortedKeys(seen)
	out := make([]*monitoredres.MonitoredResource, 0, len(keys))

	for _, k := range keys {
		r, _ := proto.Clone(seen[k].res).(*monitoredres.MonitoredResource)
		out = append(out, r)
	}

	return out
}
