package gcpresourcemanager

import (
	"cmp"
	"maps"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	permProjectGet    = "resourcemanager.projects.get"
	permProjectList   = "resourcemanager.projects.list"
	permProjectCreate = "resourcemanager.projects.create"
	stateActive       = "ACTIVE"
	stateRequested    = "DELETE_REQUESTED"
)

// findProjectLocked resolves "projects/{number}", "projects/{id}" or a bare number or id.
func (b *InMemoryBackend) findProjectLocked(ref string) (*projectRec, bool) {
	ref = strings.TrimPrefix(ref, kindProjects+"/")
	if p, ok := b.projects[ref]; ok {
		return p, true
	}

	for _, p := range b.projects {
		if p.ID == ref {
			return p, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) projectLocked(ref, perm string) (*projectRec, error) {
	p, ok := b.findProjectLocked(ref)
	if !ok {
		return nil, errDenied(perm, kindProjects+"/"+strings.TrimPrefix(ref, kindProjects+"/"))
	}

	return p, nil
}

func (b *InMemoryBackend) activeProjectLocked(ref, perm string) (*projectRec, error) {
	p, err := b.projectLocked(ref, perm)
	if err != nil {
		return nil, err
	}

	if p.requested() {
		return nil, errPrecondition("Project %s is being deleted (DELETE_REQUESTED).", p.ID)
	}

	return p, nil
}

// parentLocked checks that parent is the organization or an active folder; "" means no parent.
func (b *InMemoryBackend) parentLocked(parent, perm string) error {
	switch {
	case parent == "":
		return nil
	case strings.HasPrefix(parent, kindOrganizations+"/"):
		if parent != kindOrganizations+"/"+b.org.ID {
			return errDenied(perm, parent)
		}

		return nil
	case strings.HasPrefix(parent, kindFolders+"/"):
		f, ok := b.folders[strings.TrimPrefix(parent, kindFolders+"/")]
		if !ok {
			return errDenied(perm, parent)
		}

		if f.requested() {
			return errPrecondition("Parent %s is in the DELETE_REQUESTED state.", parent)
		}

		return nil
	default:
		return errInvalid("Invalid parent %q: use organizations/ID or folders/ID.", parent)
	}
}

func (b *InMemoryBackend) createProjectLocked(id, display, parent string, labels map[string]string) *projectRec {
	now := b.now()
	number := uniqueDigits(numberDigits, func(n string) bool { _, ok := b.projects[n]; return ok })
	p := &projectRec{
		Number: number, ID: id, DisplayName: display, Parent: parent, Labels: maps.Clone(labels),
		lifecycle: lifecycle{Created: now, Updated: now},
	}
	b.projects[number] = p
	b.enableDefaultsLocked(number)

	return p
}

// CreateProject creates a project; the returned operation is already done.
func (b *InMemoryBackend) CreateProject(in *resourcemanagerpb.CreateProjectRequest) (*longrunningpb.Operation, error) {
	pr := in.GetProject()
	if pr == nil {
		return nil, errInvalid("project must be specified.")
	}

	if err := validateProjectID(pr.GetProjectId()); err != nil {
		return nil, err
	}

	if err := validateProjectName(pr.GetDisplayName()); err != nil {
		return nil, err
	}

	if err := validateLabels(pr.GetLabels()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	b.purgeLocked()

	if _, ok := b.findProjectLocked(pr.GetProjectId()); ok {
		return nil, errExists("Requested entity already exists: project %s.", pr.GetProjectId())
	}

	if err := b.parentLocked(pr.GetParent(), permProjectCreate); err != nil {
		return nil, err
	}

	display := pr.GetDisplayName()
	if display == "" {
		display = pr.GetProjectId()
	}

	p := b.createProjectLocked(pr.GetProjectId(), display, pr.GetParent(), pr.GetLabels())

	return b.opLocked("cp", &resourcemanagerpb.CreateProjectMetadata{
		CreateTime: ts(p.Created), Gettable: true, Ready: true,
	}, p.view())
}

// GetProject returns a project by number or id, including one that is DELETE_REQUESTED.
func (b *InMemoryBackend) GetProject(name string) (*resourcemanagerpb.Project, error) {
	b.mu.Lock("GetProject")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.projectLocked(name, permProjectGet)
	if err != nil {
		return nil, err
	}

	return p.view(), nil
}

// ListProjects lists the direct children of a folder or organization, ordered by display name.
func (b *InMemoryBackend) ListProjects(in *resourcemanagerpb.ListProjectsRequest) (
	*resourcemanagerpb.ListProjectsResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if in.GetParent() == "" {
		return nil, errInvalid("parent must be specified.")
	}

	b.mu.Lock("ListProjects")
	b.purgeLocked()

	if err := b.parentLocked(in.GetParent(), permProjectList); err != nil {
		b.mu.Unlock()

		return nil, err
	}

	var all []*projectRec

	for _, p := range b.projects {
		if p.Parent == in.GetParent() && (in.GetShowDeleted() || !p.requested()) {
			all = append(all, p)
		}
	}

	out := viewsOf(sortProjects(all))
	b.mu.Unlock()

	pg := page.New(out, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &resourcemanagerpb.ListProjectsResponse{Projects: pg.Data, NextPageToken: pg.Next}, nil
}

func sortProjects(ps []*projectRec) []*projectRec {
	slices.SortFunc(ps, func(a, c *projectRec) int {
		return cmp.Or(cmp.Compare(a.DisplayName, c.DisplayName), cmp.Compare(a.ID, c.ID))
	})

	return ps
}

func viewsOf(ps []*projectRec) []*resourcemanagerpb.Project {
	out := make([]*resourcemanagerpb.Project, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.view())
	}

	return out
}

func (p *projectRec) fields(f string) ([]string, bool) {
	switch {
	case f == "displayname" || f == "name":
		return []string{p.DisplayName}, true
	case f == "id" || f == "projectid":
		return []string{p.ID}, true
	case f == "state":
		if p.requested() {
			return []string{stateRequested}, true
		}

		return []string{stateActive}, true
	case f == "parent":
		return []string{p.Parent}, true
	case f == "parent.type" || f == "parent.id":
		kind, id, _ := strings.Cut(p.Parent, "/")
		if f == "parent.id" {
			return []string{id}, true
		}

		return []string{strings.TrimSuffix(kind, "s")}, true
	case f == "labels":
		vals := make([]string, 0, 2*len(p.Labels))
		for k, v := range p.Labels {
			vals = append(vals, k, v)
		}

		return vals, true
	case strings.HasPrefix(f, "labels."):
		if v, ok := p.Labels[strings.TrimPrefix(f, "labels.")]; ok {
			return []string{v}, true
		}

		return nil, true
	default:
		return nil, false
	}
}

// SearchProjects returns the projects matching any term of the query; ACTIVE ones unless the query names a state.
func (b *InMemoryBackend) SearchProjects(in *resourcemanagerpb.SearchProjectsRequest) (
	*resourcemanagerpb.SearchProjectsResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	q, err := parseQuery(in.GetQuery(), false)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("SearchProjects")
	b.purgeLocked()

	var hits []*projectRec

	for _, p := range b.projects {
		ok, mErr := q.match(p.fields)
		if mErr != nil {
			b.mu.Unlock()

			return nil, mErr
		}

		if ok && (q.hasState || !p.requested()) {
			hits = append(hits, p)
		}
	}

	out := viewsOf(sortProjects(hits))
	b.mu.Unlock()

	pg := page.New(out, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &resourcemanagerpb.SearchProjectsResponse{Projects: pg.Data, NextPageToken: pg.Next}, nil
}

// maskHas reports whether mask updates field; a nil or empty mask updates every mutable field.
func maskHas(mask *fieldmaskpb.FieldMask, field string) bool {
	if len(mask.GetPaths()) == 0 {
		return true
	}

	return slices.ContainsFunc(mask.GetPaths(), func(p string) bool {
		return p == "*" || p == field || strings.HasPrefix(p, field+".")
	})
}

func checkMask(mask *fieldmaskpb.FieldMask, allowed ...string) error {
	for _, p := range mask.GetPaths() {
		if p != "*" && !slices.ContainsFunc(allowed, func(a string) bool { return p == a || strings.HasPrefix(p, a+".") }) {
			return errInvalid("update_mask path %q cannot be updated; allowed: %s.", p, strings.Join(allowed, ", "))
		}
	}

	return nil
}

// UpdateProject changes the display name and labels of an ACTIVE project.
func (b *InMemoryBackend) UpdateProject(in *resourcemanagerpb.UpdateProjectRequest) (*longrunningpb.Operation, error) {
	pr := in.GetProject()
	if pr == nil || pr.GetName() == "" {
		return nil, errInvalid("project.name must be specified.")
	}

	if err := checkMask(in.GetUpdateMask(), "display_name", "labels"); err != nil {
		return nil, err
	}

	if err := validateProjectName(pr.GetDisplayName()); err != nil {
		return nil, err
	}

	if err := validateLabels(pr.GetLabels()); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.activeProjectLocked(pr.GetName(), "resourcemanager.projects.update")
	if err != nil {
		return nil, err
	}

	if et := pr.GetEtag(); et != "" && et != p.view().GetEtag() {
		return nil, errAborted("The project %s was modified since it was read; re-read it and retry (etag mismatch).", p.ID)
	}

	if maskHas(in.GetUpdateMask(), "display_name") && pr.GetDisplayName() != "" {
		p.DisplayName = pr.GetDisplayName()
	}

	if maskHas(in.GetUpdateMask(), "labels") {
		p.Labels = maps.Clone(pr.GetLabels())
	}

	p.Updated = b.now()

	return b.opLocked("up", &resourcemanagerpb.UpdateProjectMetadata{}, p.view())
}

// MoveProject re-parents an ACTIVE project under the organization or an active folder.
func (b *InMemoryBackend) MoveProject(in *resourcemanagerpb.MoveProjectRequest) (*longrunningpb.Operation, error) {
	if in.GetDestinationParent() == "" {
		return nil, errInvalid("destination_parent must be specified.")
	}

	b.mu.Lock("MoveProject")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.activeProjectLocked(in.GetName(), "resourcemanager.projects.move")
	if err != nil {
		return nil, err
	}

	if err = b.parentLocked(in.GetDestinationParent(), "resourcemanager.projects.move"); err != nil {
		return nil, err
	}

	p.Parent = in.GetDestinationParent()
	p.Updated = b.now()

	return b.opLocked("mp", &resourcemanagerpb.MoveProjectMetadata{}, p.view())
}

// DeleteProject marks an ACTIVE project DELETE_REQUESTED; deleting a requested project is a no-op.
func (b *InMemoryBackend) DeleteProject(in *resourcemanagerpb.DeleteProjectRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.projectLocked(in.GetName(), "resourcemanager.projects.delete")
	if err != nil {
		return nil, err
	}

	if !p.requested() {
		now := b.now()
		p.Deleted, p.Updated = now, now
	}

	view := p.view()

	if b.retention <= 0 {
		b.dropProjectLocked(p.Number)
	}

	return b.opLocked("dp", &resourcemanagerpb.DeleteProjectMetadata{}, view)
}

// UndeleteProject restores a DELETE_REQUESTED project.
func (b *InMemoryBackend) UndeleteProject(in *resourcemanagerpb.UndeleteProjectRequest) (
	*longrunningpb.Operation, error,
) {
	b.mu.Lock("UndeleteProject")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.projectLocked(in.GetName(), "resourcemanager.projects.undelete")
	if err != nil {
		return nil, err
	}

	if !p.requested() {
		return nil, errPrecondition("Project %s is not in the DELETE_REQUESTED state.", p.ID)
	}

	if err = b.restorableParentLocked(p.Parent); err != nil {
		return nil, err
	}

	p.Deleted = time.Time{}
	p.Updated = b.now()

	return b.opLocked("ud", &resourcemanagerpb.UndeleteProjectMetadata{}, p.view())
}
