package gcpiam

import (
	"bytes"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxRolePermissions = 3000

func cloneRole(r *adminpb.Role) *adminpb.Role {
	out, _ := proto.Clone(r).(*adminpb.Role)

	return out
}

func viewRole(r *adminpb.Role, view adminpb.RoleView) *adminpb.Role {
	out := cloneRole(r)
	if view != adminpb.RoleView_FULL {
		out.IncludedPermissions = nil
	}

	return out
}

func (r *roleRec) active() bool { return r.deletedAt.IsZero() }

func normalizePermissions(in []string) ([]string, error) {
	if len(in) > maxRolePermissions {
		return nil, errInvalid("A role may include at most %d permissions.", maxRolePermissions)
	}

	out := slices.Clone(in)
	slices.Sort(out)
	out = slices.Compact(out)

	for _, p := range out {
		if !permRe.MatchString(p) {
			return nil, errInvalid("Permission %q is invalid; expected service.resource.verb.", p)
		}
	}

	return out, nil
}

func validateRoleText(r *adminpb.Role) error {
	if len(r.GetTitle()) > maxDisplayName*2 || !utf8.ValidString(r.GetTitle()) {
		return errInvalid("title is too long.")
	}

	if !utf8.ValidString(r.GetDescription()) {
		return errInvalid("description must be valid UTF-8.")
	}

	return nil
}

func (b *InMemoryBackend) purgeRolesLocked() {
	cutoff := b.now().Add(-rolePurgeAfter)

	for n, r := range b.roles {
		if !r.active() && r.deletedAt.Before(cutoff) {
			delete(b.roles, n)
		}
	}
}

// CreateRole creates a custom role at project or organization level.
func (b *InMemoryBackend) CreateRole(req *adminpb.CreateRoleRequest) (*adminpb.Role, error) {
	if err := parseRoleParent(req.GetParent()); err != nil {
		return nil, err
	}

	id := req.GetRoleId()
	if len(id) < roleIDMin || len(id) > roleIDMax || !roleIDRe.MatchString(id) {
		return nil, errInvalid("Role ID %q is invalid: it must be %d-%d characters of letters, digits, '_' or '.'.",
			id, roleIDMin, roleIDMax)
	}

	in := req.GetRole()
	if in == nil {
		return nil, errInvalid("role must be specified.")
	}

	if err := validateRoleText(in); err != nil {
		return nil, err
	}

	permsList, err := normalizePermissions(in.GetIncludedPermissions())
	if err != nil {
		return nil, err
	}

	name := req.GetParent() + "/roles/" + id

	b.mu.Lock("CreateRole")
	defer b.mu.Unlock()

	b.purgeRolesLocked()

	if _, ok := b.roles[name]; ok {
		return nil, errExists("The role %s already exists.", name)
	}

	rec := &roleRec{pb: &adminpb.Role{
		Name:                name,
		Title:               in.GetTitle(),
		Description:         in.GetDescription(),
		IncludedPermissions: permsList,
		Stage:               in.GetStage(),
		Etag:                b.nextEtag(),
	}}
	b.roles[name] = rec

	return cloneRole(rec.pb), nil
}

// GetRole returns a predefined or custom role, including its permissions.
func (b *InMemoryBackend) GetRole(name string) (*adminpb.Role, error) {
	if strings.HasPrefix(name, "roles/") {
		d, ok := predefinedByName(name)
		if !ok {
			return nil, errNotFound("The role %s was not found.", name)
		}

		return d.role(true), nil
	}

	if err := validateCustomRoleName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetRole")
	defer b.mu.RUnlock()

	rec, ok := b.roles[name]
	if !ok {
		return nil, errNotFound("The role %s was not found.", name)
	}

	return cloneRole(rec.pb), nil
}

// ListRoles lists predefined roles (empty parent) or the custom roles of a project/organization.
func (b *InMemoryBackend) ListRoles(req *adminpb.ListRolesRequest) ([]*adminpb.Role, string, error) {
	size, err := pageSizeOf(req.GetPageSize(), maxBigPage)
	if err != nil {
		return nil, "", err
	}

	if err = page.ValidateToken(req.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	var all []*adminpb.Role

	if req.GetParent() == "" {
		for _, d := range catalogDefs() {
			all = append(all, viewRole(d.role(true), req.GetView()))
		}
	} else {
		if err = parseRoleParent(req.GetParent()); err != nil {
			return nil, "", err
		}

		all = b.customRoles(req.GetParent(), req.GetShowDeleted(), req.GetView())
	}

	slices.SortFunc(all, func(x, y *adminpb.Role) int { return strings.Compare(x.GetName(), y.GetName()) })

	pg := page.New(all, req.GetPageToken(), size, defaultRolePage)

	return pg.Data, pg.Next, nil
}

func (b *InMemoryBackend) customRoles(parent string, showDeleted bool, view adminpb.RoleView) []*adminpb.Role {
	b.mu.RLock("customRoles")
	defer b.mu.RUnlock()

	var out []*adminpb.Role

	for n, r := range b.roles {
		if strings.HasPrefix(n, parent+"/roles/") && (showDeleted || r.active()) {
			out = append(out, viewRole(r.pb, view))
		}
	}

	return out
}

type roleFields struct{ title, description, permissions, stage bool }

func roleMask(m *fieldmaskpb.FieldMask) (roleFields, error) {
	if len(m.GetPaths()) == 0 {
		return roleFields{title: true, description: true, permissions: true, stage: true}, nil
	}

	var f roleFields

	for _, p := range m.GetPaths() {
		switch strings.TrimPrefix(p, "role.") {
		case "title":
			f.title = true
		case "description":
			f.description = true
		case "included_permissions", "includedPermissions":
			f.permissions = true
		case "stage":
			f.stage = true
		default:
			return roleFields{}, errInvalid("update_mask path %q is not updatable.", p)
		}
	}

	return f, nil
}

func etagMatches(given, current []byte) bool { return len(given) == 0 || bytes.Equal(given, current) }

// UpdateRole patches the masked fields of a custom role; a non-empty etag must match.
func (b *InMemoryBackend) UpdateRole(req *adminpb.UpdateRoleRequest) (*adminpb.Role, error) {
	if err := validateCustomRoleName(req.GetName()); err != nil {
		return nil, err
	}

	in := req.GetRole()
	if in == nil {
		return nil, errInvalid("role must be specified.")
	}

	fields, err := roleMask(req.GetUpdateMask())
	if err != nil {
		return nil, err
	}

	if err = validateRoleText(in); err != nil {
		return nil, err
	}

	var newPerms []string
	if fields.permissions {
		if newPerms, err = normalizePermissions(in.GetIncludedPermissions()); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateRole")
	defer b.mu.Unlock()

	rec, ok := b.roles[req.GetName()]
	if !ok {
		return nil, errNotFound("The role %s was not found.", req.GetName())
	}

	if !rec.active() {
		return nil, errPrecondition("The role %s is deleted and cannot be updated.", req.GetName())
	}

	if !etagMatches(in.GetEtag(), rec.pb.GetEtag()) {
		return nil, errAborted("There were concurrent role changes. Please retry the whole read-modify-write.")
	}

	if fields.title {
		rec.pb.Title = in.GetTitle()
	}

	if fields.description {
		rec.pb.Description = in.GetDescription()
	}

	if fields.permissions {
		rec.pb.IncludedPermissions = newPerms
	}

	if fields.stage {
		rec.pb.Stage = in.GetStage()
	}

	rec.pb.Etag = b.nextEtag()

	return cloneRole(rec.pb), nil
}

// DeleteRole soft-deletes a custom role; it can be undeleted for seven days.
func (b *InMemoryBackend) DeleteRole(req *adminpb.DeleteRoleRequest) (*adminpb.Role, error) {
	if err := validateCustomRoleName(req.GetName()); err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteRole")
	defer b.mu.Unlock()

	rec, ok := b.roles[req.GetName()]
	if !ok {
		return nil, errNotFound("The role %s was not found.", req.GetName())
	}

	if !rec.active() {
		return nil, errPrecondition("The role %s is already deleted.", req.GetName())
	}

	if !etagMatches(req.GetEtag(), rec.pb.GetEtag()) {
		return nil, errAborted("There were concurrent role changes. Please retry the whole read-modify-write.")
	}

	rec.deletedAt = b.now()
	rec.pb.Deleted = true
	rec.pb.Etag = b.nextEtag()

	return cloneRole(rec.pb), nil
}

// UndeleteRole restores a custom role deleted less than seven days ago.
func (b *InMemoryBackend) UndeleteRole(req *adminpb.UndeleteRoleRequest) (*adminpb.Role, error) {
	if err := validateCustomRoleName(req.GetName()); err != nil {
		return nil, err
	}

	b.mu.Lock("UndeleteRole")
	defer b.mu.Unlock()

	rec, ok := b.roles[req.GetName()]
	if !ok {
		return nil, errNotFound("The role %s was not found.", req.GetName())
	}

	if rec.active() {
		return nil, errPrecondition("The role %s is not deleted.", req.GetName())
	}

	if b.now().Sub(rec.deletedAt) > roleUndeleteTTL {
		return nil, errPrecondition(
			"The role %s was deleted more than 7 days ago and can no longer be undeleted.",
			req.GetName(),
		)
	}

	if !etagMatches(req.GetEtag(), rec.pb.GetEtag()) {
		return nil, errAborted("There were concurrent role changes. Please retry the whole read-modify-write.")
	}

	rec.deletedAt = time.Time{}
	rec.pb.Deleted = false
	rec.pb.Etag = b.nextEtag()

	return cloneRole(rec.pb), nil
}
