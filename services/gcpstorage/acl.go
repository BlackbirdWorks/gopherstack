package gcpstorage

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// ACLEntry is one access-control entry; the derived fields (email, domain, projectTeam) are rendered from Entity.
type ACLEntry struct {
	Entity string `json:"entity"`
	Role   string `json:"role"`
}

type aclKind int

const (
	aclBucket aclKind = iota + 1
	aclDefaultObject
	aclObject
)

const (
	roleOwner  = "OWNER"
	roleReader = "READER"
	roleWriter = "WRITER"

	entityAllUsers = "allUsers"
	entityAllAuth  = "allAuthenticatedUsers"

	maxACLEntries    = 100
	aclEtag          = "CAE="
	keyPredefined    = "predefinedAcl"
	keyPredefDef     = "predefinedDefaultObjectAcl"
	keyACL           = "acl"
	keyDefaultACL    = "defaultObjectAcl"
	projectNumWidth  = 12
	projectNumberMod = 1_000_000_000_000
)

const ublaMessage = "Cannot use ACL API to update bucket policy when uniform bucket-level access is enabled. " +
	"Read more at https://cloud.google.com/storage/docs/uniform-bucket-level-access"

func errUBLA() *APIError { return newErr(http.StatusBadRequest, "invalid", ublaMessage) }

// projectNumber derives a stable 12-digit project number from a project id.
func projectNumber(project string) string {
	var h uint64 = 14695981039346656037

	for i := range len(project) {
		h ^= uint64(project[i])
		h *= 1099511628211
	}

	n := itoa64(int64(h % projectNumberMod))

	return strings.Repeat("0", projectNumWidth-len(n)) + n
}

func teamEntity(project, team string) string { return "project-" + team + "-" + projectNumber(project) }

// emulatorOwner is the identity every upload is attributed to: the emulator does not authenticate callers.
func emulatorOwner(project string) string {
	return "user-emulator@" + project + ".iam.gserviceaccount.com"
}

// projectPrivateACL is the default bucket and default-object ACL of a new bucket.
func projectPrivateACL(project string) []ACLEntry {
	return []ACLEntry{
		{teamEntity(project, "owners"), roleOwner},
		{teamEntity(project, "editors"), roleOwner},
		{teamEntity(project, "viewers"), roleReader},
	}
}

func validPredefined(name string, kind aclKind) bool {
	switch name {
	case "private", "projectPrivate", "authenticatedRead", "publicRead":
		return true
	case "publicReadWrite":
		return kind == aclBucket
	case "bucketOwnerRead", "bucketOwnerFullControl":
		return kind != aclBucket
	default:
		return false
	}
}

// predefinedACL expands a canned ACL (docs.cloud.google.com/storage/docs/access-control/lists#predefined-acl).
func predefinedACL(name string, kind aclKind, project string) ([]ACLEntry, *APIError) {
	if !validPredefined(name, kind) {
		return nil, errInvalid("Invalid value for predefined ACL: '" + name + "'.")
	}

	owners := teamEntity(project, "owners")
	owner := ACLEntry{emulatorOwner(project), roleOwner}

	if kind == aclBucket {
		owner = ACLEntry{owners, roleOwner}
	}

	switch name {
	case "projectPrivate":
		return projectPrivateACL(project), nil
	case "authenticatedRead":
		return []ACLEntry{owner, {entityAllAuth, roleReader}}, nil
	case "publicRead":
		return []ACLEntry{owner, {entityAllUsers, roleReader}}, nil
	case "publicReadWrite":
		return []ACLEntry{owner, {entityAllUsers, roleWriter}}, nil
	case "bucketOwnerRead":
		return []ACLEntry{owner, {owners, roleReader}}, nil
	case "bucketOwnerFullControl":
		return []ACLEntry{owner, {owners, roleOwner}}, nil
	default:
		return []ACLEntry{owner}, nil
	}
}

func validEntity(e string) bool {
	switch {
	case e == entityAllUsers || e == entityAllAuth:
		return true
	case strings.HasPrefix(e, "user-"), strings.HasPrefix(e, "group-"), strings.HasPrefix(e, "domain-"):
		return len(e) > strings.IndexByte(e, '-')+1
	default:
		_, _, ok := parseTeam(e)

		return ok
	}
}

func parseTeam(e string) (string, string, bool) {
	rest, ok := strings.CutPrefix(e, "project-")
	if !ok {
		return "", "", false
	}

	team, num, ok := strings.Cut(rest, "-")
	if !ok || num == "" || !slices.Contains([]string{"owners", "editors", "viewers"}, team) {
		return "", "", false
	}

	return team, num, true
}

func validACLRole(role string, kind aclKind) bool {
	return role == roleOwner || role == roleReader || (role == roleWriter && kind == aclBucket)
}

func checkACLEntry(e ACLEntry, kind aclKind) *APIError {
	if !validEntity(e.Entity) {
		return errInvalid("Invalid entity: '" + e.Entity + "'.")
	}

	if !validACLRole(e.Role, kind) {
		return errInvalid("Invalid role: '" + e.Role + "'.")
	}

	return nil
}

// upsertACL replaces or adds the entry for e.Entity.
func upsertACL(list []ACLEntry, e ACLEntry) ([]ACLEntry, *APIError) {
	for i := range list {
		if list[i].Entity == e.Entity {
			list[i].Role = e.Role

			return list, nil
		}
	}

	if len(list) >= maxACLEntries {
		return nil, errInvalid("An ACL can have at most 100 entries.")
	}

	return append(list, e), nil
}

func findACL(list []ACLEntry, entity string) int {
	return slices.IndexFunc(list, func(e ACLEntry) bool { return e.Entity == entity })
}

// aclFromPatch resolves the ACL a create/update request asks for: a predefined ACL wins over a body array.
func aclFromPatch(p Patch, predefKey, bodyKey string, kind aclKind, project string) ([]ACLEntry, bool, *APIError) {
	if name := p.str(predefKey); name != "" {
		entries, apiErr := predefinedACL(name, kind, project)

		return entries, true, apiErr
	}

	raw, ok := p[bodyKey]
	if !ok {
		return nil, false, nil
	}

	in, apiErr := decodeField[[]ACLEntry](raw, bodyKey)
	if apiErr != nil {
		return nil, false, apiErr
	}

	var out []ACLEntry

	for _, e := range in {
		if apiErr = checkACLEntry(e, kind); apiErr != nil {
			return nil, false, apiErr
		}

		var upErr *APIError
		if out, upErr = upsertACL(out, e); upErr != nil {
			return nil, false, upErr
		}
	}

	return out, true, nil
}

func hasACLInput(p Patch, keys ...string) bool {
	for _, k := range keys {
		if _, ok := p[k]; ok {
			return true
		}
	}

	return false
}

// ubla reports whether uniform bucket-level access is enabled.
func (b *Bucket) ubla() bool {
	var cfg struct {
		Uniform struct {
			Enabled bool `json:"enabled"`
		} `json:"uniformBucketLevelAccess"`
		PolicyOnly struct {
			Enabled bool `json:"enabled"`
		} `json:"bucketPolicyOnly"`
	}

	raw, ok := b.Config["iamConfiguration"]
	if !ok || json.Unmarshal(raw, &cfg) != nil {
		return false
	}

	return cfg.Uniform.Enabled || cfg.PolicyOnly.Enabled
}

// applyBucketACL applies acl/defaultObjectAcl bodies and predefined ACL parameters to b.
func applyBucketACL(b *Bucket, p Patch, create bool) *APIError {
	if !hasACLInput(p, keyPredefined, keyPredefDef, keyACL, keyDefaultACL) {
		if create {
			b.ACL, b.DefaultObjectACL = projectPrivateACL(b.Project), projectPrivateACL(b.Project)
		}

		return nil
	}

	if !create && b.ubla() {
		return errUBLA()
	}

	acl, set, apiErr := aclFromPatch(p, keyPredefined, keyACL, aclBucket, b.Project)
	if apiErr != nil {
		return apiErr
	}

	switch {
	case set:
		b.ACL = acl
	case create:
		b.ACL = projectPrivateACL(b.Project)
	}

	def, set, apiErr := aclFromPatch(p, keyPredefDef, keyDefaultACL, aclDefaultObject, b.Project)
	if apiErr != nil {
		return apiErr
	}

	switch {
	case set:
		b.DefaultObjectACL = def
	case create:
		b.DefaultObjectACL = projectPrivateACL(b.Project)
	}

	return nil
}

// objectACL resolves the ACL of a new object version from the request, else the bucket default.
func objectACL(bk *Bucket, p Patch) ([]ACLEntry, *APIError) {
	if hasACLInput(p, keyPredefined, keyACL) && bk.ubla() {
		return nil, errUBLA()
	}

	acl, set, apiErr := aclFromPatch(p, keyPredefined, keyACL, aclObject, bk.Project)
	if apiErr != nil {
		return nil, apiErr
	}

	if !set {
		return slices.Clone(bk.DefaultObjectACL), nil
	}

	return acl, nil
}

// aclResource is the wire form of one entry.
type aclResource struct {
	ProjectTeam *projectTeam `json:"projectTeam,omitempty"`
	Kind        string       `json:"kind"`
	ID          string       `json:"id"`
	SelfLink    string       `json:"selfLink"`
	Bucket      string       `json:"bucket"`
	Object      string       `json:"object,omitempty"`
	Generation  string       `json:"generation,omitempty"`
	Entity      string       `json:"entity"`
	Role        string       `json:"role"`
	Email       string       `json:"email,omitempty"`
	Domain      string       `json:"domain,omitempty"`
	Etag        string       `json:"etag"`
}

type projectTeam struct {
	ProjectNumber string `json:"projectNumber"`
	Team          string `json:"team"`
}

type aclTarget struct {
	bucket string
	object string
	gen    int64
	kind   aclKind
}

func (t aclTarget) kindName() string {
	if t.kind == aclBucket {
		return "storage#bucketAccessControl"
	}

	return "storage#objectAccessControl"
}

func (t aclTarget) listKind() string {
	if t.kind == aclBucket {
		return "storage#bucketAccessControls"
	}

	return "storage#objectAccessControls"
}

func (t aclTarget) path() string {
	switch t.kind {
	case aclBucket:
		return "/storage/v1/b/" + t.bucket + "/acl/"
	case aclDefaultObject:
		return "/storage/v1/b/" + t.bucket + "/defaultObjectAcl/"
	default:
		return "/storage/v1/b/" + t.bucket + "/o/" + url.PathEscape(t.object) + "/acl/"
	}
}

func renderACL(base string, t aclTarget, e ACLEntry) aclResource {
	res := aclResource{
		Kind: t.kindName(), Bucket: t.bucket, Entity: e.Entity, Role: e.Role, Etag: aclEtag,
		SelfLink: base + t.path() + url.PathEscape(e.Entity),
		ID:       t.bucket + "/" + e.Entity,
	}

	if t.kind == aclObject {
		res.Object = t.object
		res.ID = t.bucket + "/" + t.object + "/" + itoa64(t.gen) + "/" + e.Entity

		if t.gen != 0 {
			res.Generation = itoa64(t.gen)
		}
	}

	switch {
	case strings.HasPrefix(e.Entity, "user-"), strings.HasPrefix(e.Entity, "group-"):
		res.Email = e.Entity[strings.IndexByte(e.Entity, '-')+1:]
	case strings.HasPrefix(e.Entity, "domain-"):
		res.Domain = strings.TrimPrefix(e.Entity, "domain-")
	default:
		if team, num, ok := parseTeam(e.Entity); ok {
			res.ProjectTeam = &projectTeam{ProjectNumber: num, Team: team}
		}
	}

	return res
}

func renderACLList(base string, t aclTarget, list []ACLEntry) []aclResource {
	out := make([]aclResource, 0, len(list))
	for _, e := range list {
		out = append(out, renderACL(base, t, e))
	}

	return out
}
