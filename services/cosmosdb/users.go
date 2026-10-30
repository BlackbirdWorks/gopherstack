package cosmosdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	usersSegment       = "users"
	permissionsSegment = "permissions"

	// PermissionModeAll and PermissionModeRead are the permissionMode values.
	PermissionModeAll  = "All"
	PermissionModeRead = "Read"

	containerLinkSegments = 4
	documentLinkSegments  = 6
)

var (
	// ErrUserNotFound is returned for a user id absent from its database.
	ErrUserNotFound = errors.New("cosmosdb: user not found")
	// ErrUserAlreadyExists is returned when a user id is already taken in the database.
	ErrUserAlreadyExists = errors.New("cosmosdb: user already exists")
	// ErrPermissionNotFound is returned for a permission id absent from its user.
	ErrPermissionNotFound = errors.New("cosmosdb: permission not found")
	// ErrPermissionAlreadyExists is returned when a permission id is already taken for the user.
	ErrPermissionAlreadyExists = errors.New("cosmosdb: permission already exists")
	// ErrInvalidPermission is returned for a permission with a bad mode, resource link or partition key.
	ErrInvalidPermission = errors.New("cosmosdb: invalid permission")
)

// UserInfo is a read-only snapshot of a database user.
type UserInfo struct {
	Modified time.Time
	ID       string
	RID      string
}

// PermissionSpec is the caller-supplied shape for creating or replacing a permission.
type PermissionSpec struct {
	ID           string
	Mode         string
	Resource     string
	PartitionKey json.RawMessage
}

// PermissionInfo is a read-only snapshot of a permission. Link is the canonical id-based resource path.
type PermissionInfo struct {
	Modified         time.Time
	ID               string
	RID              string
	DBID             string
	UserID           string
	Mode             string
	Resource         string
	Link             string
	PartitionKeyJSON string
	PartitionKey     json.RawMessage
}

type storedUser struct {
	Modified    time.Time `json:"Modified"`
	Permissions map[string]*storedPermission
	ID          string
}

type storedPermission struct {
	Modified         time.Time `json:"Modified"`
	ID               string
	Mode             string
	Resource         string
	Link             string
	PartitionKeyJSON string          `json:"PartitionKeyJSON,omitempty"`
	PartitionKey     json.RawMessage `json:"PartitionKey,omitempty"`
}

func userInfo(dbID string, u *storedUser) UserInfo {
	return UserInfo{ID: u.ID, RID: fakeRID("dbs/" + dbID + "/users/" + u.ID), Modified: u.Modified}
}

func permissionInfo(dbID, userID string, p *storedPermission) PermissionInfo {
	return PermissionInfo{
		ID:               p.ID,
		RID:              fakeRID("dbs/" + dbID + "/users/" + userID + "/permissions/" + p.ID),
		DBID:             dbID,
		UserID:           userID,
		Mode:             p.Mode,
		Resource:         p.Resource,
		Link:             p.Link,
		PartitionKey:     p.PartitionKey,
		PartitionKeyJSON: p.PartitionKeyJSON,
		Modified:         p.Modified,
	}
}

func (b *InMemoryBackend) userLocked(dbID, userID string) (*storedUser, error) {
	d, ok := b.databases[dbID]
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	u, ok := d.Users[userID]
	if !ok {
		return nil, ErrUserNotFound
	}

	return u, nil
}

// CreateUser creates a user in dbID.
func (b *InMemoryBackend) CreateUser(dbID, id string) (UserInfo, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	d, ok := b.databases[dbID]
	if !ok {
		return UserInfo{}, ErrDatabaseNotFound
	}

	if _, dup := d.Users[id]; dup {
		return UserInfo{}, ErrUserAlreadyExists
	}

	if d.Users == nil {
		d.Users = make(map[string]*storedUser)
	}

	u := &storedUser{ID: id, Permissions: make(map[string]*storedPermission), Modified: b.now()}
	d.Users[id] = u

	return userInfo(dbID, u), nil
}

// GetUser returns one user.
func (b *InMemoryBackend) GetUser(dbID, id string) (UserInfo, error) {
	b.mu.RLock("GetUser")
	defer b.mu.RUnlock()

	u, err := b.userLocked(dbID, id)
	if err != nil {
		return UserInfo{}, err
	}

	return userInfo(dbID, u), nil
}

// ListUsers returns the users of dbID sorted by id.
func (b *InMemoryBackend) ListUsers(dbID string) ([]UserInfo, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	d, ok := b.databases[dbID]
	if !ok {
		return nil, ErrDatabaseNotFound
	}

	out := make([]UserInfo, 0, len(d.Users))
	for _, u := range d.Users {
		out = append(out, userInfo(dbID, u))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

// ReplaceUser renames user id to newID, keeping its permissions.
func (b *InMemoryBackend) ReplaceUser(dbID, id, newID string) (UserInfo, error) {
	b.mu.Lock("ReplaceUser")
	defer b.mu.Unlock()

	u, err := b.userLocked(dbID, id)
	if err != nil {
		return UserInfo{}, err
	}

	d := b.databases[dbID]

	if newID != id {
		if _, dup := d.Users[newID]; dup {
			return UserInfo{}, ErrUserAlreadyExists
		}

		delete(d.Users, id)
		u.ID = newID
		d.Users[newID] = u
	}

	u.Modified = bumpTimestampFrom(b.now(), u.Modified, true)

	return userInfo(dbID, u), nil
}

// DeleteUser removes a user and its permissions.
func (b *InMemoryBackend) DeleteUser(dbID, id string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	if _, err := b.userLocked(dbID, id); err != nil {
		return err
	}

	delete(b.databases[dbID].Users, id)

	return nil
}

// CreatePermission adds a permission to a user after resolving its resource link.
func (b *InMemoryBackend) CreatePermission(dbID, userID string, spec PermissionSpec) (PermissionInfo, error) {
	b.mu.Lock("CreatePermission")
	defer b.mu.Unlock()

	u, err := b.userLocked(dbID, userID)
	if err != nil {
		return PermissionInfo{}, err
	}

	if _, dup := u.Permissions[spec.ID]; dup {
		return PermissionInfo{}, ErrPermissionAlreadyExists
	}

	p, err := b.buildPermissionLocked(spec)
	if err != nil {
		return PermissionInfo{}, err
	}

	p.Modified = b.now()

	if u.Permissions == nil {
		u.Permissions = make(map[string]*storedPermission)
	}

	u.Permissions[p.ID] = p

	return permissionInfo(dbID, userID, p), nil
}

// GetPermission returns one permission.
func (b *InMemoryBackend) GetPermission(dbID, userID, id string) (PermissionInfo, error) {
	b.mu.RLock("GetPermission")
	defer b.mu.RUnlock()

	u, err := b.userLocked(dbID, userID)
	if err != nil {
		return PermissionInfo{}, err
	}

	p, ok := u.Permissions[id]
	if !ok {
		return PermissionInfo{}, ErrPermissionNotFound
	}

	return permissionInfo(dbID, userID, p), nil
}

// ListPermissions returns a user's permissions sorted by id.
func (b *InMemoryBackend) ListPermissions(dbID, userID string) ([]PermissionInfo, error) {
	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	u, err := b.userLocked(dbID, userID)
	if err != nil {
		return nil, err
	}

	out := make([]PermissionInfo, 0, len(u.Permissions))
	for _, p := range u.Permissions {
		out = append(out, permissionInfo(dbID, userID, p))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

// ReplacePermission replaces permission id with spec; spec.ID may rename it.
func (b *InMemoryBackend) ReplacePermission(dbID, userID, id string, spec PermissionSpec) (PermissionInfo, error) {
	b.mu.Lock("ReplacePermission")
	defer b.mu.Unlock()

	u, err := b.userLocked(dbID, userID)
	if err != nil {
		return PermissionInfo{}, err
	}

	old, ok := u.Permissions[id]
	if !ok {
		return PermissionInfo{}, ErrPermissionNotFound
	}

	if _, dup := u.Permissions[spec.ID]; dup && spec.ID != id {
		return PermissionInfo{}, ErrPermissionAlreadyExists
	}

	p, err := b.buildPermissionLocked(spec)
	if err != nil {
		return PermissionInfo{}, err
	}

	p.Modified = bumpTimestampFrom(b.now(), old.Modified, true)

	delete(u.Permissions, id)
	u.Permissions[p.ID] = p

	return permissionInfo(dbID, userID, p), nil
}

// DeletePermission removes a permission.
func (b *InMemoryBackend) DeletePermission(dbID, userID, id string) error {
	b.mu.Lock("DeletePermission")
	defer b.mu.Unlock()

	u, err := b.userLocked(dbID, userID)
	if err != nil {
		return err
	}

	if _, ok := u.Permissions[id]; !ok {
		return ErrPermissionNotFound
	}

	delete(u.Permissions, id)

	return nil
}

func (b *InMemoryBackend) buildPermissionLocked(spec PermissionSpec) (*storedPermission, error) {
	if spec.Mode != PermissionModeAll && spec.Mode != PermissionModeRead {
		return nil, fmt.Errorf("%w: permissionMode must be %q or %q", ErrInvalidPermission,
			PermissionModeAll, PermissionModeRead)
	}

	link, err := b.resolveLinkLocked(spec.Resource)
	if err != nil {
		return nil, err
	}

	p := &storedPermission{ID: spec.ID, Mode: spec.Mode, Resource: spec.Resource, Link: link}

	if len(spec.PartitionKey) == 0 || string(spec.PartitionKey) == "null" {
		return p, nil
	}

	pk, err := parsePartitionKeyHeader(string(spec.PartitionKey))
	if err != nil {
		return nil, fmt.Errorf("%w: resourcePartitionKey is malformed", ErrInvalidPermission)
	}

	p.PartitionKey = spec.PartitionKey
	p.PartitionKeyJSON = pk

	return p, nil
}

// resolveLinkLocked turns a container or document link (id- or RID-based) into the canonical id-based path.
func (b *InMemoryBackend) resolveLinkLocked(link string) (string, error) {
	segs := strings.Split(strings.Trim(link, "/"), "/")
	if len(segs) != containerLinkSegments && len(segs) != documentLinkSegments {
		return "", fmt.Errorf("%w: resource must link a container or a document", ErrInvalidPermission)
	}

	_, childOK := collectionKindsFor(segs[len(segs)-2])
	badChild := len(segs) == documentLinkSegments && (!childOK || segs[4] == conflictsSegment)

	if segs[0] != dbsSegment || segs[2] != collsSegment || badChild {
		return "", fmt.Errorf("%w: resource must link a container, document, stored procedure, trigger or UDF",
			ErrInvalidPermission)
	}

	notFound := fmt.Errorf("%w: resource %q does not exist", ErrInvalidPermission, link)

	dbID, ok := matchLinkSegment(b.databases, segs[1], func(id string) string { return "dbs/" + id })
	if !ok {
		return "", notFound
	}

	cID, ok := matchLinkSegment(b.databases[dbID].Containers, segs[3], func(id string) string {
		return "dbs/" + dbID + "/colls/" + id
	})
	if !ok {
		return "", notFound
	}

	path := "dbs/" + dbID + "/colls/" + cID
	if len(segs) == containerLinkSegments {
		return path, nil
	}

	if segs[4] != docsSegment {
		return b.resolveScriptLinkLocked(b.databases[dbID].Containers[cID], path, segs[4], segs[5], notFound)
	}

	for _, doc := range b.databases[dbID].Containers[cID].Documents {
		if doc.ID == segs[5] || fakeRID(path+"/docs/"+doc.ID) == segs[5] {
			return path + "/docs/" + doc.ID, nil
		}
	}

	return "", notFound
}

func matchLinkSegment[T any](m map[string]*T, seg string, path func(id string) string) (string, bool) {
	if _, ok := m[seg]; ok {
		return seg, true
	}

	for id := range m {
		if fakeRID(path(id)) == seg {
			return id, true
		}
	}

	return "", false
}

// resolveScriptLinkLocked resolves a stored procedure, trigger or UDF link (id or RID) to its id-based path.
func (b *InMemoryBackend) resolveScriptLinkLocked(
	c *storedContainer, path, collection, seg string, notFound error,
) (string, error) {
	var scripts map[string]*storedScript

	switch scriptKind(collection) {
	case kindSproc:
		scripts = c.Sprocs
	case kindTrigger:
		scripts = c.Triggers
	case kindUDF:
		scripts = c.UDFs
	}

	id, ok := matchLinkSegment(scripts, seg, func(id string) string { return path + "/" + collection + "/" + id })
	if !ok {
		return "", notFound
	}

	return path + "/" + collection + "/" + id, nil
}
