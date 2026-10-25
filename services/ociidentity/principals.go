package ociidentity

import (
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxUsers          = 2000
	maxGroups         = 250
	maxDynamicGroups  = 50
	maxGroupsPerUser  = 250
	maxUserNameLen    = 255
	maxGroupNameLen   = 100
	maxDynGroupNameLn = 100
)

// CreateUserIn is the CreateUser request body.
type CreateUserIn struct {
	Tags
	CompartmentID string `json:"compartmentId"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	Email         string `json:"email"`
}

// UpdateUserIn is the UpdateUser request body.
type UpdateUserIn struct {
	Description *string `json:"description"`
	Email       *string `json:"email"`
	DBUserName  *string `json:"dbUserName"`
	Tags
}

// CreateGroupIn is the CreateGroup request body.
type CreateGroupIn struct {
	Tags
	CompartmentID string `json:"compartmentId"`
	Name          string `json:"name"`
	Description   string `json:"description"`
}

// UpdateGroupIn is the UpdateGroup request body.
type UpdateGroupIn struct {
	Description *string `json:"description"`
	Tags
}

// CreateDynamicGroupIn is the CreateDynamicGroup request body.
type CreateDynamicGroupIn struct {
	Tags
	CompartmentID string `json:"compartmentId"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	MatchingRule  string `json:"matchingRule"`
}

// UpdateDynamicGroupIn is the UpdateDynamicGroup request body.
type UpdateDynamicGroupIn struct {
	Description  *string `json:"description"`
	MatchingRule *string `json:"matchingRule"`
	Tags
}

// NameFilter holds the common list filters of tenancy-level resources.
type NameFilter struct {
	CompartmentID string
	Name          string
	State         string
}

func requireTenancy(compartment string) *APIError {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	if compartment != TenancyID {
		return errParam("compartmentId must be the tenancy OCID for users, groups and dynamic groups")
	}

	return nil
}

func (b *InMemoryBackend) listStates() []string {
	return []string{stateCreating, stateActive, "INACTIVE", stateDeleting, stateDeleted}
}

func (u *User) view() userWire {
	return userWire{Meta: u.Meta, Email: u.Email, DBUserName: u.DBUserName, Capabilities: u.Capabilities,
		EmailVerified: u.Email != ""}
}

func defaultCapabilities() Capabilities {
	return Capabilities{
		CanUseConsolePassword: true, CanUseAPIKeys: true, CanUseAuthTokens: true, CanUseSMTPCredentials: true,
		CanUseCustomerSecretKeys: true, CanUseOAuth2ClientCredentials: true, CanUseDBCredentials: true,
	}
}

func validEmail(e string) *APIError {
	if e != "" && !strings.Contains(e, "@") {
		return errParam("email is not a valid address")
	}

	return nil
}

func (b *InMemoryBackend) userByID(id string) (*User, *APIError) {
	u, ok := b.users[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return u, nil
}

// CreateUser creates an IAM user.
func (b *InMemoryBackend) CreateUser(in CreateUserIn) (userWire, string, error) {
	if aerr := requireTenancy(in.CompartmentID); aerr != nil {
		return userWire{}, "", aerr
	}

	if aerr := validateName(in.Name, maxUserNameLen); aerr != nil {
		return userWire{}, "", aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return userWire{}, "", aerr
	}

	if aerr := validEmail(in.Email); aerr != nil {
		return userWire{}, "", aerr
	}

	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if len(b.users) >= maxUsers {
		return userWire{}, "", errLimit("The tenancy user limit (2000) has been reached")
	}

	for _, u := range b.users {
		if strs.Equal(u.Name, in.Name) {
			return userWire{}, "", errConflict("Conflict",
				"User "+in.Name+" already exists in the tenancy",
			)
		}
	}

	u := &User{
		Meta: b.newMeta("user", TenancyID, in.Name, in.Description, in.Tags), Email: in.Email,
		Capabilities: defaultCapabilities(), Internal: b.newInternal(),
	}
	b.users[u.ID] = u

	return u.view(), u.Internal.Etag, nil
}

// GetUser returns one user.
func (b *InMemoryBackend) GetUser(id string) (userWire, string, error) {
	b.mu.RLock("GetUser")
	defer b.mu.RUnlock()

	u, aerr := b.userByID(id)
	if aerr != nil {
		return userWire{}, "", aerr
	}

	return u.view(), u.Internal.Etag, nil
}

// ListUsers lists users.
func (b *InMemoryBackend) ListUsers(f NameFilter, o ListOpts) (page.Page[userWire], error) {
	if aerr := b.checkListArgs(f, o); aerr != nil {
		return page.Page[userWire]{}, aerr
	}

	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	acc := listAccess[User]{fields: func(v *User) (string, string, time.Time, int64) {
		return v.Name, v.LifecycleState, v.TimeCreated, v.Internal.Seq
	}}

	return listNamed(b.users, f, o, acc, (*User).view), nil
}

func (b *InMemoryBackend) checkListArgs(f NameFilter, o ListOpts) *APIError {
	if aerr := requireTenancy(f.CompartmentID); aerr != nil {
		return aerr
	}

	if aerr := validListOpts(o, "TIMECREATED", "NAME"); aerr != nil {
		return aerr
	}

	return validState(f.State, b.listStates()...)
}

// UpdateUser changes description, email and tags.
func (b *InMemoryBackend) UpdateUser(id, ifMatch string, in UpdateUserIn) (userWire, string, error) {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	u, aerr := b.userByID(id)
	if aerr != nil {
		return userWire{}, "", aerr
	}

	if aerr = checkEtag(u.Internal.Etag, ifMatch); aerr != nil {
		return userWire{}, "", aerr
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return userWire{}, "", aerr
		}

		u.Description = *in.Description
	}

	if in.Email != nil {
		if aerr = validEmail(*in.Email); aerr != nil {
			return userWire{}, "", aerr
		}

		u.Email = *in.Email
	}

	if in.DBUserName != nil {
		u.DBUserName = *in.DBUserName
	}

	applyTags(&u.Tags, in.Tags)
	u.Internal.Etag = newEtag()

	return u.view(), u.Internal.Etag, nil
}

// DeleteUser removes a user that belongs to no group, along with its credentials.
func (b *InMemoryBackend) DeleteUser(id, ifMatch string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	u, aerr := b.userByID(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(u.Internal.Etag, ifMatch); aerr != nil {
		return aerr
	}

	for _, m := range b.members {
		if m.UserID == id {
			return errConflict("Conflict",
				"User "+id+" is still a member of a group; remove it from all groups first",
			)
		}
	}

	for _, k := range u.APIKeys {
		b.Auth.Remove(apiKeyID(id, k.Fingerprint))
	}

	delete(b.users, id)

	return nil
}

func (b *InMemoryBackend) groupByID(id string) (*Group, *APIError) {
	g, ok := b.groups[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return g, nil
}

// CreateGroup creates an IAM group.
func (b *InMemoryBackend) CreateGroup(in CreateGroupIn) (Meta, string, error) {
	if aerr := requireTenancy(in.CompartmentID); aerr != nil {
		return Meta{}, "", aerr
	}

	if aerr := validateName(in.Name, maxGroupNameLen); aerr != nil {
		return Meta{}, "", aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return Meta{}, "", aerr
	}

	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if len(b.groups) >= maxGroups {
		return Meta{}, "", errLimit("The tenancy group limit (250) has been reached")
	}

	for _, g := range b.groups {
		if strs.Equal(g.Name, in.Name) {
			return Meta{}, "", errConflict("Conflict",
				"Group "+in.Name+" already exists in the tenancy",
			)
		}
	}

	g := &Group{Meta: b.newMeta("group", TenancyID, in.Name, in.Description, in.Tags), Internal: b.newInternal()}
	b.groups[g.ID] = g

	return g.Meta, g.Internal.Etag, nil
}

// GetGroup returns one group.
func (b *InMemoryBackend) GetGroup(id string) (Meta, string, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, aerr := b.groupByID(id)
	if aerr != nil {
		return Meta{}, "", aerr
	}

	return g.Meta, g.Internal.Etag, nil
}

// ListGroups lists groups.
func (b *InMemoryBackend) ListGroups(f NameFilter, o ListOpts) (page.Page[Meta], error) {
	if aerr := b.checkListArgs(f, o); aerr != nil {
		return page.Page[Meta]{}, aerr
	}

	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	acc := listAccess[Group]{fields: func(v *Group) (string, string, time.Time, int64) {
		return v.Name, v.LifecycleState, v.TimeCreated, v.Internal.Seq
	}}

	return listNamed(b.groups, f, o, acc, func(g *Group) Meta { return g.Meta }), nil
}

// UpdateGroup changes description and tags.
func (b *InMemoryBackend) UpdateGroup(id, ifMatch string, in UpdateGroupIn) (Meta, string, error) {
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	g, aerr := b.groupByID(id)
	if aerr != nil {
		return Meta{}, "", aerr
	}

	if aerr = checkEtag(g.Internal.Etag, ifMatch); aerr != nil {
		return Meta{}, "", aerr
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return Meta{}, "", aerr
		}

		g.Description = *in.Description
	}

	applyTags(&g.Tags, in.Tags)
	g.Internal.Etag = newEtag()

	return g.Meta, g.Internal.Etag, nil
}

// DeleteGroup removes an empty group.
func (b *InMemoryBackend) DeleteGroup(id, ifMatch string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	g, aerr := b.groupByID(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(g.Internal.Etag, ifMatch); aerr != nil {
		return aerr
	}

	for _, m := range b.members {
		if m.GroupID == id {
			return errConflict("Conflict", "Group "+id+" is not empty; remove its members first")
		}
	}

	delete(b.groups, id)

	return nil
}

func (m *Membership) view() membershipWire {
	return membershipWire{
		TimeCreated: m.TimeCreated, ID: m.ID, CompartmentID: m.CompartmentID, GroupID: m.GroupID, UserID: m.UserID,
		LifecycleState: stateActive,
	}
}

// AddUserToGroup creates a membership.
func (b *InMemoryBackend) AddUserToGroup(userID, groupID string) (membershipWire, string, error) {
	if userID == "" || groupID == "" {
		return membershipWire{}, "", errMissing("userId and groupId are required")
	}

	b.mu.Lock("AddUserToGroup")
	defer b.mu.Unlock()

	if _, aerr := b.userByID(userID); aerr != nil {
		return membershipWire{}, "", aerr
	}

	if _, aerr := b.groupByID(groupID); aerr != nil {
		return membershipWire{}, "", aerr
	}

	count := 0

	for _, m := range b.members {
		if m.UserID == userID {
			count++

			if m.GroupID == groupID {
				return membershipWire{}, "", errConflict("Conflict",
					"The user is already a member of the group",
				)
			}
		}
	}

	if count >= maxGroupsPerUser {
		return membershipWire{}, "", errLimit("A user can be a member of at most 250 groups")
	}

	m := &Membership{
		ID: newOCID("groupmembership"), CompartmentID: TenancyID, GroupID: groupID, UserID: userID,
		TimeCreated: b.nowUTC(), Seq: b.nextSeq(),
	}
	b.members[m.ID] = m

	return m.view(), newEtag(), nil
}

// GetMembership returns one membership.
func (b *InMemoryBackend) GetMembership(id string) (membershipWire, error) {
	b.mu.RLock("GetMembership")
	defer b.mu.RUnlock()

	m, ok := b.members[id]
	if !ok {
		return membershipWire{}, errNotFound(id)
	}

	return m.view(), nil
}

// ListMemberships lists memberships filtered by user and/or group.
func (b *InMemoryBackend) ListMemberships(
	compartment, userID, groupID string,
	o ListOpts,
) (page.Page[membershipWire], error) {
	if aerr := requireTenancy(compartment); aerr != nil {
		return page.Page[membershipWire]{}, aerr
	}

	b.mu.RLock("ListMemberships")
	defer b.mu.RUnlock()

	var all []*Membership

	for _, m := range b.members {
		if (userID == "" || m.UserID == userID) && (groupID == "" || m.GroupID == groupID) {
			all = append(all, m)
		}
	}

	pg := paginate(all, ListOpts{Page: o.Page, Limit: o.Limit}, func(m *Membership) string { return m.ID },
		func(m *Membership) (time.Time, int64) { return m.TimeCreated, m.Seq })

	out := make([]membershipWire, 0, len(pg.Data))
	for _, m := range pg.Data {
		out = append(out, m.view())
	}

	return page.Page[membershipWire]{Data: out, Next: pg.Next}, nil
}

// RemoveUserFromGroup deletes a membership.
func (b *InMemoryBackend) RemoveUserFromGroup(id string) error {
	b.mu.Lock("RemoveUserFromGroup")
	defer b.mu.Unlock()

	if _, ok := b.members[id]; !ok {
		return errNotFound(id)
	}

	delete(b.members, id)

	return nil
}

func (d *DynamicGroup) view() dynamicGroupWire {
	return dynamicGroupWire{Meta: d.Meta, MatchingRule: d.MatchingRule}
}

func (b *InMemoryBackend) dynGroupByID(id string) (*DynamicGroup, *APIError) {
	d, ok := b.dynGroups[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return d, nil
}

// CreateDynamicGroup creates a dynamic group after validating its matching rule.
func (b *InMemoryBackend) CreateDynamicGroup(in CreateDynamicGroupIn) (dynamicGroupWire, string, error) {
	if aerr := requireTenancy(in.CompartmentID); aerr != nil {
		return dynamicGroupWire{}, "", aerr
	}

	if aerr := validateName(in.Name, maxDynGroupNameLn); aerr != nil {
		return dynamicGroupWire{}, "", aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return dynamicGroupWire{}, "", aerr
	}

	if err := ValidateMatchingRule(in.MatchingRule); err != nil {
		return dynamicGroupWire{}, "", errParam("Invalid matchingRule: " + err.Error())
	}

	b.mu.Lock("CreateDynamicGroup")
	defer b.mu.Unlock()

	if len(b.dynGroups) >= maxDynamicGroups {
		return dynamicGroupWire{}, "", errLimit("The tenancy dynamic group limit (50) has been reached")
	}

	for _, d := range b.dynGroups {
		if strs.Equal(d.Name, in.Name) {
			return dynamicGroupWire{}, "", errConflict("Conflict",
				"Dynamic group "+in.Name+" already exists",
			)
		}
	}

	d := &DynamicGroup{
		Meta:         b.newMeta("dynamicgroup", TenancyID, in.Name, in.Description, in.Tags),
		MatchingRule: in.MatchingRule, Internal: b.newInternal(),
	}
	b.dynGroups[d.ID] = d

	return d.view(), d.Internal.Etag, nil
}

// GetDynamicGroup returns one dynamic group.
func (b *InMemoryBackend) GetDynamicGroup(id string) (dynamicGroupWire, string, error) {
	b.mu.RLock("GetDynamicGroup")
	defer b.mu.RUnlock()

	d, aerr := b.dynGroupByID(id)
	if aerr != nil {
		return dynamicGroupWire{}, "", aerr
	}

	return d.view(), d.Internal.Etag, nil
}

// ListDynamicGroups lists dynamic groups.
func (b *InMemoryBackend) ListDynamicGroups(f NameFilter, o ListOpts) (page.Page[dynamicGroupWire], error) {
	if aerr := b.checkListArgs(f, o); aerr != nil {
		return page.Page[dynamicGroupWire]{}, aerr
	}

	b.mu.RLock("ListDynamicGroups")
	defer b.mu.RUnlock()

	acc := listAccess[DynamicGroup]{fields: func(v *DynamicGroup) (string, string, time.Time, int64) {
		return v.Name, v.LifecycleState, v.TimeCreated, v.Internal.Seq
	}}

	return listNamed(b.dynGroups, f, o, acc, (*DynamicGroup).view), nil
}

// UpdateDynamicGroup changes description, matching rule and tags.
func (b *InMemoryBackend) UpdateDynamicGroup(
	id, ifMatch string,
	in UpdateDynamicGroupIn,
) (dynamicGroupWire, string, error) {
	b.mu.Lock("UpdateDynamicGroup")
	defer b.mu.Unlock()

	d, aerr := b.dynGroupByID(id)
	if aerr != nil {
		return dynamicGroupWire{}, "", aerr
	}

	if aerr = checkEtag(d.Internal.Etag, ifMatch); aerr != nil {
		return dynamicGroupWire{}, "", aerr
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return dynamicGroupWire{}, "", aerr
		}

		d.Description = *in.Description
	}

	if in.MatchingRule != nil {
		if err := ValidateMatchingRule(*in.MatchingRule); err != nil {
			return dynamicGroupWire{}, "", errParam("Invalid matchingRule: " + err.Error())
		}

		d.MatchingRule = *in.MatchingRule
	}

	applyTags(&d.Tags, in.Tags)
	d.Internal.Etag = newEtag()

	return d.view(), d.Internal.Etag, nil
}

// DeleteDynamicGroup removes a dynamic group.
func (b *InMemoryBackend) DeleteDynamicGroup(id, ifMatch string) error {
	b.mu.Lock("DeleteDynamicGroup")
	defer b.mu.Unlock()

	d, aerr := b.dynGroupByID(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(d.Internal.Etag, ifMatch); aerr != nil {
		return aerr
	}

	delete(b.dynGroups, id)

	return nil
}

func nameFilter(q *req) NameFilter {
	return NameFilter{CompartmentID: q.query("compartmentId"), Name: q.query("name"), State: q.query("lifecycleState")}
}

func (h *Handler) createUser(q *req) error {
	var in CreateUserIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateUser(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getUser(q *req) error {
	v, etag, err := h.Backend.GetUser(q.param("userId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listUsers(q *req) error {
	pg, err := h.Backend.ListUsers(nameFilter(q), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateUser(q *req) error {
	var in UpdateUserIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateUser(q.param("userId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteUser(q *req) error {
	return done(q, h.Backend.DeleteUser(q.param("userId"), q.ifMatch()))
}

func (h *Handler) createGroup(q *req) error {
	var in CreateGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateGroup(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getGroup(q *req) error {
	v, etag, err := h.Backend.GetGroup(q.param("groupId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listGroups(q *req) error {
	pg, err := h.Backend.ListGroups(nameFilter(q), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateGroup(q *req) error {
	var in UpdateGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateGroup(q.param("groupId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteGroup(q *req) error {
	return done(q, h.Backend.DeleteGroup(q.param("groupId"), q.ifMatch()))
}

func (h *Handler) addUserToGroup(q *req) error {
	var in struct {
		UserID  string `json:"userId"`
		GroupID string `json:"groupId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.AddUserToGroup(in.UserID, in.GroupID)

	return reply(q, v, etag, err)
}

func (h *Handler) getMembership(q *req) error {
	v, err := h.Backend.GetMembership(q.param("membershipId"))

	return reply(q, v, "", err)
}

func (h *Handler) listMemberships(q *req) error {
	pg, err := h.Backend.ListMemberships(q.query("compartmentId"), q.query("userId"), q.query("groupId"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) removeFromGroup(q *req) error {
	return done(q, h.Backend.RemoveUserFromGroup(q.param("membershipId")))
}

func (h *Handler) createDynamicGroup(q *req) error {
	var in CreateDynamicGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateDynamicGroup(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getDynamicGroup(q *req) error {
	v, etag, err := h.Backend.GetDynamicGroup(q.param("dynamicGroupId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listDynamicGroups(q *req) error {
	pg, err := h.Backend.ListDynamicGroups(nameFilter(q), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateDynamicGroup(q *req) error {
	var in UpdateDynamicGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateDynamicGroup(q.param("dynamicGroupId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteDynamicGroup(q *req) error {
	return done(q, h.Backend.DeleteDynamicGroup(q.param("dynamicGroupId"), q.ifMatch()))
}
