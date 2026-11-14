package azuregraph

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	kindAdministrativeUnit = "administrativeUnit"
	collAdminUnits         = "administrativeUnits"
	typeAdminUnit          = "AdministrativeUnit"
	segScopedRoleMembers   = "scopedrolemembers"
	propDeletedDateTime    = "deletedDateTime"
	membershipDynamic      = "dynamic"
	ruleOn                 = "On"
	rulePaused             = "Paused"
	visibilityHidden       = "HiddenMembership"
)

// AdministrativeUnit is a Microsoft Entra administrative unit.
type AdministrativeUnit struct {
	DeletedDateTime               *time.Time `json:"deletedDateTime"`
	IsMemberManagementRestricted  *bool      `json:"isMemberManagementRestricted"`
	Visibility                    *string    `json:"visibility"`
	MembershipType                *string    `json:"membershipType"`
	MembershipRule                *string    `json:"membershipRule"`
	MembershipRuleProcessingState *string    `json:"membershipRuleProcessingState"`
	Description                   *string    `json:"description"`
	ID                            string     `json:"id"`
	DisplayName                   string     `json:"displayName"`
}

func (*AdministrativeUnit) normalize() {}

// RoleMemberInfo identifies the principal of a scopedRoleMembership.
type RoleMemberInfo struct {
	ID                string `json:"id"`
	DisplayName       string `json:"displayName"`
	UserPrincipalName string `json:"userPrincipalName"`
}

// ScopedRoleMembership assigns a directory role to a user within one administrative unit.
type ScopedRoleMembership struct {
	RoleMemberInfo       RoleMemberInfo `json:"roleMemberInfo"`
	ID                   string         `json:"id"`
	AdministrativeUnitID string         `json:"administrativeUnitId"`
	RoleID               string         `json:"roleId"`
}

func adminUnitReadOnly() []string { return []string{"id", propDeletedDateTime} }

func (b *Backend) findAdminUnit(r Ref) (*AdministrativeUnit, error) {
	if r.Field != "" {
		return nil, badRequest("The alternate key '%s' is not supported on administrativeUnits.", r.Field)
	}

	if !isGUID(r.ID) {
		return nil, badRequest("Invalid object identifier '%s'.", r.ID)
	}

	if u, ok := b.adminUnits.get(lc(r.ID)); ok {
		return u, nil
	}

	return nil, notFound(r.ID)
}

func validateAdminUnit(u *AdministrativeUnit) error {
	if strings.TrimSpace(u.DisplayName) == "" {
		return badRequest("A value is required for property 'displayName' of resource '%s'.", typeAdminUnit)
	}

	if u.Visibility != nil && *u.Visibility != visibilityHidden {
		return badRequest("Invalid value specified for property 'visibility' of resource '%s'.", typeAdminUnit)
	}

	if u.MembershipType != nil && !strings.EqualFold(*u.MembershipType, membershipDynamic) {
		return badRequest("Invalid value specified for property 'membershipType' of resource '%s'.", typeAdminUnit)
	}

	dynamic := u.MembershipType != nil
	if dynamic != (u.MembershipRule != nil) {
		return badRequest("The membershipType and membershipRule properties must be set together.")
	}

	if st := u.MembershipRuleProcessingState; st != nil && *st != ruleOn && *st != rulePaused {
		return badRequest("Invalid value specified for property 'membershipRuleProcessingState' of resource '%s'.",
			typeAdminUnit)
	}

	return nil
}

// CreateAdministrativeUnit creates an administrative unit.
func (b *Backend) CreateAdministrativeUnit(body map[string]jsontext.Value) (AdministrativeUnit, error) {
	if err := requireProps(typeAdminUnit, body, "displayName"); err != nil {
		return AdministrativeUnit{}, err
	}

	b.mu.Lock("CreateAdministrativeUnit")
	defer b.mu.Unlock()

	u, err := applyBody(&AdministrativeUnit{ID: newID()}, body, adminUnitReadOnly()...)
	if err != nil {
		return AdministrativeUnit{}, err
	}

	if err = validateAdminUnit(u); err != nil {
		return AdministrativeUnit{}, err
	}

	b.adminUnits.put(u.ID, u)

	return *u, nil
}

// GetAdministrativeUnit returns an administrative unit.
func (b *Backend) GetAdministrativeUnit(r Ref) (AdministrativeUnit, error) {
	b.mu.RLock("GetAdministrativeUnit")
	defer b.mu.RUnlock()

	u, err := b.findAdminUnit(r)
	if err != nil {
		return AdministrativeUnit{}, err
	}

	return *u, nil
}

// ListAdministrativeUnits returns every administrative unit in creation order.
func (b *Backend) ListAdministrativeUnits() []AdministrativeUnit {
	b.mu.RLock("ListAdministrativeUnits")
	defer b.mu.RUnlock()

	return b.adminUnits.slice()
}

// UpdateAdministrativeUnit applies a PATCH body.
func (b *Backend) UpdateAdministrativeUnit(r Ref, body map[string]jsontext.Value) error {
	b.mu.Lock("UpdateAdministrativeUnit")
	defer b.mu.Unlock()

	cur, err := b.findAdminUnit(r)
	if err != nil {
		return err
	}

	next, err := applyBody(cur, body, adminUnitReadOnly()...)
	if err != nil {
		return err
	}

	if err = validateAdminUnit(next); err != nil {
		return err
	}

	b.adminUnits.put(cur.ID, next)

	return nil
}

// DeleteAdministrativeUnit removes an administrative unit with its membership and scoped role members.
func (b *Backend) DeleteAdministrativeUnit(r Ref) error {
	b.mu.Lock("DeleteAdministrativeUnit")
	defer b.mu.Unlock()

	u, err := b.findAdminUnit(r)
	if err != nil {
		return err
	}

	id := lc(u.ID)
	b.adminUnits.del(id)
	delete(b.auMembers, id)
	delete(b.auRoles, id)

	return nil
}

// dropAdminUnitRefs removes a deleted principal from every administrative unit. Callers hold the write lock.
func (b *Backend) dropAdminUnitRefs(id string) {
	for au, m := range b.auMembers {
		b.auMembers[au] = without(m, id)
	}

	for au, roles := range b.auRoles {
		b.auRoles[au] = slices.DeleteFunc(
			roles,
			func(s ScopedRoleMembership) bool { return lc(s.RoleMemberInfo.ID) == id },
		)
	}
}

// AddAdministrativeUnitMember adds a user or group to an administrative unit.
func (b *Backend) AddAdministrativeUnitMember(auID, memberID string) error {
	b.mu.Lock("AddAdministrativeUnitMember")
	defer b.mu.Unlock()

	u, err := b.findAdminUnit(Ref{ID: auID})
	if err != nil {
		return err
	}

	m, ok := b.lookup(memberID)
	if !ok {
		return notFound(memberID)
	}

	if m.objKind() != kindUser && m.objKind() != kindGroup {
		return badRequest("Invalid object type for the members relationship.")
	}

	id, mid := lc(u.ID), lc(memberID)
	if slices.Contains(b.auMembers[id], mid) {
		return badRequest(
			"One or more added object references already exist for the following modified properties: '%s'.",
			propMembers,
		)
	}

	b.auMembers[id] = append(slices.Clone(b.auMembers[id]), mid)

	return nil
}

// RemoveAdministrativeUnitMember removes a member from an administrative unit.
func (b *Backend) RemoveAdministrativeUnitMember(auID, memberID string) error {
	b.mu.Lock("RemoveAdministrativeUnitMember")
	defer b.mu.Unlock()

	u, err := b.findAdminUnit(Ref{ID: auID})
	if err != nil {
		return err
	}

	id, mid := lc(u.ID), lc(memberID)
	if !slices.Contains(b.auMembers[id], mid) {
		return notFound(memberID)
	}

	b.auMembers[id] = without(b.auMembers[id], mid)

	return nil
}

// AdministrativeUnitMembers lists the members of an administrative unit.
func (b *Backend) AdministrativeUnitMembers(auID string) ([]Object, error) {
	b.mu.RLock("AdministrativeUnitMembers")
	defer b.mu.RUnlock()

	u, err := b.findAdminUnit(Ref{ID: auID})
	if err != nil {
		return nil, err
	}

	return b.objects(b.auMembers[lc(u.ID)]), nil
}

// AddScopedRoleMember assigns an activated directory role to a user with the administrative unit as scope.
func (b *Backend) AddScopedRoleMember(auID string, body map[string]jsontext.Value) (ScopedRoleMembership, error) {
	if err := requireProps("ScopedRoleMembership", body, "roleId", "roleMemberInfo"); err != nil {
		return ScopedRoleMembership{}, err
	}

	var (
		roleID string
		info   RoleMemberInfo
	)

	if json.Unmarshal(body["roleId"], &roleID, jsonx.V1()) != nil ||
		json.Unmarshal(body["roleMemberInfo"], &info, jsonx.V1()) != nil || info.ID == "" {
		return ScopedRoleMembership{}, badRequest("Invalid value specified for property 'roleMemberInfo'.")
	}

	b.mu.Lock("AddScopedRoleMember")
	defer b.mu.Unlock()

	u, err := b.findAdminUnit(Ref{ID: auID})
	if err != nil {
		return ScopedRoleMembership{}, err
	}

	if !slices.ContainsFunc(b.dirRolesList(), func(id string) bool { return strings.EqualFold(id, roleID) }) {
		return ScopedRoleMembership{}, notFound(roleID)
	}

	m, ok := b.lookup(info.ID)
	if !ok {
		return ScopedRoleMembership{}, notFound(info.ID)
	}

	user, isUser := m.(*User)
	if !isUser {
		return ScopedRoleMembership{}, badRequest("Invalid object type for the roleMemberInfo property.")
	}

	id := lc(u.ID)

	for _, s := range b.auRoles[id] {
		if strings.EqualFold(s.RoleID, roleID) && strings.EqualFold(s.RoleMemberInfo.ID, user.ID) {
			return ScopedRoleMembership{}, badRequest(
				"One or more added object references already exist for the following modified properties: 'scopedRoleMembers'.",
			)
		}
	}

	rec := ScopedRoleMembership{
		ID:                   newID(),
		AdministrativeUnitID: u.ID,
		RoleID:               lc(roleID),
		RoleMemberInfo: RoleMemberInfo{
			ID:                user.ID,
			DisplayName:       user.DisplayName,
			UserPrincipalName: user.UserPrincipalName,
		},
	}
	b.auRoles[id] = append(slices.Clone(b.auRoles[id]), rec)

	return rec, nil
}

func (b *Backend) dirRolesList() []string {
	out := make([]string, 0, len(b.dirRoles))
	for _, id := range b.dirRoles {
		out = append(out, id)
	}

	return out
}

// ScopedRoleMembers lists the scoped role members of an administrative unit.
func (b *Backend) ScopedRoleMembers(auID string) ([]ScopedRoleMembership, error) {
	b.mu.RLock("ScopedRoleMembers")
	defer b.mu.RUnlock()

	u, err := b.findAdminUnit(Ref{ID: auID})
	if err != nil {
		return nil, err
	}

	return slices.Clone(b.auRoles[lc(u.ID)]), nil
}

// RemoveScopedRoleMember deletes one scoped role membership.
func (b *Backend) RemoveScopedRoleMember(auID, id string) error {
	b.mu.Lock("RemoveScopedRoleMember")
	defer b.mu.Unlock()

	u, err := b.findAdminUnit(Ref{ID: auID})
	if err != nil {
		return err
	}

	key := lc(u.ID)

	i := slices.IndexFunc(b.auRoles[key], func(s ScopedRoleMembership) bool { return strings.EqualFold(s.ID, id) })
	if i < 0 {
		return notFound(id)
	}

	b.auRoles[key] = slices.Delete(slices.Clone(b.auRoles[key]), i, i+1)

	return nil
}

func (h *Handler) adminUnitSub(c *echo.Context, id string, tail []string) error {
	switch strings.ToLower(tail[0]) {
	case propMembers:
		return h.refs(c, id, tail[1:], refOps{
			add: h.Backend.AddAdministrativeUnitMember, remove: h.Backend.RemoveAdministrativeUnitMember,
			load: h.Backend.AdministrativeUnitMembers,
		})
	case segScopedRoleMembers:
		return h.scopedRoleMembers(c, id, tail[1:])
	}

	return h.writeErr(c, segmentErr(tail[0]))
}

func (h *Handler) scopedRoleMembers(c *echo.Context, id string, tail []string) error {
	r, b := c.Request(), h.Backend

	switch {
	case len(tail) == 0 && r.Method == http.MethodGet:
		items, err := b.ScopedRoleMembers(id)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.list(
			c,
			"scopedRoleMemberships",
			"ScopedRoleMembership",
			fieldsOf[ScopedRoleMembership](),
			mapAll(items),
		)
	case len(tail) == 0 && r.Method == http.MethodPost:
		body, err := readJSON(c)
		if err != nil {
			return h.writeErr(c, err)
		}

		rec, err := b.AddScopedRoleMember(id, body)
		if err != nil {
			return h.writeErr(c, err)
		}

		return h.entity(c, http.StatusCreated, "scopedRoleMemberships", toMap(rec))
	case len(tail) == 1 && r.Method == http.MethodGet:
		items, err := b.ScopedRoleMembers(id)
		if err != nil {
			return h.writeErr(c, err)
		}

		for _, s := range items {
			if strings.EqualFold(s.ID, tail[0]) {
				return h.entity(c, http.StatusOK, "scopedRoleMemberships", toMap(s))
			}
		}

		return h.writeErr(c, notFound(tail[0]))
	case len(tail) == 1 && r.Method == http.MethodDelete:
		return h.noContent(c, b.RemoveScopedRoleMember(id, tail[0]))
	}

	return h.methodNotAllowed(c)
}
