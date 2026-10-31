package azuregraph

import (
	"encoding/json"
	"strings"
	"unicode"
)

var userReadOnly = []string{"id", "createdDateTime"} //nolint:gochecknoglobals // fixed list

const minPasswordLen = 8

func (b *Backend) findUser(r Ref) (*User, error) {
	if r.Field != "" {
		return nil, badRequest("The alternate key '%s' is not supported on users.", r.Field)
	}

	if isGUID(r.ID) {
		if u, ok := b.users.get(lc(r.ID)); ok {
			return u, nil
		}

		return nil, notFound(r.ID)
	}

	for _, u := range b.users.all() {
		if strings.EqualFold(u.UserPrincipalName, r.ID) {
			return u, nil
		}
	}

	return nil, notFound(r.ID)
}

func passwordComplex(p string) bool {
	if len(p) < minPasswordLen {
		return false
	}

	var lower, upper, digit, other bool

	for _, r := range p {
		switch {
		case unicode.IsLower(r):
			lower = true
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsDigit(r):
			digit = true
		default:
			other = true
		}
	}

	n := 0
	for _, f := range []bool{lower, upper, digit, other} {
		if f {
			n++
		}
	}

	return n >= 3 //nolint:mnd // 3 of 4 character classes
}

func (b *Backend) validateUser(u *User, self string) error {
	local, dom, ok := strings.Cut(u.UserPrincipalName, "@")
	if !ok || local == "" || dom == "" {
		return badRequest("Invalid value specified for property 'userPrincipalName' of resource 'User'.")
	}

	if !b.verifiedDomain(dom) {
		return badRequest("The domain portion of the userPrincipalName property is invalid. " +
			"You must use one of the verified domain names in your organization.")
	}

	for _, o := range b.users.all() {
		if o.ID != self && strings.EqualFold(o.UserPrincipalName, u.UserPrincipalName) {
			return badRequest("Another object with the same value for property userPrincipalName already exists.")
		}
	}

	return nil
}

func requireProps(kind string, body map[string]json.RawMessage, props ...string) error {
	for _, p := range props {
		if v, ok := body[p]; !ok || string(v) == "null" {
			return badRequest("A value is required for property '%s' of resource '%s'.", p, kind)
		}
	}

	return nil
}

// CreateUser creates a user. passwordProfile is validated and discarded.
func (b *Backend) CreateUser(body map[string]json.RawMessage) (User, error) {
	if err := requireProps("User", body,
		"accountEnabled", "displayName", "mailNickname", "passwordProfile", "userPrincipalName"); err != nil {
		return User{}, err
	}

	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	seed := &User{ID: newID(), CreatedDateTime: b.stamp(), UserType: "Member"}
	seed.normalize()

	u, err := applyBody(seed, body, userReadOnly...)
	if err != nil {
		return User{}, err
	}

	if u.PasswordProfile == nil || !passwordComplex(u.PasswordProfile.Password) {
		return User{}, badRequest(
			"The specified password does not comply with password complexity requirements. Please provide a different password.")
	}

	u.PasswordProfile = nil

	if err = b.validateUser(u, u.ID); err != nil {
		return User{}, err
	}

	b.users.put(u.ID, u)

	return *u, nil
}

// GetUser returns a user by object ID or user principal name.
func (b *Backend) GetUser(r Ref) (User, error) {
	b.mu.RLock("GetUser")
	defer b.mu.RUnlock()

	u, err := b.findUser(r)
	if err != nil {
		return User{}, err
	}

	return *u, nil
}

// ListUsers returns every user in creation order.
func (b *Backend) ListUsers() []User {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	return b.users.slice()
}

// UpdateUser applies a PATCH body.
func (b *Backend) UpdateUser(r Ref, body map[string]json.RawMessage) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	cur, err := b.findUser(r)
	if err != nil {
		return err
	}

	next, err := applyBody(cur, body, userReadOnly...)
	if err != nil {
		return err
	}

	if next.PasswordProfile != nil && next.PasswordProfile.Password != "" && !passwordComplex(next.PasswordProfile.Password) {
		return badRequest(
			"The specified password does not comply with password complexity requirements. Please provide a different password.")
	}

	next.PasswordProfile = nil

	if err = b.validateUser(next, cur.ID); err != nil {
		return err
	}

	b.users.put(cur.ID, next)

	return nil
}

// DeleteUser removes a user.
func (b *Backend) DeleteUser(r Ref) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	u, err := b.findUser(r)
	if err != nil {
		return err
	}

	b.removeObject(u.ID)

	return nil
}
