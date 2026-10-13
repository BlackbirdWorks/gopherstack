package cognitoidp

import (
	"fmt"
	"strings"
)

const (
	attrPreferredUsername = "preferred_username"
)

// usernameCaseSensitive reports the pool's UsernameConfiguration.CaseSensitive (true unless set false).
func usernameCaseSensitive(pool *UserPool) bool {
	if v, ok := pool.Settings.UsernameConfiguration["CaseSensitive"].(bool); ok {
		return v
	}

	return true
}

// resolveLoginNameLocked maps a sign-in identifier to the stored username: an exact username, a
// case-insensitive match when the pool is case-insensitive, or a UsernameAttributes/AliasAttributes
// value (email, phone number, preferred username). Unmatched input is returned unchanged.
// Caller must hold b.mu.
func (b *InMemoryBackend) resolveLoginNameLocked(pool *UserPool, input string) string {
	if _, ok := b.users.Get(userKey(pool.ID, input)); ok {
		return input
	}

	caseSensitive := usernameCaseSensitive(pool)
	same := func(a, b string) bool {
		if caseSensitive {
			return a == b
		}

		return strings.EqualFold(a, b)
	}

	users := b.usersByPool.Get(pool.ID)

	for _, u := range users {
		if same(u.Username, input) {
			return u.Username
		}
	}

	for _, u := range users {
		if loginAliasMatches(pool, u, input, same) {
			return u.Username
		}
	}

	return input
}

func loginAliasMatches(pool *UserPool, u *User, input string, same func(a, b string) bool) bool {
	for _, attr := range pool.Settings.UsernameAttributes {
		if v := u.Attributes[attr]; v != "" && same(v, input) {
			return true
		}
	}

	for _, attr := range pool.Settings.AliasAttributes {
		v := u.Attributes[attr]
		if v == "" || !same(v, input) {
			continue
		}

		if attr == attrPreferredUsername || u.Attributes[attr+"_verified"] == attrVerifiedTrue {
			return true
		}
	}

	return false
}

// usernameExistsLocked reports whether username is taken in poolID, ignoring case when the pool is
// case-insensitive. Caller must hold b.mu.
func (b *InMemoryBackend) usernameExistsLocked(poolID, username string) bool {
	if _, ok := b.users.Get(userKey(poolID, username)); ok {
		return true
	}

	pool, ok := b.pools.Get(poolID)
	if !ok || usernameCaseSensitive(pool) {
		return false
	}

	for _, u := range b.usersByPool.Get(poolID) {
		if strings.EqualFold(u.Username, username) {
			return true
		}
	}

	return false
}

// aliasOwnersLocked lists the other users that already own an alias user holds, as (owner, attribute) pairs.
func (b *InMemoryBackend) aliasOwnersLocked(pool *UserPool, user *User) []aliasClaim {
	caseSensitive := usernameCaseSensitive(pool)
	same := func(a, b string) bool {
		if caseSensitive {
			return a == b
		}

		return strings.EqualFold(a, b)
	}

	var claims []aliasClaim

	for _, attr := range pool.Settings.AliasAttributes {
		v := user.Attributes[attr]
		if v == "" || (attr != attrPreferredUsername && user.Attributes[attr+"_verified"] != attrVerifiedTrue) {
			continue
		}

		for _, other := range b.usersByPool.Get(pool.ID) {
			if other != user && same(other.Attributes[attr], v) && ownsAlias(other, attr) {
				claims = append(claims, aliasClaim{owner: other, attr: attr})
			}
		}
	}

	return claims
}

type aliasClaim struct {
	owner *User
	attr  string
}

func ownsAlias(u *User, attr string) bool {
	return attr == attrPreferredUsername || u.Attributes[attr+"_verified"] == attrVerifiedTrue
}

// claimAliasesLocked gives user the sign-in aliases it holds. An alias another user already owns
// fails with AliasExistsException, unless force is set, which takes it from that user. Caller holds b.mu.
func (b *InMemoryBackend) claimAliasesLocked(pool *UserPool, user *User, force bool) error {
	claims := b.aliasOwnersLocked(pool, user)

	if len(claims) > 0 && !force {
		return fmt.Errorf("%w: an account with the given alias already exists", ErrAliasExists)
	}

	for _, c := range claims {
		if c.attr == attrPreferredUsername {
			delete(c.owner.Attributes, c.attr)
		} else {
			c.owner.Attributes[c.attr+"_verified"] = "false"
		}
	}

	return nil
}
