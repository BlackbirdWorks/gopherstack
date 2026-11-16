package azurearm

import "strings"

// Allowed reports whether principalID holds a role assignment at or above resourceID whose role grants
// action (matched against dataActions when data is set, else actions), after notActions/notDataActions.
func (p *AuthorizationProvider) Allowed(principalID, resourceID, action string, data bool) bool {
	p.mu.RLock("Allowed")
	defer p.mu.RUnlock()

	for _, a := range p.roleAssigns {
		if !strings.EqualFold(a.PrincipalID, principalID) || !scopeWithin(resourceID, a.Scope) {
			continue
		}

		for _, perm := range p.permissionsOf(a.RoleGUID) {
			if permits(perm, action, data) {
				return true
			}
		}
	}

	return false
}

func (p *AuthorizationProvider) permissionsOf(guid string) []authPermission {
	key := strings.ToLower(guid)
	if b, ok := p.builtinRoles[key]; ok {
		return b.Permissions
	}

	if d, ok := p.roleDefs[key]; ok {
		return d.Permissions
	}

	return nil
}

func permits(perm authPermission, action string, data bool) bool {
	allow, deny := perm.Actions, perm.NotActions
	if data {
		allow, deny = perm.DataActions, perm.NotDataActions
	}

	granted := false

	for _, pat := range allow {
		if globMatch(pat, action) {
			granted = true

			break
		}
	}

	if !granted {
		return false
	}

	for _, pat := range deny {
		if globMatch(pat, action) {
			return false
		}
	}

	return true
}

// globMatch matches an RBAC action pattern, where * stands for any run of characters, case-insensitively.
func globMatch(pattern, s string) bool {
	pattern, s = strings.ToLower(pattern), strings.ToLower(s)

	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == s
	}

	if !strings.HasPrefix(s, parts[0]) {
		return false
	}

	s = s[len(parts[0]):]
	last := parts[len(parts)-1]

	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(s, mid)
		if i < 0 {
			return false
		}

		s = s[i+len(mid):]
	}

	return strings.HasSuffix(s, last)
}
