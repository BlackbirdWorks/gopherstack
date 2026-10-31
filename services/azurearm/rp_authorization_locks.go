package azurearm

import (
	"net/http"
	"sort"
	"strings"
)

const (
	lockCanNotDelete = "CanNotDelete"
	lockReadOnly     = "ReadOnly"
	lockCodeNotFound = "LockNotFound"
	lockCodeName     = "InvalidLockName"
	lockMaxName      = 260
	lockBadNameChars = `<>%&:\?/+`
)

func (p *AuthorizationProvider) doLock(req authReq) (authResp, error) {
	if req.Name == "" {
		if req.Method == http.MethodGet {
			return p.listLocks(req)
		}

		return authResp{}, ErrResourceNotFound
	}

	switch req.Method {
	case http.MethodGet:
		return p.getLock(req)
	case http.MethodPut:
		return p.putLock(req)
	case http.MethodDelete:
		return p.deleteLock(req)
	}

	return authResp{}, ErrResourceNotFound
}

func lockBody(l *storedLock) map[string]any {
	owners := make([]map[string]any, 0, len(l.Owners))
	for _, o := range l.Owners {
		owners = append(owners, map[string]any{"applicationId": o})
	}

	props := map[string]any{authFieldLevel: l.Level, "owners": owners}
	if l.Notes != "" {
		props["notes"] = l.Notes
	}

	return map[string]any{
		authFieldID: authID(l.Scope, authLocks, l.Name), fieldName: l.Name,
		fieldType: namespaceMicrosoftAuthorization + "/" + authLocks, fieldProperties: props,
	}
}

func (p *AuthorizationProvider) getLock(req authReq) (authResp, error) {
	p.mu.RLock("getLock")
	defer p.mu.RUnlock()

	l, ok := p.locks[authKey(req.Scope.id, req.Name)]
	if !ok {
		return authResp{}, authErr(http.StatusNotFound, lockCodeNotFound, "The lock '%s' could not be found.", req.Name)
	}

	return authResp{Status: http.StatusOK, Body: lockBody(l)}, nil
}

func (p *AuthorizationProvider) deleteLock(req authReq) (authResp, error) {
	p.mu.Lock("deleteLock")
	defer p.mu.Unlock()

	key := authKey(req.Scope.id, req.Name)

	l, ok := p.locks[key]
	if !ok {
		return authResp{Status: http.StatusNoContent}, nil
	}

	delete(p.locks, key)

	return authResp{Status: http.StatusOK, Body: lockBody(l)}, nil
}

func (p *AuthorizationProvider) listLocks(req authReq) (authResp, error) {
	f, err := parseAuthFilter(req.Query.Get("$filter"), nil, []string{authFnAtScope})
	if err != nil {
		return authResp{}, err
	}

	p.mu.RLock("listLocks")
	defer p.mu.RUnlock()

	var out []map[string]any

	for _, l := range p.locks {
		visible := scopeWithin(l.Scope, req.Scope.id)
		if f.hasFn(authFnAtScope) {
			visible = scopeWithin(req.Scope.id, l.Scope)
		}

		if visible {
			out = append(out, lockBody(l))
		}
	}

	return listBody(out), nil
}

func validLockName(name string) bool {
	return name != "" && len(name) <= lockMaxName && !strings.ContainsAny(name, lockBadNameChars) &&
		strings.IndexFunc(name, func(r rune) bool { return r < ' ' }) < 0
}

func (p *AuthorizationProvider) putLock(req authReq) (authResp, error) {
	if !validLockName(req.Name) {
		return authResp{}, authErr(http.StatusBadRequest, lockCodeName,
			"The lock name '%s' is invalid. Names are 1-%d characters and cannot contain %s.",
			req.Name, lockMaxName, lockBadNameChars)
	}

	props := mapOf(req.Body[fieldProperties])

	level := strOf(props, authFieldLevel)
	switch {
	case strings.EqualFold(level, lockCanNotDelete):
		level = lockCanNotDelete
	case strings.EqualFold(level, lockReadOnly):
		level = lockReadOnly
	default:
		return authResp{}, authBad("The lock level '%s' is invalid. Allowed values are CanNotDelete and ReadOnly.", level)
	}

	rec := &storedLock{Name: req.Name, Scope: req.Scope.id, Level: level, Notes: strOf(props, "notes")}

	if raw, ok := props["owners"].([]any); ok {
		for _, o := range raw {
			if app := strOf(mapOf(o), "applicationId"); app != "" {
				rec.Owners = append(rec.Owners, app)
			}
		}
	}

	p.mu.Lock("putLock")
	defer p.mu.Unlock()

	key := authKey(req.Scope.id, req.Name)
	_, exists := p.locks[key]
	p.locks[key] = rec

	status := http.StatusCreated
	if exists {
		status = http.StatusOK
	}

	return authResp{Status: status, Body: lockBody(rec)}, nil
}

// lockedScopes returns the scopes of the locks that forbid an operation on target: for a delete any lock at, above
// or below target; otherwise a ReadOnly lock at or above it.
func (p *AuthorizationProvider) lockedScopes(target string, isDelete bool) []string {
	p.mu.RLock("lockedScopes")
	defer p.mu.RUnlock()

	seen := map[string]bool{}

	var out []string

	for _, l := range p.locks {
		blocks := scopeWithin(target, l.Scope) && (isDelete || l.Level == lockReadOnly)
		if isDelete && !blocks {
			blocks = scopeWithin(l.Scope, target)
		}

		if blocks && !seen[strings.ToLower(l.Scope)] {
			seen[strings.ToLower(l.Scope)] = true

			out = append(out, l.Scope)
		}
	}

	sort.Strings(out)

	return out
}
