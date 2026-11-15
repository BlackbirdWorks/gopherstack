package azuresearch

import (
	"strings"
)

// authenticate resolves the target service and the caller's role.
func (h *Handler) authenticate(req *request) error {
	b := h.Backend
	key := req.r.Header.Get(headerAPIKey)
	token, bearer := strings.CutPrefix(req.r.Header.Get("Authorization"), "Bearer ")

	name := hostLabel(req.r)
	if name == "" || !b.HasService(name) {
		name = ""
	}

	if name == "" && key != "" {
		name, _ = b.owner(key)
	}

	if name == "" {
		if names := b.ServiceNames(); len(names) == 1 {
			name = names[0]
		}
	}

	if name == "" {
		return ErrForbidden
	}

	req.svc = name

	brole := roleAdmin

	if bearer {
		var err error
		if brole, err = h.bearerRole(name, token); err != nil {
			return err
		}
	}

	return b.authorize(req, name, key, bearer, brole)
}

// bearerRole resolves what a bearer token may do; without token validation any token is an administrator.
func (h *Handler) bearerRole(name, token string) (keyRole, error) {
	if !h.Settings.ValidateTokens || h.Authority == nil {
		return roleAdmin, nil
	}

	principal, err := h.Authority.Authenticate(token)
	if err != nil {
		return roleNone, ErrUnauthorized
	}

	rid := h.Backend.resourceID(name)
	checks := []struct {
		action string
		role   keyRole
		data   bool
	}{
		{actionDocsRead, roleQuery, true},
		{actionDocsWrite, roleWrite, true},
		{actionObjects, roleObjects, false},
	}

	role := roleNone

	for _, c := range checks {
		if h.Authority.Allowed(principal, rid, c.action, c.data) {
			role |= c.role
		}
	}

	if role == roleNone {
		return roleNone, ErrForbidden
	}

	return role, nil
}

func (b *InMemoryBackend) resourceID(name string) string {
	b.mu.RLock("resourceID")
	defer b.mu.RUnlock()

	if s, err := b.svc(name); err == nil {
		return s.Config.ResourceID
	}

	return ""
}

func (b *InMemoryBackend) authorize(req *request, name, key string, bearer bool, brole keyRole) error {
	b.mu.RLock("authorize")
	defer b.mu.RUnlock()

	s, err := b.svc(name)
	if err != nil {
		return ErrForbidden
	}

	cfg := s.Config
	if strings.EqualFold(cfg.PublicNetworkAccess, accessDisabled) || !ipAllowed(req.r.RemoteAddr, cfg.IPRules) {
		return ErrForbidden
	}

	switch {
	case bearer && (cfg.AuthMode == authAADOrAPIKey || cfg.DisableLocalAuth):
		req.role = brole

		return nil
	case key != "" && !cfg.DisableLocalAuth:
		req.role = s.roleOf(key)
	}

	if req.role != roleNone {
		return nil
	}

	if cfg.AuthMode == authAADOrAPIKey && cfg.AADFailureMode != "http403" && key == "" {
		return ErrUnauthorized
	}

	return ErrForbidden
}

// requireAdmin rejects query-key callers.
func (req *request) requireAdmin() error { return req.require(roleObjects) }

func (req *request) requireWrite() error { return req.require(roleWrite) }

func (req *request) require(r keyRole) error {
	if req.role&r == 0 {
		return ErrForbidden
	}

	return nil
}

// TokenAuthority validates Microsoft Entra bearer tokens and evaluates role assignments.
type TokenAuthority interface {
	Authenticate(token string) (principalID string, err error)
	Allowed(principalID, resourceID, action string, data bool) bool
}

const (
	actionDocsRead  = "Microsoft.Search/searchServices/indexes/documents/read"
	actionDocsWrite = "Microsoft.Search/searchServices/indexes/documents/write"
	actionObjects   = "Microsoft.Search/searchServices/indexes/write"
)
