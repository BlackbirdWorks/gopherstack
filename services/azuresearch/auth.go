package azuresearch

import (
	"strings"
)

// authenticate resolves the target service and the caller's role.
func (h *Handler) authenticate(req *request) error {
	b := h.Backend
	key := req.r.Header.Get(headerAPIKey)
	bearer := strings.HasPrefix(req.r.Header.Get("Authorization"), "Bearer ")

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

	return b.authorize(req, name, key, bearer)
}

func (b *InMemoryBackend) authorize(req *request, name, key string, bearer bool) error {
	b.mu.RLock("authorize")
	defer b.mu.RUnlock()

	s, err := b.svc(name)
	if err != nil {
		return ErrForbidden
	}

	cfg := s.Config
	if strings.EqualFold(cfg.PublicNetworkAccess, accessDisabled) {
		return ErrForbidden
	}

	switch {
	case bearer && (cfg.AuthMode == authAADOrAPIKey || cfg.DisableLocalAuth):
		req.role = roleAdmin

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
func (req *request) requireAdmin() error {
	if req.role != roleAdmin {
		return ErrForbidden
	}

	return nil
}
