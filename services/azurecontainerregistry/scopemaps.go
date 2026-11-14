package azurecontainerregistry

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	actionsRepoPrefix = "repositories/"
	tokenPassword1    = "password1"
	tokenPassword2    = "password2"
)

// scopeVerbs are the scope map action verbs on Learn: container-registry-token-based-repository-permissions.
func scopeVerbs() []string {
	return []string{"content/delete", "content/read", "content/write", "metadata/read", "metadata/write"}
}

// needVerb maps the access a request needs to the scope map verb that grants it.
func needVerb(action string) (string, bool) {
	switch action {
	case actionPull:
		return "content/read", true
	case actionPush:
		return "content/write", true
	case actionDelete:
		return "content/delete", true
	case actionMetaRead:
		return "metadata/read", true
	case actionMetaWrite:
		return "metadata/write", true
	}

	return "", false
}

// ScopeMap is a named set of repository permissions applied to tokens.
type ScopeMap struct {
	CreatedAt   time.Time `json:"createdAt"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Actions     []string  `json:"actions"`
	System      bool      `json:"system,omitempty"`
}

// TokenSecret is one of a token's two passwords; only a digest is kept.
type TokenSecret struct {
	Created time.Time  `json:"created"`
	Expiry  *time.Time `json:"expiry,omitempty"`
	Digest  string     `json:"digest"`
}

// Token authenticates with a generated password and is limited by its scope map.
type Token struct {
	CreatedAt time.Time      `json:"createdAt"`
	ScopeMap  string         `json:"scopeMap"`
	Name      string         `json:"name"`
	Passwords []*TokenSecret `json:"passwords,omitempty"`
	Disabled  bool           `json:"disabled,omitempty"`
}

// TokenPassword describes a token password; Value is set only when it has just been generated.
type TokenPassword struct {
	Created time.Time
	Expiry  *time.Time
	Name    string
	Value   string
}

func systemScopeMaps(created time.Time) map[string]*ScopeMap {
	mk := func(name, desc string, verbs ...string) *ScopeMap {
		actions := make([]string, 0, len(verbs))
		for _, v := range verbs {
			actions = append(actions, actionsRepoPrefix+"*/"+v)
		}

		return &ScopeMap{Name: name, Description: desc, Actions: actions, CreatedAt: created, System: true}
	}

	return map[string]*ScopeMap{
		"_repositories_admin": mk("_repositories_admin",
			"Can perform all read, write and delete operations on the registry", scopeVerbs()...),
		"_repositories_pull": mk("_repositories_pull", "Can pull any repository of the registry", "content/read"),
		"_repositories_push": mk("_repositories_push",
			"Can push to any repository of the registry", "content/read", "content/write"),
	}
}

// splitAction cuts repositories/<repo>/<content|metadata>/<verb> into repo and verb.
func splitAction(a string) (string, string, bool) {
	rest, ok := strings.CutPrefix(a, actionsRepoPrefix)
	if !ok {
		return "", "", false
	}

	for _, v := range scopeVerbs() {
		if repo, found := strings.CutSuffix(rest, "/"+v); found && repo != "" {
			return repo, v, true
		}
	}

	return "", "", false
}

// validAction requires a known verb; the repository may be * or end in /* with a single wildcard (Learn:
// container-registry-token-based-repository-permissions).
func validAction(a string) bool {
	repo, _, ok := splitAction(a)
	if !ok || strings.Count(repo, "*") > 1 {
		return false
	}

	return !strings.Contains(repo, "*") || repo == "*" || strings.HasSuffix(repo, "/*")
}

func repoMatches(pattern, repo string) bool {
	switch {
	case pattern == "*":
		return true
	case strings.HasSuffix(pattern, "/*"):
		return strings.HasPrefix(repo, strings.TrimSuffix(pattern, "*"))
	}

	return pattern == repo
}

// scopeMapAllows reports whether sm grants verb on repo; wildcard rules add up.
func scopeMapAllows(sm *ScopeMap, repo, verb string) bool {
	return slices.ContainsFunc(sm.Actions, func(a string) bool {
		pattern, v, ok := splitAction(a)

		return ok && v == verb && repoMatches(pattern, repo)
	})
}

func (r *registry) scopeMap(name string) (*ScopeMap, bool) {
	if sm, ok := systemScopeMaps(r.CreatedAt)[strs.Fold(name)]; ok {
		return sm, true
	}

	sm, ok := r.ScopeMaps[strs.Fold(name)]

	return sm, ok
}

// ScopeMaps lists the system and user scope maps of a registry, sorted by name.
func (b *InMemoryBackend) ScopeMaps(reg string) ([]ScopeMap, error) {
	b.mu.RLock("ScopeMaps")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return nil, ErrRegistryNotFound
	}

	var out []ScopeMap

	for _, sm := range systemScopeMaps(r.CreatedAt) {
		out = append(out, *sm)
	}

	for _, sm := range r.ScopeMaps {
		out = append(out, *sm)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// SetScopeMap creates or replaces a user scope map; system scope maps cannot be changed.
func (b *InMemoryBackend) SetScopeMap(reg, name, description string, actions []string) (ScopeMap, error) {
	if len(actions) == 0 || slices.ContainsFunc(actions, func(a string) bool { return !validAction(a) }) {
		return ScopeMap{}, ErrInvalidScopeMap
	}

	b.mu.Lock("SetScopeMap")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return ScopeMap{}, ErrRegistryNotFound
	}

	if _, system := systemScopeMaps(r.CreatedAt)[strs.Fold(name)]; system {
		return ScopeMap{}, ErrScopeMapReadOnly
	}

	if r.ScopeMaps == nil {
		r.ScopeMaps = map[string]*ScopeMap{}
	}

	key := strs.Fold(name)

	created := b.now().UTC()
	if old, exists := r.ScopeMaps[key]; exists {
		created = old.CreatedAt
	}

	sm := &ScopeMap{Name: name, Description: description, Actions: slices.Clone(actions), CreatedAt: created}
	r.ScopeMaps[key] = sm

	return *sm, nil
}

// DeleteScopeMap removes a user scope map that no token uses.
func (b *InMemoryBackend) DeleteScopeMap(reg, name string) error {
	b.mu.Lock("DeleteScopeMap")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return ErrRegistryNotFound
	}

	key := strs.Fold(name)
	if _, system := systemScopeMaps(r.CreatedAt)[key]; system {
		return ErrScopeMapReadOnly
	}

	if _, found := r.ScopeMaps[key]; !found {
		return ErrScopeMapNotFound
	}

	for _, t := range r.Tokens {
		if strs.Equal(t.ScopeMap, name) {
			return ErrScopeMapInUse
		}
	}

	delete(r.ScopeMaps, key)

	return nil
}

// SetToken creates a token or updates its scope map and status; passwords are kept.
func (b *InMemoryBackend) SetToken(reg, name, scopeMap string, enabled bool) error {
	b.mu.Lock("SetToken")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return ErrRegistryNotFound
	}

	if _, found := r.scopeMap(scopeMap); !found {
		return ErrScopeMapNotFound
	}

	if r.Tokens == nil {
		r.Tokens = map[string]*Token{}
	}

	key := strs.Fold(name)

	t, exists := r.Tokens[key]
	if !exists {
		t = &Token{Name: name, CreatedAt: b.now().UTC()}
		r.Tokens[key] = t
	}

	t.ScopeMap, t.Disabled = scopeMap, !enabled

	return nil
}

// DeleteToken removes a token and its passwords.
func (b *InMemoryBackend) DeleteToken(reg, name string) error {
	b.mu.Lock("DeleteToken")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return ErrRegistryNotFound
	}

	if _, found := r.Tokens[strs.Fold(name)]; !found {
		return ErrTokenNotFound
	}

	delete(r.Tokens, strs.Fold(name))

	return nil
}

func passwordSlot(which string) (int, bool) {
	switch strs.Fold(which) {
	case tokenPassword1:
		return 0, true
	case tokenPassword2:
		return 1, true
	}

	return 0, false
}

func passwordName(slot int) string {
	if slot == 0 {
		return tokenPassword1
	}

	return tokenPassword2
}

func digest(secret string) string {
	sum := sha256.Sum256([]byte(secret))

	return hex.EncodeToString(sum[:])
}

// GenerateTokenPassword replaces password1 or password2 of a token and returns the new value, which is never
// shown again.
func (b *InMemoryBackend) GenerateTokenPassword(reg, token, which string, expiry *time.Time) (TokenPassword, error) {
	slot, ok := passwordSlot(which)
	if !ok {
		return TokenPassword{}, ErrInvalidTokenName
	}

	b.mu.Lock("GenerateTokenPassword")
	defer b.mu.Unlock()

	r, found := b.regs[strs.Fold(reg)]
	if !found {
		return TokenPassword{}, ErrRegistryNotFound
	}

	t, found := r.Tokens[strs.Fold(token)]
	if !found {
		return TokenPassword{}, ErrTokenNotFound
	}

	for len(t.Passwords) < slot+1 {
		t.Passwords = append(t.Passwords, nil)
	}

	value := awsid.NewN("", passwordChars)
	t.Passwords[slot] = &TokenSecret{Created: b.now().UTC(), Expiry: expiry, Digest: digest(value)}

	created := t.Passwords[slot].Created

	return TokenPassword{Name: passwordName(slot), Created: created, Expiry: expiry, Value: value}, nil
}

// TokenPasswords lists the generated passwords of a token without their values.
func (b *InMemoryBackend) TokenPasswords(reg, token string) ([]TokenPassword, error) {
	b.mu.RLock("TokenPasswords")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return nil, ErrRegistryNotFound
	}

	t, ok := r.Tokens[strs.Fold(token)]
	if !ok {
		return nil, ErrTokenNotFound
	}

	var out []TokenPassword

	for i, s := range t.Passwords {
		if s != nil {
			out = append(out,
				TokenPassword{Name: passwordName(i), Created: s.Created, Expiry: s.Expiry})
		}
	}

	return out, nil
}

// VerifyToken reports whether user and password are an enabled token's unexpired credentials.
func (b *InMemoryBackend) VerifyToken(reg, user, password string) bool {
	b.mu.RLock("VerifyToken")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok || password == "" {
		return false
	}

	t, ok := r.Tokens[strs.Fold(user)]
	if !ok || t.Disabled {
		return false
	}

	sum := digest(password)
	now := b.now()

	for _, s := range t.Passwords {
		if s != nil && subtle.ConstantTimeCompare([]byte(sum), []byte(s.Digest)) == 1 &&
			(s.Expiry == nil || now.Before(*s.Expiry)) {
			return true
		}
	}

	return false
}

// tokenAllows reports whether the token's current scope map grants the access n; scope map edits apply at once.
func (b *InMemoryBackend) tokenAllows(reg, token string, n need) bool {
	if n.typ != scopeRepo {
		return false
	}

	verb, ok := needVerb(n.action)
	if !ok {
		return false
	}

	b.mu.RLock("tokenAllows")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return false
	}

	t, ok := r.Tokens[strs.Fold(token)]
	if !ok || t.Disabled {
		return false
	}

	sm, ok := r.scopeMap(t.ScopeMap)

	return ok && scopeMapAllows(sm, n.name, verb)
}

func scopeActionOrder() []string {
	return []string{actionPull, actionPush, actionDelete, actionMetaRead, actionMetaWrite}
}

// grantedScopes narrows requested repository scopes to what the token's scope map allows.
func (b *InMemoryBackend) grantedScopes(reg, token string, scopes []string) []string {
	var out []string

	for _, group := range scopes {
		for s := range strings.FieldsSeq(group) {
			parts := strings.SplitN(s, ":", scopeParts)
			if len(parts) != scopeParts || parts[0] != scopeRepo {
				continue
			}

			var granted []string

			for _, a := range scopeActionOrder() {
				if (slices.Contains(strings.Split(parts[2], ","), a) || parts[2] == "*") &&
					b.tokenAllows(reg, token, need{typ: scopeRepo, name: parts[1], action: a}) {
					granted = append(granted, a)
				}
			}

			if len(granted) > 0 {
				out = append(out, scopeRepo+":"+parts[1]+":"+strings.Join(granted, ","))
			}
		}
	}

	return out
}

// AccessTokenScopes returns the scopes recorded in an access token of reg, as repository:name:actions strings.
func (b *InMemoryBackend) AccessTokenScopes(tok, reg string) ([]string, bool) {
	access, ok := b.accessScopes(tok, reg)
	if !ok {
		return nil, false
	}

	out := make([]string, 0, len(access))
	for _, a := range access {
		out = append(out, a.Type+":"+a.Name+":"+strings.Join(a.Actions, ","))
	}

	return out, true
}

// TokenView is a token with the metadata of its passwords.
type TokenView struct {
	CreatedAt time.Time
	Name      string
	ScopeMap  string
	Passwords []TokenPassword
	Enabled   bool
}

// Tokens lists a registry's tokens, sorted by name.
func (b *InMemoryBackend) Tokens(reg string) ([]TokenView, error) {
	b.mu.RLock("Tokens")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return nil, ErrRegistryNotFound
	}

	out := make([]TokenView, 0, len(r.Tokens))

	for _, t := range r.Tokens {
		v := TokenView{Name: t.Name, ScopeMap: t.ScopeMap, CreatedAt: t.CreatedAt, Enabled: !t.Disabled}

		for i, s := range t.Passwords {
			if s != nil {
				v.Passwords = append(v.Passwords,
					TokenPassword{Name: passwordName(i), Created: s.Created, Expiry: s.Expiry})
			}
		}

		out = append(out, v)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}
