package opensearch

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"golang.org/x/crypto/blake2b"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxActionGroupDepth = 8

var errBadDLSQuery = errors.New("invalid document level security query")

// SecurityPrincipal is an authenticated caller: an internal user or an IAM principal.
type SecurityPrincipal struct {
	Attributes   map[string]string
	Name         string
	BackendRoles []string
	IAM          bool
}

// String renders the principal the way OpenSearch security exceptions do.
func (p SecurityPrincipal) String() string {
	return fmt.Sprintf("User [name=%s, backend_roles=[%s], requestedTenant=null]",
		p.Name, strings.Join(p.BackendRoles, ","))
}

// indexGrant is one resolved index_permissions entry of a mapped role.
type indexGrant struct {
	DLS      string
	Patterns []string
	Actions  []string
	FLS      []string
	Masked   []string
}

// EffectivePerms is the flattened permission set of a principal.
type EffectivePerms struct {
	Roles     []string
	Cluster   []string
	Grants    []indexGrant
	Principal SecurityPrincipal
}

// wildcardMatch matches s against a security plugin pattern: * and ? globs, or /regex/.
func wildcardMatch(pattern, s string) bool {
	if len(pattern) >= 2 && strings.HasPrefix(pattern, "/") && strings.HasSuffix(pattern, "/") {
		re, err := regexp.Compile("^(?:" + pattern[1:len(pattern)-1] + ")$")

		return err == nil && re.MatchString(s)
	}

	return globMatch(pattern, s)
}

func globMatch(pattern, s string) bool {
	px, sx := 0, 0
	star, mark := -1, 0

	for sx < len(s) {
		switch {
		case px < len(pattern) && (pattern[px] == '?' || pattern[px] == s[sx]):
			px++
			sx++
		case px < len(pattern) && pattern[px] == '*':
			star, mark = px, sx
			px++
		case star >= 0:
			px = star + 1
			mark++
			sx = mark
		default:
			return false
		}
	}

	for px < len(pattern) && pattern[px] == '*' {
		px++
	}

	return px == len(pattern)
}

func matchesAny(patterns []string, s string) bool {
	return slices.ContainsFunc(patterns, func(p string) bool { return wildcardMatch(p, s) })
}

// expandActions resolves action group names (recursively) into raw action patterns.
func expandActions(names []string, groups map[string]*SecurityActionGroup) []string {
	var out []string

	var walk func(name string, depth int)

	walk = func(name string, depth int) {
		if depth > maxActionGroupDepth {
			return
		}

		g, ok := groups[name]
		if !ok {
			g, ok = groups[strings.ToLower(name)]
		}

		if !ok {
			out = append(out, name)

			return
		}

		for _, a := range g.AllowedActions {
			walk(a, depth+1)
		}
	}

	for _, n := range names {
		walk(n, 0)
	}

	return out
}

func substitutePrincipal(s string, p SecurityPrincipal) string {
	if !strings.Contains(s, "${") {
		return s
	}

	r := strings.NewReplacer("${user_name}", p.Name, "${user.name}", p.Name)
	s = r.Replace(s)

	for k, v := range p.Attributes {
		s = strings.ReplaceAll(s, "${attr.internal."+k+"}", v)
	}

	return s
}

func mappingMatches(m *RoleMapping, p SecurityPrincipal) bool {
	if matchesAny(m.Users, p.Name) {
		return true
	}

	for _, br := range p.BackendRoles {
		if matchesAny(m.BackendRoles, br) {
			return true
		}
	}

	if len(m.AndBackendRoles) == 0 {
		return false
	}

	for _, want := range m.AndBackendRoles {
		if !slices.ContainsFunc(p.BackendRoles, func(br string) bool { return wildcardMatch(want, br) }) {
			return false
		}
	}

	return true
}

func mergedRoles(cfg *SecurityConfig) map[string]*SecurityRole {
	out := builtinRoles()
	if cfg != nil {
		for n, r := range cfg.Roles {
			if _, reserved := out[n]; !reserved {
				out[n] = r
			}
		}
	}

	return out
}

func mergedActionGroups(cfg *SecurityConfig) map[string]*SecurityActionGroup {
	out := builtinActionGroups()
	if cfg != nil {
		for n, g := range cfg.ActionGroups {
			if _, reserved := out[n]; !reserved {
				out[n] = g
			}
		}
	}

	return out
}

// buildEffectivePerms resolves the roles mapped to p and flattens their permissions.
func buildEffectivePerms(cfg *SecurityConfig, p SecurityPrincipal) EffectivePerms {
	eff := EffectivePerms{Principal: p}

	if cfg == nil {
		return eff
	}

	roles := mergedRoles(cfg)
	groups := mergedActionGroups(cfg)

	names := make([]string, 0, len(cfg.RolesMapping))
	for n := range cfg.RolesMapping {
		names = append(names, n)
	}

	sort.Strings(names)

	for _, n := range names {
		role, ok := roles[n]
		if !ok || !mappingMatches(cfg.RolesMapping[n], p) {
			continue
		}

		eff.Roles = append(eff.Roles, n)
		eff.Cluster = append(eff.Cluster, expandActions(role.ClusterPermissions, groups)...)

		for _, ip := range role.IndexPermissions {
			patterns := make([]string, len(ip.IndexPatterns))
			for i, pat := range ip.IndexPatterns {
				patterns[i] = substitutePrincipal(pat, p)
			}

			eff.Grants = append(eff.Grants, indexGrant{
				Patterns: patterns,
				Actions:  expandActions(ip.AllowedActions, groups),
				DLS:      substitutePrincipal(ip.DLS, p),
				FLS:      ip.FLS,
				Masked:   ip.MaskedFields,
			})
		}
	}

	return eff
}

// hasRole reports whether any of the principal's mapped roles is in names.
func (e EffectivePerms) hasRole(names ...string) bool {
	return slices.ContainsFunc(e.Roles, func(r string) bool { return slices.Contains(names, r) })
}

// AllowCluster reports whether a cluster-level action is permitted.
func (e EffectivePerms) AllowCluster(action string) bool {
	return matchesAny(e.Cluster, action)
}

func (e EffectivePerms) grantsFor(index, action string) []indexGrant {
	var out []indexGrant

	for _, g := range e.Grants {
		if matchesAny(g.Patterns, index) && matchesAny(g.Actions, action) {
			out = append(out, g)
		}
	}

	return out
}

// AllowIndex reports whether an index-level action on index is permitted.
func (e EffectivePerms) AllowIndex(index, action string) bool {
	return len(e.grantsFor(index, action)) > 0
}

// fieldRule is the FLS / masking view of one granting index_permissions entry.
type fieldRule struct {
	includes []string
	excludes []string
	masks    []string
}

func (r fieldRule) allFields() bool { return len(r.includes) == 0 && len(r.excludes) == 0 }

func fieldPatternMatches(patterns []string, path string) bool {
	for p := path; p != ""; {
		if matchesAny(patterns, p) {
			return true
		}

		i := strings.LastIndex(p, ".")
		if i < 0 {
			break
		}

		p = p[:i]
	}

	return false
}

// descends reports whether a pattern may match a descendant of path (include-mode recursion).
func fieldPatternDescends(patterns []string, path string) bool {
	return slices.ContainsFunc(patterns, func(p string) bool { return strings.HasPrefix(p, path+".") })
}

func (r fieldRule) visible(path string) bool {
	switch {
	case len(r.includes) > 0:
		return fieldPatternMatches(r.includes, path)
	case len(r.excludes) > 0:
		return !fieldPatternMatches(r.excludes, path)
	default:
		return true
	}
}

func (r fieldRule) mayShowDescendant(path string) bool {
	return len(r.includes) > 0 && fieldPatternDescends(r.includes, path)
}

func maskField(spec string) (string, string) {
	field, rest, _ := strings.Cut(spec, "::")

	return field, rest
}

func (r fieldRule) maskSpec(path string) (string, bool) {
	for _, spec := range r.masks {
		field, rest := maskField(spec)
		if wildcardMatch(field, path) {
			return rest, true
		}
	}

	return "", false
}

func newFieldRule(g indexGrant) fieldRule {
	r := fieldRule{masks: g.Masked}

	for _, f := range g.FLS {
		if ex, ok := strings.CutPrefix(f, "~"); ok {
			r.excludes = append(r.excludes, ex)
		} else {
			r.includes = append(r.includes, f)
		}
	}

	if len(r.includes) > 0 {
		r.excludes = nil
	}

	return r
}

// accessRestriction is the combined DLS / FLS / masking applied to one index and action.
type accessRestriction struct {
	queries []map[string]any
	rules   []fieldRule
	hasDLS  bool
}

// restricted reports whether any DLS, FLS or masking applies.
func (a accessRestriction) restricted() bool {
	if a.hasDLS {
		return true
	}

	for _, r := range a.rules {
		if !r.allFields() || len(r.masks) > 0 {
			return true
		}
	}

	return false
}

func (a accessRestriction) fieldRestricted() bool {
	for _, r := range a.rules {
		if !r.allFields() || len(r.masks) > 0 {
			return true
		}
	}

	return false
}

// restrictionFor combines DLS / FLS / masking across every grant that allows the action.
// Any grant without DLS makes documents unrestricted; otherwise DLS queries are OR-ed.
func (e EffectivePerms) restrictionFor(index, action string) (accessRestriction, error) {
	grants := e.grantsFor(index, action)
	res := accessRestriction{}

	unrestricted := false

	for _, g := range grants {
		res.rules = append(res.rules, newFieldRule(g))

		if g.DLS == "" {
			unrestricted = true

			continue
		}

		var q map[string]any
		if err := json.Unmarshal([]byte(g.DLS), &q, jsonx.V1()); err != nil {
			return accessRestriction{}, fmt.Errorf("%w: %w", errBadDLSQuery, err)
		}

		res.queries = append(res.queries, q)
	}

	if unrestricted {
		res.queries = nil
	}

	res.hasDLS = len(res.queries) > 0

	return res, nil
}

// dlsQuery returns the OR of the DLS queries, or nil when documents are unrestricted.
func (a accessRestriction) dlsQuery() map[string]any {
	switch len(a.queries) {
	case 0:
		return nil
	case 1:
		return a.queries[0]
	default:
		should := make([]any, len(a.queries))
		for i, q := range a.queries {
			should[i] = q
		}

		return map[string]any{queryKeyBool: map[string]any{"should": should, "minimum_should_match": 1}}
	}
}

// docVisible reports whether a document passes the DLS filter.
func (a accessRestriction) docVisible(doc map[string]any) bool {
	q := a.dlsQuery()
	if q == nil {
		return true
	}

	pred, err := compileQuery(q)
	if err != nil {
		return false
	}

	return pred(doc)
}

func (a accessRestriction) pathVisible(path string) bool {
	for _, r := range a.rules {
		if r.visible(path) {
			return true
		}
	}

	return false
}

func (a accessRestriction) mayShowDescendant(path string) bool {
	return slices.ContainsFunc(a.rules, func(r fieldRule) bool { return r.mayShowDescendant(path) })
}

// maskSpecFor returns the mask applying to a visible path: every rule that shows the path must mask it.
func (a accessRestriction) maskSpecFor(path string) (string, bool) {
	spec := ""
	seen := false

	for _, r := range a.rules {
		if !r.visible(path) {
			continue
		}

		s, ok := r.maskSpec(path)
		if !ok {
			return "", false
		}

		if !seen {
			spec, seen = s, true
		}
	}

	return spec, seen
}

// Apply returns a copy of doc with FLS and field masking applied.
func (a accessRestriction) Apply(doc map[string]any) map[string]any {
	if !a.fieldRestricted() {
		return doc
	}

	return a.walk(doc, "")
}

func (a accessRestriction) walk(obj map[string]any, prefix string) map[string]any {
	out := make(map[string]any, len(obj))

	for k, v := range obj {
		path := prefix + k

		switch t := v.(type) {
		case map[string]any:
			if !a.pathVisible(path) && !a.mayShowDescendant(path) {
				continue
			}

			sub := a.walk(t, path+".")
			if len(sub) > 0 || a.pathVisible(path) {
				out[k] = sub
			}
		case []any:
			if kept, ok := a.walkArray(t, path); ok {
				out[k] = kept
			}
		default:
			if a.pathVisible(path) {
				out[k] = a.maskValue(path, v)
			}
		}
	}

	return out
}

func (a accessRestriction) walkArray(arr []any, path string) ([]any, bool) {
	hasMap := slices.ContainsFunc(arr, isJSONObject)
	if !hasMap {
		if !a.pathVisible(path) {
			return nil, false
		}

		out := make([]any, len(arr))
		for i, v := range arr {
			out[i] = a.maskValue(path, v)
		}

		return out, true
	}

	out := make([]any, 0, len(arr))

	for _, v := range arr {
		if m, ok := v.(map[string]any); ok {
			if sub := a.walk(m, path+"."); len(sub) > 0 {
				out = append(out, sub)
			}

			continue
		}

		if a.pathVisible(path) {
			out = append(out, v)
		}
	}

	return out, len(out) > 0
}

func (a accessRestriction) maskValue(path string, v any) any {
	spec, ok := a.maskSpecFor(path)
	if !ok {
		return v
	}

	s, isStr := v.(string)
	if !isStr {
		return v
	}

	return applyMask(spec, s)
}

// applyMask masks s: spec is empty (BLAKE2b-512), a hash algorithm name, or "/regex/::replacement".
func applyMask(spec, s string) string {
	switch {
	case spec == "":
		sum := blake2b.Sum512([]byte(s))

		return hex.EncodeToString(sum[:])
	case strings.EqualFold(spec, "SHA-256"):
		sum := sha256.Sum256([]byte(s))

		return hex.EncodeToString(sum[:])
	case strings.EqualFold(spec, "SHA-512"):
		sum := sha512.Sum512([]byte(s))

		return hex.EncodeToString(sum[:])
	case strings.HasPrefix(spec, "/"):
		pat, repl, _ := strings.Cut(spec, "::")

		re, err := regexp.Compile(strings.Trim(pat, "/"))
		if err != nil {
			return strings.Repeat("*", len(s))
		}

		return re.ReplaceAllString(s, repl)
	default:
		return strings.Repeat("*", len(s))
	}
}

func isJSONObject(v any) bool {
	_, ok := v.(map[string]any)

	return ok
}
