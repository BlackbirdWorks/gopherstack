package opensearch

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/crypto/bcrypt"
)

const (
	anonymousAuthWindow  = 30 * 24 * time.Hour
	minMasterPasswordLen = 8
	maxMasterPasswordLen = 128
	bcryptMaxPasswordLen = 72
	minElasticFGACMinor  = 7
	elasticFGACMajor     = 6
	minSessionTimeout    = 1
	maxSessionTimeout    = 1440
)

var (
	iamMasterARNPattern = regexp.MustCompile(`^arn:aws[a-z-]*:iam::\d{12}:(user|role)/.+$`)
	elasticVersionRe    = regexp.MustCompile(`^Elasticsearch_(\d+)\.(\d+)$`)
)

// hashMasterPassword returns a bcrypt hash; the password itself is never stored.
func hashMasterPassword(password string) string {
	hash, err := bcrypt.GenerateFromPassword(truncatePassword(password), bcrypt.DefaultCost)
	if err != nil {
		return ""
	}

	return string(hash)
}

func truncatePassword(password string) []byte {
	b := []byte(password)
	if len(b) > bcryptMaxPasswordLen {
		b = b[:bcryptMaxPasswordLen]
	}

	return b
}

// masterPasswordMatches verifies a password against a bcrypt hash or a legacy "<salt>$<pbkdf2>" hash.
func masterPasswordMatches(stored, password string) bool {
	if strings.HasPrefix(stored, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(stored), truncatePassword(password)) == nil
	}

	return legacyPasswordMatches(stored, password)
}

// secError is a security plugin REST API error.
type secError struct {
	Extra   map[string]any
	Code    string
	Message string
	Status  int
}

func (e *secError) Error() string { return e.Message }

func secNotFound(name string) *secError {
	return &secError{Status: http.StatusNotFound, Code: secCodeNotFound, Message: "Resource '" + name + "' not found."}
}

func secDomainMissing() *secError {
	return &secError{Status: http.StatusNotFound, Code: secCodeNotFound, Message: "domain not found"}
}

func secForbidden(msg string) *secError {
	return &secError{Status: http.StatusForbidden, Code: "FORBIDDEN", Message: msg}
}

func secBadRequest(msg string) *secError {
	return &secError{Status: http.StatusBadRequest, Code: jsonKeyError, Message: msg}
}

func secInvalidKeys(keys []string) *secError {
	sort.Strings(keys)

	return &secError{
		Status: http.StatusBadRequest, Code: jsonKeyError, Message: "Invalid configuration",
		Extra: map[string]any{"invalid_keys": map[string]any{"keys": strings.Join(keys, ",")}},
	}
}

func secMissingKeys(keys ...string) *secError {
	return &secError{
		Status: http.StatusBadRequest, Code: jsonKeyError, Message: "Invalid configuration",
		Extra: map[string]any{"missing_mandatory_keys": map[string]any{"keys": strings.Join(keys, ",")}},
	}
}

func newSecurityConfig(domain string) *SecurityConfig {
	return &SecurityConfig{
		Domain:       domain,
		Users:        map[string]*InternalUser{},
		Roles:        map[string]*SecurityRole{},
		RolesMapping: map[string]*RoleMapping{},
		ActionGroups: map[string]*SecurityActionGroup{},
		Tenants:      map[string]*SecurityTenant{},
	}
}

// ensureSecurityConfigLocked returns the domain's security config, creating an empty one.
func (b *InMemoryBackend) ensureSecurityConfigLocked(domain string) *SecurityConfig {
	cfg, ok := b.securityConfigs.Get(domain)
	if !ok {
		cfg = newSecurityConfig(domain)
		b.securityConfigs.Put(cfg)
	}

	cfg.initMaps()

	return cfg
}

// initMaps allocates any map a restored snapshot left nil.
func (c *SecurityConfig) initMaps() {
	if c.Users == nil {
		c.Users = map[string]*InternalUser{}
	}

	if c.Roles == nil {
		c.Roles = map[string]*SecurityRole{}
	}

	if c.RolesMapping == nil {
		c.RolesMapping = map[string]*RoleMapping{}
	}

	if c.ActionGroups == nil {
		c.ActionGroups = map[string]*SecurityActionGroup{}
	}

	if c.Tenants == nil {
		c.Tenants = map[string]*SecurityTenant{}
	}
}

func addMappingMember(cfg *SecurityConfig, role string, users, backendRoles []string) {
	m, ok := cfg.RolesMapping[role]
	if !ok {
		m = &RoleMapping{}
		cfg.RolesMapping[role] = m
	}

	for _, u := range users {
		if u != "" && !slices.Contains(m.Users, u) {
			m.Users = append(m.Users, u)
		}
	}

	for _, br := range backendRoles {
		if br != "" && !slices.Contains(m.BackendRoles, br) {
			m.BackendRoles = append(m.BackendRoles, br)
		}
	}
}

// syncSecurityLocked seeds the security config from the domain's AdvancedSecurityOptions: the master
// user is stored in the internal user database and mapped to all_access and security_manager.
func (b *InMemoryBackend) syncSecurityLocked(d *Domain, prev *AdvancedSecurityOptions) {
	aso := d.AdvancedSecurityOptions
	if aso == nil || !aso.Enabled {
		return
	}

	cfg := b.ensureSecurityConfigLocked(d.Name)
	changed := prev == nil || !prev.Enabled || prev.MasterUserName != aso.MasterUserName ||
		prev.MasterUserARN != aso.MasterUserARN

	seedInternalMaster(cfg, aso, changed, prev == nil || prev.MasterUserPasswordHash != aso.MasterUserPasswordHash)

	if aso.MasterUserARN != "" && changed {
		mapMaster(cfg, nil, []string{aso.MasterUserARN})
	}

	if s := aso.SAMLOptions; s != nil && s.Enabled {
		mapMaster(cfg, []string{s.MasterUserName}, []string{s.MasterBackendRole})
	}
}

func mapMaster(cfg *SecurityConfig, users, backendRoles []string) {
	for _, role := range []string{roleAllAccess, roleSecurityManager} {
		addMappingMember(cfg, role, users, backendRoles)
	}
}

func seedInternalMaster(cfg *SecurityConfig, aso *AdvancedSecurityOptions, mapToRoles, newPassword bool) {
	if !aso.InternalUserDatabaseEnabled || aso.MasterUserName == "" {
		return
	}

	u, ok := cfg.Users[aso.MasterUserName]
	if !ok {
		u = &InternalUser{BackendRoles: []string{}, Attributes: map[string]string{}}
		cfg.Users[aso.MasterUserName] = u
	}

	if aso.MasterUserPasswordHash != "" && (newPassword || u.Hash == "") {
		u.Hash = aso.MasterUserPasswordHash
	}

	if mapToRoles {
		mapMaster(cfg, []string{aso.MasterUserName}, nil)
	}
}

// seedAllSecurityLocked re-derives security configs for every FGAC domain lacking one (restore of an old snapshot).
func (b *InMemoryBackend) seedAllSecurityLocked() {
	for _, d := range b.domains.All() {
		if d.AdvancedSecurityOptions != nil && d.AdvancedSecurityOptions.Enabled {
			if _, ok := b.securityConfigs.Get(d.Name); !ok {
				b.syncSecurityLocked(d, nil)
			}
		}
	}
}

// finalizeAdvancedSecurityLocked runs after the options are applied: it stamps the anonymous-auth
// window and seeds the security config.
func (b *InMemoryBackend) finalizeAdvancedSecurityLocked(d *Domain, prev *AdvancedSecurityOptions) {
	aso := d.AdvancedSecurityOptions
	if aso == nil {
		return
	}

	switch {
	case !aso.AnonymousAuthEnabled:
		aso.AnonymousAuthDisableDate = time.Time{}
	case prev == nil || !prev.AnonymousAuthEnabled || prev.AnonymousAuthDisableDate.IsZero():
		aso.AnonymousAuthDisableDate = b.clock().Add(anonymousAuthWindow)
	default:
		aso.AnonymousAuthDisableDate = prev.AnonymousAuthDisableDate
	}

	b.syncSecurityLocked(d, prev)
}

// mergeAdvancedSecurity keeps stored values for members an update request omitted.
func mergeAdvancedSecurity(prev, next *AdvancedSecurityOptions) {
	if prev == nil || next == nil {
		return
	}

	if !next.fieldsSet.enabled {
		next.Enabled = prev.Enabled
	}

	if !next.fieldsSet.internal {
		next.InternalUserDatabaseEnabled = prev.InternalUserDatabaseEnabled
	}

	if !next.fieldsSet.anonymous {
		next.AnonymousAuthEnabled = prev.AnonymousAuthEnabled
	}

	if next.SAMLOptions == nil {
		next.SAMLOptions = prev.SAMLOptions
	}

	if next.JWTOptions == nil {
		next.JWTOptions = prev.JWTOptions
	}

	if next.IAMFederationOptions == nil {
		next.IAMFederationOptions = prev.IAMFederationOptions
	}

	if next.MasterUserName == "" && next.MasterUserARN == "" {
		next.MasterUserName, next.MasterUserARN = prev.MasterUserName, prev.MasterUserARN
		next.MasterUserPasswordHash = prev.MasterUserPasswordHash
	}
}

// engineSupportsFGAC reports whether the engine version supports fine-grained access control.
func engineSupportsFGAC(version string) bool {
	m := elasticVersionRe.FindStringSubmatch(version)
	if m == nil {
		return true
	}

	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])

	return major > elasticFGACMajor || (major == elasticFGACMajor && minor >= minElasticFGACMinor)
}

func fgacRequirementError(d *Domain) error {
	switch {
	case !engineSupportsFGAC(d.EngineVersion):
		return fmt.Errorf(
			"%w: Fine-grained access control requires Elasticsearch 6.7 or later or OpenSearch",
			ErrValidation,
		)
	case d.EncryptionAtRestOptions == nil || !d.EncryptionAtRestOptions.Enabled:
		return fmt.Errorf("%w: You must enable encryption at rest to use fine-grained access control", ErrValidation)
	case d.NodeToNodeEncryptionOptions == nil || !d.NodeToNodeEncryptionOptions.Enabled:
		return fmt.Errorf(
			"%w: You must enable node-to-node encryption to use fine-grained access control",
			ErrValidation,
		)
	case d.DomainEndpointOptions == nil || !d.DomainEndpointOptions.EnforceHTTPS:
		return fmt.Errorf("%w: You must enforce HTTPS (EnforceHTTPS) to use fine-grained access control", ErrValidation)
	}

	return nil
}

func masterUserError(aso, prev *AdvancedSecurityOptions) error {
	alreadyEnabled := prev != nil && prev.Enabled

	if aso.InternalUserDatabaseEnabled {
		switch {
		case aso.MasterUserARN != "":
			return fmt.Errorf(
				"%w: MasterUserARN can only be set when InternalUserDatabaseEnabled is false",
				ErrValidation,
			)
		case aso.MasterUserName == "" || aso.MasterUserPasswordHash == "":
			if !alreadyEnabled || aso.MasterUserName == "" {
				return fmt.Errorf("%w: MasterUserName and MasterUserPassword are required when "+
					"InternalUserDatabaseEnabled is true", ErrValidation)
			}
		}

		return nil
	}

	if aso.MasterUserName != "" || aso.MasterUserPasswordHash != "" {
		return fmt.Errorf("%w: MasterUserName and MasterUserPassword can only be set when "+
			"InternalUserDatabaseEnabled is true", ErrValidation)
	}

	if aso.MasterUserARN == "" {
		return fmt.Errorf("%w: MasterUserARN is required when InternalUserDatabaseEnabled is false", ErrValidation)
	}

	return nil
}

func federationOptionsError(aso *AdvancedSecurityOptions) error {
	if s := aso.SAMLOptions; s != nil && s.Enabled {
		if s.IDPEntityID == "" || s.IDPMetadataContent == "" {
			return fmt.Errorf("%w: SAMLOptions.Idp requires EntityId and MetadataContent", ErrValidation)
		}

		if s.SessionTimeoutMinutes != 0 &&
			(s.SessionTimeoutMinutes < minSessionTimeout || s.SessionTimeoutMinutes > maxSessionTimeout) {
			return fmt.Errorf("%w: SAMLOptions.SessionTimeoutMinutes must be between %d and %d",
				ErrValidation, minSessionTimeout, maxSessionTimeout)
		}
	}

	if j := aso.JWTOptions; j != nil && j.Enabled {
		if j.PublicKey == "" && j.JwksURL == "" {
			return fmt.Errorf("%w: JWTOptions requires PublicKey or JwksUrl", ErrValidation)
		}

		if j.PublicKey != "" && j.JwksURL != "" {
			return fmt.Errorf("%w: JWTOptions accepts only one of PublicKey and JwksUrl", ErrValidation)
		}
	}

	return nil
}

// validateAdvancedSecurity enforces the fine-grained access control prerequisites AWS documents:
// https://docs.aws.amazon.com/opensearch-service/latest/developerguide/fgac.html
func validateAdvancedSecurity(d *Domain, prev *AdvancedSecurityOptions) error {
	aso := d.AdvancedSecurityOptions
	if aso == nil {
		return nil
	}

	if prev != nil && prev.Enabled && !aso.Enabled {
		return fmt.Errorf("%w: Fine-grained access control cannot be disabled once it is enabled", ErrValidation)
	}

	if !aso.Enabled {
		if aso.AnonymousAuthEnabled ||
			(aso.SAMLOptions != nil && aso.SAMLOptions.Enabled) ||
			(aso.JWTOptions != nil && aso.JWTOptions.Enabled) ||
			(aso.IAMFederationOptions != nil && aso.IAMFederationOptions.Enabled) {
			return fmt.Errorf("%w: SAML, JWT, IAM federation and anonymous auth require fine-grained access control",
				ErrValidation)
		}

		return nil
	}

	if err := fgacRequirementError(d); err != nil {
		return err
	}

	if err := masterUserError(aso, prev); err != nil {
		return err
	}

	return federationOptionsError(aso)
}

// validateAdvancedSecurityRequest checks request-only members (the plaintext password).
func validateAdvancedSecurityRequest(aso *advancedSecurityOptionsJSON) error {
	if aso == nil || aso.MasterUserOptions == nil {
		return nil
	}

	mu := aso.MasterUserOptions

	if mu.MasterUserARN != "" && !iamMasterARNPattern.MatchString(mu.MasterUserARN) {
		return fmt.Errorf("%w: MasterUserARN %q is not a valid IAM user or role ARN", ErrValidation, mu.MasterUserARN)
	}

	if mu.MasterUserPassword != "" {
		return masterPasswordComplexity(mu.MasterUserPassword)
	}

	return nil
}

func masterPasswordComplexity(pw string) error {
	if len(pw) < minMasterPasswordLen || len(pw) > maxMasterPasswordLen {
		return fmt.Errorf("%w: MasterUserPassword must be between %d and %d characters",
			ErrValidation, minMasterPasswordLen, maxMasterPasswordLen)
	}

	var upper, lower, digit, special bool

	for _, r := range pw {
		switch {
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsLower(r):
			lower = true
		case unicode.IsDigit(r):
			digit = true
		default:
			special = true
		}
	}

	if !upper || !lower || !digit || !special {
		return fmt.Errorf("%w: MasterUserPassword must contain at least one uppercase letter, one lowercase "+
			"letter, one number and one special character", ErrValidation)
	}

	return nil
}

// AuthenticateInternalUser verifies basic credentials against the domain's internal user database.
func (b *InMemoryBackend) AuthenticateInternalUser(domain, user, pass string) (securityPrincipal, bool) {
	b.mu.RLock("AuthenticateInternalUser")
	cfg, ok := b.securityConfigs.Get(domain)

	var (
		hash string
		p    securityPrincipal
	)

	if ok {
		if u, found := cfg.Users[user]; found {
			hash = u.Hash
			p = securityPrincipal{
				Name: user, BackendRoles: slices.Clone(u.BackendRoles), Attributes: maps.Clone(u.Attributes),
			}
		}
	}

	b.mu.RUnlock()

	if hash == "" {
		return securityPrincipal{}, false
	}

	if !masterPasswordMatches(hash, pass) {
		return securityPrincipal{}, false
	}

	return p, true
}

// EffectivePerms resolves the permissions of an authenticated principal.
func (b *InMemoryBackend) EffectivePerms(domain string, p securityPrincipal) effectivePerms {
	b.mu.RLock("EffectivePerms")
	defer b.mu.RUnlock()

	cfg, _ := b.securityConfigs.Get(domain)

	return buildEffectivePerms(cfg, p)
}

// AnonymousAuthActive reports whether anonymous requests are currently accepted for the options.
func (b *InMemoryBackend) AnonymousAuthActive(aso *AdvancedSecurityOptions) bool {
	return aso.AnonymousAuthEnabled && b.clock().Before(aso.AnonymousAuthDisableDate)
}

// securityKind names a security plugin REST API resource collection.
type securityKind string

const (
	kindInternalUsers securityKind = "internalusers"
	kindRoles         securityKind = "roles"
	kindRolesMapping  securityKind = "rolesmapping"
	kindActionGroups  securityKind = "actiongroups"
	kindTenants       securityKind = "tenants"
)

func (k securityKind) valid() bool {
	switch k {
	case kindInternalUsers, kindRoles, kindRolesMapping, kindActionGroups, kindTenants:
		return true
	default:
		return false
	}
}

func userView(u *InternalUser) map[string]any {
	return map[string]any{
		"hash":              "",
		"reserved":          u.Reserved,
		"hidden":            u.Hidden,
		jsonKeyBackendRoles: nonNilStrings(u.BackendRoles),
		"attributes":        nonNilAttrs(u.Attributes),
		"description":       u.Description,
		"static":            false,
	}
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

func nonNilAttrs(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return m
}

func roleView(r *SecurityRole) *SecurityRole {
	cp := *r
	cp.ClusterPermissions = nonNilStrings(r.ClusterPermissions)

	cp.IndexPermissions = make([]RoleIndexPermission, len(r.IndexPermissions))
	for i, ip := range r.IndexPermissions {
		ip.IndexPatterns = nonNilStrings(ip.IndexPatterns)
		ip.AllowedActions = nonNilStrings(ip.AllowedActions)
		ip.FLS = nonNilStrings(ip.FLS)
		ip.MaskedFields = nonNilStrings(ip.MaskedFields)
		cp.IndexPermissions[i] = ip
	}

	cp.TenantPermissions = make([]RoleTenantPermission, len(r.TenantPermissions))
	for i, tp := range r.TenantPermissions {
		tp.TenantPatterns = nonNilStrings(tp.TenantPatterns)
		tp.AllowedActions = nonNilStrings(tp.AllowedActions)
		cp.TenantPermissions[i] = tp
	}

	return &cp
}

func mappingView(m *RoleMapping) *RoleMapping {
	cp := *m
	cp.BackendRoles = nonNilStrings(m.BackendRoles)
	cp.Hosts = nonNilStrings(m.Hosts)
	cp.Users = nonNilStrings(m.Users)
	cp.AndBackendRoles = nonNilStrings(m.AndBackendRoles)

	return &cp
}

func groupView(g *SecurityActionGroup) *SecurityActionGroup {
	cp := *g
	cp.AllowedActions = nonNilStrings(g.AllowedActions)

	return &cp
}

// securityView returns the API representation of one resource.
func securityView(cfg *SecurityConfig, kind securityKind, name string) (any, bool) {
	switch kind {
	case kindInternalUsers:
		return viewOf(cfg.Users, name, func(u *InternalUser) (any, bool) { return userView(u), !u.Hidden })
	case kindRoles:
		return viewOf(mergedRoles(cfg), name, func(r *SecurityRole) (any, bool) { return roleView(r), !r.Hidden })
	case kindRolesMapping:
		return viewOf(cfg.RolesMapping, name, func(m *RoleMapping) (any, bool) { return mappingView(m), !m.Hidden })
	case kindActionGroups:
		return viewOf(mergedActionGroups(cfg), name, func(g *SecurityActionGroup) (any, bool) {
			return groupView(g), !g.Hidden
		})
	case kindTenants:
		return viewOf(cfg.Tenants, name, func(t *SecurityTenant) (any, bool) { return t, !t.Hidden })
	}

	return nil, false
}

func viewOf[T any](m map[string]*T, name string, view func(*T) (any, bool)) (any, bool) {
	v, ok := m[name]
	if !ok {
		return nil, false
	}

	out, visible := view(v)
	if !visible {
		return nil, false
	}

	return out, true
}

func securityNames(cfg *SecurityConfig, kind securityKind) []string {
	var names []string

	switch kind {
	case kindInternalUsers:
		names = slices.Collect(maps.Keys(cfg.Users))
	case kindRoles:
		names = slices.Collect(maps.Keys(mergedRoles(cfg)))
	case kindRolesMapping:
		names = slices.Collect(maps.Keys(cfg.RolesMapping))
	case kindActionGroups:
		names = slices.Collect(maps.Keys(mergedActionGroups(cfg)))
	case kindTenants:
		names = slices.Collect(maps.Keys(cfg.Tenants))
	}

	sort.Strings(names)

	return names
}

func (b *InMemoryBackend) securityConfigRLocked(domain string) (*SecurityConfig, *secError) {
	if !b.domains.Has(domain) {
		return nil, secDomainMissing()
	}

	cfg, ok := b.securityConfigs.Get(domain)
	if !ok {
		return newSecurityConfig(domain), nil
	}

	return cfg, nil
}

// SecurityList returns all visible resources of a kind keyed by name.
func (b *InMemoryBackend) SecurityList(domain string, kind securityKind) (map[string]any, *secError) {
	b.mu.RLock("SecurityList")
	defer b.mu.RUnlock()

	cfg, err := b.securityConfigRLocked(domain)
	if err != nil {
		return nil, err
	}

	out := map[string]any{}

	for _, n := range securityNames(cfg, kind) {
		if v, ok := securityView(cfg, kind, n); ok {
			out[n] = v
		}
	}

	return out, nil
}

// SecurityGet returns one resource.
func (b *InMemoryBackend) SecurityGet(domain string, kind securityKind, name string) (any, *secError) {
	b.mu.RLock("SecurityGet")
	defer b.mu.RUnlock()

	cfg, err := b.securityConfigRLocked(domain)
	if err != nil {
		return nil, err
	}

	v, ok := securityView(cfg, kind, name)
	if !ok {
		return nil, secNotFound(name)
	}

	return v, nil
}

// SecurityDelete removes a custom resource.
func (b *InMemoryBackend) SecurityDelete(domain string, kind securityKind, name string) *secError {
	b.mu.Lock("SecurityDelete")
	defer b.mu.Unlock()

	if !b.domains.Has(domain) {
		return secDomainMissing()
	}

	cfg := b.ensureSecurityConfigLocked(domain)

	if err := guardReadOnly(cfg, kind, name); err != nil {
		return err
	}

	if !deleteResource(cfg, kind, name) {
		return secNotFound(name)
	}

	return nil
}

func deleteResource(cfg *SecurityConfig, kind securityKind, name string) bool {
	switch kind {
	case kindInternalUsers:
		_, ok := cfg.Users[name]
		delete(cfg.Users, name)

		return ok
	case kindRoles:
		_, ok := cfg.Roles[name]
		delete(cfg.Roles, name)

		return ok
	case kindRolesMapping:
		_, ok := cfg.RolesMapping[name]
		delete(cfg.RolesMapping, name)

		return ok
	case kindActionGroups:
		_, ok := cfg.ActionGroups[name]
		delete(cfg.ActionGroups, name)

		return ok
	case kindTenants:
		_, ok := cfg.Tenants[name]
		delete(cfg.Tenants, name)

		return ok
	}

	return false
}

// guardReadOnly rejects writes to static, reserved or built-in resources.
func guardReadOnly(cfg *SecurityConfig, kind securityKind, name string) *secError {
	var reserved, static bool

	switch kind {
	case kindInternalUsers:
		if u, ok := cfg.Users[name]; ok {
			reserved = u.Reserved
		}
	case kindRoles:
		if r, ok := mergedRoles(cfg)[name]; ok {
			reserved, static = r.Reserved, r.Static
		}
	case kindRolesMapping:
		if m, ok := cfg.RolesMapping[name]; ok {
			reserved = m.Reserved
		}
	case kindActionGroups:
		if g, ok := mergedActionGroups(cfg)[name]; ok {
			reserved, static = g.Reserved, g.Static
		}
	case kindTenants:
		if t, ok := cfg.Tenants[name]; ok {
			reserved = t.Reserved
		}
	}

	switch {
	case static:
		return secForbidden("Resource '" + name + "' is read-only.")
	case reserved:
		return secForbidden("Resource '" + name + "' is reserved.")
	}

	return nil
}

// SecurityPut creates or replaces a resource from a JSON request body.
func (b *InMemoryBackend) SecurityPut(domain string, kind securityKind, name string, body []byte) (bool, *secError) {
	b.mu.Lock("SecurityPut")
	defer b.mu.Unlock()

	if !b.domains.Has(domain) {
		return false, secDomainMissing()
	}

	cfg := b.ensureSecurityConfigLocked(domain)

	if err := guardReadOnly(cfg, kind, name); err != nil {
		return false, err
	}

	return putResource(cfg, kind, name, body)
}

func keyCheck(body []byte, allowed ...string) *secError {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return secBadRequest("Could not parse content of request.")
	}

	var bad []string

	for k := range raw {
		if !slices.Contains(allowed, k) {
			bad = append(bad, k)
		}
	}

	if len(bad) > 0 {
		return secInvalidKeys(bad)
	}

	return nil
}

func decodeStrict(body []byte, v any) *secError {
	if err := json.Unmarshal(body, v); err != nil {
		return secBadRequest("Could not parse content of request.")
	}

	return nil
}

func putResource(cfg *SecurityConfig, kind securityKind, name string, body []byte) (bool, *secError) {
	switch kind {
	case kindInternalUsers:
		return putUser(cfg, name, body)
	case kindRoles:
		return putRole(cfg, name, body)
	case kindRolesMapping:
		return putMapping(cfg, name, body)
	case kindActionGroups:
		return putActionGroup(cfg, name, body)
	case kindTenants:
		return putTenant(cfg, name, body)
	}

	return false, secBadRequest("unknown resource")
}

type userRequest struct {
	Attributes   map[string]string `json:"attributes"`
	Password     string            `json:"password"`
	Hash         string            `json:"hash"`
	Description  string            `json:"description"`
	BackendRoles []string          `json:"backend_roles"`
	LegacyRoles  []string          `json:"opendistro_security_roles"`
}

func putUser(cfg *SecurityConfig, name string, body []byte) (bool, *secError) {
	if err := keyCheck(body, "password", "hash", jsonKeyBackendRoles, "attributes", "description",
		"opendistro_security_roles"); err != nil {
		return false, err
	}

	var req userRequest
	if err := decodeStrict(body, &req); err != nil {
		return false, err
	}

	if name == "" {
		return false, secBadRequest("Resource name must not be empty.")
	}

	hash := req.Hash

	switch {
	case req.Password != "":
		hash = hashMasterPassword(req.Password)
	case hash != "":
		if _, err := bcrypt.Cost([]byte(hash)); err != nil {
			return false, secBadRequest("An invalid hash was supplied: the hash must be a bcrypt hash.")
		}
	default:
		return false, secBadRequest("Please re-send this request with a password or hash.")
	}

	_, existed := cfg.Users[name]
	cfg.Users[name] = &InternalUser{
		Hash: hash, BackendRoles: nonNilStrings(req.BackendRoles), Attributes: nonNilAttrs(req.Attributes),
		Description: req.Description,
	}

	for _, role := range req.LegacyRoles {
		addMappingMember(cfg, role, []string{name}, nil)
	}

	return !existed, nil
}

type roleRequest struct {
	Description        string                 `json:"description"`
	ClusterPermissions []string               `json:"cluster_permissions"`
	IndexPermissions   []RoleIndexPermission  `json:"index_permissions"`
	TenantPermissions  []RoleTenantPermission `json:"tenant_permissions"`
}

func validateRoleIndexPermission(ip RoleIndexPermission) *secError {
	if len(ip.IndexPatterns) == 0 {
		return secMissingKeys("index_patterns")
	}

	if len(ip.AllowedActions) == 0 {
		return secMissingKeys("allowed_actions")
	}

	if ip.DLS != "" {
		var q map[string]any
		if err := json.Unmarshal([]byte(ip.DLS), &q); err != nil || len(q) == 0 {
			return secBadRequest("Invalid DLS query: dls must be a JSON object.")
		}
	}

	incl, excl := false, false

	for _, f := range ip.FLS {
		if strings.HasPrefix(f, "~") {
			excl = true
		} else {
			incl = true
		}
	}

	if incl && excl {
		return secBadRequest("Inclusive and exclusive field level security rules cannot be mixed.")
	}

	return nil
}

func putRole(cfg *SecurityConfig, name string, body []byte) (bool, *secError) {
	if err := keyCheck(body, "description", "cluster_permissions", "index_permissions",
		"tenant_permissions"); err != nil {
		return false, err
	}

	var req roleRequest
	if err := decodeStrict(body, &req); err != nil {
		return false, err
	}

	for _, ip := range req.IndexPermissions {
		if err := validateRoleIndexPermission(ip); err != nil {
			return false, err
		}
	}

	for _, tp := range req.TenantPermissions {
		if len(tp.TenantPatterns) == 0 {
			return false, secMissingKeys("tenant_patterns")
		}
	}

	_, existed := cfg.Roles[name]
	cfg.Roles[name] = &SecurityRole{
		Description: req.Description, ClusterPermissions: req.ClusterPermissions,
		IndexPermissions: req.IndexPermissions, TenantPermissions: req.TenantPermissions,
	}

	return !existed, nil
}

func putMapping(cfg *SecurityConfig, name string, body []byte) (bool, *secError) {
	if err := keyCheck(body, "description", jsonKeyBackendRoles, "hosts", "users", "and_backend_roles"); err != nil {
		return false, err
	}

	if _, ok := mergedRoles(cfg)[name]; !ok {
		return false, secNotFound(name)
	}

	var req RoleMapping
	if err := decodeStrict(body, &req); err != nil {
		return false, err
	}

	_, existed := cfg.RolesMapping[name]
	cfg.RolesMapping[name] = &RoleMapping{
		Description: req.Description, BackendRoles: req.BackendRoles, Hosts: req.Hosts, Users: req.Users,
		AndBackendRoles: req.AndBackendRoles,
	}

	return !existed, nil
}

func putActionGroup(cfg *SecurityConfig, name string, body []byte) (bool, *secError) {
	if err := keyCheck(body, "description", "allowed_actions", "type"); err != nil {
		return false, err
	}

	var req SecurityActionGroup
	if err := decodeStrict(body, &req); err != nil {
		return false, err
	}

	if len(req.AllowedActions) == 0 {
		return false, secMissingKeys("allowed_actions")
	}

	switch req.Type {
	case "":
		req.Type = actionGroupTypeAll
	case actionGroupTypeCluster, actionGroupTypeIndex, actionGroupTypeAll:
	default:
		return false, secBadRequest("Invalid action group type: must be one of cluster, index or all.")
	}

	_, existed := cfg.ActionGroups[name]
	cfg.ActionGroups[name] = &SecurityActionGroup{
		Description: req.Description, Type: req.Type, AllowedActions: req.AllowedActions,
	}

	return !existed, nil
}

func putTenant(cfg *SecurityConfig, name string, body []byte) (bool, *secError) {
	if err := keyCheck(body, "description"); err != nil {
		return false, err
	}

	var req SecurityTenant
	if err := decodeStrict(body, &req); err != nil {
		return false, err
	}

	_, existed := cfg.Tenants[name]
	cfg.Tenants[name] = &SecurityTenant{Description: req.Description}

	return !existed, nil
}

// ChangeOwnPassword implements PUT /_plugins/_security/api/account.
func (b *InMemoryBackend) ChangeOwnPassword(domain, user, current, next string) *secError {
	b.mu.Lock("ChangeOwnPassword")
	defer b.mu.Unlock()

	cfg, ok := b.securityConfigs.Get(domain)
	if !ok {
		return secNotFound(user)
	}

	u, ok := cfg.Users[user]
	if !ok {
		return secNotFound(user)
	}

	if !masterPasswordMatches(u.Hash, current) {
		return secBadRequest("Could not validate your current password.")
	}

	if next == "" {
		return secBadRequest("Please re-send this request with a password.")
	}

	u.Hash = hashMasterPassword(next)

	return nil
}

// SecurityTenantAccess lists tenant name -> writable for the principal's mapped roles.
func (b *InMemoryBackend) SecurityTenantAccess(domain string, perms effectivePerms) map[string]bool {
	b.mu.RLock("SecurityTenantAccess")
	defer b.mu.RUnlock()

	cfg, _ := b.securityConfigs.Get(domain)
	roles := mergedRoles(cfg)
	names := []string{"global_tenant"}

	if cfg != nil {
		names = append(names, slices.Collect(maps.Keys(cfg.Tenants))...)
	}

	out := map[string]bool{}

	for _, n := range names {
		for _, rn := range perms.Roles {
			for _, tp := range roles[rn].TenantPermissions {
				if !matchesAny(tp.TenantPatterns, n) {
					continue
				}

				if _, seen := out[n]; !seen {
					out[n] = false
				}

				if slices.Contains(tp.AllowedActions, "kibana_all_write") || slices.Contains(tp.AllowedActions, "*") {
					out[n] = true
				}
			}
		}
	}

	return out
}
