package gcpidentitytoolkit

import (
	"errors"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	batchGetDefault   = 20
	queryDefault      = 500
	maxImportUsers    = 1000
	tenantIDRandLen   = 5
	tenantSuffixBytes = 8
	tenantPageSize    = 20
)

var tenantNameRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9-]{3,19}$`)

// AdminCreate creates an account without issuing tokens (projects/{p}/accounts).
func (b *InMemoryBackend) AdminCreate(c call, r *authRequest) (map[string]any, error) {
	b.mu.Lock("AdminCreate")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(c.project, effectiveTenant(c, r))
	if err != nil {
		return nil, err
	}

	claims := ""

	if r.CustomAttributes != nil {
		if claims, err = parseClaims(*r.CustomAttributes); err != nil {
			return nil, err
		}
	}

	n := newUser{
		uid: r.LocalID.first(), email: r.Email.first(), password: r.Password, displayName: deref(r.DisplayName),
		photoURL: deref(r.PhotoURL), phone: r.PhoneNumber.first(), customAttrs: claims,
		emailVerified: r.EmailVerified != nil && *r.EmailVerified, disabled: r.Disabled != nil && *r.Disabled,
	}

	u, err := b.createUserLocked(sc, n, false)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyKind: "identitytoolkit#SignupNewUserResponse", "localId": u.UID}, nil
}

// uploadRequest is the body of accounts:batchCreate.
type uploadRequest struct {
	HashAlgorithm     string     `json:"hashAlgorithm"`
	SignerKey         string     `json:"signerKey"`
	SaltSeparator     string     `json:"saltSeparator"`
	PasswordHashOrder string     `json:"passwordHashOrder"`
	TenantID          string     `json:"tenantId"`
	Users             []userWire `json:"users"`
	Rounds            flexInt    `json:"rounds"`
	MemoryCost        flexInt    `json:"memoryCost"`
	CPUMemCost        flexInt    `json:"cpuMemCost"`
	BlockSize         flexInt    `json:"blockSize"`
	Parallelization   flexInt    `json:"parallelization"`
	DKLen             flexInt    `json:"dkLen"`
	AllowOverwrite    bool       `json:"allowOverwrite"`
}

func (u *uploadRequest) spec() (*hashSpec, error) {
	if u.HashAlgorithm == "" {
		return nil, nil //nolint:nilnil // no hash algorithm supplied
	}

	key, err1 := decodeOptional(u.SignerKey)
	sep, err2 := decodeOptional(u.SaltSeparator)

	if err1 != nil || err2 != nil {
		return nil, idtk("INVALID_HASH_ALGORITHM", "Invalid base64 in hash parameters.")
	}

	h := &hashSpec{
		Algorithm: strings.ToUpper(u.HashAlgorithm), SignerKey: key, SaltSeparator: sep,
		PasswordHashOrder: u.PasswordHashOrder, Rounds: int64(u.Rounds), MemoryCost: int64(u.MemoryCost),
		CPUMemCost: int64(u.CPUMemCost), BlockSize: int64(u.BlockSize), Parallelization: int64(u.Parallelization),
		DKLen: int64(u.DKLen),
	}

	if h.validate() != nil {
		return nil, idtk("INVALID_HASH_ALGORITHM", "Unsupported or incomplete hash configuration.")
	}

	return h, nil
}

func decodeOptional(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}

	return decodeB64(s)
}

// BatchCreate imports accounts with optional password hashes (accounts:batchCreate).
func (b *InMemoryBackend) BatchCreate(c call, r *uploadRequest) (map[string]any, error) {
	if len(r.Users) == 0 {
		return nil, idtk("MISSING_USER_ACCOUNT")
	}

	if len(r.Users) > maxImportUsers {
		return nil, idtk("TOO_MANY_USERS", "Too many users in one request.")
	}

	spec, err := r.spec()
	if err != nil {
		return nil, err
	}

	b.mu.Lock("BatchCreate")
	defer b.mu.Unlock()

	tenant := r.TenantID
	if c.tenant != "" {
		tenant = c.tenant
	}

	sc, err := b.scopeLocked(c.project, tenant)
	if err != nil {
		return nil, err
	}

	var failures []map[string]any

	for i := range r.Users {
		if msg := b.importOneLocked(sc, &r.Users[i], spec, r.AllowOverwrite); msg != "" {
			failures = append(failures, map[string]any{"index": i, "message": msg})
		}
	}

	out := map[string]any{keyKind: "identitytoolkit#UploadAccountResponse"}
	if len(failures) > 0 {
		out["error"] = failures
	}

	return out, nil
}

func (b *InMemoryBackend) importOneLocked(sc *scope, w *userWire, spec *hashSpec, overwrite bool) string {
	if w.LocalID == "" {
		return "localId is missing"
	}

	n := newUser{
		uid: w.LocalID, email: w.Email, displayName: w.DisplayName, photoURL: w.PhotoURL, phone: w.PhoneNumber,
		emailVerified: w.EmailVerified, disabled: w.Disabled, createdAt: int64(w.CreatedAt),
		lastLoginAt: int64(w.LastLoginAt),
	}

	if w.CustomAttributes != "" {
		claims, err := parseClaims(w.CustomAttributes)
		if err != nil {
			return "Invalid custom claims"
		}

		n.customAttrs = claims
	}

	if msg := importPassword(&n, w, spec); msg != "" {
		return msg
	}

	for _, p := range w.ProviderUserInfo {
		if p.ProviderID == passwordProv || p.ProviderID == phoneProv || p.ProviderID == "" || p.RawID == "" {
			continue
		}

		n.providers = append(n.providers, providerRec(p))
	}

	if _, err := b.createUserLocked(sc, n, overwrite); err != nil {
		if ae, ok := errors.AsType[*APIError](err); ok {
			return ae.Message
		}

		return err.Error()
	}

	return ""
}

func importPassword(n *newUser, w *userWire, spec *hashSpec) string {
	if w.PasswordHash == "" {
		return ""
	}

	if spec == nil || w.Email == "" {
		return "Password hash requires an email and a hash algorithm"
	}

	hash, err1 := decodeB64(w.PasswordHash)
	salt, err2 := decodeOptional(w.Salt)

	if err1 != nil || err2 != nil {
		return "Invalid base64 in passwordHash or salt"
	}

	n.passwordHash, n.salt, n.hash = hash, salt, spec

	return ""
}

// BatchDelete deletes accounts; without force only disabled accounts are deleted.
func (b *InMemoryBackend) BatchDelete(c call, ids []string, force bool, tenant string) (map[string]any, error) {
	if len(ids) == 0 {
		return nil, idtk("MISSING_LOCAL_ID")
	}

	if len(ids) > maxImportUsers {
		return nil, idtk("TOO_MANY_USERS", "Too many users in one request.")
	}

	b.mu.Lock("BatchDelete")
	defer b.mu.Unlock()

	if c.tenant != "" {
		tenant = c.tenant
	}

	sc, err := b.scopeLocked(c.project, tenant)
	if err != nil {
		return nil, err
	}

	var failures []map[string]any

	for i, id := range ids {
		u, ok := sc.users[id]
		if !ok {
			continue
		}

		if !force && !u.Disabled {
			failures = append(failures, map[string]any{
				"index": i, "localId": id, "message": "NOT_DISABLED : Disable the account before batch deletion.",
			})

			continue
		}

		delete(sc.users, id)
	}

	out := map[string]any{}
	if len(failures) > 0 {
		out["errors"] = failures
	}

	return out, nil
}

// BatchGet pages through accounts with their full password hashes.
func (b *InMemoryBackend) BatchGet(c call, tenant, token string, maxResults int) (map[string]any, error) {
	if maxResults < 0 || maxResults > maxPage {
		return nil, idtk("INVALID_MAX_RESULTS")
	}

	if err := page.ValidateToken(token); err != nil {
		return nil, idtk("INVALID_PAGE_TOKEN")
	}

	b.mu.Lock("BatchGet")
	defer b.mu.Unlock()

	if c.tenant != "" {
		tenant = c.tenant
	}

	sc, err := b.scopeLocked(c.project, tenant)
	if err != nil {
		return nil, err
	}

	all := sc.sorted()
	pg := page.New(all, token, maxResults, batchGetDefault)
	users := make([]userWire, 0, len(pg.Data))

	for _, u := range pg.Data {
		users = append(users, b.toWire(sc, u, true))
	}

	out := map[string]any{keyKind: "identitytoolkit#DownloadAccountResponse"}
	if len(users) > 0 {
		out["users"] = users
	}

	if pg.Next != "" {
		out["nextPageToken"] = pg.Next
	}

	return out, nil
}

// queryRequest is the body of accounts:query.
type queryRequest struct {
	ReturnUserInfo *bool  `json:"returnUserInfo"`
	SortBy         string `json:"sortBy"`
	Order          string `json:"order"`
	TenantID       string `json:"tenantId"`
	Expression     []struct {
		Email       string `json:"email"`
		PhoneNumber string `json:"phoneNumber"`
		UserID      string `json:"userId"`
	} `json:"expression"`
	Limit  flexInt `json:"limit"`
	Offset flexInt `json:"offset"`
}

// Query filters, sorts and pages accounts (accounts:query).
func (b *InMemoryBackend) Query(c call, r *queryRequest) (map[string]any, error) {
	if r.Limit < 0 || r.Offset < 0 {
		return nil, idtk("INVALID_QUERY", "limit and offset must be non-negative")
	}

	b.mu.Lock("Query")
	defer b.mu.Unlock()

	tenant := r.TenantID
	if c.tenant != "" {
		tenant = c.tenant
	}

	sc, err := b.scopeLocked(c.project, tenant)
	if err != nil {
		return nil, err
	}

	matched := make([]*userRec, 0)

	for _, u := range sc.sorted() {
		if r.matches(u) {
			matched = append(matched, u)
		}
	}

	sortUsers(matched, r.SortBy, r.Order)

	out := map[string]any{"recordsCount": itoa(len(matched))}
	if r.ReturnUserInfo != nil && !*r.ReturnUserInfo {
		return out, nil
	}

	limit := int(r.Limit)
	if limit == 0 {
		limit = queryDefault
	}

	start := min(int(r.Offset), len(matched))
	end := min(start+limit, len(matched))
	users := make([]userWire, 0, end-start)

	for _, u := range matched[start:end] {
		users = append(users, b.toWire(sc, u, true))
	}

	if len(users) > 0 {
		out["userInfo"] = users
	}

	return out, nil
}

func (r *queryRequest) matches(u *userRec) bool {
	for _, e := range r.Expression {
		if (e.Email != "" && !strs.Equal(e.Email, u.Email)) ||
			(e.PhoneNumber != "" && e.PhoneNumber != u.Phone) || (e.UserID != "" && e.UserID != u.UID) {
			return false
		}
	}

	return true
}

func sortUsers(us []*userRec, by, order string) {
	less := func(a, b *userRec) bool { return a.CreatedAt < b.CreatedAt }

	switch by {
	case "USER_ID":
		less = func(a, b *userRec) bool { return a.UID < b.UID }
	case "NAME":
		less = func(a, b *userRec) bool { return a.DisplayName < b.DisplayName }
	case "LAST_LOGIN_AT":
		less = func(a, b *userRec) bool { return a.LastLoginAt < b.LastLoginAt }
	case "USER_EMAIL":
		less = func(a, b *userRec) bool { return a.Email < b.Email }
	}

	sort.SliceStable(us, func(i, j int) bool {
		if order == "DESC" {
			return less(us[j], us[i])
		}

		return less(us[i], us[j])
	})
}

func itoa(n int) string { return strconv.Itoa(n) }

// ClearAccounts deletes every account in a project or tenant (emulator reset endpoint).
func (b *InMemoryBackend) ClearAccounts(project, tenant string) error {
	b.mu.Lock("ClearAccounts")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(project, tenant)
	if err != nil {
		return err
	}

	clear(sc.users)

	return nil
}

// OOBCode is one stored out-of-band code, as listed by the emulator endpoint.
type OOBCode struct {
	Email       string `json:"email"`
	OobCode     string `json:"oobCode"`
	OobLink     string `json:"oobLink"`
	RequestType string `json:"requestType"`
}

// OOBCodes lists the live codes of a project, oldest first. No email is ever sent; tests read codes here.
func (b *InMemoryBackend) OOBCodes(project, tenant string) ([]OOBCode, error) {
	b.mu.Lock("OOBCodes")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(project, tenant)
	if err != nil {
		return nil, err
	}

	type entry struct {
		rec  *oobRec
		code string
	}

	var list []entry

	for code, rec := range sc.proj.Codes {
		if rec.TenantID == tenant {
			list = append(list, entry{rec, code})
		}
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].rec.Created != list[j].rec.Created {
			return list[i].rec.Created < list[j].rec.Created
		}

		return list[i].code < list[j].code
	})

	out := make([]OOBCode, 0, len(list))
	for _, e := range list {
		out = append(
			out,
			OOBCode{Email: e.rec.Email, OobCode: e.code, OobLink: e.rec.Link, RequestType: e.rec.RequestType},
		)
	}

	return out, nil
}

// GetConfig returns the project configuration.
func (b *InMemoryBackend) GetConfig(project string) *idtkv2.GoogleCloudIdentitytoolkitAdminV2Config {
	b.mu.Lock("GetConfig")
	defer b.mu.Unlock()

	p := b.projectLocked(project)

	return withHashConfig(p)
}

func withHashConfig(p *projectRec) *idtkv2.GoogleCloudIdentitytoolkitAdminV2Config {
	cfg := cloneJSON(p.Config)
	if cfg.SignIn == nil {
		cfg.SignIn = &idtkv2.GoogleCloudIdentitytoolkitAdminV2SignInConfig{}
	}

	cfg.SignIn.HashConfig = hashConfigWire(p.Hash)

	return cfg
}

func hashConfigWire(h hashSpec) *idtkv2.GoogleCloudIdentitytoolkitAdminV2HashConfig {
	return &idtkv2.GoogleCloudIdentitytoolkitAdminV2HashConfig{
		Algorithm: h.Algorithm, MemoryCost: h.MemoryCost, Rounds: h.Rounds,
		SignerKey: hashB64(h.SignerKey), SaltSeparator: hashB64(h.SaltSeparator),
	}
}

// maskedPatch keeps only the fields named by a comma-separated updateMask (all when empty).
func maskedPatch(patch map[string]any, mask string) map[string]any {
	if mask == "" {
		return patch
	}

	out := map[string]any{}

	for path := range strings.SplitSeq(mask, ",") {
		parts := strings.Split(strings.TrimSpace(path), ".")

		var val any = patch

		for _, part := range parts {
			m, ok := val.(map[string]any)
			if !ok {
				val = nil

				break
			}

			val = m[part]
		}

		cur := out

		for i, part := range parts {
			if i == len(parts)-1 {
				cur[part] = val

				break
			}

			next, ok := cur[part].(map[string]any)
			if !ok {
				next = map[string]any{}
				cur[part] = next
			}

			cur = next
		}
	}

	return out
}

// UpdateConfig merges a patch into the project configuration.
func (b *InMemoryBackend) UpdateConfig(
	project string,
	patch map[string]any,
	mask string,
) (*idtkv2.GoogleCloudIdentitytoolkitAdminV2Config, error) {
	for _, k := range []string{"name", "signIn.hashConfig"} {
		if mask == k {
			return nil, errInvalidArg("Field %q is output only.", k)
		}
	}

	b.mu.Lock("UpdateConfig")
	defer b.mu.Unlock()

	p := b.projectLocked(project)

	next, err := patched(p.Config, maskedPatch(patch, mask))
	if err != nil {
		return nil, err
	}

	next.Name = p.Config.Name
	p.Config = next

	return withHashConfig(p), nil
}

func tenantName(project, id string) string { return "projects/" + project + "/tenants/" + id }

// CreateTenant creates a tenant with a generated id.
func (b *InMemoryBackend) CreateTenant(
	project string,
	in *idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant,
) (*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant, error) {
	if !tenantNameRE.MatchString(in.DisplayName) {
		return nil, errInvalidArg(
			"Invalid tenant display name: must be 4-20 characters, start with a letter " +
				"and contain only letters, digits and hyphens.",
		)
	}

	b.mu.Lock("CreateTenant")
	defer b.mu.Unlock()

	p := b.projectLocked(project)

	var id string

	for {
		id = strs.Fold(in.DisplayName)
		if len(id) > 20-tenantIDRandLen-1 {
			id = id[:20-tenantIDRandLen-1]
		}

		id += "-" + strings.ToLower(randomToken(tenantSuffixBytes))[:tenantIDRandLen]
		id = strings.NewReplacer("_", "x").Replace(id)

		if _, taken := p.Tenants[id]; !taken {
			break
		}
	}

	t := cloneJSON(in)
	t.Name = tenantName(project, id)
	p.Tenants[id] = t
	p.Users[id] = map[string]*userRec{}

	return cloneJSON(t), nil
}

// GetTenant returns one tenant.
func (b *InMemoryBackend) GetTenant(project, id string) (*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant, error) {
	b.mu.Lock("GetTenant")
	defer b.mu.Unlock()

	t, ok := b.projectLocked(project).Tenants[id]
	if !ok {
		return nil, errNotFound("TENANT_NOT_FOUND")
	}

	return cloneJSON(t), nil
}

// ListTenants pages through a project's tenants.
func (b *InMemoryBackend) ListTenants(
	project, token string,
	size int,
) (page.Page[*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant]{}, errInvalidArg("Invalid page token.")
	}

	b.mu.Lock("ListTenants")
	defer b.mu.Unlock()

	p := b.projectLocked(project)
	ids := make([]string, 0, len(p.Tenants))

	for id := range p.Tenants {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	all := make([]*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant, 0, len(ids))
	for _, id := range ids {
		all = append(all, cloneJSON(p.Tenants[id]))
	}

	return page.New(all, token, size, tenantPageSize), nil
}

// UpdateTenant merges a patch into a tenant.
func (b *InMemoryBackend) UpdateTenant(
	project, id string,
	patch map[string]any,
	mask string,
) (*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant, error) {
	b.mu.Lock("UpdateTenant")
	defer b.mu.Unlock()

	p := b.projectLocked(project)

	cur, ok := p.Tenants[id]
	if !ok {
		return nil, errNotFound("TENANT_NOT_FOUND")
	}

	next, err := patched(cur, maskedPatch(patch, mask))
	if err != nil {
		return nil, err
	}

	if next.DisplayName != cur.DisplayName && !tenantNameRE.MatchString(next.DisplayName) {
		return nil, errInvalidArg("Invalid tenant display name.")
	}

	next.Name = cur.Name
	p.Tenants[id] = next

	return cloneJSON(next), nil
}

// DeleteTenant removes a tenant and its accounts.
func (b *InMemoryBackend) DeleteTenant(project, id string) error {
	b.mu.Lock("DeleteTenant")
	defer b.mu.Unlock()

	p := b.projectLocked(project)
	if _, ok := p.Tenants[id]; !ok {
		return errNotFound("TENANT_NOT_FOUND")
	}

	delete(p.Tenants, id)
	delete(p.Users, id)

	for code, rec := range p.Codes {
		if rec.TenantID == id {
			delete(p.Codes, code)
		}
	}

	return nil
}
