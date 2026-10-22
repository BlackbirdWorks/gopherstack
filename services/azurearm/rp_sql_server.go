package azurearm

import (
	"maps"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

var (
	sqlServerNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	sqlLoginRE      = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`)
)

var sqlReservedLogins = map[string]bool{ //nolint:gochecknoglobals // read-only
	"admin": true, "administrator": true, "sa": true, "root": true, "dbmanager": true, "loginmanager": true,
	"dbo": true, "guest": true, "public": true, "sys": true, "information_schema": true, "azure_superuser": true,
}

const (
	sqlPasswordMin  = 8
	sqlPasswordMax  = 128
	sqlPasswordKind = 3
)

var sqlServerOwnedProps = []string{ //nolint:gochecknoglobals // read-only
	"administratorLogin", "administratorLoginPassword", sqlPropVersion, sqlPropState, "fullyQualifiedDomainName",
	sqlPropMinTLS, sqlPropPublic, "gopherstackEndpoint",
}

func validSQLServerName(name string) error {
	if !sqlServerNameRE.MatchString(name) {
		return invalidSQL("The server name %q is not valid: use 1-63 lowercase letters, digits and hyphens, "+
			"not starting or ending with a hyphen.", name)
	}

	return nil
}

func validSQLLogin(login string) error {
	if !sqlLoginRE.MatchString(login) || sqlReservedLogins[strings.ToLower(login)] {
		return invalidSQL("Invalid value given for parameter AdministratorLogin. Specify a valid parameter value.")
	}

	return nil
}

func validSQLPassword(login, pw string) error {
	n := len([]rune(pw))

	switch {
	case n < sqlPasswordMin:
		return &sqlPasswordError{reason: "too short"}
	case n > sqlPasswordMax:
		return &sqlPasswordError{reason: "too long"}
	}

	if strings.IndexFunc(pw, unicode.IsControl) >= 0 ||
		(login != "" && strings.Contains(strings.ToLower(pw), strings.ToLower(login))) ||
		passwordClasses(pw) < sqlPasswordKind {
		return &sqlPasswordError{reason: "not complex enough"}
	}

	return nil
}

func passwordClasses(pw string) int {
	var upper, lower, digit, other bool

	for _, r := range pw {
		switch {
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsLower(r):
			lower = true
		case unicode.IsDigit(r):
			digit = true
		default:
			other = true
		}
	}

	n := 0

	for _, b := range []bool{upper, lower, digit, other} {
		if b {
			n++
		}
	}

	return n
}

func validSQLTLS(v string) error {
	switch v {
	case "", "None", "1.0", "1.1", sqlDefaultTLS:
		return nil
	}

	return invalidSQL("minimalTlsVersion must be None, 1.0, 1.1 or 1.2.")
}

func validSQLPublicAccess(v string) error {
	switch v {
	case "", sqlDefaultPublic, sqlValDisabled, "SecuredByPerimeter":
		return nil
	}

	return invalidSQL("publicNetworkAccess must be Enabled, Disabled or SecuredByPerimeter.")
}

// serverDraft is a server's validated request, built without holding the provider lock.
type serverDraft struct {
	rec  *sqlServer
	spec SQLSpec
}

func (p *SQLProvider) draftServer(id ResourceID, body map[string]any, patch bool) (*serverDraft, error) {
	p.mu.RLock("draftServer")
	defer p.mu.RUnlock()

	existing, found := p.res[sqlKey(id)]

	switch {
	case patch && !found:
		return nil, ErrSQLServerNotFound
	case !found:
		if err := p.checkNewServer(id); err != nil {
			return nil, err
		}
	}

	rec := p.freshServer(id, existing)
	if !patch {
		rec.Extra, rec.Tags = map[string]any{}, nil
	}

	props, _ := body[fieldProperties].(map[string]any)

	applyServerEnvelope(rec, body)

	spec, err := applyServerProps(rec, props, found)
	if err != nil {
		return nil, err
	}

	return &serverDraft{rec: rec, spec: spec}, nil
}

func (p *SQLProvider) checkNewServer(id ResourceID) error {
	if err := validSQLServerName(id.LeafName()); err != nil {
		return err
	}

	if p.nameTakenElsewhere(id) {
		return ErrSQLNameTaken
	}

	if len(p.res) >= sqlMaxServers {
		return invalidSQL("At most %d servers are supported.", sqlMaxServers)
	}

	return nil
}

func (p *SQLProvider) freshServer(id ResourceID, existing *sqlServer) *sqlServer {
	if existing != nil {
		rec := cloneServer(existing)
		rec.Extra = maps.Clone(existing.Extra)

		return rec
	}

	return &sqlServer{
		Name: id.LeafName(), ResourceGrp: id.ResourceGroup, Location: DefaultLocation, MinTLS: sqlDefaultTLS,
		PublicAccess: sqlDefaultPublic, CreatedAt: p.now().UTC(), Extra: map[string]any{},
		Databases: map[string]*sqlDatabase{}, Firewall: map[string]sqlFirewall{}, Pools: map[string]*sqlPool{},
		Settings: map[string]map[string]any{},
	}
}

func applyServerEnvelope(rec *sqlServer, body map[string]any) {
	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.Location = loc
	}

	if _, ok := body[fieldTags]; ok {
		rec.Tags = stringTags(body[fieldTags])
	}

	if ident, ok := body[acFieldIdentity].(map[string]any); ok && len(ident) > 0 {
		rec.Identity = ident
	}
}

func applyServerProps(rec *sqlServer, props map[string]any, existed bool) (SQLSpec, error) {
	if v, ok := props[sqlPropVersion].(string); ok && v != "" && v != sqlVersion {
		return SQLSpec{}, invalidSQL("version must be %s.", sqlVersion)
	}

	if err := applyServerStrings(rec, props); err != nil {
		return SQLSpec{}, err
	}

	maps.Copy(rec.Extra, ehExtra(map[string]any{fieldProperties: props}, sqlServerOwnedProps...))
	syncServerAdmin(rec)

	pw, _ := props["administratorLoginPassword"].(string)

	switch {
	case pw != "":
		if err := validSQLPassword(rec.Login, pw); err != nil {
			return SQLSpec{}, err
		}
	case !existed && rec.Login != "":
		return SQLSpec{}, invalidSQL(
			"Invalid value given for parameter AdministratorLoginPassword. Specify a valid parameter value.",
		)
	}

	return SQLSpec{AdminLogin: rec.Login, AdminPassword: pw}, nil
}

func applyServerStrings(rec *sqlServer, props map[string]any) error {
	if v, ok := props["administratorLogin"].(string); ok && v != "" {
		if err := validSQLLogin(v); err != nil {
			return err
		}

		rec.Login = v
	}

	if v, ok := props[sqlPropMinTLS].(string); ok {
		if err := validSQLTLS(v); err != nil {
			return err
		}

		if v != "" {
			rec.MinTLS = v
		}
	}

	if v, ok := props[sqlPropPublic].(string); ok {
		if err := validSQLPublicAccess(v); err != nil {
			return err
		}

		if v != "" {
			rec.PublicAccess = v
		}
	}

	return nil
}

// syncServerAdmin mirrors an Entra administrator given on the server (properties.administrators) into the
// administrators/ActiveDirectory child.
func syncServerAdmin(rec *sqlServer) {
	adm, ok := rec.Extra["administrators"].(map[string]any)
	if !ok {
		return
	}

	login, _ := adm["login"].(string)
	if login == "" {
		return
	}

	sid, _ := adm["sid"].(string)
	tenant, _ := adm["tenantId"].(string)
	rec.Admin = &sqlAdmin{Login: login, SID: sid, TenantID: tenant}
}

func (p *SQLProvider) putServer(id ResourceID, body map[string]any) (map[string]any, error) {
	return p.commitServer(id, body, false)
}

// Patch merges an Update request into an existing server.
func (p *SQLProvider) Patch(id ResourceID, body map[string]any) (map[string]any, error) {
	return p.commitServer(id, body, true)
}

func (p *SQLProvider) commitServer(id ResourceID, body map[string]any, patch bool) (map[string]any, error) {
	d, err := p.draftServer(id, body, patch)
	if err != nil {
		return nil, err
	}

	key := sqlKey(id)

	if _, err = p.dataPlane.EnsureServer(key, d.spec); err != nil {
		return nil, engineSQLError(err)
	}

	p.mu.Lock("commitServer")
	defer p.mu.Unlock()

	if cur, ok := p.res[key]; ok {
		d.rec.Databases, d.rec.Firewall, d.rec.Pools, d.rec.Settings = cur.Databases, cur.Firewall, cur.Pools, cur.Settings

		if d.rec.Admin == nil {
			d.rec.Admin = cur.Admin
		}
	}

	p.res[key] = d.rec

	return p.serverBody(id, d.rec), nil
}

func (p *SQLProvider) serverBody(id ResourceID, rec *sqlServer) map[string]any {
	typed := map[string]any{
		sqlPropVersion: sqlVersion, sqlPropState: sqlValReady, "fullyQualifiedDomainName": rec.Name + sqlHostSuffix,
		sqlPropMinTLS: rec.MinTLS, sqlPropPublic: rec.PublicAccess,
		"restrictOutboundNetworkAccess": sqlValDisabled,
	}

	if rec.Login != "" {
		typed["administratorLogin"] = rec.Login
	}

	if ep, ok := p.dataPlane.Endpoint(sqlKey(id)); ok && ep.Port != 0 {
		typed["gopherstackEndpoint"] = ep.Host + ":" + strconv.Itoa(ep.Port)
	}

	out := map[string]any{
		"id": id.ARMID(), fieldName: rec.Name, fieldType: namespaceMicrosoftSQL + "/servers",
		fieldLocation: rec.Location, fieldTags: tagsOrEmpty(rec.Tags), sqlPropKind: sqlKind,
		fieldProperties: ehMerge(map[string]any{sqlPropEndpoints: []any{}}, rec.Extra, typed),
	}

	if len(rec.Identity) > 0 {
		out[acFieldIdentity] = redisIdentity(id, rec.Identity)
	}

	return out
}
