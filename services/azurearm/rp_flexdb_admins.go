package azurearm

import (
	"maps"
	"strings"

	"github.com/google/uuid"
)

const (
	flexAdminType       = "administrators"
	flexAdminMySQLName  = "ActiveDirectory"
	flexAdminPrincipal  = "principalType"
	flexAdminPrincipalN = "principalName"
	flexAdminTenant     = "tenantId"
	flexAdminIdentity   = "identityResourceId"
	flexAdminFailover   = "failover"
	flexAdminSID        = "sid"
)

type flexAdmin struct {
	Props map[string]any `json:"props"`
	Name  string         `json:"name"`
}

//nolint:gochecknoglobals // static enum from the PostgreSQL flexible server spec
var flexAdminPrincipalTypes = []string{"User", "Group", "ServicePrincipal", "Unknown"}

func flexGUID(props map[string]any, key string) error {
	v, _ := flexStr(props, key)
	if _, err := uuid.Parse(v); err != nil {
		return flexInvalid("properties.%s must be a GUID.", key)
	}

	return nil
}

// adminProps validates and normalises an Entra administrator for the engine of this provider.
func (p *FlexDBProvider) adminProps(rec *flexServer, name string, in map[string]any) (map[string]any, error) {
	if p.flavor.kind == flexKindMySQL {
		return p.mysqlAdminProps(rec, name, in)
	}

	if _, err := uuid.Parse(name); err != nil {
		return nil, flexInvalid("The administrator name '%s' must be the object id (a GUID) of the principal.", name)
	}

	if v, _ := flexStr(rec.AuthConfig, "activeDirectoryAuth"); v != flexEnabled {
		return nil, flexInvalid("Microsoft Entra authentication must be enabled on server '%s' before an "+
			"administrator can be added.", rec.Name)
	}

	if v, _ := flexStr(in, flexAdminPrincipalN); v == "" {
		return nil, flexInvalid("properties.%s is required.", flexAdminPrincipalN)
	}

	typ, _ := flexStr(in, flexAdminPrincipal)

	canon, ok := canonicalIn(flexAdminPrincipalTypes, typ)
	if !ok {
		return nil, flexInvalid("properties.%s must be one of User, Group, ServicePrincipal or Unknown.",
			flexAdminPrincipal)
	}

	if err := flexGUID(in, flexAdminTenant); err != nil {
		return nil, err
	}

	return map[string]any{
		"objectId": name, flexAdminPrincipalN: in[flexAdminPrincipalN], flexAdminPrincipal: canon,
		flexAdminTenant: in[flexAdminTenant],
	}, nil
}

func (p *FlexDBProvider) mysqlAdminProps(rec *flexServer, name string, in map[string]any) (map[string]any, error) {
	if !strings.EqualFold(name, flexAdminMySQLName) {
		return nil, flexInvalid("The administrator name must be '%s'.", flexAdminMySQLName)
	}

	if v, _ := flexStr(in, fieldLogin); v == "" {
		return nil, flexInvalid("properties.login is required.")
	}

	for _, k := range []string{flexAdminSID, flexAdminTenant} {
		if err := flexGUID(in, k); err != nil {
			return nil, err
		}
	}

	ident, _ := flexStr(in, flexAdminIdentity)
	if ident == "" {
		return nil, flexInvalid("properties.%s is required.", flexAdminIdentity)
	}

	attached := flexMap(rec.Identity, "userAssignedIdentities")
	found := false

	for k := range attached {
		found = found || strings.EqualFold(k, ident)
	}

	if !found {
		return nil, flexInvalid("The identity '%s' is not assigned to server '%s'.", ident, rec.Name)
	}

	return map[string]any{
		"administratorType": flexAdminMySQLName,
		fieldLogin:          in[fieldLogin],
		flexAdminSID:        in[flexAdminSID],
		flexAdminTenant:     in[flexAdminTenant],
		flexAdminIdentity:   ident,
	}, nil
}

func (p *FlexDBProvider) writeAdmin(id ResourceID, body map[string]any) (flexResult, error) {
	name := id.LeafName()

	p.mu.Lock("writeAdmin")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return flexResult{}, err
	}

	if p.stateOf(rec) != flexStateReady {
		return flexResult{}, p.notReady(rec, "Create or update administrator")
	}

	props, err := p.adminProps(rec, name, flexMap(body, fieldProperties))
	if err != nil {
		return flexResult{}, err
	}

	if rec.Admins == nil {
		rec.Admins = map[string]flexAdmin{}
	}

	rec.Admins[strings.ToLower(name)] = flexAdmin{Name: name, Props: props}

	return flexResult{
		body: p.adminBody(id, rec.Admins[strings.ToLower(name)]),
		opID: p.newOp(id.ARMID(), true, "", nil),
	}, nil
}

func (p *FlexDBProvider) adminBody(id ResourceID, a flexAdmin) map[string]any {
	rid := id
	rid.Names = []string{id.Names[0], a.Name}

	return p.childEnvelope(rid, flexAdminType, maps.Clone(a.Props))
}

func (p *FlexDBProvider) deleteAdmin(id ResourceID) (string, error) {
	p.mu.Lock("deleteAdmin")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return "", err
	}

	k := strings.ToLower(id.LeafName())
	if _, ok := rec.Admins[k]; !ok {
		return "", p.childNotFound(id)
	}

	delete(rec.Admins, k)

	return p.newOp(id.ARMID(), false, "", nil), nil
}
