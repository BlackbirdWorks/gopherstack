package azurearm

import (
	"net/netip"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// sqlUntrackedCollections are server collections whose members the emulator never creates, so they are always
// empty (a dropped database is removed, not kept restorable).
var sqlUntrackedCollections = map[string]bool{ //nolint:gochecknoglobals // read-only
	"restorabledroppeddatabases": true,
}

const (
	sqlAdminName     = "ActiveDirectory"
	sqlAzureServices = "AllowAllWindowsAzureIps"
	sqlZeroIP        = "0.0.0.0"
)

func putSQLFirewall(srv *sqlServer, id ResourceID, body map[string]any) (map[string]any, error) {
	name := id.LeafName()
	if err := validSQLChildName(name); err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	start, _ := props["startIpAddress"].(string)
	end, _ := props["endIpAddress"].(string)

	if err := validSQLIPRange(name, start, end); err != nil {
		return nil, err
	}

	key := strings.ToLower(name)
	if _, exists := srv.Firewall[key]; !exists && len(srv.Firewall) >= sqlMaxFirewall {
		return nil, invalidSQL("At most %d firewall rules per server are supported.", sqlMaxFirewall)
	}

	fw := sqlFirewall{Name: name, Start: start, End: end}
	srv.Firewall[key] = fw

	return firewallSQLBody(id, fw), nil
}

func validSQLIPRange(name, start, end string) error {
	a, errA := netip.ParseAddr(start)
	b, errB := netip.ParseAddr(end)

	if errA != nil || errB != nil || !a.Is4() || !b.Is4() {
		return invalidSQL("startIpAddress and endIpAddress must be IPv4 addresses.")
	}

	if a.Compare(b) > 0 {
		return invalidSQL("startIpAddress must not be greater than endIpAddress.")
	}

	if strings.EqualFold(name, sqlAzureServices) && (start != sqlZeroIP || end != sqlZeroIP) {
		return invalidSQL("The rule %s must span 0.0.0.0 to 0.0.0.0.", sqlAzureServices)
	}

	return nil
}

func firewallSQLBody(id ResourceID, fw sqlFirewall) map[string]any {
	return map[string]any{
		"id": id.ARMID(), fieldName: fw.Name, fieldType: namespaceMicrosoftSQL + "/servers/" + sqlFirewallType,
		sqlPropKind:     sqlKind,
		fieldProperties: map[string]any{"startIpAddress": fw.Start, "endIpAddress": fw.End},
	}
}

func putSQLAdmin(srv *sqlServer, id ResourceID, body map[string]any) (map[string]any, error) {
	if !strings.EqualFold(id.LeafName(), sqlAdminName) {
		return nil, invalidSQL("The administrator is named %q.", sqlAdminName)
	}

	props, _ := body[fieldProperties].(map[string]any)
	login, _ := props["login"].(string)
	sid, _ := props["sid"].(string)
	tenant, _ := props["tenantId"].(string)

	if login == "" || validGUID(sid) != nil {
		return nil, invalidSQL("login and a GUID sid are required.")
	}

	if tenant != "" && validGUID(tenant) != nil {
		return nil, invalidSQL("tenantId must be a GUID.")
	}

	srv.Admin = &sqlAdmin{Login: login, SID: sid, TenantID: tenant}

	return adminSQLBody(id, srv.Admin), nil
}

func validGUID(s string) error {
	_, err := uuid.Parse(s)

	return err //nolint:wrapcheck // only nil-ness is used
}

func adminSQLBody(id ResourceID, a *sqlAdmin) map[string]any {
	props := map[string]any{"administratorType": sqlAdminName, "login": a.Login, "sid": a.SID}
	if a.TenantID != "" {
		props["tenantId"] = a.TenantID
	}

	return map[string]any{
		"id": id.ARMID(), fieldName: sqlAdminName, fieldType: namespaceMicrosoftSQL + "/servers/" + sqlAdminsType,
		fieldProperties: props,
	}
}

func (p *SQLProvider) deleteChild(id ResourceID, kind sqlKindOf) error {
	srv, err := p.server(id)
	if err != nil {
		return err
	}

	switch kind {
	case sqlKindFirewall:
		key := strings.ToLower(id.LeafName())
		if _, ok := srv.Firewall[key]; !ok {
			return ErrSQLChildNotFound
		}

		delete(srv.Firewall, key)
	case sqlKindAdmin:
		if srv.Admin == nil {
			return ErrSQLChildNotFound
		}

		srv.Admin = nil
	case sqlKindPool:
		return deleteSQLPool(srv, id)
	case sqlKindServerSetting, sqlKindDBSetting:
		return deleteSetting(srv, id, kind)
	case sqlKindUnsupported, sqlKindServer, sqlKindDatabase:
		return errUnsupportedSQLType(id)
	}

	return nil
}

// ListChildren lists the children of type childType under a server or database.
func (p *SQLProvider) ListChildren(parent ResourceID, childType string) ([]map[string]any, error) {
	p.mu.RLock("ListChildren")
	defer p.mu.RUnlock()

	srv, err := p.server(parent)
	if err != nil {
		return nil, err
	}

	if len(parent.Names) == 1 && sqlUntrackedCollections[strings.ToLower(childType)] {
		return []map[string]any{}, nil
	}

	child := parent
	child.Types = append(slices.Clone(parent.Types), childType)

	kind := classifySQLPath(child)
	if kind == sqlKindUnsupported {
		return nil, errUnsupportedSQLType(child)
	}

	if len(parent.Names) == sqlParentWithDB {
		if _, ok := srv.Databases[strings.ToLower(parent.Names[1])]; !ok {
			return nil, ErrSQLChildNotFound
		}
	}

	return p.listKind(srv, parent, child, kind), nil
}

func (p *SQLProvider) listKind(srv *sqlServer, parent, child ResourceID, kind sqlKindOf) []map[string]any {
	out := []map[string]any{}
	at := func(name string) ResourceID {
		c := child
		c.Names = append(slices.Clone(parent.Names), name)

		return c
	}

	switch kind {
	case sqlKindDatabase:
		for _, db := range sortedMap(srv.Databases) {
			out = append(out, p.databaseBody(at(db.Name), db))
		}
	case sqlKindFirewall:
		for _, fw := range sortedMap(srv.Firewall) {
			out = append(out, firewallSQLBody(at(fw.Name), fw))
		}
	case sqlKindPool:
		for _, pool := range sortedMap(srv.Pools) {
			out = append(out, p.poolBody(at(pool.Name), pool))
		}
	case sqlKindAdmin:
		if srv.Admin != nil {
			out = append(out, adminSQLBody(at(sqlAdminName), srv.Admin))
		}
	case sqlKindServerSetting, sqlKindDBSetting:
		def := settingDefFor(child, kind)
		if body, err := settingBody(srv, at(def.name), kind); err == nil {
			out = append(out, body)
		}
	case sqlKindUnsupported, sqlKindServer:
	}

	return out
}

func sortedMap[V any](m map[string]V) []V {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	out := make([]V, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k])
	}

	return out
}
