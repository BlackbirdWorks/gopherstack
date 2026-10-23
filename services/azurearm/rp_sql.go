package azurearm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	namespaceMicrosoftSQL = "Microsoft.Sql"
	sqlServersType        = "servers"
	sqlDatabasesType      = "databases"
	sqlFirewallType       = "firewallRules"
	sqlAdminsType         = "administrators"
	sqlPoolsType          = "elasticPools"
	sqlHostSuffix         = ".database.windows.net"
	sqlVersion            = "12.0"
	sqlKind               = "v12.0"
	sqlDefaultTLS         = "1.2"
	sqlDefaultPublic      = "Enabled"
	sqlMaxServers         = 32
	sqlMaxDatabases       = 100
	sqlMaxFirewall        = 128
	sqlMaxPools           = 100
	sqlMaxOperations      = 1024
	sqlCodeConflict       = "Conflict"
	sqlCodeInvalid        = "InvalidParameterValue"
	sqlCodePassword       = "PasswordNotComplex"
	sqlStatusSucceeded    = "Succeeded"
)

func sqlAPIVersions() []string {
	return []string{
		"2024-05-01-preview", "2023-08-01-preview", "2023-08-01", "2022-05-01-preview", "2021-11-01",
		"2021-02-01-preview", "2020-11-01-preview", "2014-04-01",
	}
}

type sqlKindOf int

const (
	sqlKindUnsupported sqlKindOf = iota
	sqlKindServer
	sqlKindDatabase
	sqlKindFirewall
	sqlKindAdmin
	sqlKindPool
	sqlKindServerSetting
	sqlKindDBSetting
)

type sqlFirewall struct {
	Name  string `json:"name"`
	Start string `json:"start"`
	End   string `json:"end"`
}

type sqlAdmin struct {
	Login    string `json:"login"`
	SID      string `json:"sid"`
	TenantID string `json:"tenantId"`
}

type sqlDatabase struct {
	CreatedAt  time.Time         `json:"createdAt"`
	Tags       map[string]string `json:"tags,omitempty"`
	Extra      map[string]any    `json:"extra,omitempty"`
	SKU        map[string]any    `json:"sku,omitempty"`
	Settings   map[string]any    `json:"settings,omitempty"`
	Name       string            `json:"name"`
	Location   string            `json:"location"`
	Collation  string            `json:"collation"`
	PoolID     string            `json:"poolId,omitempty"`
	DatabaseID string            `json:"databaseId"`
	MaxSize    int64             `json:"maxSize"`
}

type sqlPool struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags,omitempty"`
	Extra     map[string]any    `json:"extra,omitempty"`
	SKU       map[string]any    `json:"sku,omitempty"`
	Name      string            `json:"name"`
	Location  string            `json:"location"`
	MaxSize   int64             `json:"maxSize"`
}

// sqlServer is the ARM-side state of one logical server. The engine, its port and the databases' data live
// behind the SQL seam; the admin password is never stored here.
type sqlServer struct {
	CreatedAt    time.Time                 `json:"createdAt"`
	Tags         map[string]string         `json:"tags,omitempty"`
	Extra        map[string]any            `json:"extra,omitempty"`
	Identity     map[string]any            `json:"identity,omitempty"`
	Databases    map[string]*sqlDatabase   `json:"databases,omitempty"`
	Firewall     map[string]sqlFirewall    `json:"firewall,omitempty"`
	Pools        map[string]*sqlPool       `json:"pools,omitempty"`
	Settings     map[string]map[string]any `json:"settings,omitempty"`
	Admin        *sqlAdmin                 `json:"admin,omitempty"`
	Name         string                    `json:"name"`
	ResourceGrp  string                    `json:"resourceGroup"`
	Location     string                    `json:"location"`
	Login        string                    `json:"login,omitempty"`
	MinTLS       string                    `json:"minTls"`
	PublicAccess string                    `json:"publicAccess"`
}

// SQLProvider implements ResourceProvider for Microsoft.Sql. ARM keeps the resource metadata and child
// resources; the SQL Server engines and their databases live behind the SQL seam.
type SQLProvider struct {
	now       func() time.Time
	dataPlane SQL
	mu        *lockmetrics.RWMutex
	opMu      *lockmetrics.RWMutex
	res       map[string]*sqlServer
	ops       map[string]struct{}
	opOrder   []string
}

// NewSQLProvider creates a SQLProvider; dataPlane may be nil.
func NewSQLProvider(dataPlane SQL) *SQLProvider {
	if dataPlane == nil {
		dataPlane = noopSQL{}
	}

	return &SQLProvider{
		now: time.Now, dataPlane: dataPlane, mu: lockmetrics.New("azurearm.sqlprovider"),
		opMu: lockmetrics.New("azurearm.sqlops"), res: map[string]*sqlServer{}, ops: map[string]struct{}{},
	}
}

var _ ResourceProvider = (*SQLProvider)(nil)

// Namespace implements ResourceProvider.
func (p *SQLProvider) Namespace() string { return namespaceMicrosoftSQL }

// ResourceTypes implements ResourceProvider.
func (p *SQLProvider) ResourceTypes() []ResourceTypeDef {
	v := sqlAPIVersions()
	serverDefs, dbDefs := sqlServerSettingDefs(), sqlDBSettingDefs()
	defs := make([]ResourceTypeDef, 0, sqlFixedTypes+len(serverDefs)+len(dbDefs))
	defs = append(defs,
		ResourceTypeDef{Type: sqlServersType, APIVersions: v, HasChildren: true},
		ResourceTypeDef{Type: sqlServersType + "/" + sqlDatabasesType, APIVersions: v, HasChildren: true},
		ResourceTypeDef{Type: sqlServersType + "/" + sqlFirewallType, APIVersions: v},
		ResourceTypeDef{Type: sqlServersType + "/" + sqlAdminsType, APIVersions: v},
		ResourceTypeDef{Type: sqlServersType + "/" + sqlPoolsType, APIVersions: v},
	)

	for _, d := range serverDefs {
		defs = append(defs, ResourceTypeDef{Type: sqlServersType + "/" + d.canonical, APIVersions: v})
	}

	for _, d := range dbDefs {
		defs = append(defs, ResourceTypeDef{
			Type: sqlServersType + "/" + sqlDatabasesType + "/" + d.canonical, APIVersions: v,
		})
	}

	return defs
}

func classifySQLPath(id ResourceID) sqlKindOf {
	types := strings.ToLower(strings.Join(id.Types, "/"))

	switch types {
	case sqlServersType:
		return sqlKindServer
	case sqlServersType + "/" + strings.ToLower(sqlDatabasesType):
		return sqlKindDatabase
	case sqlServersType + "/" + strings.ToLower(sqlFirewallType):
		return sqlKindFirewall
	case sqlServersType + "/" + strings.ToLower(sqlAdminsType):
		return sqlKindAdmin
	case sqlServersType + "/" + strings.ToLower(sqlPoolsType):
		return sqlKindPool
	}

	if len(id.Types) == 2 && sqlServerSettingDef(id.Types[1]) != nil {
		return sqlKindServerSetting
	}

	if len(id.Types) == 3 && strings.EqualFold(id.Types[1], sqlDatabasesType) && sqlDBSettingDef(id.Types[2]) != nil {
		return sqlKindDBSetting
	}

	return sqlKindUnsupported
}

func errUnsupportedSQLType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.Sql resource type %q", ErrResourceNotFound, id.ResourceType())
}

func sqlServerID(id ResourceID) ResourceID {
	return ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: namespaceMicrosoftSQL,
		Types: []string{sqlServersType}, Names: id.Names[:1],
	}
}

func sqlKey(id ResourceID) string { return strings.ToLower(sqlServerID(id).ARMID()) }

// sqlInvalidError is a request the provider refuses; msg is returned to the caller verbatim.
type sqlInvalidError struct{ msg string }

func (e *sqlInvalidError) Error() string { return e.msg }

func (e *sqlInvalidError) Is(target error) bool { return target == ErrSQLInvalid }

func invalidSQL(format string, args ...any) error {
	return fmt.Errorf("%w: %w", ErrInvalidRequestBody, &sqlInvalidError{msg: fmt.Sprintf(format, args...)})
}

// engineSQLError wraps a failure of the SQL engine, which is not the caller's fault.
func engineSQLError(err error) error { return fmt.Errorf("%w: %w", ErrSQLEngine, err) }

func (p *SQLProvider) server(id ResourceID) (*sqlServer, error) {
	rec, ok := p.res[sqlKey(id)]
	if !ok {
		return nil, ErrSQLServerNotFound
	}

	return rec, nil
}

// Put implements ResourceProvider.
func (p *SQLProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	switch kind := classifySQLPath(id); kind {
	case sqlKindServer:
		return p.putServer(id, body)
	case sqlKindDatabase:
		return p.putDatabase(id, body)
	case sqlKindUnsupported:
		return nil, errUnsupportedSQLType(id)
	default:
		p.mu.Lock("Put")
		defer p.mu.Unlock()

		return p.putChild(id, kind, body)
	}
}

func (p *SQLProvider) putChild(id ResourceID, kind sqlKindOf, body map[string]any) (map[string]any, error) {
	rec, err := p.server(id)
	if err != nil {
		return nil, err
	}

	switch kind {
	case sqlKindFirewall:
		return putSQLFirewall(rec, id, body)
	case sqlKindAdmin:
		return putSQLAdmin(rec, id, body)
	case sqlKindPool:
		return p.putPool(rec, id, body)
	case sqlKindServerSetting, sqlKindDBSetting:
		return p.putSetting(rec, id, kind, body)
	case sqlKindUnsupported, sqlKindServer, sqlKindDatabase:
	}

	return nil, errUnsupportedSQLType(id)
}

// Get implements ResourceProvider.
func (p *SQLProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	kind := classifySQLPath(id)
	if kind == sqlKindUnsupported {
		return nil, errUnsupportedSQLType(id)
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	rec, err := p.server(id)
	if err != nil {
		return nil, err
	}

	return p.childBody(id, kind, rec)
}

func (p *SQLProvider) childBody(id ResourceID, kind sqlKindOf, rec *sqlServer) (map[string]any, error) {
	switch kind {
	case sqlKindDatabase:
		db, ok := rec.Databases[strings.ToLower(id.LeafName())]
		if !ok {
			return nil, ErrSQLChildNotFound
		}

		return p.databaseBody(id, db), nil
	case sqlKindFirewall:
		fw, ok := rec.Firewall[strings.ToLower(id.LeafName())]
		if !ok {
			return nil, ErrSQLChildNotFound
		}

		return firewallSQLBody(id, fw), nil
	case sqlKindAdmin:
		if rec.Admin == nil || !strings.EqualFold(id.LeafName(), sqlAdminName) {
			return nil, ErrSQLChildNotFound
		}

		return adminSQLBody(id, rec.Admin), nil
	case sqlKindPool:
		pool, ok := rec.Pools[strings.ToLower(id.LeafName())]
		if !ok {
			return nil, ErrSQLChildNotFound
		}

		return p.poolBody(id, pool), nil
	case sqlKindServerSetting, sqlKindDBSetting:
		return settingBody(rec, id, kind)
	case sqlKindUnsupported, sqlKindServer:
	}

	return p.serverBody(id, rec), nil
}

// Delete implements ResourceProvider.
func (p *SQLProvider) Delete(_ context.Context, id ResourceID) error {
	switch kind := classifySQLPath(id); kind {
	case sqlKindServer:
		return p.deleteServer(id)
	case sqlKindDatabase:
		return p.deleteDatabase(id)
	case sqlKindUnsupported:
		return errUnsupportedSQLType(id)
	default:
		p.mu.Lock("Delete")
		defer p.mu.Unlock()

		return p.deleteChild(id, kind)
	}
}

func (p *SQLProvider) deleteServer(id ResourceID) error {
	key := sqlKey(id)

	p.mu.Lock("deleteServer")
	_, ok := p.res[key]
	delete(p.res, key)
	p.mu.Unlock()

	if !ok {
		return ErrSQLServerNotFound
	}

	return p.dataPlane.DeleteServer(key)
}

// List implements ResourceProvider; only servers are listable here, children use ListChildren.
func (p *SQLProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	out := []map[string]any{}

	if classifySQLPath(id) == sqlKindServer {
		for _, rec := range p.res {
			if id.ResourceGroup != "" && !resourceGroupsEqual(rec.ResourceGrp, id.ResourceGroup) {
				continue
			}

			rid := ResourceID{
				SubscriptionID: id.SubscriptionID, ResourceGroup: rec.ResourceGrp,
				Namespace: namespaceMicrosoftSQL, Types: []string{sqlServersType}, Names: []string{rec.Name},
			}
			out = append(out, p.serverBody(rid, rec))
		}
	}

	slices.SortFunc(out, func(a, b map[string]any) int { return strings.Compare(nameField(a), nameField(b)) })

	return out, nil
}

// ListKeys implements ResourceProvider; SQL servers have no keys.
func (p *SQLProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *SQLProvider) Reset() {
	p.mu.Lock("Reset")
	p.res = map[string]*sqlServer{}
	p.mu.Unlock()

	p.opMu.Lock("Reset")
	p.ops, p.opOrder = map[string]struct{}{}, nil
	p.opMu.Unlock()
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *SQLProvider) DeleteResourcesInGroup(ctx context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")

	var keys []string

	for key, rec := range p.res {
		if resourceGroupsEqual(rec.ResourceGrp, resourceGroup) {
			keys = append(keys, key)
			delete(p.res, key)
		}
	}

	p.mu.Unlock()

	for _, key := range keys {
		if err := p.dataPlane.DeleteServer(key); err != nil {
			logger.Load(ctx).Log(ctx, slog.LevelWarn, "azurearm: delete sql server failed", "server", key, "error", err)
		}
	}
}

func (p *SQLProvider) nameTakenElsewhere(id ResourceID) bool {
	for _, r := range p.res {
		if strings.EqualFold(r.Name, id.LeafName()) && !resourceGroupsEqual(r.ResourceGrp, id.ResourceGroup) {
			return true
		}
	}

	return false
}

// CreateOperation registers an async operation and returns its ID.
func (p *SQLProvider) CreateOperation() string {
	p.opMu.Lock("CreateOperation")
	defer p.opMu.Unlock()

	id := newOperationID()

	p.ops[id] = struct{}{}
	if p.opOrder = append(p.opOrder, id); len(p.opOrder) > sqlMaxOperations {
		delete(p.ops, p.opOrder[0])
		p.opOrder = p.opOrder[1:]
	}

	return id
}

// OperationKnown reports whether id was issued by CreateOperation and has not aged out.
func (p *SQLProvider) OperationKnown(id string) bool {
	p.opMu.RLock("OperationKnown")
	defer p.opMu.RUnlock()

	_, ok := p.ops[id]

	return ok
}

// sqlPasswordError carries the reason a password was refused.
type sqlPasswordError struct{ reason string }

func (e *sqlPasswordError) Error() string { return "azurearm: sql password: " + e.reason }

func (e *sqlPasswordError) Is(target error) bool { return target == ErrSQLPasswordNotValid }

func sqlErrorDetails(err error) (errorEntry, bool) {
	if pw, ok := errors.AsType[*sqlPasswordError](err); ok {
		return errorEntry{
			sqlCodePassword,
			"Password validation failed. The password does not meet policy requirements because it is " +
				pw.reason + ".",
			http.StatusBadRequest,
		}, true
	}

	table := []struct {
		err   error
		entry errorEntry
	}{
		{
			ErrSQLServerNotFound,
			notFoundEntry("The Resource 'Microsoft.Sql/servers' under the given scope was not found."),
		},
		{ErrSQLChildNotFound, notFoundEntry("The requested Microsoft.Sql resource was not found.")},
		{ErrSQLNameTaken, errorEntry{sqlCodeConflict, "The server name is already in use.", http.StatusConflict}},
		{ErrSQLConflict, errorEntry{sqlCodeConflict, "The resource is in use.", http.StatusConflict}},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	if inv, ok := errors.AsType[*sqlInvalidError](err); ok {
		return errorEntry{sqlCodeInvalid, inv.msg, http.StatusBadRequest}, true
	}

	if errors.Is(err, ErrSQLEngine) {
		return errorEntry{
			sqlCodeUnavailable,
			"The SQL engine failed: " + err.Error(),
			http.StatusServiceUnavailable,
		}, true
	}

	return errorEntry{}, false
}

func cloneServer(r *sqlServer) *sqlServer {
	c := *r
	c.Extra, c.Tags, c.Identity = maps.Clone(r.Extra), maps.Clone(r.Tags), maps.Clone(r.Identity)
	c.Databases, c.Firewall, c.Pools = maps.Clone(r.Databases), maps.Clone(r.Firewall), maps.Clone(r.Pools)
	c.Settings = maps.Clone(r.Settings)

	return &c
}
