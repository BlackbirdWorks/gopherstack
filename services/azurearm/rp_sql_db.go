package azurearm

import (
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	sqlDefaultCollation = "SQL_Latin1_General_CP1_CI_AS"
	sqlDefaultMaxSize   = int64(34359738368)
	sqlMaxNameLen       = 128
	sqlMasterDB         = "master"
	sqlCreateDefault    = "Default"
)

var sqlBadNameRE = regexp.MustCompile(`[<>*%&:\\/?\x00-\x1f]`)

var sqlDBOwnedProps = []string{ //nolint:gochecknoglobals // read-only
	"collation", sqlPropMaxSize, "elasticPoolId", "databaseId", sqlPropStatus, sqlPropCreationDate, "createMode",
	"currentServiceObjectiveName", "requestedServiceObjectiveName", "currentSku",
}

func sqlDefaultMaintenance(id ResourceID) string {
	return "/subscriptions/" + id.SubscriptionID +
		"/providers/Microsoft.Maintenance/publicMaintenanceConfigurations/SQL_Default"
}

func validSQLChildName(name string) error {
	if name == "" || len([]rune(name)) > sqlMaxNameLen || sqlBadNameRE.MatchString(name) ||
		strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
		return invalidSQL("The name %q is not valid.", name)
	}

	return nil
}

func sqlSKUOf(body map[string]any) map[string]any {
	raw, _ := body[ehFieldSKU].(map[string]any)
	if name, _ := raw[fieldName].(string); name == "" {
		return nil
	}

	return maps.Clone(raw)
}

func sqlDefaultSKU() map[string]any {
	return map[string]any{fieldName: "S0", sqlPropTier: sqlValStandard, sqlPropCapacity: float64(sqlDefaultSKUDTU)}
}

func (p *SQLProvider) putDatabase(id ResourceID, body map[string]any) (map[string]any, error) {
	db, err := p.draftDatabase(id, body, false)
	if err != nil {
		return nil, err
	}

	return p.commitDatabase(id, db)
}

// PatchDatabase merges an Update request into an existing database.
func (p *SQLProvider) PatchDatabase(id ResourceID, body map[string]any) (map[string]any, error) {
	db, err := p.draftDatabase(id, body, true)
	if err != nil {
		return nil, err
	}

	return p.commitDatabase(id, db)
}

func (p *SQLProvider) draftDatabase(id ResourceID, body map[string]any, patch bool) (*sqlDatabase, error) {
	p.mu.RLock("draftDatabase")
	defer p.mu.RUnlock()

	srv, err := p.server(id)
	if err != nil {
		return nil, err
	}

	name := id.LeafName()
	existing, found := srv.Databases[strings.ToLower(name)]

	if err = checkNewDatabase(srv, name, found, patch); err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	db := p.freshDatabase(name, srv, existing, patch)

	if loc, _ := body[fieldLocation].(string); loc != "" {
		db.Location = loc
	}

	if _, ok := body[fieldTags]; ok {
		db.Tags = stringTags(body[fieldTags])
	}

	if sku := sqlSKUOf(body); sku != nil {
		db.SKU = sku
	}

	if err = applyDBProps(db, srv, props, found); err != nil {
		return nil, err
	}

	return db, nil
}

func checkNewDatabase(srv *sqlServer, name string, found, patch bool) error {
	if patch && !found {
		return ErrSQLChildNotFound
	}

	if err := validSQLChildName(name); err != nil {
		return err
	}

	if strings.EqualFold(name, sqlMasterDB) {
		return invalidSQL("The database name 'master' is reserved.")
	}

	if !found && len(srv.Databases) >= sqlMaxDatabases {
		return invalidSQL("At most %d databases per server are supported.", sqlMaxDatabases)
	}

	return nil
}

func (p *SQLProvider) freshDatabase(name string, srv *sqlServer, existing *sqlDatabase, patch bool) *sqlDatabase {
	if existing == nil {
		return &sqlDatabase{
			Name: name, Location: srv.Location, Collation: sqlDefaultCollation, MaxSize: sqlDefaultMaxSize,
			SKU: sqlDefaultSKU(), DatabaseID: uuid.NewString(), CreatedAt: p.now().UTC(), Extra: map[string]any{},
		}
	}

	db := *existing
	db.Extra, db.Tags, db.SKU = maps.Clone(existing.Extra), maps.Clone(existing.Tags), maps.Clone(existing.SKU)

	if !patch {
		db.Extra, db.Tags = map[string]any{}, nil
	}

	return &db
}

func applyDBProps(db *sqlDatabase, srv *sqlServer, props map[string]any, found bool) error {
	if mode, _ := props["createMode"].(string); mode != "" && mode != sqlCreateDefault {
		return invalidSQL("createMode %q is not supported; only Default is.", mode)
	}

	if c, ok := props["collation"].(string); ok && c != "" {
		if found && c != db.Collation {
			return invalidSQL("The collation of an existing database cannot be changed.")
		}

		db.Collation = c
	}

	if v, ok := props[sqlPropMaxSize].(float64); ok && v > 0 {
		db.MaxSize = int64(v)
	}

	if v, ok := props["elasticPoolId"].(string); ok {
		if err := checkPoolRef(srv, v); err != nil {
			return err
		}

		db.PoolID = v
	}

	maps.Copy(db.Extra, ehExtra(map[string]any{fieldProperties: props}, sqlDBOwnedProps...))

	return nil
}

func checkPoolRef(srv *sqlServer, poolID string) error {
	if poolID == "" {
		return nil
	}

	parts := strings.Split(strings.TrimRight(poolID, "/"), "/")
	if len(parts) == 0 {
		return invalidSQL("elasticPoolId is not a valid resource ID.")
	}

	if _, ok := srv.Pools[strings.ToLower(parts[len(parts)-1])]; !ok {
		return ErrSQLChildNotFound
	}

	return nil
}

func (p *SQLProvider) commitDatabase(id ResourceID, db *sqlDatabase) (map[string]any, error) {
	if err := p.dataPlane.EnsureDatabase(sqlKey(id), db.Name, db.Collation); err != nil {
		return nil, engineSQLError(err)
	}

	p.mu.Lock("commitDatabase")
	defer p.mu.Unlock()

	srv, err := p.server(id)
	if err != nil {
		return nil, err
	}

	if cur, ok := srv.Databases[strings.ToLower(db.Name)]; ok {
		db.Settings = cur.Settings
	}

	srv.Databases[strings.ToLower(db.Name)] = db

	return p.databaseBody(id, db), nil
}

func (p *SQLProvider) deleteDatabase(id ResourceID) error {
	p.mu.Lock("deleteDatabase")

	srv, err := p.server(id)
	if err != nil {
		p.mu.Unlock()

		return err
	}

	key := strings.ToLower(id.LeafName())
	_, ok := srv.Databases[key]
	delete(srv.Databases, key)
	p.mu.Unlock()

	if !ok {
		return ErrSQLChildNotFound
	}

	if err = p.dataPlane.DropDatabase(sqlKey(id), id.LeafName()); err != nil {
		return engineSQLError(err)
	}

	return nil
}

func (p *SQLProvider) databaseBody(id ResourceID, db *sqlDatabase) map[string]any {
	sku := maps.Clone(db.SKU)
	defaults := map[string]any{
		sqlPropZone: false, "readScale": sqlValDisabled, "catalogCollation": sqlDefaultCollation,
		"defaultSecondaryLocation": DefaultLocation, "isInfraEncryptionEnabled": false, "isLedgerOn": false,
		"currentBackupStorageRedundancy": sqlValGeo, "requestedBackupStorageRedundancy": sqlValGeo,
		"licenseType": "LicenseIncluded", "maintenanceConfigurationId": sqlDefaultMaintenance(id),
	}
	typed := map[string]any{
		"collation": db.Collation, sqlPropMaxSize: db.MaxSize, sqlPropStatus: "Online", "databaseId": db.DatabaseID,
		sqlPropCreationDate: db.CreatedAt.Format(time.RFC3339Nano), "currentServiceObjectiveName": sku[fieldName],
		"requestedServiceObjectiveName": sku[fieldName], "currentSku": sku,
	}

	if db.PoolID != "" {
		typed["elasticPoolId"] = db.PoolID
	}

	out := map[string]any{
		"id": id.ARMID(), fieldName: db.Name, fieldType: namespaceMicrosoftSQL + "/servers/databases",
		fieldLocation: db.Location, fieldTags: tagsOrEmpty(db.Tags), sqlPropKind: sqlKind + ",user", ehFieldSKU: sku,
		fieldProperties: ehMerge(defaults, db.Extra, typed),
	}

	return out
}

var sqlPoolOwnedProps = []string{ //nolint:gochecknoglobals // read-only
	sqlPropMaxSize, sqlPropState, sqlPropCreationDate,
}

func (p *SQLProvider) putPool(srv *sqlServer, id ResourceID, body map[string]any) (map[string]any, error) {
	name := id.LeafName()
	if err := validSQLChildName(name); err != nil {
		return nil, err
	}

	key := strings.ToLower(name)
	if _, exists := srv.Pools[key]; !exists && len(srv.Pools) >= sqlMaxPools {
		return nil, invalidSQL("At most %d elastic pools per server are supported.", sqlMaxPools)
	}

	pool := &sqlPool{
		Name: name, Location: srv.Location, MaxSize: sqlDefaultMaxSize, CreatedAt: p.now().UTC(),
		SKU: map[string]any{
			fieldName:       "StandardPool",
			sqlPropTier:     sqlValStandard,
			sqlPropCapacity: float64(sqlDefaultPoolCapacity),
		},
		Extra: map[string]any{},
	}

	if cur, ok := srv.Pools[key]; ok {
		pool.CreatedAt = cur.CreatedAt
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		pool.Location = loc
	}

	pool.Tags = stringTags(body[fieldTags])

	if sku := sqlSKUOf(body); sku != nil {
		pool.SKU = sku
	}

	props, _ := body[fieldProperties].(map[string]any)
	if v, ok := props[sqlPropMaxSize].(float64); ok && v > 0 {
		pool.MaxSize = int64(v)
	}

	maps.Copy(pool.Extra, ehExtra(map[string]any{fieldProperties: props}, sqlPoolOwnedProps...))
	srv.Pools[key] = pool

	return p.poolBody(id, pool), nil
}

func (p *SQLProvider) poolBody(id ResourceID, pool *sqlPool) map[string]any {
	defaults := map[string]any{
		sqlPropZone: false, "licenseType": "LicenseIncluded", "maintenanceConfigurationId": sqlDefaultMaintenance(id),
		"perDatabaseSettings": map[string]any{"minCapacity": float64(0), "maxCapacity": pool.SKU[sqlPropCapacity]},
	}
	typed := map[string]any{
		sqlPropMaxSize:      pool.MaxSize,
		sqlPropState:        sqlValReady,
		sqlPropCreationDate: pool.CreatedAt.Format(time.RFC3339Nano),
	}

	return map[string]any{
		"id": id.ARMID(), fieldName: pool.Name, fieldType: namespaceMicrosoftSQL + "/servers/elasticPools",
		fieldLocation: pool.Location, fieldTags: tagsOrEmpty(pool.Tags), ehFieldSKU: pool.SKU,
		fieldProperties: ehMerge(defaults, pool.Extra, typed),
	}
}

func deleteSQLPool(srv *sqlServer, id ResourceID) error {
	key := strings.ToLower(id.LeafName())
	if _, ok := srv.Pools[key]; !ok {
		return ErrSQLChildNotFound
	}

	for _, db := range srv.Databases {
		if strings.HasSuffix(strings.ToLower(db.PoolID), "/"+key) {
			return ErrSQLConflict
		}
	}

	delete(srv.Pools, key)

	return nil
}
