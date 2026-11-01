package azurearm

import (
	"context"
	"regexp"
	"strconv"
	"strings"
)

var (
	flexCharsetRE = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)
	pgCharsetRE   = regexp.MustCompile(`^[a-zA-Z]+\w*$`)
	pgCollationRE = regexp.MustCompile(`^[a-zA-Z\-]+([. ]|\w)*$`)
)

// validCharset applies the DatabaseProperties patterns of the PostgreSQL spec; MySQL declares none.
func (f *flexFlavor) validCharset(charset, collation string) bool {
	if f.kind == flexKindPostgres {
		return pgCharsetRE.MatchString(charset) && pgCollationRE.MatchString(collation)
	}

	return flexCharsetRE.MatchString(charset) && flexCharsetRE.MatchString(collation)
}

func (p *FlexDBProvider) writeDatabase(id ResourceID, body map[string]any) (flexResult, error) {
	props := flexMap(body, fieldProperties)
	name := id.LeafName()
	f := p.flavor

	if !f.dbNameRE.MatchString(name) {
		return flexResult{}, flexInvalid("The database name '%s' is not valid.", name)
	}

	if f.isSystemDB(name) {
		return flexResult{}, flexInvalid("The database '%s' is a system database and cannot be modified.", name)
	}

	charset, collation := f.defaultCharset, f.defaultCollate
	if v, ok := flexStr(props, "charset"); ok && v != "" {
		charset = v
	}

	if v, ok := flexStr(props, flexPropCollation); ok && v != "" {
		collation = v
	}

	if !f.validCharset(charset, collation) {
		return flexResult{}, flexInvalid("The charset or collation is not valid.")
	}

	db := flexDatabase{Name: name, Charset: charset, Collation: collation}

	eng, key, created, err := p.storeDatabase(id, db)
	if err != nil {
		return flexResult{}, err
	}

	if created && eng != nil {
		if err = eng.CreateDatabase(p.ctx, key, db.Name, db.Charset, db.Collation); err != nil {
			p.mu.Lock("writeDatabase")
			if rec, ok := p.res[key]; ok {
				delete(rec.Databases, strings.ToLower(name))
			}
			p.mu.Unlock()

			return flexResult{}, flexEngineError()
		}
	}

	return flexResult{body: p.databaseBody(id, db), opID: p.newOp(id.ARMID(), true, "", nil)}, nil
}

func flexEngineError() error {
	return &flexError{redisCodeUnavailable, flexOpFailedMsg, 503}
}

func (p *FlexDBProvider) storeDatabase(id ResourceID, db flexDatabase) (FlexEngine, string, bool, error) {
	p.mu.Lock("storeDatabase")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return nil, "", false, err
	}

	if p.stateOf(rec) != flexStateReady {
		return nil, "", false, p.notReady(rec, "Create or update database")
	}

	k := strings.ToLower(db.Name)

	if cur, ok := rec.Databases[k]; ok {
		if cur.Charset != db.Charset || cur.Collation != db.Collation {
			return nil, "", false, flexInvalid("The charset and collation of an existing database cannot be changed.")
		}

		return nil, "", false, nil
	}

	if rec.Databases == nil {
		rec.Databases = map[string]flexDatabase{}
	}

	rec.Databases[k] = db

	return p.engine, p.key(id), true, nil
}

func (p *FlexDBProvider) deleteDatabase(id ResourceID) (string, error) {
	name := id.LeafName()

	if p.flavor.isSystemDB(name) {
		return "", flexInvalid("The database '%s' is a system database and cannot be dropped.", name)
	}

	p.mu.Lock("deleteDatabase")

	rec, err := p.server(id)
	if err != nil {
		p.mu.Unlock()

		return "", err
	}

	k := strings.ToLower(name)
	db, ok := rec.Databases[k]

	if !ok {
		p.mu.Unlock()

		return "", p.childNotFound(id)
	}

	delete(rec.Databases, k)

	eng, key := p.engine, p.key(id)
	p.mu.Unlock()

	if eng != nil {
		if err = eng.DropDatabase(p.ctx, key, db.Name); err != nil {
			return "", flexEngineError()
		}
	}

	return p.newOp(id.ARMID(), false, "", nil), nil
}

func (p *FlexDBProvider) configValue(rec *flexServer, c flexConfigDef) (string, string) {
	if v, ok := rec.Config[c.name]; ok {
		return v, "user-override"
	}

	return p.configDefault(rec, c), "system-default"
}

func (p *FlexDBProvider) configDefault(rec *flexServer, c flexConfigDef) string {
	if c.name == cfgMaxConn {
		if v := p.maxConnections(rec); v != "" {
			return v
		}
	}

	if c.name == "server_version" {
		return rec.Version
	}

	if c.name == "sql_generate_invisible_primary_key" && rec.Version == "5.7" {
		return cfgOff
	}

	return c.def
}

func (p *FlexDBProvider) flexBool(b bool) any {
	if !p.flavor.boolStrings {
		return b
	}

	if b {
		return "True"
	}

	return "False"
}

func (p *FlexDBProvider) configEnvelope(id ResourceID, rec *flexServer, c flexConfigDef) map[string]any {
	value, source := p.configValue(rec, c)
	def := p.configDefault(rec, c)

	props := map[string]any{
		"value": value, "source": source, "defaultValue": def, "dataType": c.dataType,
		netPropDescription: c.desc, "allowedValues": p.allowedValues(rec, c), "isReadOnly": p.flexBool(c.readOnly),
		"isDynamicConfig":        p.flexBool(!c.static && !c.readOnly),
		"isConfigPendingRestart": p.flexBool(rec.Pending[c.name]),
		"documentationLink": "https://learn.microsoft.com/azure/" + map[string]string{
			flexKindPostgres: "postgresql", flexKindMySQL: flexKindMySQL,
		}[p.flavor.kind] + "/flexible-server/concepts-server-parameters",
	}

	if c.unit != "" {
		props["unit"] = c.unit
	}

	if p.flavor.kind == flexKindMySQL {
		props["currentValue"] = value
	}

	return p.childEnvelope(id, flexConfigsType, props)
}

// mysqlConnRange is the per-SKU max_connections range of the MySQL server parameters page.
func (p *FlexDBProvider) mysqlConnRange(rec *flexServer, c flexConfigDef) (int, int, bool) {
	if p.flavor.kind != flexKindMySQL || c.name != cfgMaxConn {
		return 0, 0, false
	}

	s, ok := p.flavor.skuByName(rec.SKUName)

	return mysqlMinConns, s.connMax, ok && s.connMax > 0
}

func (p *FlexDBProvider) checkConnRange(rec *flexServer, c flexConfigDef, value string, reset bool) error {
	lo, hi, ok := p.mysqlConnRange(rec, c)
	if !ok || reset {
		return nil
	}

	if n, err := strconv.Atoi(value); err != nil || n < lo || n > hi {
		return flexInvalid("The value for '%s' is not valid: allowed values are %d-%d.", c.name, lo, hi)
	}

	return nil
}

func (p *FlexDBProvider) allowedValues(rec *flexServer, c flexConfigDef) string {
	if lo, hi, ok := p.mysqlConnRange(rec, c); ok {
		return strconv.Itoa(lo) + "-" + strconv.Itoa(hi)
	}

	return c.allowedValues()
}

func (p *FlexDBProvider) configBody(id ResourceID, rec *flexServer) (map[string]any, error) {
	c, ok := p.flavor.config(id.LeafName())
	if !ok {
		return nil, p.childNotFound(id)
	}

	id.Names = []string{id.Names[0], c.name}

	return p.configEnvelope(id, rec, c), nil
}

func (p *FlexDBProvider) writeConfig(id ResourceID, body map[string]any) (flexResult, error) {
	c, ok := p.flavor.config(id.LeafName())
	if !ok {
		return flexResult{}, p.childNotFound(id)
	}

	props := flexMap(body, fieldProperties)
	value, hasValue := flexStr(props, "value")
	source, _ := flexStr(props, "source")

	if c.readOnly {
		return flexResult{}, flexInvalid("The server parameter '%s' is read-only.", c.name)
	}

	reset := source == "system-default"

	if !reset {
		if !hasValue {
			return flexResult{}, flexInvalid("A value is required for the server parameter '%s'.", c.name)
		}

		if why := configValueProblem(c, value); why != "" {
			return flexResult{}, flexInvalid("The value for '%s' is not valid: %s.", c.name, why)
		}
	}

	p.mu.Lock("writeConfig")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return flexResult{}, err
	}

	if err = p.checkConnRange(rec, c, value, reset); err != nil {
		return flexResult{}, err
	}

	if rec.Config == nil {
		rec.Config = map[string]string{}
	}

	if reset {
		delete(rec.Config, c.name)
	} else {
		rec.Config[c.name] = value
	}

	if c.static {
		if rec.Pending == nil {
			rec.Pending = map[string]bool{}
		}

		rec.Pending[c.name] = true
	}

	id.Names = []string{id.Names[0], c.name}

	return flexResult{body: p.configEnvelope(id, rec, c), opID: p.newOp(id.ARMID(), true, "", nil)}, nil
}

// CheckName answers checkNameAvailability for a server name. Names are unique across the subscription.
func (p *FlexDBProvider) CheckName(_ context.Context, name, typ string) map[string]any {
	want := p.flavor.ns + "/" + flexServersType
	out := map[string]any{fieldName: name, fieldType: typ, "nameAvailable": true}

	switch {
	case !strings.EqualFold(typ, want):
		out["nameAvailable"], out["reason"] = false, webReasonInvalid
		out["message"] = "The resource type must be " + want + "."
	case validFlexName(name) != nil:
		out["nameAvailable"], out["reason"] = false, webReasonInvalid
		out["message"] = "The server name must be 3 to 63 characters of lowercase letters, digits and hyphens, " +
			"and cannot start or end with a hyphen."
	default:
		p.mu.RLock("CheckName")
		defer p.mu.RUnlock()

		for _, r := range p.res {
			if strings.EqualFold(r.Name, name) {
				out["nameAvailable"], out["reason"] = false, kvReasonAlreadyExists
				out["message"] = "Server '" + name + "' already exists. Choose a different name."
			}
		}
	}

	return out
}

// Capabilities returns the location capability list in the flavor's v1 shape.
func (p *FlexDBProvider) Capabilities() []map[string]any {
	if p.flavor.kind == flexKindPostgres {
		return []map[string]any{p.pgCapabilities()}
	}

	return []map[string]any{p.mysqlCapabilities()}
}

func (p *FlexDBProvider) pgCapabilities() map[string]any {
	f := p.flavor
	editions := make([]any, 0, flexTierCount)

	for _, tier := range []string{tierBurstable, tierGeneralPurpose, tierMemoryOptimized} {
		versions := make([]any, 0, len(f.versions))

		for _, v := range f.versions {
			cores := []any{}

			for _, s := range f.skus {
				if s.tier == tier {
					cores = append(cores, map[string]any{
						fieldName:                   s.name,
						"vCores":                    s.vcores,
						"supportedMemoryPerVcoreMB": s.memGiB * flexMemMB / s.vcores,
						acrPropStatus:               flexAvailable,
					})
				}
			}

			versions = append(
				versions,
				map[string]any{fieldName: v, acrPropStatus: flexAvailable, "supportedVcores": cores},
			)
		}

		sizes := make([]any, 0, len(f.storageSizes))

		for _, gb := range f.storageSizes {
			t, iops := pgStorageTier(gb)
			sizes = append(sizes, map[string]any{
				fieldName: t, "storageSizeMB": gb * flexMemMB, "supportedIops": iops, acrPropStatus: flexAvailable,
			})
		}

		editions = append(editions, map[string]any{
			fieldName: tier, acrPropStatus: flexAvailable, flexPropSupportedVersions: versions,
			flexPropStorageEditions: []any{map[string]any{
				fieldName: "ManagedDisk", acrPropStatus: flexAvailable, "supportedStorageMB": sizes,
			}},
		})
	}

	return map[string]any{
		fieldName: "FlexibleServerCapabilities", "zone": aksPluginNone, acrPropStatus: flexAvailable,
		"supportedHAMode": []string{flexHASameZone, flexHAZoneRedundant}, "geoBackupSupported": true,
		"zoneRedundantHaSupported": true, "zoneRedundantHaAndGeoBackupSupported": true,
		"fastProvisioningSupported": false, flexPropServerEditions: editions,
	}
}

func (p *FlexDBProvider) mysqlSKUCaps(tier string, v2 bool) []any {
	out := []any{}

	for _, s := range p.flavor.skus {
		if s.tier != tier {
			continue
		}

		c := map[string]any{
			fieldName: s.name, "vCores": s.vcores, "supportedMemoryPerVCoreMB": s.memGiB * flexMemMB / s.vcores,
			"supportedIops": s.maxIops,
		}

		if v2 {
			c["supportedHAMode"] = []string{flexHASameZone, flexHAZoneRedundant}
			c["supportedZones"] = []string{"1", "2", "3"}

			if tier == tierBurstable {
				c["supportedHAMode"] = []string{}
			}
		}

		out = append(out, c)
	}

	return out
}

func (p *FlexDBProvider) mysqlStorageCaps() []any {
	f := p.flavor

	return []any{
		map[string]any{
			fieldName:                "Premium_LRS",
			"minStorageSize":         f.minStorage * flexMemMB,
			"maxStorageSize":         f.maxStorage * flexMemMB,
			"minBackupRetentionDays": f.minRetention,
			"maxBackupRetentionDays": flexMinRetentionMax,
			"minBackupIntervalHours": flexBackupHours,
			"maxBackupIntervalHours": flexBackupHours,
		},
	}
}

func (p *FlexDBProvider) mysqlCapabilities() map[string]any {
	editions := make([]any, 0, flexTierCount)

	for _, tier := range []string{tierBurstable, tierGeneralPurpose, tierMemoryOptimized} {
		versions := make([]any, 0, len(p.flavor.versions))
		for _, v := range p.flavor.versions {
			versions = append(versions, map[string]any{fieldName: v, "supportedSkus": p.mysqlSKUCaps(tier, false)})
		}

		editions = append(editions, map[string]any{
			fieldName: tier, flexPropSupportedVersions: versions, flexPropStorageEditions: p.mysqlStorageCaps(),
		})
	}

	return map[string]any{
		"zone": aksPluginNone, "supportedHAMode": []string{flexHASameZone, flexHAZoneRedundant},
		"supportedGeoBackupRegions": []string{}, flexPropServerEditions: editions,
	}
}

// CapabilitySet returns the MySQL v2 capability set named aksDefaultName.
func (p *FlexDBProvider) CapabilitySet(sub string) map[string]any {
	editions := make([]any, 0, flexTierCount)

	for _, tier := range []string{tierBurstable, tierGeneralPurpose, tierMemoryOptimized} {
		def := map[string]string{tierBurstable: "Standard_B1ms", tierGeneralPurpose: "Standard_D2ads_v5",
			tierMemoryOptimized: "Standard_E2ads_v5"}[tier]
		editions = append(editions, map[string]any{
			fieldName: tier, "defaultSku": def, "defaultStorageSize": p.flavor.defaultStorage * flexMemMB,
			flexPropStorageEditions: p.mysqlStorageCaps(), "supportedSkus": p.mysqlSKUCaps(tier, true),
		})
	}

	versions := make([]any, 0, len(p.flavor.versions))
	for _, v := range p.flavor.versions {
		versions = append(versions, map[string]any{fieldName: v})
	}

	return map[string]any{
		"id": "/subscriptions/" + sub + "/providers/" + p.flavor.ns + "/locations/" + DefaultLocation +
			"/capabilitySets/default",
		fieldName: aksDefaultName, fieldType: p.flavor.ns + "/locations/capabilitySets",
		fieldProperties: map[string]any{
			"supportedGeoBackupRegions": []string{}, flexPropServerEditions: editions,
			flexPropSupportedVersions: versions,
		},
	}
}
