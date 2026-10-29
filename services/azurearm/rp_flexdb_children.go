package azurearm

import (
	"context"
	"regexp"
	"strings"
)

var flexCharsetRE = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)

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

	if v, ok := flexStr(props, "collation"); ok && v != "" {
		collation = v
	}

	if !flexCharsetRE.MatchString(charset) || !flexCharsetRE.MatchString(collation) {
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
	return &flexError{"ServiceUnavailable", flexOpFailedMsg, 503}
}

func (p *FlexDBProvider) storeDatabase(id ResourceID, db flexDatabase) (FlexEngine, string, bool, error) {
	p.mu.Lock("storeDatabase")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
		return nil, "", false, err
	}

	if p.stateOf(rec) != flexStateReady {
		return nil, "", false, flexConflict(flexCodeNotReady, "The server is not in the Ready state.")
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
	if c.name == "max_connections" && p.flavor.kind == "postgres" {
		if v := p.maxConnections(rec); v != "" {
			return v
		}
	}

	if c.name == "server_version" {
		return rec.Version
	}

	if c.name == "sql_generate_invisible_primary_key" && rec.Version == "5.7" {
		return "OFF"
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
		"description": c.desc, "allowedValues": c.allowedValues(), "isReadOnly": p.flexBool(c.readOnly),
		"isDynamicConfig":        p.flexBool(!c.static && !c.readOnly),
		"isConfigPendingRestart": p.flexBool(rec.Pending[c.name]),
		"documentationLink": "https://learn.microsoft.com/azure/" + map[string]string{
			"postgres": "postgresql", "mysql": "mysql",
		}[p.flavor.kind] + "/flexible-server/concepts-server-parameters",
	}

	if c.unit != "" {
		props["unit"] = c.unit
	}

	if p.flavor.kind == "mysql" {
		props["currentValue"] = value
	}

	return p.childEnvelope(id, flexConfigsType, props)
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

		if err := validConfigValue(c, value); err != nil {
			return flexResult{}, flexInvalid("The value for '%s' is not valid: %s.", c.name, err.Error())
		}
	}

	p.mu.Lock("writeConfig")
	defer p.mu.Unlock()

	rec, err := p.server(id)
	if err != nil {
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
	out := map[string]any{"name": name, "type": typ, "nameAvailable": true}

	switch {
	case !strings.EqualFold(typ, want):
		out["nameAvailable"], out["reason"] = false, "Invalid"
		out["message"] = "The resource type must be " + want + "."
	case validFlexName(name) != nil:
		out["nameAvailable"], out["reason"] = false, "Invalid"
		out["message"] = "The server name must be 3 to 63 characters of lowercase letters, digits and hyphens, " +
			"and cannot start or end with a hyphen."
	default:
		p.mu.RLock("CheckName")
		defer p.mu.RUnlock()

		for _, r := range p.res {
			if strings.EqualFold(r.Name, name) {
				out["nameAvailable"], out["reason"] = false, "AlreadyExists"
				out["message"] = "Server '" + name + "' already exists. Choose a different name."
			}
		}
	}

	return out
}

func int64s(vs ...int) []int64 {
	out := make([]int64, len(vs))
	for i, v := range vs {
		out[i] = int64(v)
	}

	return out
}

// Capabilities returns the location capability list in the flavor's v1 shape.
func (p *FlexDBProvider) Capabilities() []map[string]any {
	if p.flavor.kind == "postgres" {
		return []map[string]any{p.pgCapabilities()}
	}

	return []map[string]any{p.mysqlCapabilities()}
}

func (p *FlexDBProvider) pgCapabilities() map[string]any {
	f := p.flavor
	editions := []any{}

	for _, tier := range []string{tierBurstable, tierGeneralPurpose, tierMemoryOptimized} {
		versions := []any{}

		for _, v := range f.versions {
			cores := []any{}

			for _, s := range f.skus {
				if s.tier == tier {
					cores = append(cores, map[string]any{
						fieldName: s.name, "vCores": s.vcores, "supportedMemoryPerVcoreMB": s.memGiB * flexMemMB / s.vcores,
						"status": "Available",
					})
				}
			}

			versions = append(versions, map[string]any{fieldName: v, "status": "Available", "supportedVcores": cores})
		}

		sizes := []any{}

		for _, gb := range f.storageSizes {
			t, iops := pgStorageTier(gb)
			sizes = append(sizes, map[string]any{
				fieldName: t, "storageSizeMB": gb * flexMemMB, "supportedIops": iops, "status": "Available",
			})
		}

		editions = append(editions, map[string]any{
			fieldName: tier, "status": "Available", "supportedServerVersions": versions,
			"supportedStorageEditions": []any{map[string]any{
				fieldName: "ManagedDisk", "status": "Available", "supportedStorageMB": sizes,
			}},
		})
	}

	return map[string]any{
		fieldName: "FlexibleServerCapabilities", "zone": "none", "status": "Available",
		"supportedHAMode": []string{flexHASameZone, flexHAZoneRedundant}, "geoBackupSupported": true,
		"zoneRedundantHaSupported": true, "zoneRedundantHaAndGeoBackupSupported": true,
		"fastProvisioningSupported": false, "supportedFlexibleServerEditions": editions,
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
			"supportedIops": mysqlMaxIops,
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

	return []any{map[string]any{
		fieldName: "Premium_LRS", "minStorageSize": f.minStorage * flexMemMB, "maxStorageSize": f.maxStorage * flexMemMB,
		"minBackupRetentionDays": f.minRetention, "maxBackupRetentionDays": flexMinRetentionMax,
		"minBackupIntervalHours": 24, "maxBackupIntervalHours": 24,
	}}
}

func (p *FlexDBProvider) mysqlCapabilities() map[string]any {
	editions := []any{}

	for _, tier := range []string{tierBurstable, tierGeneralPurpose, tierMemoryOptimized} {
		versions := []any{}
		for _, v := range p.flavor.versions {
			versions = append(versions, map[string]any{fieldName: v, "supportedSkus": p.mysqlSKUCaps(tier, false)})
		}

		editions = append(editions, map[string]any{
			fieldName: tier, "supportedServerVersions": versions, "supportedStorageEditions": p.mysqlStorageCaps(),
		})
	}

	return map[string]any{
		"zone": "none", "supportedHAMode": []string{flexHASameZone, flexHAZoneRedundant},
		"supportedGeoBackupRegions": []string{}, "supportedFlexibleServerEditions": editions,
	}
}

// CapabilitySet returns the MySQL v2 capability set named "default".
func (p *FlexDBProvider) CapabilitySet(sub string) map[string]any {
	editions := []any{}

	for _, tier := range []string{tierBurstable, tierGeneralPurpose, tierMemoryOptimized} {
		def := map[string]string{tierBurstable: "Standard_B1ms", tierGeneralPurpose: "Standard_D2ads_v5",
			tierMemoryOptimized: "Standard_E2ads_v5"}[tier]
		editions = append(editions, map[string]any{
			fieldName: tier, "defaultSku": def, "defaultStorageSize": p.flavor.defaultStorage * flexMemMB,
			"supportedStorageEditions": p.mysqlStorageCaps(), "supportedSkus": p.mysqlSKUCaps(tier, true),
		})
	}

	versions := []any{}
	for _, v := range p.flavor.versions {
		versions = append(versions, map[string]any{fieldName: v})
	}

	return map[string]any{
		"id": "/subscriptions/" + sub + "/providers/" + p.flavor.ns + "/locations/" + DefaultLocation +
			"/capabilitySets/default",
		fieldName: "default", fieldType: p.flavor.ns + "/locations/capabilitySets",
		fieldProperties: map[string]any{
			"supportedGeoBackupRegions": []string{}, "supportedFlexibleServerEditions": editions,
			"supportedServerVersions": versions,
		},
	}
}
