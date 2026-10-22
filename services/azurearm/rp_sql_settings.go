package azurearm

import (
	"maps"
	"strings"
)

// sqlSettingDef is a singleton child that every server or database has: it always exists, reports its defaults
// until a PUT stores properties over them, and a DELETE restores the defaults.
type sqlSettingDef struct {
	defaults  func() map[string]any
	canonical string
	name      string
	enum      string
	enumKey   string
}

func sqlAuditDefaults() map[string]any {
	return map[string]any{
		sqlPropState:                  sqlValDisabled,
		sqlPropRetentionDays:          float64(0),
		"auditActionsAndGroups":       []any{},
		"isStorageSecondaryKeyInUse":  false,
		"isAzureMonitorTargetEnabled": false,
		"queueDelayMs":                float64(sqlAuditQueueDelayMs),
		"isManagedIdentityInUse":      false,
	}
}

func sqlAlertDefaults() map[string]any {
	return map[string]any{
		sqlPropState: sqlValDisabled, "disabledAlerts": []any{}, "emailAddresses": []any{}, "emailAccountAdmins": false,
		sqlPropRetentionDays: float64(0),
	}
}

func sqlServerSettingDefs() []sqlSettingDef {
	return []sqlSettingDef{
		{canonical: "auditingSettings", name: sqlValDefault, defaults: sqlAuditDefaults},
		{canonical: "extendedAuditingSettings", name: sqlValDefault, defaults: sqlAuditDefaults},
		{canonical: "devOpsAuditingSettings", name: sqlCreateDefault, defaults: func() map[string]any {
			return map[string]any{
				sqlPropState:                  sqlValDisabled,
				"isAzureMonitorTargetEnabled": false,
				"queueDelayMs":                float64(sqlAuditQueueDelayMs),
			}
		}},
		{canonical: "securityAlertPolicies", name: sqlCreateDefault, defaults: sqlAlertDefaults},
		{canonical: "connectionPolicies", name: sqlValDefault, defaults: func() map[string]any {
			return map[string]any{"connectionType": sqlCreateDefault}
		}},
		{canonical: "encryptionProtector", name: sqlValCurrent, defaults: func() map[string]any {
			return map[string]any{
				"serverKeyName": "ServiceManaged", "serverKeyType": "ServiceManaged", "autoRotationEnabled": false,
			}
		}},
		{canonical: "sqlVulnerabilityAssessments", name: sqlValDefault, defaults: func() map[string]any {
			return map[string]any{sqlPropState: sqlValDisabled}
		}},
		{canonical: "azureADOnlyAuthentications", name: sqlCreateDefault, defaults: func() map[string]any {
			return map[string]any{"azureADOnlyAuthentication": false}
		}},
	}
}

func sqlDBSettingDefs() []sqlSettingDef {
	return []sqlSettingDef{
		{
			canonical: "transparentDataEncryption",
			name:      sqlValCurrent,
			enumKey:   sqlPropState,
			enum:      "Enabled,Disabled",
			defaults:  func() map[string]any { return map[string]any{sqlPropState: sqlDefaultPublic} },
		},
		{canonical: "backupShortTermRetentionPolicies", name: sqlValDefault, defaults: func() map[string]any {
			return map[string]any{
				sqlPropRetentionDays:        float64(sqlShortRetentionDays),
				"diffBackupIntervalInHours": float64(sqlDiffBackupHours),
			}
		}},
		{canonical: "backupLongTermRetentionPolicies", name: sqlValDefault, defaults: sqlLTRDefaults},
		{canonical: "longTermRetentionPolicies", name: sqlValDefault, defaults: sqlLTRDefaults},
		{canonical: "geoBackupPolicies", name: sqlCreateDefault, defaults: func() map[string]any {
			return map[string]any{sqlPropState: sqlDefaultPublic, "storageType": sqlValGeo}
		}},
		{canonical: "auditingSettings", name: sqlValDefault, defaults: sqlAuditDefaults},
		{canonical: "extendedAuditingSettings", name: sqlValDefault, defaults: sqlAuditDefaults},
		{canonical: "securityAlertPolicies", name: sqlCreateDefault, defaults: sqlAlertDefaults},
		{canonical: "advancedThreatProtectionSettings", name: sqlCreateDefault, defaults: func() map[string]any {
			return map[string]any{sqlPropState: sqlValDisabled}
		}},
		{canonical: "ledgerDigestUploads", name: sqlValCurrent, defaults: func() map[string]any {
			return map[string]any{sqlPropState: sqlValDisabled}
		}},
	}
}

func sqlLTRDefaults() map[string]any {
	return map[string]any{
		"weeklyRetention":  sqlValNoRetention,
		"monthlyRetention": sqlValNoRetention,
		"yearlyRetention":  sqlValNoRetention,
		"weekOfYear":       float64(0),
	}
}

func findSettingDef(defs []sqlSettingDef, typ string) *sqlSettingDef {
	for i := range defs {
		if strings.EqualFold(defs[i].canonical, typ) {
			return &defs[i]
		}
	}

	return nil
}

func sqlServerSettingDef(typ string) *sqlSettingDef {
	return findSettingDef(sqlServerSettingDefs(), typ)
}

func sqlDBSettingDef(typ string) *sqlSettingDef { return findSettingDef(sqlDBSettingDefs(), typ) }

func settingDefFor(id ResourceID, kind sqlKindOf) *sqlSettingDef {
	typ := id.Types[len(id.Types)-1]
	if kind == sqlKindDBSetting {
		return sqlDBSettingDef(typ)
	}

	return sqlServerSettingDef(typ)
}

func storedSetting(srv *sqlServer, id ResourceID, kind sqlKindOf, def *sqlSettingDef) map[string]any {
	key := strings.ToLower(def.canonical)

	if kind == sqlKindServerSetting {
		return srv.Settings[key]
	}

	db := srv.Databases[strings.ToLower(id.Names[1])]
	if db == nil {
		return nil
	}

	m, _ := db.Settings[key].(map[string]any)

	return m
}

func settingBody(srv *sqlServer, id ResourceID, kind sqlKindOf) (map[string]any, error) {
	def := settingDefFor(id, kind)
	if def == nil || !strings.EqualFold(id.LeafName(), def.name) {
		return nil, ErrSQLChildNotFound
	}

	if kind == sqlKindDBSetting {
		if _, ok := srv.Databases[strings.ToLower(id.Names[1])]; !ok {
			return nil, ErrSQLChildNotFound
		}
	}

	props := def.defaults()
	maps.Copy(props, storedSetting(srv, id, kind, def))

	return map[string]any{
		"id": id.ARMID(), fieldName: def.name,
		fieldType:       namespaceMicrosoftSQL + "/" + strings.Join(id.Types, "/"),
		fieldProperties: props,
	}, nil
}

func (p *SQLProvider) putSetting(
	srv *sqlServer,
	id ResourceID,
	kind sqlKindOf,
	body map[string]any,
) (map[string]any, error) {
	def := settingDefFor(id, kind)
	if def == nil {
		return nil, errUnsupportedSQLType(id)
	}

	if !strings.EqualFold(id.LeafName(), def.name) {
		return nil, invalidSQL("The %s resource is named %q.", def.canonical, def.name)
	}

	props, _ := body[fieldProperties].(map[string]any)
	if err := def.check(props); err != nil {
		return nil, err
	}

	if kind == sqlKindServerSetting {
		if srv.Settings == nil {
			srv.Settings = map[string]map[string]any{}
		}

		srv.Settings[strings.ToLower(def.canonical)] = maps.Clone(props)

		return settingBody(srv, id, kind)
	}

	db, ok := srv.Databases[strings.ToLower(id.Names[1])]
	if !ok {
		return nil, ErrSQLChildNotFound
	}

	if db.Settings == nil {
		db.Settings = map[string]any{}
	}

	db.Settings[strings.ToLower(def.canonical)] = maps.Clone(props)

	return settingBody(srv, id, kind)
}

func (d *sqlSettingDef) check(props map[string]any) error {
	if d.enumKey == "" {
		return nil
	}

	v, ok := props[d.enumKey].(string)
	if !ok {
		return nil
	}

	for allowed := range strings.SplitSeq(d.enum, ",") {
		if strings.EqualFold(v, allowed) {
			return nil
		}
	}

	return invalidSQL("%s must be one of %s.", d.enumKey, d.enum)
}

func deleteSetting(srv *sqlServer, id ResourceID, kind sqlKindOf) error {
	def := settingDefFor(id, kind)
	if def == nil || !strings.EqualFold(id.LeafName(), def.name) {
		return ErrSQLChildNotFound
	}

	key := strings.ToLower(def.canonical)

	if kind == sqlKindServerSetting {
		delete(srv.Settings, key)

		return nil
	}

	db, ok := srv.Databases[strings.ToLower(id.Names[1])]
	if !ok {
		return ErrSQLChildNotFound
	}

	delete(db.Settings, key)

	return nil
}
