package backup

type lifecycleJSON struct {
	MoveToColdStorageAfterDays          int64 `json:"MoveToColdStorageAfterDays,omitempty"`
	DeleteAfterDays                     int64 `json:"DeleteAfterDays,omitempty"`
	OptInToArchiveForSupportedResources bool  `json:"OptInToArchiveForSupportedResources,omitempty"`
}

type copyActionJSON struct {
	DestinationBackupVaultArn string        `json:"DestinationBackupVaultArn"`
	Lifecycle                 lifecycleJSON `json:"Lifecycle,omitzero"`
}

type backupRuleJSON struct {
	RecoveryPointTags          map[string]string `json:"RecoveryPointTags,omitempty"`
	Lifecycle                  *lifecycleJSON    `json:"Lifecycle,omitempty"`
	RuleName                   string            `json:"RuleName"`
	RuleID                     string            `json:"RuleId,omitempty"`
	TargetBackupVaultName      string            `json:"TargetBackupVaultName"`
	ScheduleExpression         string            `json:"ScheduleExpression,omitempty"`
	ScheduleExpressionTimezone string            `json:"ScheduleExpressionTimezone,omitempty"`
	CopyActions                []copyActionJSON  `json:"CopyActions,omitempty"`
	StartWindowMinutes         int64             `json:"StartWindowMinutes,omitempty"`
	CompletionWindowMinutes    int64             `json:"CompletionWindowMinutes,omitempty"`
	EnableContinuousBackup     bool              `json:"EnableContinuousBackup,omitempty"`
}

type advancedBackupSettingJSON struct {
	BackupOptions map[string]string `json:"BackupOptions,omitempty"`
	ResourceType  string            `json:"ResourceType"`
}

type backupPlanBodyDoc struct {
	BackupPlanName         string                      `json:"BackupPlanName"`
	Rules                  []backupRuleJSON            `json:"Rules"`
	AdvancedBackupSettings []advancedBackupSettingJSON `json:"AdvancedBackupSettings,omitempty"`
}

func lifecycleFromJSON(lj *lifecycleJSON) *Lifecycle {
	if lj == nil {
		return nil
	}

	return &Lifecycle{
		MoveToColdStorageAfterDays:          lj.MoveToColdStorageAfterDays,
		DeleteAfterDays:                     lj.DeleteAfterDays,
		OptInToArchiveForSupportedResources: lj.OptInToArchiveForSupportedResources,
	}
}

func lifecycleToJSON(lc *Lifecycle) *lifecycleJSON {
	if lc == nil {
		return nil
	}

	return &lifecycleJSON{
		MoveToColdStorageAfterDays:          lc.MoveToColdStorageAfterDays,
		DeleteAfterDays:                     lc.DeleteAfterDays,
		OptInToArchiveForSupportedResources: lc.OptInToArchiveForSupportedResources,
	}
}

// calculatedLifecycleToJSON renders a CalculatedLifecycle as epoch-seconds
// timestamps (the restjson1 wire format), not Go's default RFC3339 encoding.
func calculatedLifecycleToJSON(cl *CalculatedLifecycle) map[string]any {
	if cl == nil {
		return nil
	}

	out := map[string]any{}
	if cl.MoveToColdStorageAt != nil {
		out["MoveToColdStorageAt"] = epochSeconds(*cl.MoveToColdStorageAt)
	}
	if cl.DeleteAt != nil {
		out["DeleteAt"] = epochSeconds(*cl.DeleteAt)
	}

	return out
}

func copyActionsFromJSON(in []copyActionJSON) []CopyAction {
	out := make([]CopyAction, 0, len(in))
	for _, ca := range in {
		act := CopyAction{
			DestinationBackupVaultArn: ca.DestinationBackupVaultArn,
		}
		if lc := lifecycleFromJSON(&ca.Lifecycle); lc != nil {
			act.Lifecycle = *lc
		}
		out = append(out, act)
	}

	return out
}

func copyActionsToJSON(in []CopyAction) []copyActionJSON {
	out := make([]copyActionJSON, 0, len(in))
	for _, ca := range in {
		out = append(out, copyActionJSON{
			DestinationBackupVaultArn: ca.DestinationBackupVaultArn,
			Lifecycle: lifecycleJSON{
				MoveToColdStorageAfterDays:          ca.Lifecycle.MoveToColdStorageAfterDays,
				DeleteAfterDays:                     ca.Lifecycle.DeleteAfterDays,
				OptInToArchiveForSupportedResources: ca.Lifecycle.OptInToArchiveForSupportedResources,
			},
		})
	}

	return out
}

func advancedSettingsFromJSON(in []advancedBackupSettingJSON) []AdvancedBackupSetting {
	out := make([]AdvancedBackupSetting, 0, len(in))
	for _, s := range in {
		out = append(out, AdvancedBackupSetting(s))
	}

	return out
}

func advancedSettingsToJSON(in []AdvancedBackupSetting) []advancedBackupSettingJSON {
	out := make([]advancedBackupSettingJSON, 0, len(in))
	for _, s := range in {
		out = append(out, advancedBackupSettingJSON(s))
	}

	return out
}

func tagConditionsFromJSON(in []tagConditionJSON) []TagCondition {
	out := make([]TagCondition, 0, len(in))
	for _, tc := range in {
		out = append(out, TagCondition(tc))
	}

	return out
}

func stringConditionsFromJSON(in []stringConditionJSON) []StringCondition {
	out := make([]StringCondition, 0, len(in))
	for _, sc := range in {
		out = append(out, StringCondition(sc))
	}

	return out
}

func selectionConditionsFromJSON(in *selectionConditionsJSON) *SelectionConditions {
	if in == nil {
		return nil
	}

	return &SelectionConditions{
		StringEquals:    stringConditionsFromJSON(in.StringEquals),
		StringLike:      stringConditionsFromJSON(in.StringLike),
		StringNotEquals: stringConditionsFromJSON(in.StringNotEquals),
		StringNotLike:   stringConditionsFromJSON(in.StringNotLike),
	}
}

func rulesFromJSON(in []backupRuleJSON) []Rule {
	rules := make([]Rule, 0, len(in))
	for _, r := range in {
		rules = append(rules, Rule{
			RuleName:                   r.RuleName,
			RuleID:                     r.RuleID,
			TargetVaultName:            r.TargetBackupVaultName,
			ScheduleExpression:         r.ScheduleExpression,
			ScheduleExpressionTimezone: r.ScheduleExpressionTimezone,
			StartWindowMinutes:         r.StartWindowMinutes,
			CompletionWindowMinutes:    r.CompletionWindowMinutes,
			EnableContinuousBackup:     r.EnableContinuousBackup,
			Lifecycle:                  lifecycleFromJSON(r.Lifecycle),
			CopyActions:                copyActionsFromJSON(r.CopyActions),
			RecoveryPointTags:          r.RecoveryPointTags,
		})
	}

	return rules
}

func rulesToJSON(rules []Rule) []backupRuleJSON {
	out := make([]backupRuleJSON, 0, len(rules))
	for _, r := range rules {
		rj := backupRuleJSON{
			RuleName:                   r.RuleName,
			RuleID:                     r.RuleID,
			TargetBackupVaultName:      r.TargetVaultName,
			ScheduleExpression:         r.ScheduleExpression,
			ScheduleExpressionTimezone: r.ScheduleExpressionTimezone,
			StartWindowMinutes:         r.StartWindowMinutes,
			CompletionWindowMinutes:    r.CompletionWindowMinutes,
			EnableContinuousBackup:     r.EnableContinuousBackup,
			RecoveryPointTags:          r.RecoveryPointTags,
		}
		if r.Lifecycle != nil {
			rj.Lifecycle = lifecycleToJSON(r.Lifecycle)
		}
		if len(r.CopyActions) > 0 {
			rj.CopyActions = copyActionsToJSON(r.CopyActions)
		}
		out = append(out, rj)
	}

	return out
}

type tagConditionJSON struct {
	ConditionType  string `json:"ConditionType"`
	ConditionKey   string `json:"ConditionKey"`
	ConditionValue string `json:"ConditionValue"`
}

type stringConditionJSON struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type selectionConditionsJSON struct {
	StringEquals    []stringConditionJSON `json:"StringEquals,omitempty"`
	StringLike      []stringConditionJSON `json:"StringLike,omitempty"`
	StringNotEquals []stringConditionJSON `json:"StringNotEquals,omitempty"`
	StringNotLike   []stringConditionJSON `json:"StringNotLike,omitempty"`
}
