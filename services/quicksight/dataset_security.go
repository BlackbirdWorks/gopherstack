package quicksight

import (
	"encoding/json"
	"slices"
)

const dataSetUseAsRLSRules = "RLS_RULES"

func validateDataSetSecurity(s DataSetSecurity, allowUseAs bool) error {
	if s.UseAs != "" && (!allowUseAs || s.UseAs != dataSetUseAsRLSRules) {
		return ErrValidation
	}

	if rls := s.RowLevelPermissionDataSet; rls != nil {
		if arnVal, _ := rls["Arn"].(string); arnVal == "" {
			return ErrValidation
		}

		switch rls["PermissionPolicy"] {
		case "GRANT_ACCESS", "DENY_ACCESS":
		default:
			return ErrValidation
		}

		if v, ok := rls["FormatVersion"].(string); ok && !slices.Contains([]string{"VERSION_1", "VERSION_2"}, v) {
			return ErrValidation
		}
	}

	return nil
}

func cloneJSONValue[T any](v T) T {
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}

	var out T
	if json.Unmarshal(raw, &out) != nil {
		return v
	}

	return out
}

func cloneDataSetSecurity(s DataSetSecurity) DataSetSecurity {
	if s.RowLevelPermissionDataSet != nil {
		s.RowLevelPermissionDataSet = cloneJSONValue(s.RowLevelPermissionDataSet)
	}
	if s.RowLevelPermissionTagConfiguration != nil {
		s.RowLevelPermissionTagConfiguration = cloneJSONValue(s.RowLevelPermissionTagConfiguration)
	}
	if s.ColumnLevelPermissionRules != nil {
		s.ColumnLevelPermissionRules = cloneJSONValue(s.ColumnLevelPermissionRules)
	}

	return s
}

func dataSetSecurityFromBody(body map[string]any) DataSetSecurity {
	rules, _ := body["ColumnLevelPermissionRules"].([]any)

	return DataSetSecurity{
		RowLevelPermissionDataSet:          mapField(body, "RowLevelPermissionDataSet"),
		RowLevelPermissionTagConfiguration: mapField(body, "RowLevelPermissionTagConfiguration"),
		ColumnLevelPermissionRules:         rules,
		UseAs:                              strField(body, "UseAs"),
	}
}

func addDataSetSummarySecurity(m map[string]any, s DataSetSecurity) {
	m["ColumnLevelPermissionRulesApplied"] = len(s.ColumnLevelPermissionRules) > 0
	m["RowLevelPermissionTagConfigurationApplied"] = s.RowLevelPermissionTagConfiguration != nil
	if s.RowLevelPermissionDataSet != nil {
		m["RowLevelPermissionDataSet"] = s.RowLevelPermissionDataSet
	}
	if s.UseAs != "" {
		m["UseAs"] = s.UseAs
	}
}
