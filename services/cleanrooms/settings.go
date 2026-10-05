package cleanrooms

import (
	"maps"
	"slices"
)

// CollaborationSettings carries the optional collaboration members of
// CreateCollaboration and UpdateCollaboration.
type CollaborationSettings struct {
	CreatorMLMemberAbilities *MLMemberAbilities
	DataEncryptionMetadata   *DataEncryptionMetadata
	AnalyticsEngine          string
	AllowedResultRegions     []string
}

// ConfiguredTableSettings carries the optional selectedAnalysisMethods member.
type ConfiguredTableSettings struct {
	SelectedAnalysisMethods []string
}

// AnalysisTemplateSettings carries the optional errorMessageConfiguration and
// syntheticDataParameters members.
type AnalysisTemplateSettings struct {
	ErrorMessageConfiguration *ErrorMessageConfiguration
	SyntheticDataParameters   map[string]any
	Schema                    map[string]any
}

func validAnalyticsEngines() []string { return []string{"SPARK", "CLEAN_ROOMS_SQL"} }

func validSelectedMethods() []string { return []string{"DIRECT_QUERY", "DIRECT_JOB"} }

func validResultRegions() []string {
	return []string{
		"us-west-1", "us-west-2", "us-east-1", "us-east-2", "af-south-1", "ap-east-1",
		"ap-east-2", "ap-south-2", "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
		"ap-southeast-5", "ap-southeast-4", "ap-southeast-7", "ap-south-1", "ap-northeast-3",
		"ap-northeast-1", "ap-northeast-2", "ca-central-1", "ca-west-1", "eu-south-1",
		"eu-west-3", "eu-south-2", "eu-central-2", "eu-central-1", "eu-north-1", "eu-west-1",
		"eu-west-2", "me-south-1", "me-central-1", "il-central-1", "sa-east-1", "mx-central-1",
	}
}

func allIn(vals, allowed []string) bool {
	for _, v := range vals {
		if !slices.Contains(allowed, v) {
			return false
		}
	}

	return true
}

func firstCollaborationSettings(s []CollaborationSettings) CollaborationSettings {
	if len(s) == 0 {
		return CollaborationSettings{}
	}

	return s[0]
}

func (s CollaborationSettings) validate() error {
	if s.AnalyticsEngine != "" && !slices.Contains(validAnalyticsEngines(), s.AnalyticsEngine) {
		return ErrValidation
	}
	if !allIn(s.AllowedResultRegions, validResultRegions()) {
		return ErrValidation
	}

	return validateMLAbilities(s.CreatorMLMemberAbilities)
}

func validMLMemberAbilities() []string {
	return []string{"CAN_RECEIVE_MODEL_OUTPUT", "CAN_RECEIVE_INFERENCE_OUTPUT"}
}

func validateMLAbilities(a *MLMemberAbilities) error {
	if a == nil {
		return nil
	}
	if a.CustomMLMemberAbilities == nil || !allIn(a.CustomMLMemberAbilities, validMLMemberAbilities()) {
		return ErrValidation
	}

	return nil
}

func cloneMLAbilities(a *MLMemberAbilities) *MLMemberAbilities {
	if a == nil {
		return nil
	}

	return &MLMemberAbilities{CustomMLMemberAbilities: slices.Clone(a.CustomMLMemberAbilities)}
}

func validateSelectedMethods(methods []string) error {
	if !allIn(methods, validSelectedMethods()) {
		return ErrValidation
	}

	return nil
}

func validateErrorMessageConfiguration(c *ErrorMessageConfiguration) error {
	if c != nil && c.Type != "DETAILED" {
		return ErrValidation
	}

	return nil
}

// validateSyntheticDataParameters checks the mlSyntheticDataParameters union
// member's required fields (validators.go validateMLSyntheticDataParameters).
func validateSyntheticDataParameters(p map[string]any) error {
	if p == nil {
		return nil
	}

	ml, ok := p["mlSyntheticDataParameters"].(map[string]any)
	if !ok || len(p) != 1 {
		return ErrValidation
	}

	for _, k := range []string{"epsilon", "maxMembershipInferenceAttackScore"} {
		if _, isNum := ml[k].(float64); !isNum {
			return ErrValidation
		}
	}

	cc, ok := ml["columnClassification"].(map[string]any)
	if !ok {
		return ErrValidation
	}

	mapping, ok := cc["columnMapping"].([]any)
	if !ok {
		return ErrValidation
	}

	for _, m := range mapping {
		col, isMap := m.(map[string]any)
		if !isMap || col["columnName"] == nil || col["columnType"] == nil {
			return ErrValidation
		}
	}

	return nil
}

func cloneCollaboration(c *Collaboration) *Collaboration {
	out := *c
	out.Tags = maps.Clone(c.Tags)
	out.MemberAbilities = slices.Clone(c.MemberAbilities)
	out.AutoApprovedChangeTypes = slices.Clone(c.AutoApprovedChangeTypes)
	out.AllowedResultRegions = slices.Clone(c.AllowedResultRegions)
	if c.DataEncryptionMetadata != nil {
		d := *c.DataEncryptionMetadata
		out.DataEncryptionMetadata = &d
	}
	out.Members = make([]*MemberSummary, len(c.Members))
	for i, m := range c.Members {
		mc := *m
		mc.MLAbilities = cloneMLAbilities(m.MLAbilities)
		out.Members[i] = &mc
	}

	return &out
}

func cloneConfiguredTable(ct *ConfiguredTable) *ConfiguredTable {
	out := *ct
	out.Tags = maps.Clone(ct.Tags)
	out.TableReference = maps.Clone(ct.TableReference)
	out.AllowedColumns = slices.Clone(ct.AllowedColumns)
	out.AnalysisRuleTypes = slices.Clone(ct.AnalysisRuleTypes)
	out.SelectedAnalysisMethods = slices.Clone(ct.SelectedAnalysisMethods)

	return &out
}

func cloneAnalysisTemplate(t *AnalysisTemplate) *AnalysisTemplate {
	out := *t
	out.Tags = maps.Clone(t.Tags)
	out.SyntheticDataParameters = maps.Clone(t.SyntheticDataParameters)
	out.Schema = maps.Clone(t.Schema)
	if t.ErrorMessageConfiguration != nil {
		e := *t.ErrorMessageConfiguration
		out.ErrorMessageConfiguration = &e
	}

	return &out
}
