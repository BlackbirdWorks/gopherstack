package sagemaker

import (
	"encoding/json"
	"fmt"
)

// UpdateProjectOptions holds the parameters for UpdateProject.
type UpdateProjectOptions struct {
	Tags                    map[string]string
	ServiceCatalogUpdate    *serviceCatalogUpdateDetails
	Description             string
	TemplateProviderUpdates []cfnUpdateTemplateProvider
}

type provisioningParameter struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type serviceCatalogUpdateDetails struct {
	ProvisioningArtifactID string                  `json:"ProvisioningArtifactId"`
	ProvisioningParameters []provisioningParameter `json:"ProvisioningParameters"`
}

type cfnUpdateTemplateProvider struct {
	TemplateName string                  `json:"TemplateName"`
	TemplateURL  string                  `json:"TemplateURL"`
	Parameters   []provisioningParameter `json:"Parameters"`
}

type updateTemplateProviderInput struct {
	CfnTemplateProvider *cfnUpdateTemplateProvider `json:"CfnTemplateProvider"`
}

// applyProvisioningUpdate merges ServiceCatalogProvisioningUpdateDetails
// (ProvisioningArtifactId, ProvisioningParameters) into the stored details.
func applyProvisioningUpdate(stored json.RawMessage, upd *serviceCatalogUpdateDetails) (json.RawMessage, error) {
	if upd == nil {
		return stored, nil
	}

	if len(stored) == 0 {
		return nil, fmt.Errorf(
			"%w: project has no ServiceCatalogProvisioningDetails to update", ErrValidation,
		)
	}

	var details map[string]any
	if err := json.Unmarshal(stored, &details); err != nil {
		return nil, fmt.Errorf("%w: stored provisioning details: %w", ErrValidation, err)
	}

	if upd.ProvisioningArtifactID != "" {
		details["ProvisioningArtifactId"] = upd.ProvisioningArtifactID
	}

	if upd.ProvisioningParameters != nil {
		details["ProvisioningParameters"] = upd.ProvisioningParameters
	}

	return json.Marshal(details)
}

// applyTemplateProviderUpdates replaces TemplateURL/Parameters of each stored
// CfnTemplateProvider matched by TemplateName.
func applyTemplateProviderUpdates(
	stored json.RawMessage,
	updates []cfnUpdateTemplateProvider,
) (json.RawMessage, error) {
	if len(updates) == 0 {
		return stored, nil
	}

	var providers []map[string]map[string]any
	if len(stored) > 0 {
		if err := json.Unmarshal(stored, &providers); err != nil {
			return nil, fmt.Errorf("%w: stored template providers: %w", ErrValidation, err)
		}
	}

	for _, u := range updates {
		if !updateOneTemplateProvider(providers, u) {
			return nil, fmt.Errorf("%w: template provider %q not found in project", ErrValidation, u.TemplateName)
		}
	}

	return json.Marshal(providers)
}

func updateOneTemplateProvider(providers []map[string]map[string]any, u cfnUpdateTemplateProvider) bool {
	for _, entry := range providers {
		cfn := entry["CfnTemplateProvider"]
		if cfn == nil || cfn["TemplateName"] != u.TemplateName {
			continue
		}

		cfn["TemplateURL"] = u.TemplateURL
		if u.Parameters != nil {
			cfn["Parameters"] = u.Parameters
		}

		return true
	}

	return false
}

// templateProviderDetails converts stored CreateTemplateProvider entries to the
// TemplateProviderDetail wire shape (CfnTemplateProviderDetail key).
func templateProviderDetails(stored json.RawMessage) []map[string]any {
	var providers []map[string]map[string]any
	if err := json.Unmarshal(stored, &providers); err != nil {
		return nil
	}

	out := make([]map[string]any, 0, len(providers))

	for _, entry := range providers {
		if cfn := entry["CfnTemplateProvider"]; cfn != nil {
			out = append(out, map[string]any{"CfnTemplateProviderDetail": cfn})
		}
	}

	return out
}
