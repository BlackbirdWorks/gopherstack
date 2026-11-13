package sagemaker

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
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
func applyProvisioningUpdate(stored jsontext.Value, upd *serviceCatalogUpdateDetails) (jsontext.Value, error) {
	if upd == nil {
		return stored, nil
	}

	if len(stored) == 0 {
		return nil, fmt.Errorf(
			"%w: project has no ServiceCatalogProvisioningDetails to update", ErrValidation,
		)
	}

	var details map[string]any
	if err := json.Unmarshal(stored, &details, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: stored provisioning details: %w", ErrValidation, err)
	}

	if upd.ProvisioningArtifactID != "" {
		details["ProvisioningArtifactId"] = upd.ProvisioningArtifactID
	}

	if upd.ProvisioningParameters != nil {
		details["ProvisioningParameters"] = upd.ProvisioningParameters
	}

	return json.Marshal(details, jsonx.V1())
}

// applyTemplateProviderUpdates replaces TemplateURL/Parameters of each stored
// CfnTemplateProvider matched by TemplateName.
func applyTemplateProviderUpdates(
	stored jsontext.Value,
	updates []cfnUpdateTemplateProvider,
) (jsontext.Value, error) {
	if len(updates) == 0 {
		return stored, nil
	}

	var providers []map[string]map[string]any
	if len(stored) > 0 {
		if err := json.Unmarshal(stored, &providers, jsonx.V1()); err != nil {
			return nil, fmt.Errorf("%w: stored template providers: %w", ErrValidation, err)
		}
	}

	for _, u := range updates {
		if !updateOneTemplateProvider(providers, u) {
			return nil, fmt.Errorf("%w: template provider %q not found in project", ErrValidation, u.TemplateName)
		}
	}

	return json.Marshal(providers, jsonx.V1())
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
func templateProviderDetails(stored jsontext.Value) []map[string]any {
	var providers []map[string]map[string]any
	if err := json.Unmarshal(stored, &providers, jsonx.V1()); err != nil {
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
