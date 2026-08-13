package glue

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// createIntegrationInput holds input for CreateIntegration.
type createIntegrationInput struct {
	Tags            map[string]string `json:"Tags,omitempty"`
	IntegrationName string            `json:"IntegrationName"`
	SourceArn       string            `json:"SourceArn"`
	TargetArn       string            `json:"TargetArn"`
}

// createIntegrationOutput holds the result for CreateIntegration.
type createIntegrationOutput struct {
	IntegrationName string  `json:"IntegrationName"`
	IntegrationArn  string  `json:"IntegrationArn"`
	SourceArn       string  `json:"SourceArn"`
	TargetArn       string  `json:"TargetArn"`
	Status          string  `json:"Status"`
	CreateTime      float64 `json:"CreateTime"`
}

func (h *Handler) handleCreateIntegration(
	_ context.Context,
	in *createIntegrationInput,
) (*createIntegrationOutput, error) {
	ig, err := h.Backend.CreateIntegration(in.IntegrationName, in.SourceArn, in.TargetArn, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createIntegrationOutput{
		IntegrationName: ig.IntegrationName,
		IntegrationArn:  ig.IntegrationArn,
		SourceArn:       ig.SourceArn,
		TargetArn:       ig.TargetArn,
		Status:          ig.Status,
		CreateTime:      awstime.Epoch(ig.CreatedAt),
	}, nil
}

// createIntegrationResourcePropertyInput holds input for CreateIntegrationResourceProperty.
type createIntegrationResourcePropertyInput struct {
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

// createIntegrationResourcePropertyOutput holds the result for CreateIntegrationResourceProperty.
type createIntegrationResourcePropertyOutput struct {
	ResourceArn      string            `json:"ResourceArn"`
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	CreateTime       string            `json:"CreateTime,omitempty"`
}

func (h *Handler) handleCreateIntegrationResourceProperty(
	_ context.Context,
	in *createIntegrationResourcePropertyInput,
) (*createIntegrationResourcePropertyOutput, error) {
	prop, err := h.Backend.CreateIntegrationResourceProperty(
		in.ResourceArn,
		in.SourceProperties,
		in.TargetProperties,
	)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResourcePropertyOutput{
		ResourceArn:      prop.ResourceArn,
		SourceProperties: prop.SourceProperties,
		TargetProperties: prop.TargetProperties,
		CreateTime:       prop.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}, nil
}

// createIntegrationTablePropertiesInput holds input for CreateIntegrationTableProperties.
type createIntegrationTablePropertiesInput struct {
	SourceTableConfig map[string]any `json:"SourceTableConfig,omitempty"`
	TargetTableConfig map[string]any `json:"TargetTableConfig,omitempty"`
	ResourceArn       string         `json:"ResourceArn"`
	TableName         string         `json:"TableName"`
}

func (h *Handler) handleCreateIntegrationTableProperties(
	_ context.Context,
	in *createIntegrationTablePropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateIntegrationTableProperties(
		in.ResourceArn, in.TableName, in.SourceTableConfig, in.TargetTableConfig,
	)
}

// deleteIntegrationInput holds input for DeleteIntegration.
type deleteIntegrationInput struct {
	IntegrationIdentifier string `json:"IntegrationIdentifier"`
}

// deleteIntegrationOutput holds the result for DeleteIntegration.
type deleteIntegrationOutput struct {
	IntegrationName string  `json:"IntegrationName"`
	IntegrationArn  string  `json:"IntegrationArn"`
	SourceArn       string  `json:"SourceArn"`
	TargetArn       string  `json:"TargetArn"`
	Status          string  `json:"Status"`
	CreateTime      float64 `json:"CreateTime"`
}

func (h *Handler) handleDeleteIntegration(
	_ context.Context,
	in *deleteIntegrationInput,
) (*deleteIntegrationOutput, error) {
	ig, err := h.Backend.DeleteIntegration(in.IntegrationIdentifier)
	if err != nil {
		return nil, err
	}

	return &deleteIntegrationOutput{
		IntegrationName: ig.IntegrationName,
		IntegrationArn:  ig.IntegrationArn,
		SourceArn:       ig.SourceArn,
		TargetArn:       ig.TargetArn,
		Status:          "DELETING",
		CreateTime:      awstime.Epoch(ig.CreatedAt),
	}, nil
}

// deleteIntegrationResourcePropertyInput holds input for DeleteIntegrationResourceProperty.
type deleteIntegrationResourcePropertyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDeleteIntegrationResourceProperty(
	_ context.Context,
	in *deleteIntegrationResourcePropertyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteIntegrationResourceProperty(in.ResourceArn)
}

// deleteIntegrationTablePropertiesInput holds input for DeleteIntegrationTableProperties.
type deleteIntegrationTablePropertiesInput struct {
	ResourceArn string `json:"ResourceArn"`
	TableName   string `json:"TableName"`
}

func (h *Handler) handleDeleteIntegrationTableProperties(
	_ context.Context,
	in *deleteIntegrationTablePropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteIntegrationTableProperties(in.ResourceArn, in.TableName)
}

// describeInboundIntegrationsInput holds input for DescribeInboundIntegrations.
type describeInboundIntegrationsInput struct {
	IntegrationArn string `json:"IntegrationArn,omitempty"`
	TargetArn      string `json:"TargetArn,omitempty"`
	Marker         string `json:"Marker,omitempty"`
	MaxRecords     int    `json:"MaxRecords,omitempty"`
}

// describeInboundIntegrationsOutput holds the result for DescribeInboundIntegrations.
type describeInboundIntegrationsOutput struct {
	Marker       string `json:"Marker,omitempty"`
	Integrations []any  `json:"Integrations"`
}

func (h *Handler) handleDescribeInboundIntegrations(
	_ context.Context,
	in *describeInboundIntegrationsInput,
) (*describeInboundIntegrationsOutput, error) {
	all := h.Backend.ListIntegrations()

	result := make([]any, 0, len(all))
	for _, ig := range all {
		// Filter by IntegrationArn when specified.
		if in.IntegrationArn != "" && ig.IntegrationArn != in.IntegrationArn {
			continue
		}

		result = append(result, ig)
	}

	return &describeInboundIntegrationsOutput{Integrations: result}, nil
}

// describeIntegrationsInput holds input for DescribeIntegrations.
type describeIntegrationsInput struct{}

// describeIntegrationsOutput holds the result for DescribeIntegrations.
type describeIntegrationsOutput struct {
	Integrations []any `json:"Integrations"`
}

func (h *Handler) handleDescribeIntegrations(
	_ context.Context,
	_ *describeIntegrationsInput,
) (*describeIntegrationsOutput, error) {
	list := h.Backend.ListIntegrations()
	result := make([]any, 0, len(list))
	for _, ig := range list {
		result = append(result, ig)
	}

	return &describeIntegrationsOutput{Integrations: result}, nil
}

// getIntegrationResourcePropertyInput holds input for GetIntegrationResourceProperty.
type getIntegrationResourcePropertyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

// getIntegrationResourcePropertyOutput holds the result for GetIntegrationResourceProperty.
type getIntegrationResourcePropertyOutput struct {
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

func (h *Handler) handleGetIntegrationResourceProperty(
	_ context.Context,
	in *getIntegrationResourcePropertyInput,
) (*getIntegrationResourcePropertyOutput, error) {
	prop, err := h.Backend.GetIntegrationResourceProperty(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getIntegrationResourcePropertyOutput{
		ResourceArn:      prop.ResourceArn,
		SourceProperties: prop.SourceProperties,
		TargetProperties: prop.TargetProperties,
	}, nil
}

// getIntegrationTablePropertiesInput holds input for GetIntegrationTableProperties.
type getIntegrationTablePropertiesInput struct {
	ResourceArn string `json:"ResourceArn"`
	TableName   string `json:"TableName"`
}

// getIntegrationTablePropertiesOutput holds the result for GetIntegrationTableProperties.
type getIntegrationTablePropertiesOutput struct {
	SourceTableConfig map[string]any `json:"SourceTableConfig,omitempty"`
	TargetTableConfig map[string]any `json:"TargetTableConfig,omitempty"`
	ResourceArn       string         `json:"ResourceArn"`
	TableName         string         `json:"TableName"`
}

func (h *Handler) handleGetIntegrationTableProperties(
	_ context.Context,
	in *getIntegrationTablePropertiesInput,
) (*getIntegrationTablePropertiesOutput, error) {
	prop, err := h.Backend.GetIntegrationTableProperties(in.ResourceArn, in.TableName)
	if err != nil {
		return nil, err
	}

	return &getIntegrationTablePropertiesOutput{
		ResourceArn:       prop.ResourceArn,
		TableName:         prop.TableName,
		SourceTableConfig: prop.SourceTableConfig,
		TargetTableConfig: prop.TargetTableConfig,
	}, nil
}

// listIntegrationResourcePropertiesInput holds input for ListIntegrationResourceProperties.
type listIntegrationResourcePropertiesInput struct {
	Marker     string `json:"Marker,omitempty"`
	MaxRecords int32  `json:"MaxRecords,omitempty"`
}

// integrationResourcePropertyOut is one entry of ListIntegrationResourceProperties'
// response list, matching the shape already used by
// Create/GetIntegrationResourceProperty.
type integrationResourcePropertyOut struct {
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

// listIntegrationResourcePropertiesOutput holds the result for ListIntegrationResourceProperties.
type listIntegrationResourcePropertiesOutput struct {
	Marker                          string                           `json:"Marker,omitempty"`
	IntegrationResourcePropertyList []integrationResourcePropertyOut `json:"IntegrationResourcePropertyList"`
}

func (h *Handler) handleListIntegrationResourceProperties(
	_ context.Context,
	_ *listIntegrationResourcePropertiesInput,
) (*listIntegrationResourcePropertiesOutput, error) {
	props := h.Backend.ListIntegrationResourceProperties()
	list := make([]integrationResourcePropertyOut, 0, len(props))

	for _, p := range props {
		list = append(list, integrationResourcePropertyOut{
			ResourceArn:      p.ResourceArn,
			SourceProperties: p.SourceProperties,
			TargetProperties: p.TargetProperties,
		})
	}

	return &listIntegrationResourcePropertiesOutput{IntegrationResourcePropertyList: list}, nil
}

// modifyIntegrationInput holds input for ModifyIntegration.
type modifyIntegrationInput struct {
	IntegrationIdentifier string `json:"IntegrationIdentifier"`
}

// modifyIntegrationOutput holds the result for ModifyIntegration.
type modifyIntegrationOutput struct {
	IntegrationName string  `json:"IntegrationName"`
	IntegrationArn  string  `json:"IntegrationArn"`
	SourceArn       string  `json:"SourceArn"`
	TargetArn       string  `json:"TargetArn"`
	Status          string  `json:"Status"`
	CreateTime      float64 `json:"CreateTime"`
}

func (h *Handler) handleModifyIntegration(
	_ context.Context,
	in *modifyIntegrationInput,
) (*modifyIntegrationOutput, error) {
	ig, err := h.Backend.ModifyIntegration(in.IntegrationIdentifier)
	if err != nil {
		return nil, err
	}

	return &modifyIntegrationOutput{
		IntegrationName: ig.IntegrationName,
		IntegrationArn:  ig.IntegrationArn,
		SourceArn:       ig.SourceArn,
		TargetArn:       ig.TargetArn,
		Status:          stateActive,
		CreateTime:      awstime.Epoch(ig.CreatedAt),
	}, nil
}

// updateIntegrationResourcePropertyInput holds input for UpdateIntegrationResourceProperty.
type updateIntegrationResourcePropertyInput struct {
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

// updateIntegrationResourcePropertyOutput holds the result for UpdateIntegrationResourceProperty.
type updateIntegrationResourcePropertyOutput struct {
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

func (h *Handler) handleUpdateIntegrationResourceProperty(
	_ context.Context,
	in *updateIntegrationResourcePropertyInput,
) (*updateIntegrationResourcePropertyOutput, error) {
	prop, err := h.Backend.UpdateIntegrationResourceProperty(in.ResourceArn, in.SourceProperties, in.TargetProperties)
	if err != nil {
		return nil, err
	}

	return &updateIntegrationResourcePropertyOutput{
		ResourceArn:      prop.ResourceArn,
		SourceProperties: prop.SourceProperties,
		TargetProperties: prop.TargetProperties,
	}, nil
}

// updateIntegrationTablePropertiesInput holds input for UpdateIntegrationTableProperties.
type updateIntegrationTablePropertiesInput struct {
	SourceTableConfig map[string]any `json:"SourceTableConfig,omitempty"`
	TargetTableConfig map[string]any `json:"TargetTableConfig,omitempty"`
	ResourceArn       string         `json:"ResourceArn"`
	TableName         string         `json:"TableName"`
}

func (h *Handler) handleUpdateIntegrationTableProperties(
	_ context.Context,
	in *updateIntegrationTablePropertiesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateIntegrationTableProperties(
		in.ResourceArn, in.TableName, in.SourceTableConfig, in.TargetTableConfig,
	)
}
