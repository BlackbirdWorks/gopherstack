package glue

import (
	"context"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// createIntegrationInput holds input for CreateIntegration.
type createIntegrationInput struct {
	AdditionalEncryptionContext map[string]string    `json:"AdditionalEncryptionContext,omitempty"`
	IntegrationConfig           *IntegrationConfig   `json:"IntegrationConfig,omitempty"`
	IntegrationName             string               `json:"IntegrationName"`
	SourceArn                   string               `json:"SourceArn"`
	TargetArn                   string               `json:"TargetArn"`
	DataFilter                  string               `json:"DataFilter,omitempty"`
	Description                 string               `json:"Description,omitempty"`
	KmsKeyID                    string               `json:"KmsKeyId,omitempty"`
	Tags                        []integrationTagWire `json:"Tags,omitempty"`
}

// integrationTagWire is the Key/Value list form Integration.Tags uses on the wire.
type integrationTagWire struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// integrationWire is the shared Integration response shape for create, modify, delete and describe.
type integrationWire struct {
	AdditionalEncryptionContext map[string]string    `json:"AdditionalEncryptionContext,omitempty"`
	IntegrationConfig           *IntegrationConfig   `json:"IntegrationConfig,omitempty"`
	IntegrationName             string               `json:"IntegrationName"`
	IntegrationArn              string               `json:"IntegrationArn"`
	SourceArn                   string               `json:"SourceArn"`
	TargetArn                   string               `json:"TargetArn"`
	Status                      string               `json:"Status"`
	DataFilter                  string               `json:"DataFilter,omitempty"`
	Description                 string               `json:"Description,omitempty"`
	KmsKeyID                    string               `json:"KmsKeyId,omitempty"`
	Tags                        []integrationTagWire `json:"Tags,omitempty"`
	// Epoch number, not RFC3339: the SDK rejects a string ("expected IntegrationTimestamp to be a JSON Number").
	CreateTime float64 `json:"CreateTime"`
}

func integrationTagMap(tags []integrationTagWire) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func toIntegrationWire(ig *Integration) integrationWire {
	tags := make([]integrationTagWire, 0, len(ig.Tags))
	for _, k := range slices.Sorted(maps.Keys(ig.Tags)) {
		tags = append(tags, integrationTagWire{Key: k, Value: ig.Tags[k]})
	}

	return integrationWire{
		IntegrationName:             ig.IntegrationName,
		IntegrationArn:              ig.IntegrationArn,
		SourceArn:                   ig.SourceArn,
		TargetArn:                   ig.TargetArn,
		Status:                      ig.Status,
		DataFilter:                  ig.DataFilter,
		Description:                 ig.Description,
		KmsKeyID:                    ig.KmsKeyID,
		AdditionalEncryptionContext: ig.AdditionalEncryptionContext,
		IntegrationConfig:           ig.IntegrationConfig,
		Tags:                        tags,
		CreateTime:                  awstime.Epoch(ig.CreatedAt),
	}
}

func (h *Handler) handleCreateIntegration(
	_ context.Context,
	in *createIntegrationInput,
) (*integrationWire, error) {
	ig, err := h.Backend.CreateIntegrationWithOptions(
		in.IntegrationName, in.SourceArn, in.TargetArn, integrationTagMap(in.Tags),
		IntegrationOptions{
			DataFilter:                  in.DataFilter,
			Description:                 in.Description,
			KmsKeyID:                    in.KmsKeyID,
			AdditionalEncryptionContext: in.AdditionalEncryptionContext,
			IntegrationConfig:           in.IntegrationConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	w := toIntegrationWire(ig)

	return &w, nil
}

// createIntegrationResourcePropertyInput holds input for CreateIntegrationResourceProperty.
type createIntegrationResourcePropertyInput struct {
	SourceProcessingProperties map[string]any `json:"SourceProcessingProperties,omitempty"`
	TargetProcessingProperties map[string]any `json:"TargetProcessingProperties,omitempty"`
	ResourceArn                string         `json:"ResourceArn"`
}

// createIntegrationResourcePropertyOutput holds the result for
// CreateIntegrationResourceProperty. SourceProcessingProperties/
// TargetProcessingProperties (not the fictitious "SourceProperties"/
// "TargetProperties") are the real member names -- glue@v1.157.0
// api_op_CreateIntegrationResourceProperty.go:62-66. That op's real output has
// no CreateTime member at all (previously fabricated here) and does carry
// ResourcePropertyArn, which this backend now mints.
type createIntegrationResourcePropertyOutput struct {
	SourceProcessingProperties map[string]any `json:"SourceProcessingProperties,omitempty"`
	TargetProcessingProperties map[string]any `json:"TargetProcessingProperties,omitempty"`
	ResourceArn                string         `json:"ResourceArn"`
	ResourcePropertyArn        string         `json:"ResourcePropertyArn"`
}

func (h *Handler) handleCreateIntegrationResourceProperty(
	_ context.Context,
	in *createIntegrationResourcePropertyInput,
) (*createIntegrationResourcePropertyOutput, error) {
	prop, err := h.Backend.CreateIntegrationResourceProperty(
		in.ResourceArn,
		in.SourceProcessingProperties,
		in.TargetProcessingProperties,
	)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResourcePropertyOutput{
		ResourceArn:                prop.ResourceArn,
		ResourcePropertyArn:        prop.ResourcePropertyArn,
		SourceProcessingProperties: prop.SourceProcessingProperties,
		TargetProcessingProperties: prop.TargetProcessingProperties,
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

func (h *Handler) handleDeleteIntegration(
	_ context.Context,
	in *deleteIntegrationInput,
) (*integrationWire, error) {
	ig, err := h.Backend.DeleteIntegration(in.IntegrationIdentifier)
	if err != nil {
		return nil, err
	}

	w := toIntegrationWire(ig)
	w.Status = "DELETING"

	return &w, nil
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
	MaxRecords     int32  `json:"MaxRecords,omitempty"`
}

// defaultDescribeInboundIntegrationsLimit is used when
// DescribeInboundIntegrationsInput.MaxRecords is unset.
const defaultDescribeInboundIntegrationsLimit = 100

// inboundIntegrationSummary mirrors types.InboundIntegration: CreateTime,
// IntegrationArn, SourceArn, Status, TargetArn are all required members;
// Errors and IntegrationConfig are real members with no backing state in
// this backend's Integration model (models.go) and are omitted rather than
// fabricated, matching integrationSummary above.
//
// CreateTime is an epoch float (via pkgs/awstime), not an RFC3339 string --
// deserializeDocumentInboundIntegration (glue@v1.152.0 deserializers.go)
// requires "CreateTime" to be a JSON Number ("expected IntegrationTimestamp
// to be a JSON Number"), same as Integration.CreateTime.
type inboundIntegrationSummary struct {
	IntegrationArn string  `json:"IntegrationArn"`
	SourceArn      string  `json:"SourceArn"`
	Status         string  `json:"Status"`
	TargetArn      string  `json:"TargetArn"`
	CreateTime     float64 `json:"CreateTime"`
}

func toInboundIntegrationSummary(ig *Integration) inboundIntegrationSummary {
	return inboundIntegrationSummary{
		IntegrationArn: ig.IntegrationArn,
		SourceArn:      ig.SourceArn,
		Status:         ig.Status,
		TargetArn:      ig.TargetArn,
		CreateTime:     awstime.Epoch(ig.CreatedAt),
	}
}

// describeInboundIntegrationsOutput holds the result for
// DescribeInboundIntegrations. The real field is InboundIntegrations, not
// Integrations (api_op_DescribeInboundIntegrations.go).
type describeInboundIntegrationsOutput struct {
	Marker              string                      `json:"Marker,omitempty"`
	InboundIntegrations []inboundIntegrationSummary `json:"InboundIntegrations"`
}

func (h *Handler) handleDescribeInboundIntegrations(
	_ context.Context,
	in *describeInboundIntegrationsInput,
) (*describeInboundIntegrationsOutput, error) {
	all := h.Backend.ListIntegrations()

	matching := make([]*Integration, 0, len(all))
	for _, ig := range all {
		if in.IntegrationArn != "" && ig.IntegrationArn != in.IntegrationArn {
			continue
		}

		if in.TargetArn != "" && ig.TargetArn != in.TargetArn {
			continue
		}

		matching = append(matching, ig)
	}

	limit := int(in.MaxRecords)
	if limit <= 0 {
		limit = defaultDescribeInboundIntegrationsLimit
	}

	page, next := paginateSlice(matching, in.Marker, limit)

	result := make([]inboundIntegrationSummary, 0, len(page))
	for _, ig := range page {
		result = append(result, toInboundIntegrationSummary(ig))
	}

	return &describeInboundIntegrationsOutput{InboundIntegrations: result, Marker: next}, nil
}

// defaultDescribeIntegrationsLimit is used when DescribeIntegrationsInput.MaxRecords is unset.
const defaultDescribeIntegrationsLimit = 100

// integrationFilter mirrors aws-sdk-go-v2/service/glue/types.IntegrationFilter.
// Real supported Name keys are "Status", "IntegrationName" and "SourceArn"
// (api_op_DescribeIntegrations.go doc comment), all of which are real
// Integration fields (models.go).
type integrationFilter struct {
	Name   string   `json:"Name,omitempty"`
	Values []string `json:"Values,omitempty"`
}

// describeIntegrationsInput holds input for DescribeIntegrations.
type describeIntegrationsInput struct {
	IntegrationIdentifier string              `json:"IntegrationIdentifier,omitempty"`
	Marker                string              `json:"Marker,omitempty"`
	Filters               []integrationFilter `json:"Filters,omitempty"`
	MaxRecords            int32               `json:"MaxRecords,omitempty"`
}

// describeIntegrationsOutput holds the result for DescribeIntegrations.
type describeIntegrationsOutput struct {
	Marker       string            `json:"Marker,omitempty"`
	Integrations []integrationWire `json:"Integrations"`
}

func integrationFieldValue(ig *Integration, name string) string {
	switch name {
	case "Status":
		return ig.Status
	case "IntegrationName":
		return ig.IntegrationName
	case "SourceArn":
		return ig.SourceArn
	default:
		return ""
	}
}

func matchesIntegrationFilters(ig *Integration, filters []integrationFilter) bool {
	for _, f := range filters {
		if f.Name == "" {
			continue
		}

		got := integrationFieldValue(ig, f.Name)

		matched := slices.Contains(f.Values, got)

		if !matched {
			return false
		}
	}

	return true
}

func (h *Handler) handleDescribeIntegrations(
	_ context.Context,
	in *describeIntegrationsInput,
) (*describeIntegrationsOutput, error) {
	list := h.Backend.ListIntegrations()

	matching := make([]*Integration, 0, len(list))

	for _, ig := range list {
		if in.IntegrationIdentifier != "" && ig.IntegrationArn != in.IntegrationIdentifier {
			continue
		}

		if !matchesIntegrationFilters(ig, in.Filters) {
			continue
		}

		matching = append(matching, ig)
	}

	limit := int(in.MaxRecords)
	if limit <= 0 {
		limit = defaultDescribeIntegrationsLimit
	}

	page, next := paginateSlice(matching, in.Marker, limit)

	result := make([]integrationWire, 0, len(page))
	for _, ig := range page {
		result = append(result, toIntegrationWire(ig))
	}

	return &describeIntegrationsOutput{Integrations: result, Marker: next}, nil
}

// getIntegrationResourcePropertyInput holds input for GetIntegrationResourceProperty.
type getIntegrationResourcePropertyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

// getIntegrationResourcePropertyOutput holds the result for GetIntegrationResourceProperty.
type getIntegrationResourcePropertyOutput struct {
	SourceProcessingProperties map[string]any `json:"SourceProcessingProperties,omitempty"`
	TargetProcessingProperties map[string]any `json:"TargetProcessingProperties,omitempty"`
	ResourceArn                string         `json:"ResourceArn"`
	ResourcePropertyArn        string         `json:"ResourcePropertyArn"`
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
		ResourceArn:                prop.ResourceArn,
		ResourcePropertyArn:        prop.ResourcePropertyArn,
		SourceProcessingProperties: prop.SourceProcessingProperties,
		TargetProcessingProperties: prop.TargetProcessingProperties,
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

// defaultListIntegrationResourcePropertiesLimit is used when
// ListIntegrationResourcePropertiesInput.MaxRecords is unset.
const defaultListIntegrationResourcePropertiesLimit = 100

// listIntegrationResourcePropertiesInput holds input for ListIntegrationResourceProperties.
type listIntegrationResourcePropertiesInput struct {
	Marker     string `json:"Marker,omitempty"`
	MaxRecords int32  `json:"MaxRecords,omitempty"`
}

// integrationResourcePropertyOut is one entry of ListIntegrationResourceProperties'
// response list, matching the shape already used by
// Create/GetIntegrationResourceProperty.
type integrationResourcePropertyOut struct {
	SourceProcessingProperties map[string]any `json:"SourceProcessingProperties,omitempty"`
	TargetProcessingProperties map[string]any `json:"TargetProcessingProperties,omitempty"`
	ResourceArn                string         `json:"ResourceArn"`
	ResourcePropertyArn        string         `json:"ResourcePropertyArn"`
}

// listIntegrationResourcePropertiesOutput holds the result for ListIntegrationResourceProperties.
type listIntegrationResourcePropertiesOutput struct {
	Marker                          string                           `json:"Marker,omitempty"`
	IntegrationResourcePropertyList []integrationResourcePropertyOut `json:"IntegrationResourcePropertyList"`
}

func (h *Handler) handleListIntegrationResourceProperties(
	_ context.Context,
	in *listIntegrationResourcePropertiesInput,
) (*listIntegrationResourcePropertiesOutput, error) {
	props := h.Backend.ListIntegrationResourceProperties()

	limit := int(in.MaxRecords)
	if limit <= 0 {
		limit = defaultListIntegrationResourcePropertiesLimit
	}

	page, next := paginateSlice(props, in.Marker, limit)

	list := make([]integrationResourcePropertyOut, 0, len(page))

	for _, p := range page {
		list = append(list, integrationResourcePropertyOut{
			ResourceArn:                p.ResourceArn,
			ResourcePropertyArn:        p.ResourcePropertyArn,
			SourceProcessingProperties: p.SourceProcessingProperties,
			TargetProcessingProperties: p.TargetProcessingProperties,
		})
	}

	return &listIntegrationResourcePropertiesOutput{IntegrationResourcePropertyList: list, Marker: next}, nil
}

// modifyIntegrationInput holds input for ModifyIntegration.
type modifyIntegrationInput struct {
	DataFilter            *string            `json:"DataFilter,omitempty"`
	Description           *string            `json:"Description,omitempty"`
	IntegrationConfig     *IntegrationConfig `json:"IntegrationConfig,omitempty"`
	IntegrationIdentifier string             `json:"IntegrationIdentifier"`
}

func (h *Handler) handleModifyIntegration(
	_ context.Context,
	in *modifyIntegrationInput,
) (*integrationWire, error) {
	ig, err := h.Backend.ModifyIntegrationWithOptions(in.IntegrationIdentifier, IntegrationModifyOptions{
		DataFilter:        in.DataFilter,
		Description:       in.Description,
		IntegrationConfig: in.IntegrationConfig,
	})
	if err != nil {
		return nil, err
	}

	w := toIntegrationWire(ig)
	w.Status = stateActive

	return &w, nil
}

// updateIntegrationResourcePropertyInput holds input for UpdateIntegrationResourceProperty.
type updateIntegrationResourcePropertyInput struct {
	SourceProcessingProperties map[string]any `json:"SourceProcessingProperties,omitempty"`
	TargetProcessingProperties map[string]any `json:"TargetProcessingProperties,omitempty"`
	ResourceArn                string         `json:"ResourceArn"`
}

// updateIntegrationResourcePropertyOutput holds the result for UpdateIntegrationResourceProperty.
type updateIntegrationResourcePropertyOutput struct {
	SourceProcessingProperties map[string]any `json:"SourceProcessingProperties,omitempty"`
	TargetProcessingProperties map[string]any `json:"TargetProcessingProperties,omitempty"`
	ResourceArn                string         `json:"ResourceArn"`
	ResourcePropertyArn        string         `json:"ResourcePropertyArn"`
}

func (h *Handler) handleUpdateIntegrationResourceProperty(
	_ context.Context,
	in *updateIntegrationResourcePropertyInput,
) (*updateIntegrationResourcePropertyOutput, error) {
	prop, err := h.Backend.UpdateIntegrationResourceProperty(
		in.ResourceArn, in.SourceProcessingProperties, in.TargetProcessingProperties,
	)
	if err != nil {
		return nil, err
	}

	return &updateIntegrationResourcePropertyOutput{
		ResourceArn:                prop.ResourceArn,
		ResourcePropertyArn:        prop.ResourcePropertyArn,
		SourceProcessingProperties: prop.SourceProcessingProperties,
		TargetProcessingProperties: prop.TargetProcessingProperties,
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
