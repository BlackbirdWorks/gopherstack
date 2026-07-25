package awsconfig

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for configuration recorder ops.
const (
	opAssociateResourceTypes                          = "AssociateResourceTypes"
	opDeleteConfigurationRecorder                     = "DeleteConfigurationRecorder"
	opDeleteServiceLinkedConfigurationRecorder        = "DeleteServiceLinkedConfigurationRecorder"
	opDescribeConfigurationRecorderStatus             = "DescribeConfigurationRecorderStatus"
	opDescribeConfigurationRecorders                  = "DescribeConfigurationRecorders"
	opDisassociateResourceTypes                       = "DisassociateResourceTypes"
	opListConfigurationRecorders                      = "ListConfigurationRecorders"
	opPutConfigurationRecorder                        = "PutConfigurationRecorder"
	opPutServiceLinkedConfigurationRecorder           = "PutServiceLinkedConfigurationRecorder"
	opPutThirdPartyServiceLinkedConfigurationRecorder = "PutThirdPartyServiceLinkedConfigurationRecorder"
	opStartConfigurationRecorder                      = "StartConfigurationRecorder"
	opStopConfigurationRecorder                       = "StopConfigurationRecorder"
)

// configurationRecorderSupportedOps returns the operation names this family handles.
func configurationRecorderSupportedOps() []string {
	return []string{
		opPutConfigurationRecorder,
		opDescribeConfigurationRecorders,
		opDescribeConfigurationRecorderStatus,
		opStartConfigurationRecorder,
		opStopConfigurationRecorder,
		opDeleteConfigurationRecorder,
		opAssociateResourceTypes,
		opDisassociateResourceTypes,
		opDeleteServiceLinkedConfigurationRecorder,
		opPutServiceLinkedConfigurationRecorder,
		opPutThirdPartyServiceLinkedConfigurationRecorder,
		opListConfigurationRecorders,
	}
}

type extractConfigRecorderNameInput struct {
	ConfigurationRecorder configNameBody `json:"ConfigurationRecorder"`
}

func extractConfigRecorderName(body []byte) string {
	var req extractConfigRecorderNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ConfigurationRecorder.Name
}

func extractTopLevelRecorderName(body []byte) string {
	var req configurationRecorderNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ConfigurationRecorderName
}

type extractFirstRecorderNameInput struct {
	ConfigurationRecorderNames []string `json:"ConfigurationRecorderNames"`
}

func extractFirstRecorderName(body []byte) string {
	var req extractFirstRecorderNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}
	if len(req.ConfigurationRecorderNames) > 0 {
		return req.ConfigurationRecorderNames[0]
	}

	return ""
}

// configurationRecorderBody is the nested JSON body for a configuration recorder.
type configurationRecorderBody struct {
	RecordingGroup *RecordingGroup `json:"recordingGroup,omitempty"`
	Name           string          `json:"name"`
	RoleARN        string          `json:"roleARN"`
}

type putConfigurationRecorderRequest struct {
	ConfigurationRecorder configurationRecorderBody `json:"ConfigurationRecorder"`
}

type putConfigurationRecorderOutput struct{}

func (h *Handler) handlePutConfigurationRecorder(
	_ context.Context,
	in *putConfigurationRecorderRequest,
) (*putConfigurationRecorderOutput, error) {
	if err := h.Backend.PutConfigurationRecorder(
		in.ConfigurationRecorder.Name,
		in.ConfigurationRecorder.RoleARN,
		in.ConfigurationRecorder.RecordingGroup,
	); err != nil {
		return nil, err
	}

	return &putConfigurationRecorderOutput{}, nil
}

type describeConfigurationRecordersInput struct {
	ConfigurationRecorderNames []string `json:"ConfigurationRecorderNames,omitempty"`
}

type describeConfigurationRecordersOutput struct {
	ConfigurationRecorders []ConfigurationRecorder `json:"ConfigurationRecorders"`
}

func (h *Handler) handleDescribeConfigurationRecorders(
	_ context.Context,
	in *describeConfigurationRecordersInput,
) (*describeConfigurationRecordersOutput, error) {
	recorders := h.Backend.DescribeConfigurationRecorders(in.ConfigurationRecorderNames)

	return &describeConfigurationRecordersOutput{ConfigurationRecorders: recorders}, nil
}

type startConfigurationRecorderOutput struct{}

func (h *Handler) handleStartConfigurationRecorder(
	_ context.Context,
	in *configurationRecorderNameInput,
) (*startConfigurationRecorderOutput, error) {
	if err := h.Backend.StartConfigurationRecorder(in.ConfigurationRecorderName); err != nil {
		return nil, err
	}

	return &startConfigurationRecorderOutput{}, nil
}

type stopConfigurationRecorderOutput struct{}

func (h *Handler) handleStopConfigurationRecorder(
	_ context.Context,
	in *configurationRecorderNameInput,
) (*stopConfigurationRecorderOutput, error) {
	if err := h.Backend.StopConfigurationRecorder(in.ConfigurationRecorderName); err != nil {
		return nil, err
	}

	return &stopConfigurationRecorderOutput{}, nil
}

type deleteConfigurationRecorderInput struct {
	ConfigurationRecorderName string `json:"ConfigurationRecorderName"`
}

type deleteConfigurationRecorderOutput struct{}

func (h *Handler) handleDeleteConfigurationRecorder(
	_ context.Context,
	in *deleteConfigurationRecorderInput,
) (*deleteConfigurationRecorderOutput, error) {
	if err := h.Backend.DeleteConfigurationRecorder(in.ConfigurationRecorderName); err != nil {
		return nil, err
	}

	return &deleteConfigurationRecorderOutput{}, nil
}

type describeConfigurationRecorderStatusInput struct {
	ConfigurationRecorderNames []string `json:"ConfigurationRecorderNames,omitempty"`
}

type describeConfigurationRecorderStatusOutput struct {
	ConfigurationRecordersStatus []ConfigurationRecorderStatus `json:"ConfigurationRecordersStatus"`
}

func (h *Handler) handleDescribeConfigurationRecorderStatus(
	_ context.Context,
	in *describeConfigurationRecorderStatusInput,
) (*describeConfigurationRecorderStatusOutput, error) {
	statuses := h.Backend.DescribeConfigurationRecorderStatus(in.ConfigurationRecorderNames)

	return &describeConfigurationRecorderStatusOutput{ConfigurationRecordersStatus: statuses}, nil
}

type associateResourceTypesInput struct {
	ConfigurationRecorderArn string   `json:"ConfigurationRecorderArn"`
	ResourceTypes            []string `json:"ResourceTypes"`
}

type associateResourceTypesOutput struct {
	ConfigurationRecorder *ConfigurationRecorder `json:"ConfigurationRecorder"`
}

func (h *Handler) handleAssociateResourceTypes(
	_ context.Context,
	in *associateResourceTypesInput,
) (*associateResourceTypesOutput, error) {
	recorder, err := h.Backend.AssociateResourceTypes(in.ConfigurationRecorderArn, in.ResourceTypes)
	if err != nil {
		return nil, err
	}

	return &associateResourceTypesOutput{ConfigurationRecorder: recorder}, nil
}

// DisassociateResourceTypes request/response types and handler.
type disassociateResourceTypesInput struct {
	ConfigurationRecorderArn string   `json:"ConfigurationRecorderArn"`
	ResourceTypes            []string `json:"ResourceTypes"`
}

func (h *Handler) handleDisassociateResourceTypes(
	_ context.Context, in *disassociateResourceTypesInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DisassociateResourceTypes(in.ConfigurationRecorderArn, in.ResourceTypes)
}

// DeleteServiceLinkedConfigurationRecorder request/response types and handler.
type deleteServiceLinkedConfigurationRecorderInput struct {
	ServicePrincipal string `json:"ServicePrincipal"`
}

type deleteServiceLinkedConfigurationRecorderOutput struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteServiceLinkedConfigurationRecorder(
	_ context.Context, in *deleteServiceLinkedConfigurationRecorderInput,
) (*deleteServiceLinkedConfigurationRecorderOutput, error) {
	name, arn, err := h.Backend.DeleteServiceLinkedConfigurationRecorder(in.ServicePrincipal)
	if err != nil {
		return nil, err
	}

	return &deleteServiceLinkedConfigurationRecorderOutput{Arn: arn, Name: name}, nil
}

// PutServiceLinkedConfigurationRecorder request/response types and handler.
type putServiceLinkedConfigurationRecorderInput struct {
	ServicePrincipal string `json:"ServicePrincipal"`
}

type putServiceLinkedConfigurationRecorderOutput struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

func (h *Handler) handlePutServiceLinkedConfigurationRecorder(
	_ context.Context, in *putServiceLinkedConfigurationRecorderInput,
) (*putServiceLinkedConfigurationRecorderOutput, error) {
	name, arn, err := h.Backend.PutServiceLinkedConfigurationRecorder(in.ServicePrincipal)
	if err != nil {
		return nil, err
	}

	return &putServiceLinkedConfigurationRecorderOutput{Arn: arn, Name: name}, nil
}

// scopeConfigurationBody mirrors ScopeConfiguration on the wire.
type scopeConfigurationBody struct {
	ScopeType       string   `json:"scopeType,omitempty"`
	IncludedRegions []string `json:"includedRegions,omitempty"`
	ScopeValues     []string `json:"scopeValues,omitempty"`
	AllRegions      bool     `json:"allRegions"`
}

// toModel converts the wire body to the backend's ScopeConfiguration. A nil
// receiver (an absent "ScopeConfiguration" in the request) returns nil,
// letting PutThirdPartyServiceLinkedConfigurationRecorder's own
// required-field validation report it.
func (s *scopeConfigurationBody) toModel() *ScopeConfiguration {
	if s == nil {
		return nil
	}

	return &ScopeConfiguration{
		AllRegions:      s.AllRegions,
		IncludedRegions: s.IncludedRegions,
		ScopeType:       s.ScopeType,
		ScopeValues:     s.ScopeValues,
	}
}

// PutThirdPartyServiceLinkedConfigurationRecorder request/response types and handler.
type putThirdPartyServiceLinkedConfigurationRecorderInput struct {
	ConnectorArn       string                  `json:"ConnectorArn"`
	ServicePrincipal   string                  `json:"ServicePrincipal"`
	ScopeConfiguration *scopeConfigurationBody `json:"ScopeConfiguration"`
	Tags               []Tag                   `json:"Tags,omitempty"`
}

type putThirdPartyServiceLinkedConfigurationRecorderOutput struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

func (h *Handler) handlePutThirdPartyServiceLinkedConfigurationRecorder(
	_ context.Context, in *putThirdPartyServiceLinkedConfigurationRecorderInput,
) (*putThirdPartyServiceLinkedConfigurationRecorderOutput, error) {
	name, arn, err := h.Backend.PutThirdPartyServiceLinkedConfigurationRecorder(
		in.ServicePrincipal, in.ConnectorArn, in.ScopeConfiguration.toModel(), in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &putThirdPartyServiceLinkedConfigurationRecorderOutput{Arn: arn, Name: name}, nil
}

// ListConfigurationRecorders request/response types and handler.
type listConfigurationRecordersOutput struct {
	ConfigurationRecorderSummaries []ConfigurationRecorderSummary `json:"ConfigurationRecorderSummaries"`
}

func (h *Handler) handleListConfigurationRecorders(
	_ context.Context, _ *emptyInput,
) (*listConfigurationRecordersOutput, error) {
	return &listConfigurationRecordersOutput{
		ConfigurationRecorderSummaries: h.Backend.ListConfigurationRecorders(),
	}, nil
}

// buildConfigurationRecorderDispatch returns dispatch entries for configuration recorder ops.
func (h *Handler) buildConfigurationRecorderDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opPutConfigurationRecorder:            service.WrapOp(h.handlePutConfigurationRecorder),
		opDescribeConfigurationRecorders:      service.WrapOp(h.handleDescribeConfigurationRecorders),
		opDescribeConfigurationRecorderStatus: service.WrapOp(h.handleDescribeConfigurationRecorderStatus),
		opStartConfigurationRecorder:          service.WrapOp(h.handleStartConfigurationRecorder),
		opStopConfigurationRecorder:           service.WrapOp(h.handleStopConfigurationRecorder),
		opDeleteConfigurationRecorder:         service.WrapOp(h.handleDeleteConfigurationRecorder),
		opAssociateResourceTypes:              service.WrapOp(h.handleAssociateResourceTypes),
		opDisassociateResourceTypes:           service.WrapOp(h.handleDisassociateResourceTypes),
		opDeleteServiceLinkedConfigurationRecorder: service.WrapOp(
			h.handleDeleteServiceLinkedConfigurationRecorder,
		),
		opPutServiceLinkedConfigurationRecorder: service.WrapOp(
			h.handlePutServiceLinkedConfigurationRecorder,
		),
		opPutThirdPartyServiceLinkedConfigurationRecorder: service.WrapOp(
			h.handlePutThirdPartyServiceLinkedConfigurationRecorder,
		),
		opListConfigurationRecorders: service.WrapOp(h.handleListConfigurationRecorders),
	}
}
