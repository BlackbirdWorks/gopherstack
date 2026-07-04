package awsconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// describeConfigRulesPageSize bounds a single DescribeConfigRules page.
const describeConfigRulesPageSize = 100

const (
	opAssociateResourceTypes              = "AssociateResourceTypes"
	opBatchGetAggregateResourceConfig     = "BatchGetAggregateResourceConfig"
	opDeleteConfigRule                    = "DeleteConfigRule"
	opDeleteConfigurationAggregator       = "DeleteConfigurationAggregator"
	opDeleteConfigurationRecorder         = "DeleteConfigurationRecorder"
	opDeleteConformancePack               = "DeleteConformancePack"
	opDeleteDeliveryChannel               = "DeleteDeliveryChannel"
	opDeleteEvaluationResults             = "DeleteEvaluationResults"
	opDeleteOrganizationConfigRule        = "DeleteOrganizationConfigRule"
	opDeleteOrganizationConformancePack   = "DeleteOrganizationConformancePack"
	opDescribeConfigRules                 = "DescribeConfigRules"
	opDescribeConfigurationRecorderStatus = "DescribeConfigurationRecorderStatus"
	opDescribeConfigurationRecorders      = "DescribeConfigurationRecorders"
	opDescribeDeliveryChannels            = "DescribeDeliveryChannels"
	opGetComplianceDetailsByConfigRule    = "GetComplianceDetailsByConfigRule"
	opPutConfigurationRecorder            = "PutConfigurationRecorder"
	opPutDeliveryChannel                  = "PutDeliveryChannel"
	opStartConfigurationRecorder          = "StartConfigurationRecorder"
	opStopConfigurationRecorder           = "StopConfigurationRecorder"
)

const awsConfigTargetPrefix = "StarlingDoveService."

var errUnknownAction = errors.New("unknown action")

type configurationRecorderNameInput struct {
	ConfigurationRecorderName string `json:"ConfigurationRecorderName"`
}

// Handler is the Echo HTTP handler for AWS Config operations.
type Handler struct {
	Backend       *InMemoryBackend
	dispatchTable map[string]service.JSONOpFunc
}

// NewHandler creates a new AWS Config handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.dispatchTable = h.buildDispatchTable()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "AWSConfig" }

// GetSupportedOperations returns the list of supported AWS Config operations.
func (h *Handler) GetSupportedOperations() []string {
	const baseOpCount = 21
	base := make([]string, 0, baseOpCount+len(extendedSupportedOperations()))
	base = append(
		base,
		opPutConfigurationRecorder,
		opDescribeConfigurationRecorders,
		opDescribeConfigurationRecorderStatus,
		opStartConfigurationRecorder,
		opStopConfigurationRecorder,
		opDeleteConfigurationRecorder,
		opPutDeliveryChannel,
		opDescribeDeliveryChannels,
		opDeleteDeliveryChannel,
		opDescribeConfigRules,
		opGetComplianceDetailsByConfigRule,
		opAssociateResourceTypes,
		opBatchGetAggregateResourceConfig,
		"BatchGetResourceConfig",
		"DeleteAggregationAuthorization",
		opDeleteConfigRule,
		opDeleteConfigurationAggregator,
		opDeleteConformancePack,
		opDeleteEvaluationResults,
		opDeleteOrganizationConfigRule,
		opDeleteOrganizationConformancePack,
	)

	return append(base, extendedSupportedOperations()...)
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "config" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this AWS Config instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches AWS Config requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), awsConfigTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the AWS Config action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, awsConfigTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts a resource identifier from the request body based on the operation.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	switch h.ExtractOperation(c) {
	case opPutConfigurationRecorder:
		return extractConfigRecorderName(body)
	case opStartConfigurationRecorder, opStopConfigurationRecorder, opDeleteConfigurationRecorder:
		return extractTopLevelRecorderName(body)
	case opDescribeConfigurationRecorders, opDescribeConfigurationRecorderStatus:
		return extractFirstRecorderName(body)
	case opPutDeliveryChannel:
		return extractDeliveryChannelName(body)
	case opDescribeDeliveryChannels, opDeleteDeliveryChannel:
		return extractFirstDeliveryChannelName(body)
	case opDeleteConfigRule, opDescribeConfigRules, opGetComplianceDetailsByConfigRule, opDeleteEvaluationResults:
		return extractNamedField(body, "ConfigRuleName")
	case opDeleteConfigurationAggregator, opBatchGetAggregateResourceConfig:
		return extractNamedField(body, "ConfigurationAggregatorName")
	case opDeleteConformancePack:
		return extractNamedField(body, "ConformancePackName")
	case opDeleteOrganizationConfigRule:
		return extractNamedField(body, "OrganizationConfigRuleName")
	case opDeleteOrganizationConformancePack:
		return extractNamedField(body, "OrganizationConformancePackName")
	case opAssociateResourceTypes:
		return extractNamedField(body, "ConfigurationRecorderArn")
	default:
		return extractTopLevelRecorderName(body)
	}
}

// configNameBody is a JSON wrapper carrying a single "name" field, used for both
// configuration recorder and delivery channel name extraction.
type configNameBody struct {
	Name string `json:"name"`
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

type extractDeliveryChannelNameInput struct {
	DeliveryChannel configNameBody `json:"DeliveryChannel"`
}

func extractDeliveryChannelName(body []byte) string {
	var req extractDeliveryChannelNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.DeliveryChannel.Name
}

type extractFirstDeliveryChannelNameInput struct {
	DeliveryChannelNames []string `json:"DeliveryChannelNames"`
}

func extractFirstDeliveryChannelName(body []byte) string {
	var req extractFirstDeliveryChannelNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}
	if len(req.DeliveryChannelNames) > 0 {
		return req.DeliveryChannelNames[0]
	}

	return ""
}

// extractNamedField extracts a top-level string field by key from a JSON body.
func extractNamedField(body []byte, key string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return ""
	}

	raw, ok := m[key]
	if !ok {
		return ""
	}

	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}

	return s
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"AWSConfig", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildDispatchTable() map[string]service.JSONOpFunc {
	base := map[string]service.JSONOpFunc{
		opPutConfigurationRecorder:            service.WrapOp(h.handlePutConfigurationRecorder),
		opDescribeConfigurationRecorders:      service.WrapOp(h.handleDescribeConfigurationRecorders),
		opDescribeConfigurationRecorderStatus: service.WrapOp(h.handleDescribeConfigurationRecorderStatus),
		opStartConfigurationRecorder:          service.WrapOp(h.handleStartConfigurationRecorder),
		opStopConfigurationRecorder:           service.WrapOp(h.handleStopConfigurationRecorder),
		opDeleteConfigurationRecorder:         service.WrapOp(h.handleDeleteConfigurationRecorder),
		opPutDeliveryChannel:                  service.WrapOp(h.handlePutDeliveryChannel),
		opDescribeDeliveryChannels:            service.WrapOp(h.handleDescribeDeliveryChannels),
		opDeleteDeliveryChannel:               service.WrapOp(h.handleDeleteDeliveryChannel),
		opDescribeConfigRules:                 service.WrapOp(h.handleDescribeConfigRules),
		opGetComplianceDetailsByConfigRule:    service.WrapOp(h.handleGetComplianceDetailsByConfigRule),
		opAssociateResourceTypes:              service.WrapOp(h.handleAssociateResourceTypes),
		opBatchGetAggregateResourceConfig:     service.WrapOp(h.handleBatchGetAggregateResourceConfig),
		"BatchGetResourceConfig":              service.WrapOp(h.handleBatchGetResourceConfig),
		"DeleteAggregationAuthorization":      service.WrapOp(h.handleDeleteAggregationAuthorization),
		opDeleteConfigRule:                    service.WrapOp(h.handleDeleteConfigRule),
		opDeleteConfigurationAggregator:       service.WrapOp(h.handleDeleteConfigurationAggregator),
		opDeleteConformancePack:               service.WrapOp(h.handleDeleteConformancePack),
		opDeleteEvaluationResults:             service.WrapOp(h.handleDeleteEvaluationResults),
		opDeleteOrganizationConfigRule:        service.WrapOp(h.handleDeleteOrganizationConfigRule),
		opDeleteOrganizationConformancePack:   service.WrapOp(h.handleDeleteOrganizationConformancePack),
	}

	maps.Copy(base, h.buildExtendedDispatchTable())

	return base
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// marshalError serialises a typed AWS error response into bytes.
// Marshaling a struct with only string fields cannot fail; error is intentionally ignored.
func marshalError(errType, message string) []byte {
	payload, _ := json.Marshal(service.JSONErrorResponse{Type: errType, Message: message})

	return payload
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSONBlob(http.StatusNotFound, marshalError("NoSuchConfigurationRecorder", err.Error()))
	case errors.Is(err, ErrNoSuchDeliveryChannel):
		return c.JSONBlob(http.StatusNotFound, marshalError("NoSuchDeliveryChannelException", err.Error()))
	case errors.Is(err, ErrNoSuchConfigRule):
		return c.JSONBlob(http.StatusNotFound, marshalError("NoSuchConfigRuleException", err.Error()))
	case errors.Is(err, ErrNoSuchAggregator):
		return c.JSONBlob(
			http.StatusNotFound,
			marshalError("NoSuchConfigurationAggregatorException", err.Error()),
		)
	case errors.Is(err, ErrNoSuchConformancePack):
		return c.JSONBlob(http.StatusNotFound, marshalError("NoSuchConformancePackException", err.Error()))
	case errors.Is(err, ErrNoSuchOrganizationConfigRule):
		return c.JSONBlob(
			http.StatusNotFound,
			marshalError("NoSuchOrganizationConfigRuleException", err.Error()),
		)
	case errors.Is(err, ErrNoSuchOrganizationConformancePack):
		return c.JSONBlob(
			http.StatusNotFound,
			marshalError("OrganizationConformancePackNotFoundException", err.Error()),
		)
	case errors.Is(err, ErrNoSuchAggregationAuthorization):
		return c.JSONBlob(
			http.StatusNotFound,
			marshalError("NoSuchAggregationAuthorizationException", err.Error()),
		)
	case errors.Is(err, ErrResourceNotFound):
		return c.JSONBlob(http.StatusBadRequest, marshalError("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSONBlob(
			http.StatusConflict,
			marshalError("MaxNumberOfConfigurationRecordersExceededException", err.Error()),
		)
	case errors.Is(err, ErrValidation), errors.Is(err, ErrNoDeliveryChannel):
		return c.JSONBlob(http.StatusBadRequest, marshalError("ValidationException", err.Error()))
	case errors.Is(err, errUnknownAction), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
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

// deliveryChannelBody is the nested JSON body for a delivery channel.
type deliveryChannelBody struct {
	ConfigSnapshotDeliveryProperties *DeliverySnapshotProperties `json:"configSnapshotDeliveryProperties,omitempty"`
	Name                             string                      `json:"name"`
	S3BucketName                     string                      `json:"s3BucketName"`
	S3KeyPrefix                      string                      `json:"s3KeyPrefix,omitempty"`
	SnsTopicARN                      string                      `json:"snsTopicARN"`
}

type handlePutDeliveryChannelInput struct {
	DeliveryChannel deliveryChannelBody `json:"DeliveryChannel"`
}

type putDeliveryChannelOutput struct{}

func (h *Handler) handlePutDeliveryChannel(
	_ context.Context,
	in *handlePutDeliveryChannelInput,
) (*putDeliveryChannelOutput, error) {
	if err := h.Backend.PutDeliveryChannel(
		in.DeliveryChannel.Name,
		in.DeliveryChannel.S3BucketName,
		in.DeliveryChannel.SnsTopicARN,
		in.DeliveryChannel.S3KeyPrefix,
		in.DeliveryChannel.ConfigSnapshotDeliveryProperties,
	); err != nil {
		return nil, err
	}

	return &putDeliveryChannelOutput{}, nil
}

type describeDeliveryChannelsInput struct {
	DeliveryChannelNames []string `json:"DeliveryChannelNames,omitempty"`
}

type describeDeliveryChannelsOutput struct {
	DeliveryChannels []DeliveryChannel `json:"DeliveryChannels"`
}

func (h *Handler) handleDescribeDeliveryChannels(
	_ context.Context,
	in *describeDeliveryChannelsInput,
) (*describeDeliveryChannelsOutput, error) {
	channels := h.Backend.DescribeDeliveryChannels(in.DeliveryChannelNames)

	return &describeDeliveryChannelsOutput{DeliveryChannels: channels}, nil
}

type deleteDeliveryChannelInput struct {
	DeliveryChannelName string `json:"DeliveryChannelName"`
}

type deleteDeliveryChannelOutput struct{}

func (h *Handler) handleDeleteDeliveryChannel(
	_ context.Context,
	in *deleteDeliveryChannelInput,
) (*deleteDeliveryChannelOutput, error) {
	if err := h.Backend.DeleteDeliveryChannel(in.DeliveryChannelName); err != nil {
		return nil, err
	}

	return &deleteDeliveryChannelOutput{}, nil
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

// --- Config Recorder Status ---

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

// --- Config Rules ---

type describeConfigRulesInput struct {
	NextToken       string   `json:"NextToken,omitempty"`
	ConfigRuleNames []string `json:"ConfigRuleNames,omitempty"`
}

type describeConfigRulesOutput struct {
	NextToken   string       `json:"NextToken,omitempty"`
	ConfigRules []ConfigRule `json:"ConfigRules"`
}

func (h *Handler) handleDescribeConfigRules(
	_ context.Context,
	in *describeConfigRulesInput,
) (*describeConfigRulesOutput, error) {
	if err := page.ValidateToken(in.NextToken); err != nil {
		return nil, fmt.Errorf("%w: invalid NextToken", ErrValidation)
	}

	all := h.Backend.DescribeConfigRules(in.ConfigRuleNames)
	p := page.New(all, in.NextToken, describeConfigRulesPageSize, describeConfigRulesPageSize)

	return &describeConfigRulesOutput{ConfigRules: p.Data, NextToken: p.Next}, nil
}

type getComplianceDetailsByConfigRuleInput struct {
	ConfigRuleName  string   `json:"ConfigRuleName"`
	NextToken       string   `json:"NextToken,omitempty"`
	ComplianceTypes []string `json:"ComplianceTypes,omitempty"`
}

type getComplianceDetailsByConfigRuleOutput struct {
	NextToken         string                     `json:"NextToken,omitempty"`
	EvaluationResults []DetailedEvaluationResult `json:"EvaluationResults"`
}

// handleGetComplianceDetailsByConfigRule returns the real per-resource compliance
// evaluation results recorded for a config rule.
func (h *Handler) handleGetComplianceDetailsByConfigRule(
	_ context.Context,
	in *getComplianceDetailsByConfigRuleInput,
) (*getComplianceDetailsByConfigRuleOutput, error) {
	results := h.Backend.GetComplianceDetailsByConfigRule(in.ConfigRuleName, in.ComplianceTypes)

	return &getComplianceDetailsByConfigRuleOutput{EvaluationResults: results}, nil
}

// --- AssociateResourceTypes ---

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

// --- BatchGetAggregateResourceConfig ---

type batchGetAggregateResourceConfigInput struct {
	ConfigurationAggregatorName string                        `json:"ConfigurationAggregatorName"`
	ResourceIdentifiers         []AggregateResourceIdentifier `json:"ResourceIdentifiers"`
}

type batchGetAggregateResourceConfigOutput struct {
	BaseConfigurationItems         []BaseConfigurationItem       `json:"BaseConfigurationItems"`
	UnprocessedResourceIdentifiers []AggregateResourceIdentifier `json:"UnprocessedResourceIdentifiers"`
}

func (h *Handler) handleBatchGetAggregateResourceConfig(
	_ context.Context,
	in *batchGetAggregateResourceConfigInput,
) (*batchGetAggregateResourceConfigOutput, error) {
	items, unprocessed := h.Backend.BatchGetAggregateResourceConfig(
		in.ConfigurationAggregatorName,
		in.ResourceIdentifiers,
	)

	return &batchGetAggregateResourceConfigOutput{
		BaseConfigurationItems:         items,
		UnprocessedResourceIdentifiers: unprocessed,
	}, nil
}

// --- BatchGetResourceConfig ---

type batchGetResourceConfigInput struct {
	ResourceKeys []ResourceKey `json:"ResourceKeys"`
}

type batchGetResourceConfigOutput struct {
	BaseConfigurationItems  []BaseConfigurationItem `json:"BaseConfigurationItems"`
	UnprocessedResourceKeys []ResourceKey           `json:"UnprocessedResourceKeys"`
}

func (h *Handler) handleBatchGetResourceConfig(
	_ context.Context,
	in *batchGetResourceConfigInput,
) (*batchGetResourceConfigOutput, error) {
	items, unprocessed := h.Backend.BatchGetResourceConfig(in.ResourceKeys)

	return &batchGetResourceConfigOutput{
		BaseConfigurationItems:  items,
		UnprocessedResourceKeys: unprocessed,
	}, nil
}

// --- DeleteAggregationAuthorization ---

type deleteAggregationAuthorizationInput struct {
	AuthorizedAccountID string `json:"AuthorizedAccountId"`
	AuthorizedAwsRegion string `json:"AuthorizedAwsRegion"`
}

type deleteAggregationAuthorizationOutput struct{}

func (h *Handler) handleDeleteAggregationAuthorization(
	_ context.Context,
	in *deleteAggregationAuthorizationInput,
) (*deleteAggregationAuthorizationOutput, error) {
	if err := h.Backend.DeleteAggregationAuthorization(in.AuthorizedAccountID, in.AuthorizedAwsRegion); err != nil {
		return nil, err
	}

	return &deleteAggregationAuthorizationOutput{}, nil
}

// --- DeleteConfigRule ---

type deleteConfigRuleInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
}

type deleteConfigRuleOutput struct{}

func (h *Handler) handleDeleteConfigRule(
	_ context.Context,
	in *deleteConfigRuleInput,
) (*deleteConfigRuleOutput, error) {
	if err := h.Backend.DeleteConfigRule(in.ConfigRuleName); err != nil {
		return nil, err
	}

	return &deleteConfigRuleOutput{}, nil
}

// --- DeleteConfigurationAggregator ---

type deleteConfigurationAggregatorInput struct {
	ConfigurationAggregatorName string `json:"ConfigurationAggregatorName"`
}

type deleteConfigurationAggregatorOutput struct{}

func (h *Handler) handleDeleteConfigurationAggregator(
	_ context.Context,
	in *deleteConfigurationAggregatorInput,
) (*deleteConfigurationAggregatorOutput, error) {
	if err := h.Backend.DeleteConfigurationAggregator(in.ConfigurationAggregatorName); err != nil {
		return nil, err
	}

	return &deleteConfigurationAggregatorOutput{}, nil
}

// --- DeleteConformancePack ---

type deleteConformancePackInput struct {
	ConformancePackName string `json:"ConformancePackName"`
}

type deleteConformancePackOutput struct{}

func (h *Handler) handleDeleteConformancePack(
	_ context.Context,
	in *deleteConformancePackInput,
) (*deleteConformancePackOutput, error) {
	if err := h.Backend.DeleteConformancePack(in.ConformancePackName); err != nil {
		return nil, err
	}

	return &deleteConformancePackOutput{}, nil
}

// --- DeleteEvaluationResults ---

type deleteEvaluationResultsInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
}

type deleteEvaluationResultsOutput struct{}

func (h *Handler) handleDeleteEvaluationResults(
	_ context.Context,
	in *deleteEvaluationResultsInput,
) (*deleteEvaluationResultsOutput, error) {
	if err := h.Backend.DeleteEvaluationResults(in.ConfigRuleName); err != nil {
		return nil, err
	}

	return &deleteEvaluationResultsOutput{}, nil
}

// --- DeleteOrganizationConfigRule ---

type deleteOrganizationConfigRuleInput struct {
	OrganizationConfigRuleName string `json:"OrganizationConfigRuleName"`
}

type deleteOrganizationConfigRuleOutput struct{}

func (h *Handler) handleDeleteOrganizationConfigRule(
	_ context.Context,
	in *deleteOrganizationConfigRuleInput,
) (*deleteOrganizationConfigRuleOutput, error) {
	if err := h.Backend.DeleteOrganizationConfigRule(in.OrganizationConfigRuleName); err != nil {
		return nil, err
	}

	return &deleteOrganizationConfigRuleOutput{}, nil
}

// --- DeleteOrganizationConformancePack ---

type deleteOrganizationConformancePackInput struct {
	OrganizationConformancePackName string `json:"OrganizationConformancePackName"`
}

type deleteOrganizationConformancePackOutput struct{}

func (h *Handler) handleDeleteOrganizationConformancePack(
	_ context.Context,
	in *deleteOrganizationConformancePackInput,
) (*deleteOrganizationConformancePackOutput, error) {
	if err := h.Backend.DeleteOrganizationConformancePack(in.OrganizationConformancePackName); err != nil {
		return nil, err
	}

	return &deleteOrganizationConformancePackOutput{}, nil
}
