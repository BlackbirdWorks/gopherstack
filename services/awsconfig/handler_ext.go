package awsconfig

import (
	"context"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for the extended ops.
const (
	opDeletePendingAggregationRequest               = "DeletePendingAggregationRequest"
	opDeleteRemediationConfiguration                = "DeleteRemediationConfiguration"
	opDeleteRemediationExceptions                   = "DeleteRemediationExceptions"
	opDeleteResourceConfig                          = "DeleteResourceConfig"
	opDeleteRetentionConfiguration                  = "DeleteRetentionConfiguration"
	opDeleteServiceLinkedConfigurationRecorder      = "DeleteServiceLinkedConfigurationRecorder"
	opDeleteStoredQuery                             = "DeleteStoredQuery"
	opDeliverConfigSnapshot                         = "DeliverConfigSnapshot"
	opDescribeAggregateComplianceByConfigRules      = "DescribeAggregateComplianceByConfigRules"
	opDescribeAggregateComplianceByConformancePacks = "DescribeAggregateComplianceByConformancePacks"
	opDescribeAggregationAuthorizations             = "DescribeAggregationAuthorizations"
	opDescribeComplianceByConfigRule                = "DescribeComplianceByConfigRule"
	opDescribeComplianceByResource                  = "DescribeComplianceByResource"
	opDescribeConfigRuleEvaluationStatus            = "DescribeConfigRuleEvaluationStatus"
	opDescribeConfigurationAggregatorSourcesStatus  = "DescribeConfigurationAggregatorSourcesStatus"
	opDescribeConfigurationAggregators              = "DescribeConfigurationAggregators"
	opDescribeConformancePackCompliance             = "DescribeConformancePackCompliance"
	opDescribeConformancePackStatus                 = "DescribeConformancePackStatus"
	opDescribeConformancePacks                      = "DescribeConformancePacks"
	opDescribeDeliveryChannelStatus                 = "DescribeDeliveryChannelStatus"
	opDescribeOrganizationConfigRuleStatuses        = "DescribeOrganizationConfigRuleStatuses"
	opDescribeOrganizationConfigRules               = "DescribeOrganizationConfigRules"
	opDescribeOrganizationConformancePackStatuses   = "DescribeOrganizationConformancePackStatuses"
	opDescribeOrganizationConformancePacks          = "DescribeOrganizationConformancePacks"
	opDescribePendingAggregationRequests            = "DescribePendingAggregationRequests"
	opDescribeRemediationConfigurations             = "DescribeRemediationConfigurations"
	opDescribeRemediationExceptions                 = "DescribeRemediationExceptions"
	opDescribeRemediationExecutionStatus            = "DescribeRemediationExecutionStatus"
	opDescribeRetentionConfigurations               = "DescribeRetentionConfigurations"
	opDisassociateResourceTypes                     = "DisassociateResourceTypes"
	opGetAggregateComplianceDetailsByConfigRule     = "GetAggregateComplianceDetailsByConfigRule"
	opGetAggregateConfigRuleComplianceSummary       = "GetAggregateConfigRuleComplianceSummary"
	opGetAggregateConformancePackComplianceSummary  = "GetAggregateConformancePackComplianceSummary"
	opGetAggregateDiscoveredResourceCounts          = "GetAggregateDiscoveredResourceCounts"
	opGetAggregateResourceConfig                    = "GetAggregateResourceConfig"
	opGetComplianceDetailsByResource                = "GetComplianceDetailsByResource"
	opGetComplianceSummaryByConfigRule              = "GetComplianceSummaryByConfigRule"
	opGetComplianceSummaryByResourceType            = "GetComplianceSummaryByResourceType"
	opGetConformancePackComplianceDetails           = "GetConformancePackComplianceDetails"
	opGetConformancePackComplianceSummary           = "GetConformancePackComplianceSummary"
	opGetCustomRulePolicy                           = "GetCustomRulePolicy"
	opGetDiscoveredResourceCounts                   = "GetDiscoveredResourceCounts"
	opGetOrganizationConfigRuleDetailedStatus       = "GetOrganizationConfigRuleDetailedStatus"
	opGetOrganizationConformancePackDetailedStatus  = "GetOrganizationConformancePackDetailedStatus"
	opGetOrganizationCustomRulePolicy               = "GetOrganizationCustomRulePolicy"
	opGetResourceConfigHistory                      = "GetResourceConfigHistory"
	opGetResourceEvaluationSummary                  = "GetResourceEvaluationSummary"
	opGetStoredQuery                                = "GetStoredQuery"
	opListAggregateDiscoveredResources              = "ListAggregateDiscoveredResources"
	opListConfigurationRecorders                    = "ListConfigurationRecorders"
	opListConformancePackComplianceScores           = "ListConformancePackComplianceScores"
	opListDiscoveredResources                       = "ListDiscoveredResources"
	opListResourceEvaluations                       = "ListResourceEvaluations"
	opListStoredQueries                             = "ListStoredQueries"
	opListTagsForResource                           = "ListTagsForResource"
	opPutAggregationAuthorization                   = "PutAggregationAuthorization"
	opPutConfigRule                                 = "PutConfigRule"
	opPutConfigurationAggregator                    = "PutConfigurationAggregator"
	opPutConformancePack                            = "PutConformancePack"
	opPutEvaluations                                = "PutEvaluations"
	opPutExternalEvaluation                         = "PutExternalEvaluation"
	opPutOrganizationConfigRule                     = "PutOrganizationConfigRule"
	opPutOrganizationConformancePack                = "PutOrganizationConformancePack"
	opPutRemediationConfigurations                  = "PutRemediationConfigurations"
	opPutRemediationExceptions                      = "PutRemediationExceptions"
	opPutResourceConfig                             = "PutResourceConfig"
	opPutRetentionConfiguration                     = "PutRetentionConfiguration"
	opPutServiceLinkedConfigurationRecorder         = "PutServiceLinkedConfigurationRecorder"
	opPutStoredQuery                                = "PutStoredQuery"
	opSelectAggregateResourceConfig                 = "SelectAggregateResourceConfig"
	opSelectResourceConfig                          = "SelectResourceConfig"
	opStartConfigRulesEvaluation                    = "StartConfigRulesEvaluation"
	opStartRemediationExecution                     = "StartRemediationExecution"
	opStartResourceEvaluation                       = "StartResourceEvaluation"
	opTagResource                                   = "TagResource"
	opUntagResource                                 = "UntagResource"
)

// extendedSupportedOperations returns the list of newly implemented operations.
func extendedSupportedOperations() []string {
	return []string{
		opDeletePendingAggregationRequest,
		opDeleteRemediationConfiguration,
		opDeleteRemediationExceptions,
		opDeleteResourceConfig,
		opDeleteRetentionConfiguration,
		opDeleteServiceLinkedConfigurationRecorder,
		opDeleteStoredQuery,
		opDeliverConfigSnapshot,
		opDescribeAggregateComplianceByConfigRules,
		opDescribeAggregateComplianceByConformancePacks,
		opDescribeAggregationAuthorizations,
		opDescribeComplianceByConfigRule,
		opDescribeComplianceByResource,
		opDescribeConfigRuleEvaluationStatus,
		opDescribeConfigurationAggregatorSourcesStatus,
		opDescribeConfigurationAggregators,
		opDescribeConformancePackCompliance,
		opDescribeConformancePackStatus,
		opDescribeConformancePacks,
		opDescribeDeliveryChannelStatus,
		opDescribeOrganizationConfigRuleStatuses,
		opDescribeOrganizationConfigRules,
		opDescribeOrganizationConformancePackStatuses,
		opDescribeOrganizationConformancePacks,
		opDescribePendingAggregationRequests,
		opDescribeRemediationConfigurations,
		opDescribeRemediationExceptions,
		opDescribeRemediationExecutionStatus,
		opDescribeRetentionConfigurations,
		opDisassociateResourceTypes,
		opGetAggregateComplianceDetailsByConfigRule,
		opGetAggregateConfigRuleComplianceSummary,
		opGetAggregateConformancePackComplianceSummary,
		opGetAggregateDiscoveredResourceCounts,
		opGetAggregateResourceConfig,
		opGetComplianceDetailsByResource,
		opGetComplianceSummaryByConfigRule,
		opGetComplianceSummaryByResourceType,
		opGetConformancePackComplianceDetails,
		opGetConformancePackComplianceSummary,
		opGetCustomRulePolicy,
		opGetDiscoveredResourceCounts,
		opGetOrganizationConfigRuleDetailedStatus,
		opGetOrganizationConformancePackDetailedStatus,
		opGetOrganizationCustomRulePolicy,
		opGetResourceConfigHistory,
		opGetResourceEvaluationSummary,
		opGetStoredQuery,
		opListAggregateDiscoveredResources,
		opListConfigurationRecorders,
		opListConformancePackComplianceScores,
		opListDiscoveredResources,
		opListResourceEvaluations,
		opListStoredQueries,
		opListTagsForResource,
		opPutAggregationAuthorization,
		opPutConfigRule,
		opPutConfigurationAggregator,
		opPutConformancePack,
		opPutEvaluations,
		opPutExternalEvaluation,
		opPutOrganizationConfigRule,
		opPutOrganizationConformancePack,
		opPutRemediationConfigurations,
		opPutRemediationExceptions,
		opPutResourceConfig,
		opPutRetentionConfiguration,
		opPutServiceLinkedConfigurationRecorder,
		opPutStoredQuery,
		opSelectAggregateResourceConfig,
		opSelectResourceConfig,
		opStartConfigRulesEvaluation,
		opStartRemediationExecution,
		opStartResourceEvaluation,
		opTagResource,
		opUntagResource,
	}
}

// --- Request/Response types for extended ops ---

type (
	emptyInput  struct{}
	emptyOutput struct{}
)

// DeletePendingAggregationRequest request/response types and handler.
type deletePendingAggregationRequestInput struct {
	RequesterAccountID string `json:"RequesterAccountId"`
	RequesterAwsRegion string `json:"RequesterAwsRegion"`
}

func (h *Handler) handleDeletePendingAggregationRequest(
	_ context.Context, in *deletePendingAggregationRequestInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeletePendingAggregationRequest(
		in.RequesterAccountID,
		in.RequesterAwsRegion,
	)
}

// DeleteRemediationConfiguration request/response types and handler.
type deleteRemediationConfigurationInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
}

func (h *Handler) handleDeleteRemediationConfiguration(
	_ context.Context, in *deleteRemediationConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteRemediationConfiguration(in.ConfigRuleName)
}

// DeleteRemediationExceptions request/response types and handler.
type deleteRemediationExceptionsInput struct {
	ConfigRuleName    string `json:"ConfigRuleName"`
	ResourceGroupName string `json:"ResourceGroupName"`
}

func (h *Handler) handleDeleteRemediationExceptions(
	_ context.Context, in *deleteRemediationExceptionsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteRemediationExceptions(
		in.ConfigRuleName,
		in.ResourceGroupName,
	)
}

// DeleteResourceConfig request/response types and handler.
type deleteResourceConfigInput struct {
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
}

func (h *Handler) handleDeleteResourceConfig(
	_ context.Context, in *deleteResourceConfigInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteResourceConfig(in.ResourceType, in.ResourceID)
}

// DeleteRetentionConfiguration request/response types and handler.
type deleteRetentionConfigurationInput struct {
	RetentionConfigurationName string `json:"RetentionConfigurationName"`
}

func (h *Handler) handleDeleteRetentionConfiguration(
	_ context.Context, in *deleteRetentionConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteRetentionConfiguration(in.RetentionConfigurationName)
}

// DeleteServiceLinkedConfigurationRecorder request/response types and handler.
type deleteServiceLinkedConfigurationRecorderInput struct {
	ServicePrincipal string `json:"ServicePrincipal"`
}

func (h *Handler) handleDeleteServiceLinkedConfigurationRecorder(
	_ context.Context, in *deleteServiceLinkedConfigurationRecorderInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteServiceLinkedConfigurationRecorder(in.ServicePrincipal)
}

// DeleteStoredQuery request/response types and handler.
type deleteStoredQueryInput struct {
	QueryName string `json:"QueryName"`
}

func (h *Handler) handleDeleteStoredQuery(
	_ context.Context, in *deleteStoredQueryInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteStoredQuery(in.QueryName)
}

// DeliverConfigSnapshot request/response types and handler.
type deliverConfigSnapshotInput struct {
	DeliveryChannelName string `json:"deliveryChannelName"`
}

func (h *Handler) handleDeliverConfigSnapshot(
	_ context.Context, in *deliverConfigSnapshotInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeliverConfigSnapshot(in.DeliveryChannelName)
}

// DescribeAggregateComplianceByConfigRules request/response types and handler.
type describeAggregateComplianceByConfigRulesOutput struct {
	AggregateComplianceByConfigRules []any `json:"AggregateComplianceByConfigRules"`
}

func (h *Handler) handleDescribeAggregateComplianceByConfigRules(
	_ context.Context, _ *emptyInput,
) (*describeAggregateComplianceByConfigRulesOutput, error) {
	return &describeAggregateComplianceByConfigRulesOutput{
		AggregateComplianceByConfigRules: h.Backend.DescribeAggregateComplianceByConfigRules(),
	}, nil
}

// DescribeAggregateComplianceByConformancePacks request/response types and handler.
type describeAggregateComplianceByConformancePacksOutput struct {
	AggregateComplianceByConformancePacks []any `json:"AggregateComplianceByConformancePacks"`
}

func (h *Handler) handleDescribeAggregateComplianceByConformancePacks(
	_ context.Context, _ *emptyInput,
) (*describeAggregateComplianceByConformancePacksOutput, error) {
	return &describeAggregateComplianceByConformancePacksOutput{
		AggregateComplianceByConformancePacks: h.Backend.DescribeAggregateComplianceByConformancePacks(),
	}, nil
}

// DescribeAggregationAuthorizations request/response types and handler.
type describeAggregationAuthorizationsOutput struct {
	AggregationAuthorizations []AggregationAuthorization `json:"AggregationAuthorizations"`
}

func (h *Handler) handleDescribeAggregationAuthorizations(
	_ context.Context, _ *emptyInput,
) (*describeAggregationAuthorizationsOutput, error) {
	return &describeAggregationAuthorizationsOutput{
		AggregationAuthorizations: h.Backend.DescribeAggregationAuthorizations(),
	}, nil
}

// DescribeComplianceByConfigRule request/response types and handler.
type describeComplianceByConfigRuleInput struct {
	ConfigRuleNames []string `json:"ConfigRuleNames"`
}
type describeComplianceByConfigRuleOutput struct {
	ComplianceByConfigRules []ComplianceByConfigRule `json:"ComplianceByConfigRules"`
}

func (h *Handler) handleDescribeComplianceByConfigRule(
	_ context.Context, in *describeComplianceByConfigRuleInput,
) (*describeComplianceByConfigRuleOutput, error) {
	return &describeComplianceByConfigRuleOutput{
		ComplianceByConfigRules: h.Backend.DescribeComplianceByConfigRule(in.ConfigRuleNames),
	}, nil
}

// DescribeComplianceByResource request/response types and handler.
type describeComplianceByResourceOutput struct {
	ComplianceByResources []any `json:"ComplianceByResources"`
}

func (h *Handler) handleDescribeComplianceByResource(
	_ context.Context, _ *emptyInput,
) (*describeComplianceByResourceOutput, error) {
	return &describeComplianceByResourceOutput{
		ComplianceByResources: h.Backend.DescribeComplianceByResource(),
	}, nil
}

// DescribeConfigRuleEvaluationStatus request/response types and handler.
type describeConfigRuleEvaluationStatusInput struct {
	ConfigRuleNames []string `json:"ConfigRuleNames"`
}
type describeConfigRuleEvaluationStatusOutput struct {
	ConfigRulesEvaluationStatus []ConfigRuleEvaluationStatus `json:"ConfigRulesEvaluationStatus"`
}

func (h *Handler) handleDescribeConfigRuleEvaluationStatus(
	_ context.Context, in *describeConfigRuleEvaluationStatusInput,
) (*describeConfigRuleEvaluationStatusOutput, error) {
	return &describeConfigRuleEvaluationStatusOutput{
		ConfigRulesEvaluationStatus: h.Backend.DescribeConfigRuleEvaluationStatus(in.ConfigRuleNames),
	}, nil
}

// DescribeConfigurationAggregatorSourcesStatus request/response types and handler.
type describeConfigurationAggregatorSourcesStatusOutput struct {
	AggregatedSourceStatusList []any `json:"AggregatedSourceStatusList"`
}

func (h *Handler) handleDescribeConfigurationAggregatorSourcesStatus(
	_ context.Context, _ *emptyInput,
) (*describeConfigurationAggregatorSourcesStatusOutput, error) {
	return &describeConfigurationAggregatorSourcesStatusOutput{
		AggregatedSourceStatusList: h.Backend.DescribeConfigurationAggregatorSourcesStatus(),
	}, nil
}

// DescribeConfigurationAggregators request/response types and handler.
type describeConfigurationAggregatorsOutput struct {
	ConfigurationAggregators []ConfigurationAggregator `json:"ConfigurationAggregators"`
}

func (h *Handler) handleDescribeConfigurationAggregators(
	_ context.Context, _ *emptyInput,
) (*describeConfigurationAggregatorsOutput, error) {
	return &describeConfigurationAggregatorsOutput{
		ConfigurationAggregators: h.Backend.DescribeConfigurationAggregators(),
	}, nil
}

// DescribeConformancePackCompliance request/response types and handler.
type describeConformancePackComplianceInput struct {
	ConformancePackName string `json:"ConformancePackName"`
}
type describeConformancePackComplianceOutput struct {
	ConformancePackRuleComplianceList []ConformancePackComplianceItem `json:"ConformancePackRuleComplianceList"`
}

func (h *Handler) handleDescribeConformancePackCompliance(
	_ context.Context, in *describeConformancePackComplianceInput,
) (*describeConformancePackComplianceOutput, error) {
	return &describeConformancePackComplianceOutput{
		ConformancePackRuleComplianceList: h.Backend.DescribeConformancePackCompliance(in.ConformancePackName),
	}, nil
}

// DescribeConformancePackStatus request/response types and handler.
type describeConformancePackStatusInput struct {
	ConformancePackNames []string `json:"ConformancePackNames"`
}
type describeConformancePackStatusOutput struct {
	ConformancePackStatusDetails []ConformancePackStatus `json:"ConformancePackStatusDetails"`
}

func (h *Handler) handleDescribeConformancePackStatus(
	_ context.Context, in *describeConformancePackStatusInput,
) (*describeConformancePackStatusOutput, error) {
	return &describeConformancePackStatusOutput{
		ConformancePackStatusDetails: h.Backend.DescribeConformancePackStatus(in.ConformancePackNames),
	}, nil
}

// DescribeConformancePacks request/response types and handler.
type describeConformancePacksOutput struct {
	ConformancePackDetails []ConformancePack `json:"ConformancePackDetails"`
}

func (h *Handler) handleDescribeConformancePacks(
	_ context.Context, _ *emptyInput,
) (*describeConformancePacksOutput, error) {
	return &describeConformancePacksOutput{
		ConformancePackDetails: h.Backend.DescribeConformancePacks(),
	}, nil
}

// DescribeDeliveryChannelStatus request/response types and handler.
type describeDeliveryChannelStatusInput struct {
	DeliveryChannelNames []string `json:"DeliveryChannelNames"`
}
type describeDeliveryChannelStatusOutput struct {
	DeliveryChannelsStatus []DeliveryChannelStatus `json:"DeliveryChannelsStatus"`
}

func (h *Handler) handleDescribeDeliveryChannelStatus(
	_ context.Context, in *describeDeliveryChannelStatusInput,
) (*describeDeliveryChannelStatusOutput, error) {
	return &describeDeliveryChannelStatusOutput{
		DeliveryChannelsStatus: h.Backend.DescribeDeliveryChannelStatus(in.DeliveryChannelNames),
	}, nil
}

// DescribeOrganizationConfigRuleStatuses request/response types and handler.
type describeOrganizationConfigRuleStatusesInput struct {
	OrganizationConfigRuleNames []string `json:"OrganizationConfigRuleNames"`
}
type describeOrganizationConfigRuleStatusesOutput struct {
	OrganizationConfigRuleStatuses []OrganizationConfigRuleStatus `json:"OrganizationConfigRuleStatuses"`
}

func (h *Handler) handleDescribeOrganizationConfigRuleStatuses(
	_ context.Context, in *describeOrganizationConfigRuleStatusesInput,
) (*describeOrganizationConfigRuleStatusesOutput, error) {
	return &describeOrganizationConfigRuleStatusesOutput{
		OrganizationConfigRuleStatuses: h.Backend.DescribeOrganizationConfigRuleStatuses(
			in.OrganizationConfigRuleNames,
		),
	}, nil
}

// DescribeOrganizationConfigRules request/response types and handler.
type describeOrganizationConfigRulesOutput struct {
	OrganizationConfigRules []OrganizationConfigRule `json:"OrganizationConfigRules"`
}

func (h *Handler) handleDescribeOrganizationConfigRules(
	_ context.Context, _ *emptyInput,
) (*describeOrganizationConfigRulesOutput, error) {
	return &describeOrganizationConfigRulesOutput{
		OrganizationConfigRules: h.Backend.DescribeOrganizationConfigRules(),
	}, nil
}

// DescribeOrganizationConformancePackStatuses request/response types and handler.
type describeOrganizationConformancePackStatusesInput struct {
	OrganizationConformancePackNames []string `json:"OrganizationConformancePackNames"`
}
type describeOrganizationConformancePackStatusesOutput struct {
	OrganizationConformancePackStatuses []OrganizationConformancePackStatus `json:"OrganizationConformancePackStatuses"`
}

func (h *Handler) handleDescribeOrganizationConformancePackStatuses(
	_ context.Context, in *describeOrganizationConformancePackStatusesInput,
) (*describeOrganizationConformancePackStatusesOutput, error) {
	return &describeOrganizationConformancePackStatusesOutput{
		OrganizationConformancePackStatuses: h.Backend.DescribeOrganizationConformancePackStatuses(
			in.OrganizationConformancePackNames,
		),
	}, nil
}

// DescribeOrganizationConformancePacks request/response types and handler.
type describeOrganizationConformancePacksOutput struct {
	OrganizationConformancePacks []OrganizationConformancePack `json:"OrganizationConformancePacks"`
}

func (h *Handler) handleDescribeOrganizationConformancePacks(
	_ context.Context, _ *emptyInput,
) (*describeOrganizationConformancePacksOutput, error) {
	return &describeOrganizationConformancePacksOutput{
		OrganizationConformancePacks: h.Backend.DescribeOrganizationConformancePacks(),
	}, nil
}

// DescribePendingAggregationRequests request/response types and handler.
type describePendingAggregationRequestsOutput struct {
	PendingAggregationRequests []any `json:"PendingAggregationRequests"`
}

func (h *Handler) handleDescribePendingAggregationRequests(
	_ context.Context, _ *emptyInput,
) (*describePendingAggregationRequestsOutput, error) {
	return &describePendingAggregationRequestsOutput{
		PendingAggregationRequests: h.Backend.DescribePendingAggregationRequests(),
	}, nil
}

// DescribeRemediationConfigurations request/response types and handler.
type describeRemediationConfigurationsInput struct {
	ConfigRuleNames []string `json:"ConfigRuleNames"`
}
type describeRemediationConfigurationsOutput struct {
	RemediationConfigurations []RemediationConfiguration `json:"RemediationConfigurations"`
}

func (h *Handler) handleDescribeRemediationConfigurations(
	_ context.Context, in *describeRemediationConfigurationsInput,
) (*describeRemediationConfigurationsOutput, error) {
	return &describeRemediationConfigurationsOutput{
		RemediationConfigurations: h.Backend.DescribeRemediationConfigurations(in.ConfigRuleNames),
	}, nil
}

// DescribeRemediationExceptions request/response types and handler.
type describeRemediationExceptionsInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
}
type describeRemediationExceptionsOutput struct {
	RemediationExceptions []RemediationException `json:"RemediationExceptions"`
}

func (h *Handler) handleDescribeRemediationExceptions(
	_ context.Context, in *describeRemediationExceptionsInput,
) (*describeRemediationExceptionsOutput, error) {
	return &describeRemediationExceptionsOutput{
		RemediationExceptions: h.Backend.DescribeRemediationExceptions(in.ConfigRuleName),
	}, nil
}

// DescribeRemediationExecutionStatus request/response types and handler.
type describeRemediationExecutionStatusOutput struct {
	RemediationExecutionStatuses []any `json:"RemediationExecutionStatuses"`
}

func (h *Handler) handleDescribeRemediationExecutionStatus(
	_ context.Context, _ *emptyInput,
) (*describeRemediationExecutionStatusOutput, error) {
	return &describeRemediationExecutionStatusOutput{
		RemediationExecutionStatuses: h.Backend.DescribeRemediationExecutionStatus(),
	}, nil
}

// DescribeRetentionConfigurations request/response types and handler.
type describeRetentionConfigurationsOutput struct {
	RetentionConfigurations []RetentionConfiguration `json:"RetentionConfigurations"`
}

func (h *Handler) handleDescribeRetentionConfigurations(
	_ context.Context, _ *emptyInput,
) (*describeRetentionConfigurationsOutput, error) {
	return &describeRetentionConfigurationsOutput{
		RetentionConfigurations: h.Backend.DescribeRetentionConfigurations(),
	}, nil
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

// GetAggregateComplianceDetailsByConfigRule request/response types and handler.
type getAggregateComplianceDetailsByConfigRuleOutput struct {
	AggregateEvaluationResults []any `json:"AggregateEvaluationResults"`
}

func (h *Handler) handleGetAggregateComplianceDetailsByConfigRule(
	_ context.Context, _ *emptyInput,
) (*getAggregateComplianceDetailsByConfigRuleOutput, error) {
	return &getAggregateComplianceDetailsByConfigRuleOutput{
		AggregateEvaluationResults: h.Backend.GetAggregateComplianceDetailsByConfigRule(),
	}, nil
}

// GetAggregateConfigRuleComplianceSummary request/response types and handler.
type getAggregateConfigRuleComplianceSummaryOutput struct {
	AggregateComplianceCounts []any `json:"AggregateComplianceCounts"`
}

func (h *Handler) handleGetAggregateConfigRuleComplianceSummary(
	_ context.Context, _ *emptyInput,
) (*getAggregateConfigRuleComplianceSummaryOutput, error) {
	return &getAggregateConfigRuleComplianceSummaryOutput{
		AggregateComplianceCounts: h.Backend.GetAggregateConfigRuleComplianceSummary(),
	}, nil
}

// GetAggregateConformancePackComplianceSummary request/response types and handler.
type getAggregateConformancePackComplianceSummaryOutput struct {
	AggregateConformancePackComplianceSummaries []any `json:"AggregateConformancePackComplianceSummaries"`
}

func (h *Handler) handleGetAggregateConformancePackComplianceSummary(
	_ context.Context, _ *emptyInput,
) (*getAggregateConformancePackComplianceSummaryOutput, error) {
	return &getAggregateConformancePackComplianceSummaryOutput{
		AggregateConformancePackComplianceSummaries: h.Backend.GetAggregateConformancePackComplianceSummary(),
	}, nil
}

// GetAggregateDiscoveredResourceCounts request/response types and handler.
type getAggregateDiscoveredResourceCountsOutput struct {
	TotalDiscoveredResources int32 `json:"TotalDiscoveredResources"`
}

func (h *Handler) handleGetAggregateDiscoveredResourceCounts(
	_ context.Context, _ *emptyInput,
) (*getAggregateDiscoveredResourceCountsOutput, error) {
	return &getAggregateDiscoveredResourceCountsOutput{
		TotalDiscoveredResources: h.Backend.GetAggregateDiscoveredResourceCounts(),
	}, nil
}

// GetAggregateResourceConfig request/response types and handler.
type getAggregateResourceConfigOutput struct {
	ConfigurationItem *BaseConfigurationItem `json:"ConfigurationItem"`
}

func (h *Handler) handleGetAggregateResourceConfig(
	_ context.Context, _ *emptyInput,
) (*getAggregateResourceConfigOutput, error) {
	return &getAggregateResourceConfigOutput{
		ConfigurationItem: h.Backend.GetAggregateResourceConfig(),
	}, nil
}

// GetComplianceDetailsByResource request/response types and handler.
type getComplianceDetailsByResourceOutput struct {
	EvaluationResults []any `json:"EvaluationResults"`
}

func (h *Handler) handleGetComplianceDetailsByResource(
	_ context.Context, _ *emptyInput,
) (*getComplianceDetailsByResourceOutput, error) {
	return &getComplianceDetailsByResourceOutput{EvaluationResults: []any{}}, nil
}

// GetComplianceSummaryByConfigRule request/response types and handler.
type getComplianceSummaryByConfigRuleOutput struct {
	ComplianceSummariesByConfigRule []ComplianceSummary `json:"ComplianceSummariesByConfigRule"`
}

func (h *Handler) handleGetComplianceSummaryByConfigRule(
	_ context.Context, _ *emptyInput,
) (*getComplianceSummaryByConfigRuleOutput, error) {
	return &getComplianceSummaryByConfigRuleOutput{
		ComplianceSummariesByConfigRule: h.Backend.GetComplianceSummaryByConfigRule(),
	}, nil
}

// GetComplianceSummaryByResourceType request/response types and handler.
type getComplianceSummaryByResourceTypeOutput struct {
	ComplianceSummariesByResourceType []any `json:"ComplianceSummariesByResourceType"`
}

func (h *Handler) handleGetComplianceSummaryByResourceType(
	_ context.Context, _ *emptyInput,
) (*getComplianceSummaryByResourceTypeOutput, error) {
	return &getComplianceSummaryByResourceTypeOutput{
		ComplianceSummariesByResourceType: h.Backend.GetComplianceSummaryByResourceType(),
	}, nil
}

// GetConformancePackComplianceDetails request/response types and handler.
type getConformancePackComplianceDetailsOutput struct {
	ConformancePackRuleEvaluationResults []any `json:"ConformancePackRuleEvaluationResults"`
}

func (h *Handler) handleGetConformancePackComplianceDetails(
	_ context.Context, _ *emptyInput,
) (*getConformancePackComplianceDetailsOutput, error) {
	return &getConformancePackComplianceDetailsOutput{
		ConformancePackRuleEvaluationResults: h.Backend.GetConformancePackComplianceDetails(),
	}, nil
}

// GetConformancePackComplianceSummary request/response types and handler.
type getConformancePackComplianceSummaryOutput struct {
	ConformancePackComplianceSummaryList []any `json:"ConformancePackComplianceSummaryList"`
}

func (h *Handler) handleGetConformancePackComplianceSummary(
	_ context.Context, _ *emptyInput,
) (*getConformancePackComplianceSummaryOutput, error) {
	return &getConformancePackComplianceSummaryOutput{
		ConformancePackComplianceSummaryList: h.Backend.GetConformancePackComplianceSummary(),
	}, nil
}

// GetCustomRulePolicy request/response types and handler.
type getCustomRulePolicyInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
}
type getCustomRulePolicyOutput struct {
	PolicyText string `json:"PolicyText"`
}

func (h *Handler) handleGetCustomRulePolicy(
	_ context.Context, in *getCustomRulePolicyInput,
) (*getCustomRulePolicyOutput, error) {
	return &getCustomRulePolicyOutput{
		PolicyText: h.Backend.GetCustomRulePolicy(in.ConfigRuleName),
	}, nil
}

// GetDiscoveredResourceCounts request/response types and handler.
type getDiscoveredResourceCountsOutput struct {
	TotalDiscoveredResources int64 `json:"TotalDiscoveredResources"`
}

func (h *Handler) handleGetDiscoveredResourceCounts(
	_ context.Context, _ *emptyInput,
) (*getDiscoveredResourceCountsOutput, error) {
	return &getDiscoveredResourceCountsOutput{
		TotalDiscoveredResources: h.Backend.GetDiscoveredResourceCounts(),
	}, nil
}

// GetOrganizationConfigRuleDetailedStatus request/response types and handler.
type getOrganizationConfigRuleDetailedStatusOutput struct {
	OrganizationConfigRuleDetailedStatus []any `json:"OrganizationConfigRuleDetailedStatus"`
}

func (h *Handler) handleGetOrganizationConfigRuleDetailedStatus(
	_ context.Context, _ *emptyInput,
) (*getOrganizationConfigRuleDetailedStatusOutput, error) {
	return &getOrganizationConfigRuleDetailedStatusOutput{
		OrganizationConfigRuleDetailedStatus: h.Backend.GetOrganizationConfigRuleDetailedStatus(),
	}, nil
}

// GetOrganizationConformancePackDetailedStatus request/response types and handler.
type getOrganizationConformancePackDetailedStatusOutput struct {
	OrganizationConformancePackDetailedStatuses []any `json:"OrganizationConformancePackDetailedStatuses"`
}

func (h *Handler) handleGetOrganizationConformancePackDetailedStatus(
	_ context.Context, _ *emptyInput,
) (*getOrganizationConformancePackDetailedStatusOutput, error) {
	return &getOrganizationConformancePackDetailedStatusOutput{
		OrganizationConformancePackDetailedStatuses: h.Backend.GetOrganizationConformancePackDetailedStatus(),
	}, nil
}

// GetOrganizationCustomRulePolicy request/response types and handler.
type getOrganizationCustomRulePolicyInput struct {
	OrganizationConfigRuleName string `json:"OrganizationConfigRuleName"`
}
type getOrganizationCustomRulePolicyOutput struct {
	PolicyText string `json:"PolicyText"`
}

func (h *Handler) handleGetOrganizationCustomRulePolicy(
	_ context.Context, in *getOrganizationCustomRulePolicyInput,
) (*getOrganizationCustomRulePolicyOutput, error) {
	return &getOrganizationCustomRulePolicyOutput{
		PolicyText: h.Backend.GetOrganizationCustomRulePolicy(in.OrganizationConfigRuleName),
	}, nil
}

// GetResourceConfigHistory request/response types and handler.
type getResourceConfigHistoryInput struct {
	ResourceType string `json:"resourceType"`
	ResourceID   string `json:"resourceId"`
}
type getResourceConfigHistoryOutput struct {
	ConfigurationItems []ResourceConfigItem `json:"ConfigurationItems"`
}

func (h *Handler) handleGetResourceConfigHistory(
	_ context.Context, in *getResourceConfigHistoryInput,
) (*getResourceConfigHistoryOutput, error) {
	return &getResourceConfigHistoryOutput{
		ConfigurationItems: h.Backend.GetResourceConfigHistory(in.ResourceType, in.ResourceID),
	}, nil
}

// GetResourceEvaluationSummary request/response types and handler.
type getResourceEvaluationSummaryOutput struct {
	ResourceEvaluationSummary *BaseConfigurationItem `json:"ResourceEvaluationSummary"`
}

func (h *Handler) handleGetResourceEvaluationSummary(
	_ context.Context, _ *emptyInput,
) (*getResourceEvaluationSummaryOutput, error) {
	return &getResourceEvaluationSummaryOutput{
		ResourceEvaluationSummary: h.Backend.GetResourceEvaluationSummary(),
	}, nil
}

// GetStoredQuery request/response types and handler.
type getStoredQueryInput struct {
	QueryName string `json:"QueryName"`
}
type getStoredQueryOutput struct {
	StoredQuery *StoredQuery `json:"StoredQuery"`
}

func (h *Handler) handleGetStoredQuery(
	_ context.Context, in *getStoredQueryInput,
) (*getStoredQueryOutput, error) {
	return &getStoredQueryOutput{
		StoredQuery: h.Backend.GetStoredQuery(in.QueryName),
	}, nil
}

// ListAggregateDiscoveredResources request/response types and handler.
type listAggregateDiscoveredResourcesOutput struct {
	ResourceIdentifiers []any `json:"ResourceIdentifiers"`
}

func (h *Handler) handleListAggregateDiscoveredResources(
	_ context.Context, _ *emptyInput,
) (*listAggregateDiscoveredResourcesOutput, error) {
	return &listAggregateDiscoveredResourcesOutput{
		ResourceIdentifiers: h.Backend.ListAggregateDiscoveredResources(),
	}, nil
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

// ListConformancePackComplianceScores request/response types and handler.
type listConformancePackComplianceScoresOutput struct {
	ConformancePackComplianceScores []any `json:"ConformancePackComplianceScores"`
}

func (h *Handler) handleListConformancePackComplianceScores(
	_ context.Context, _ *emptyInput,
) (*listConformancePackComplianceScoresOutput, error) {
	return &listConformancePackComplianceScoresOutput{
		ConformancePackComplianceScores: h.Backend.ListConformancePackComplianceScores(),
	}, nil
}

// ListDiscoveredResources request/response types and handler.
type listDiscoveredResourcesInput struct {
	ResourceType string `json:"resourceType"`
}
type listDiscoveredResourcesOutput struct {
	ResourceIdentifiers []ResourceConfigItem `json:"ResourceIdentifiers"`
}

func (h *Handler) handleListDiscoveredResources(
	_ context.Context, in *listDiscoveredResourcesInput,
) (*listDiscoveredResourcesOutput, error) {
	return &listDiscoveredResourcesOutput{
		ResourceIdentifiers: h.Backend.ListDiscoveredResources(in.ResourceType),
	}, nil
}

// ListResourceEvaluations request/response types and handler.
type listResourceEvaluationsOutput struct {
	ResourceEvaluations []any `json:"ResourceEvaluations"`
}

func (h *Handler) handleListResourceEvaluations(
	_ context.Context, _ *emptyInput,
) (*listResourceEvaluationsOutput, error) {
	return &listResourceEvaluationsOutput{
		ResourceEvaluations: h.Backend.ListResourceEvaluations(),
	}, nil
}

// ListStoredQueries request/response types and handler.
type listStoredQueriesOutput struct {
	StoredQueryMetadata []StoredQueryMetadata `json:"StoredQueryMetadata"`
}

func (h *Handler) handleListStoredQueries(
	_ context.Context, _ *emptyInput,
) (*listStoredQueriesOutput, error) {
	return &listStoredQueriesOutput{
		StoredQueryMetadata: h.Backend.ListStoredQueries(),
	}, nil
}

// ListTagsForResource request/response types and handler.
type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}
type listTagsForResourceOutput struct {
	Tags []Tag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context, in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	return &listTagsForResourceOutput{
		Tags: h.Backend.ListTagsForResource(in.ResourceArn),
	}, nil
}

// PutAggregationAuthorization request/response types and handler.
type putAggregationAuthorizationInput struct {
	AuthorizedAccountID string `json:"AuthorizedAccountId"`
	AuthorizedAwsRegion string `json:"AuthorizedAwsRegion"`
}

func (h *Handler) handlePutAggregationAuthorization(
	_ context.Context, in *putAggregationAuthorizationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutAggregationAuthorization(
		in.AuthorizedAccountID,
		in.AuthorizedAwsRegion,
	)
}

// PutConfigRule request/response types and handler.
type putConfigRuleSourceBody struct {
	Owner            string `json:"Owner"`
	SourceIdentifier string `json:"SourceIdentifier"`
}

type putConfigRuleScopeBody struct {
	ComplianceResourceID    string   `json:"ComplianceResourceId,omitempty"`
	TagKey                  string   `json:"TagKey,omitempty"`
	TagValue                string   `json:"TagValue,omitempty"`
	ComplianceResourceTypes []string `json:"ComplianceResourceTypes,omitempty"`
}

type putConfigRuleBody struct {
	Source                    *putConfigRuleSourceBody `json:"Source,omitempty"`
	Scope                     *putConfigRuleScopeBody  `json:"Scope,omitempty"`
	ConfigRuleName            string                   `json:"ConfigRuleName"`
	ConfigRuleState           string                   `json:"ConfigRuleState,omitempty"`
	Description               string                   `json:"Description,omitempty"`
	InputParameters           string                   `json:"InputParameters,omitempty"`
	MaximumExecutionFrequency string                   `json:"MaximumExecutionFrequency,omitempty"`
}

type putConfigRuleInput struct {
	ConfigRule putConfigRuleBody `json:"ConfigRule"`
}

func (h *Handler) handlePutConfigRule(
	_ context.Context, in *putConfigRuleInput,
) (*emptyOutput, error) {
	rule := &ConfigRule{
		ConfigRuleName:            in.ConfigRule.ConfigRuleName,
		Description:               in.ConfigRule.Description,
		InputParameters:           in.ConfigRule.InputParameters,
		MaximumExecutionFrequency: in.ConfigRule.MaximumExecutionFrequency,
		ConfigRuleState:           in.ConfigRule.ConfigRuleState,
	}

	if in.ConfigRule.Source != nil {
		rule.Source = &ConfigRuleSource{
			Owner:            in.ConfigRule.Source.Owner,
			SourceIdentifier: in.ConfigRule.Source.SourceIdentifier,
		}
	}

	if in.ConfigRule.Scope != nil {
		rule.Scope = &ConfigRuleScope{
			ComplianceResourceTypes: in.ConfigRule.Scope.ComplianceResourceTypes,
			ComplianceResourceID:    in.ConfigRule.Scope.ComplianceResourceID,
			TagKey:                  in.ConfigRule.Scope.TagKey,
			TagValue:                in.ConfigRule.Scope.TagValue,
		}
	}

	return &emptyOutput{}, h.Backend.PutConfigRule(rule)
}

// PutConfigurationAggregator request/response types and handler.
type putConfigurationAggregatorInput struct {
	OrganizationAggregationSource *OrganizationAggregationSource `json:"OrganizationAggregationSource,omitempty"`
	ConfigurationAggregatorName   string                         `json:"ConfigurationAggregatorName"`
	AccountAggregationSources     []AccountAggregationSource     `json:"AccountAggregationSources,omitempty"`
}

func (h *Handler) handlePutConfigurationAggregator(
	_ context.Context, in *putConfigurationAggregatorInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutConfigurationAggregator(
		in.ConfigurationAggregatorName,
		in.AccountAggregationSources,
		in.OrganizationAggregationSource,
	)
}

// PutConformancePack request/response types and handler.
type putConformancePackInput struct {
	ConformancePackName string `json:"ConformancePackName"`
	DeliveryS3Bucket    string `json:"DeliveryS3Bucket,omitempty"`
	DeliveryS3KeyPrefix string `json:"DeliveryS3KeyPrefix,omitempty"`
}

func (h *Handler) handlePutConformancePack(
	_ context.Context, in *putConformancePackInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutConformancePack(
		in.ConformancePackName,
		in.DeliveryS3Bucket,
		in.DeliveryS3KeyPrefix,
	)
}

// PutEvaluations request/response types and handler.
type putEvaluationsInput struct {
	Evaluations []EvaluationResult `json:"Evaluations"`
}

func (h *Handler) handlePutEvaluations(
	_ context.Context, in *putEvaluationsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutEvaluations(in.Evaluations)
}

// PutExternalEvaluation request/response types and handler.
type putExternalEvaluationInput struct {
	ExternalEvaluation EvaluationResult `json:"ExternalEvaluation"`
}

func (h *Handler) handlePutExternalEvaluation(
	_ context.Context, in *putExternalEvaluationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutExternalEvaluation(in.ExternalEvaluation)
}

// PutOrganizationConfigRule request/response types and handler.
type putOrganizationConfigRuleInput struct {
	OrganizationConfigRuleName string `json:"OrganizationConfigRuleName"`
}

func (h *Handler) handlePutOrganizationConfigRule(
	_ context.Context, in *putOrganizationConfigRuleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutOrganizationConfigRule(in.OrganizationConfigRuleName)
}

// PutOrganizationConformancePack request/response types and handler.
type putOrganizationConformancePackInput struct {
	OrganizationConformancePackName string `json:"OrganizationConformancePackName"`
}

func (h *Handler) handlePutOrganizationConformancePack(
	_ context.Context, in *putOrganizationConformancePackInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutOrganizationConformancePack(
		in.OrganizationConformancePackName,
	)
}

// PutRemediationConfigurations request/response types and handler.
type putRemediationConfigurationsInput struct {
	RemediationConfigurations []RemediationConfiguration `json:"RemediationConfigurations"`
}

func (h *Handler) handlePutRemediationConfigurations(
	_ context.Context, in *putRemediationConfigurationsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutRemediationConfigurations(in.RemediationConfigurations)
}

// PutRemediationExceptions request/response types and handler.
type putRemediationExceptionsInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
	ResourceType   string `json:"ResourceType"`
	ResourceID     string `json:"ResourceId"`
}

func (h *Handler) handlePutRemediationExceptions(
	_ context.Context, in *putRemediationExceptionsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutRemediationExceptions(in.ConfigRuleName, in.ResourceType, in.ResourceID)
}

// PutResourceConfig request/response types and handler.
type putResourceConfigInput struct {
	ResourceType  string `json:"ResourceType"`
	ResourceID    string `json:"ResourceId"`
	Configuration string `json:"Configuration"`
}

func (h *Handler) handlePutResourceConfig(
	_ context.Context, in *putResourceConfigInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutResourceConfig(in.ResourceType, in.ResourceID, in.Configuration)
}

// PutRetentionConfiguration request/response types and handler.
type putRetentionConfigurationInput struct {
	RetentionConfigurationName string `json:"RetentionConfigurationName"`
	RetentionPeriodInDays      int32  `json:"RetentionPeriodInDays"`
}

func (h *Handler) handlePutRetentionConfiguration(
	_ context.Context, in *putRetentionConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutRetentionConfiguration(in.RetentionConfigurationName, in.RetentionPeriodInDays)
}

// PutServiceLinkedConfigurationRecorder request/response types and handler.
func (h *Handler) handlePutServiceLinkedConfigurationRecorder(
	_ context.Context, _ *emptyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutServiceLinkedConfigurationRecorder()
}

// PutStoredQuery request/response types and handler.
type putStoredQueryBody struct {
	QueryName string `json:"QueryName"`
}
type putStoredQueryInput struct {
	StoredQuery putStoredQueryBody `json:"StoredQuery"`
}

func (h *Handler) handlePutStoredQuery(
	_ context.Context, in *putStoredQueryInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutStoredQuery(in.StoredQuery.QueryName)
}

// SelectAggregateResourceConfig request/response types and handler.
type selectAggregateResourceConfigOutput struct {
	Results []any `json:"Results"`
}

func (h *Handler) handleSelectAggregateResourceConfig(
	_ context.Context, _ *emptyInput,
) (*selectAggregateResourceConfigOutput, error) {
	return &selectAggregateResourceConfigOutput{
		Results: h.Backend.SelectAggregateResourceConfig(),
	}, nil
}

// SelectResourceConfig request/response types and handler.
type selectResourceConfigOutput struct {
	Results []any `json:"Results"`
}

func (h *Handler) handleSelectResourceConfig(
	_ context.Context, _ *emptyInput,
) (*selectResourceConfigOutput, error) {
	return &selectResourceConfigOutput{
		Results: h.Backend.SelectResourceConfig(),
	}, nil
}

// StartConfigRulesEvaluation request/response types and handler.
func (h *Handler) handleStartConfigRulesEvaluation(
	_ context.Context, _ *emptyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StartConfigRulesEvaluation()
}

// StartRemediationExecution request/response types and handler.
func (h *Handler) handleStartRemediationExecution(
	_ context.Context, _ *emptyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StartRemediationExecution()
}

// StartResourceEvaluation request/response types and handler.
type startResourceEvaluationOutput struct {
	ResourceEvaluationID string `json:"ResourceEvaluationId"`
}

func (h *Handler) handleStartResourceEvaluation(
	_ context.Context, _ *emptyInput,
) (*startResourceEvaluationOutput, error) {
	return &startResourceEvaluationOutput{
		ResourceEvaluationID: h.Backend.StartResourceEvaluation(),
	}, nil
}

// TagResource request/response types and handler.
type tagResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []Tag  `json:"Tags"`
}

func (h *Handler) handleTagResource(
	_ context.Context, in *tagResourceInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.TagResource(in.ResourceArn, in.Tags)
}

// UntagResource request/response types and handler.
type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	_ context.Context, in *untagResourceInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UntagResource(in.ResourceArn, in.TagKeys)
}

// buildExtendedDispatchTable returns dispatch entries for all extended ops.
func (h *Handler) buildExtendedDispatchTable() map[string]service.JSONOpFunc {
	m := h.buildDeleteDeliverDispatch()
	maps.Copy(m, h.buildDescribeDispatch())
	maps.Copy(m, h.buildGetDispatch())
	maps.Copy(m, h.buildListPutSelectDispatch())

	return m
}

// buildDeleteDeliverDispatch returns dispatch entries for Delete/Deliver ops.
func (h *Handler) buildDeleteDeliverDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDeletePendingAggregationRequest: service.WrapOp(
			h.handleDeletePendingAggregationRequest,
		),
		opDeleteRemediationConfiguration: service.WrapOp(
			h.handleDeleteRemediationConfiguration,
		),
		opDeleteRemediationExceptions: service.WrapOp(
			h.handleDeleteRemediationExceptions,
		),
		opDeleteResourceConfig: service.WrapOp(h.handleDeleteResourceConfig),
		opDeleteRetentionConfiguration: service.WrapOp(
			h.handleDeleteRetentionConfiguration,
		),
		opDeleteServiceLinkedConfigurationRecorder: service.WrapOp(
			h.handleDeleteServiceLinkedConfigurationRecorder,
		),
		opDeleteStoredQuery:     service.WrapOp(h.handleDeleteStoredQuery),
		opDeliverConfigSnapshot: service.WrapOp(h.handleDeliverConfigSnapshot),
		opDisassociateResourceTypes: service.WrapOp(
			h.handleDisassociateResourceTypes,
		),
	}
}

// buildDescribeDispatch returns dispatch entries for Describe ops.
func (h *Handler) buildDescribeDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDescribeAggregateComplianceByConfigRules: service.WrapOp(
			h.handleDescribeAggregateComplianceByConfigRules,
		),
		opDescribeAggregateComplianceByConformancePacks: service.WrapOp(
			h.handleDescribeAggregateComplianceByConformancePacks,
		),
		opDescribeAggregationAuthorizations: service.WrapOp(
			h.handleDescribeAggregationAuthorizations,
		),
		opDescribeComplianceByConfigRule: service.WrapOp(
			h.handleDescribeComplianceByConfigRule,
		),
		opDescribeComplianceByResource: service.WrapOp(
			h.handleDescribeComplianceByResource,
		),
		opDescribeConfigRuleEvaluationStatus: service.WrapOp(
			h.handleDescribeConfigRuleEvaluationStatus,
		),
		opDescribeConfigurationAggregatorSourcesStatus: service.WrapOp(
			h.handleDescribeConfigurationAggregatorSourcesStatus,
		),
		opDescribeConfigurationAggregators: service.WrapOp(
			h.handleDescribeConfigurationAggregators,
		),
		opDescribeConformancePackCompliance: service.WrapOp(
			h.handleDescribeConformancePackCompliance,
		),
		opDescribeConformancePackStatus: service.WrapOp(
			h.handleDescribeConformancePackStatus,
		),
		opDescribeConformancePacks: service.WrapOp(
			h.handleDescribeConformancePacks,
		),
		opDescribeDeliveryChannelStatus: service.WrapOp(
			h.handleDescribeDeliveryChannelStatus,
		),
		opDescribeOrganizationConfigRuleStatuses: service.WrapOp(
			h.handleDescribeOrganizationConfigRuleStatuses,
		),
		opDescribeOrganizationConfigRules: service.WrapOp(
			h.handleDescribeOrganizationConfigRules,
		),
		opDescribeOrganizationConformancePackStatuses: service.WrapOp(
			h.handleDescribeOrganizationConformancePackStatuses,
		),
		opDescribeOrganizationConformancePacks: service.WrapOp(
			h.handleDescribeOrganizationConformancePacks,
		),
		opDescribePendingAggregationRequests: service.WrapOp(
			h.handleDescribePendingAggregationRequests,
		),
		opDescribeRemediationConfigurations: service.WrapOp(
			h.handleDescribeRemediationConfigurations,
		),
		opDescribeRemediationExceptions: service.WrapOp(
			h.handleDescribeRemediationExceptions,
		),
		opDescribeRemediationExecutionStatus: service.WrapOp(
			h.handleDescribeRemediationExecutionStatus,
		),
		opDescribeRetentionConfigurations: service.WrapOp(
			h.handleDescribeRetentionConfigurations,
		),
	}
}

// buildGetDispatch returns dispatch entries for Get ops.
func (h *Handler) buildGetDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opGetAggregateComplianceDetailsByConfigRule: service.WrapOp(
			h.handleGetAggregateComplianceDetailsByConfigRule,
		),
		opGetAggregateConfigRuleComplianceSummary: service.WrapOp(
			h.handleGetAggregateConfigRuleComplianceSummary,
		),
		opGetAggregateConformancePackComplianceSummary: service.WrapOp(
			h.handleGetAggregateConformancePackComplianceSummary,
		),
		opGetAggregateDiscoveredResourceCounts: service.WrapOp(
			h.handleGetAggregateDiscoveredResourceCounts,
		),
		opGetAggregateResourceConfig: service.WrapOp(
			h.handleGetAggregateResourceConfig,
		),
		opGetComplianceDetailsByResource: service.WrapOp(
			h.handleGetComplianceDetailsByResource,
		),
		opGetComplianceSummaryByConfigRule: service.WrapOp(
			h.handleGetComplianceSummaryByConfigRule,
		),
		opGetComplianceSummaryByResourceType: service.WrapOp(
			h.handleGetComplianceSummaryByResourceType,
		),
		opGetConformancePackComplianceDetails: service.WrapOp(
			h.handleGetConformancePackComplianceDetails,
		),
		opGetConformancePackComplianceSummary: service.WrapOp(
			h.handleGetConformancePackComplianceSummary,
		),
		opGetCustomRulePolicy: service.WrapOp(h.handleGetCustomRulePolicy),
		opGetDiscoveredResourceCounts: service.WrapOp(
			h.handleGetDiscoveredResourceCounts,
		),
		opGetOrganizationConfigRuleDetailedStatus: service.WrapOp(
			h.handleGetOrganizationConfigRuleDetailedStatus,
		),
		opGetOrganizationConformancePackDetailedStatus: service.WrapOp(
			h.handleGetOrganizationConformancePackDetailedStatus,
		),
		opGetOrganizationCustomRulePolicy: service.WrapOp(
			h.handleGetOrganizationCustomRulePolicy,
		),
		opGetResourceConfigHistory: service.WrapOp(
			h.handleGetResourceConfigHistory,
		),
		opGetResourceEvaluationSummary: service.WrapOp(
			h.handleGetResourceEvaluationSummary,
		),
		opGetStoredQuery: service.WrapOp(h.handleGetStoredQuery),
	}
}

// buildListPutSelectDispatch returns dispatch entries for List, Put, Select, Start, Tag, and Untag ops.
func (h *Handler) buildListPutSelectDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opListAggregateDiscoveredResources: service.WrapOp(
			h.handleListAggregateDiscoveredResources,
		),
		opListConfigurationRecorders: service.WrapOp(h.handleListConfigurationRecorders),
		opListConformancePackComplianceScores: service.WrapOp(
			h.handleListConformancePackComplianceScores,
		),
		opListDiscoveredResources: service.WrapOp(h.handleListDiscoveredResources),
		opListResourceEvaluations: service.WrapOp(h.handleListResourceEvaluations),
		opListStoredQueries:       service.WrapOp(h.handleListStoredQueries),
		opListTagsForResource:     service.WrapOp(h.handleListTagsForResource),
		opPutAggregationAuthorization: service.WrapOp(
			h.handlePutAggregationAuthorization,
		),
		opPutConfigRule:              service.WrapOp(h.handlePutConfigRule),
		opPutConfigurationAggregator: service.WrapOp(h.handlePutConfigurationAggregator),
		opPutConformancePack:         service.WrapOp(h.handlePutConformancePack),
		opPutEvaluations:             service.WrapOp(h.handlePutEvaluations),
		opPutExternalEvaluation:      service.WrapOp(h.handlePutExternalEvaluation),
		opPutOrganizationConfigRule:  service.WrapOp(h.handlePutOrganizationConfigRule),
		opPutOrganizationConformancePack: service.WrapOp(
			h.handlePutOrganizationConformancePack,
		),
		opPutRemediationConfigurations: service.WrapOp(
			h.handlePutRemediationConfigurations,
		),
		opPutRemediationExceptions:  service.WrapOp(h.handlePutRemediationExceptions),
		opPutResourceConfig:         service.WrapOp(h.handlePutResourceConfig),
		opPutRetentionConfiguration: service.WrapOp(h.handlePutRetentionConfiguration),
		opPutServiceLinkedConfigurationRecorder: service.WrapOp(
			h.handlePutServiceLinkedConfigurationRecorder,
		),
		opPutStoredQuery: service.WrapOp(h.handlePutStoredQuery),
		opSelectAggregateResourceConfig: service.WrapOp(
			h.handleSelectAggregateResourceConfig,
		),
		opSelectResourceConfig:       service.WrapOp(h.handleSelectResourceConfig),
		opStartConfigRulesEvaluation: service.WrapOp(h.handleStartConfigRulesEvaluation),
		opStartRemediationExecution:  service.WrapOp(h.handleStartRemediationExecution),
		opStartResourceEvaluation:    service.WrapOp(h.handleStartResourceEvaluation),
		opTagResource:                service.WrapOp(h.handleTagResource),
		opUntagResource:              service.WrapOp(h.handleUntagResource),
	}
}
