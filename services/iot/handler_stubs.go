package iot

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// handleStub returns 501 Not Implemented for stub operations.
func (h *Handler) handleStub(c *echo.Context, operation string) error {
	return c.String(http.StatusNotImplemented, operation+" not implemented")
}

// Stub operation name constants — remaining unimplemented ops.
const (
	opCancelCertificateTransfer             = "CancelCertificateTransfer"
	opCancelDetectMitigationActionsTask     = "CancelDetectMitigationActionsTask"
	opCancelJob                             = "CancelJob"
	opCancelJobExecution                    = "CancelJobExecution"
	opClearDefaultAuthorizer                = "ClearDefaultAuthorizer"
	opConfirmTopicRuleDestination           = "ConfirmTopicRuleDestination"
	opCreateAuditSuppression                = "CreateAuditSuppression"
	opCreateAuthorizer                      = "CreateAuthorizer"
	opCreateBillingGroup                    = "CreateBillingGroup"
	opCreateCommand                         = "CreateCommand"
	opCreateCustomMetric                    = "CreateCustomMetric"
	opCreateDimension                       = "CreateDimension"
	opCreateDomainConfiguration             = "CreateDomainConfiguration"
	opCreateDynamicThingGroup               = "CreateDynamicThingGroup"
	opCreateFleetMetric                     = "CreateFleetMetric"
	opCreateJob                             = "CreateJob"
	opCreateJobTemplate                     = "CreateJobTemplate"
	opCreateKeysAndCertificate              = "CreateKeysAndCertificate"
	opCreateMitigationAction                = "CreateMitigationAction"
	opCreateOTAUpdate                       = "CreateOTAUpdate"
	opCreatePackage                         = "CreatePackage"
	opCreatePackageVersion                  = "CreatePackageVersion"
	opCreateProvisioningClaim               = "CreateProvisioningClaim"
	opCreateProvisioningTemplate            = "CreateProvisioningTemplate"
	opCreateProvisioningTemplateVersion     = "CreateProvisioningTemplateVersion"
	opCreateRoleAlias                       = "CreateRoleAlias"
	opCreateScheduledAudit                  = "CreateScheduledAudit"
	opCreateSecurityProfile                 = "CreateSecurityProfile"
	opCreateStream                          = "CreateStream"
	opDeleteAccountAuditConfiguration       = "DeleteAccountAuditConfiguration"
	opDeleteAuditSuppression                = "DeleteAuditSuppression"
	opDeleteAuthorizer                      = "DeleteAuthorizer"
	opDeleteBillingGroup                    = "DeleteBillingGroup"
	opDeleteCACertificate                   = "DeleteCACertificate"
	opDeleteCommand                         = "DeleteCommand"
	opDeleteCommandExecution                = "DeleteCommandExecution"
	opDeleteCustomMetric                    = "DeleteCustomMetric"
	opDeleteDimension                       = "DeleteDimension"
	opDeleteDomainConfiguration             = "DeleteDomainConfiguration"
	opDeleteDynamicThingGroup               = "DeleteDynamicThingGroup"
	opDeleteFleetMetric                     = "DeleteFleetMetric"
	opDeleteJob                             = "DeleteJob"
	opDeleteJobExecution                    = "DeleteJobExecution"
	opDeleteJobTemplate                     = "DeleteJobTemplate"
	opDeleteMitigationAction                = "DeleteMitigationAction"
	opDeleteOTAUpdate                       = "DeleteOTAUpdate"
	opDeletePackage                         = "DeletePackage"
	opDeletePackageVersion                  = "DeletePackageVersion"
	opDeleteProvisioningTemplate            = "DeleteProvisioningTemplate"
	opDeleteProvisioningTemplateVersion     = "DeleteProvisioningTemplateVersion"
	opDeleteRegistrationCode                = "DeleteRegistrationCode"
	opDeleteRoleAlias                       = "DeleteRoleAlias"
	opDeleteScheduledAudit                  = "DeleteScheduledAudit"
	opDeleteSecurityProfile                 = "DeleteSecurityProfile"
	opDeleteStream                          = "DeleteStream"
	opDeleteV2LoggingLevel                  = "DeleteV2LoggingLevel"
	opDescribeAccountAuditConfiguration     = "DescribeAccountAuditConfiguration"
	opDescribeAuditFinding                  = "DescribeAuditFinding"
	opDescribeAuditMitigationActionsTask    = "DescribeAuditMitigationActionsTask"
	opDescribeAuditSuppression              = "DescribeAuditSuppression"
	opDescribeAuditTask                     = "DescribeAuditTask"
	opDescribeAuthorizer                    = "DescribeAuthorizer"
	opDescribeBillingGroup                  = "DescribeBillingGroup"
	opDescribeCACertificate                 = "DescribeCACertificate"
	opDescribeCustomMetric                  = "DescribeCustomMetric"
	opDescribeDefaultAuthorizer             = "DescribeDefaultAuthorizer"
	opDescribeDetectMitigationActionsTask   = "DescribeDetectMitigationActionsTask"
	opDescribeDimension                     = "DescribeDimension"
	opDescribeDomainConfiguration           = "DescribeDomainConfiguration"
	opDescribeEncryptionConfiguration       = "DescribeEncryptionConfiguration"
	opDescribeEventConfigurations           = "DescribeEventConfigurations"
	opDescribeFleetMetric                   = "DescribeFleetMetric"
	opDescribeIndex                         = "DescribeIndex"
	opDescribeJob                           = "DescribeJob"
	opDescribeJobExecution                  = "DescribeJobExecution"
	opDescribeJobTemplate                   = "DescribeJobTemplate"
	opDescribeManagedJobTemplate            = "DescribeManagedJobTemplate"
	opDescribeMitigationAction              = "DescribeMitigationAction"
	opDescribeProvisioningTemplate          = "DescribeProvisioningTemplate"
	opDescribeProvisioningTemplateVersion   = "DescribeProvisioningTemplateVersion"
	opDescribeRoleAlias                     = "DescribeRoleAlias"
	opDescribeScheduledAudit                = "DescribeScheduledAudit"
	opDescribeSecurityProfile               = "DescribeSecurityProfile"
	opDescribeStream                        = "DescribeStream"
	opDescribeThingRegistrationTask         = "DescribeThingRegistrationTask"
	opDetachPrincipalPolicy                 = "DetachPrincipalPolicy"
	opDetachSecurityProfile                 = "DetachSecurityProfile"
	opDetachThingPrincipal                  = "DetachThingPrincipal"
	opDisassociateSbomFromPackageVersion    = "DisassociateSbomFromPackageVersion"
	opGetBehaviorModelTrainingSummaries     = "GetBehaviorModelTrainingSummaries"
	opGetBucketsAggregation                 = "GetBucketsAggregation"
	opGetCardinality                        = "GetCardinality"
	opGetCommand                            = "GetCommand"
	opGetCommandExecution                   = "GetCommandExecution"
	opGetEffectivePolicies                  = "GetEffectivePolicies"
	opGetIndexingConfiguration              = "GetIndexingConfiguration"
	opGetJobDocument                        = "GetJobDocument"
	opGetLoggingOptions                     = "GetLoggingOptions"
	opGetOTAUpdate                          = "GetOTAUpdate"
	opGetPackage                            = "GetPackage"
	opGetPackageConfiguration               = "GetPackageConfiguration"
	opGetPackageVersion                     = "GetPackageVersion"
	opGetPercentiles                        = "GetPercentiles"
	opGetRegistrationCode                   = "GetRegistrationCode"
	opGetStatistics                         = "GetStatistics"
	opGetThingConnectivityData              = "GetThingConnectivityData"
	opGetV2LoggingOptions                   = "GetV2LoggingOptions"
	opListActiveViolations                  = "ListActiveViolations"
	opListAuditFindings                     = "ListAuditFindings"
	opListAuditMitigationActionsExecutions  = "ListAuditMitigationActionsExecutions"
	opListAuditMitigationActionsTasks       = "ListAuditMitigationActionsTasks"
	opListAuditSuppressions                 = "ListAuditSuppressions"
	opListAuditTasks                        = "ListAuditTasks"
	opListAuthorizers                       = "ListAuthorizers"
	opListBillingGroups                     = "ListBillingGroups"
	opListCACertificates                    = "ListCACertificates"
	opListCertificatesByCA                  = "ListCertificatesByCA"
	opListCommandExecutions                 = "ListCommandExecutions"
	opListCommands                          = "ListCommands"
	opListCustomMetrics                     = "ListCustomMetrics"
	opListDetectMitigationActionsExecutions = "ListDetectMitigationActionsExecutions"
	opListDetectMitigationActionsTasks      = "ListDetectMitigationActionsTasks"
	opListDimensions                        = "ListDimensions"
	opListDomainConfigurations              = "ListDomainConfigurations"
	opListFleetMetrics                      = "ListFleetMetrics"
	opListIndices                           = "ListIndices"
	opListJobExecutionsForJob               = "ListJobExecutionsForJob"
	opListJobExecutionsForThing             = "ListJobExecutionsForThing"
	opListJobTemplates                      = "ListJobTemplates"
	opListJobs                              = "ListJobs"
	opListManagedJobTemplates               = "ListManagedJobTemplates"
	opListMetricValues                      = "ListMetricValues"
	opListMitigationActions                 = "ListMitigationActions"
	opListOTAUpdates                        = "ListOTAUpdates"
	opListOutgoingCertificates              = "ListOutgoingCertificates"
	opListPackageVersions                   = "ListPackageVersions"
	opListPackages                          = "ListPackages"
	opListPolicyPrincipals                  = "ListPolicyPrincipals"
	opListPrincipalPolicies                 = "ListPrincipalPolicies"
	opListPrincipalThings                   = "ListPrincipalThings"
	opListPrincipalThingsV2                 = "ListPrincipalThingsV2"
	opListProvisioningTemplateVersions      = "ListProvisioningTemplateVersions"
	opListProvisioningTemplates             = "ListProvisioningTemplates"
	opListRelatedResourcesForAuditFinding   = "ListRelatedResourcesForAuditFinding"
	opListRoleAliases                       = "ListRoleAliases"
	opListSbomValidationResults             = "ListSbomValidationResults"
	opListScheduledAudits                   = "ListScheduledAudits"
	opListSecurityProfiles                  = "ListSecurityProfiles"
	opListSecurityProfilesForTarget         = "ListSecurityProfilesForTarget"
	opListStreams                           = "ListStreams"
	opListTagsForResource                   = "ListTagsForResource"
	opListTargetsForPolicy                  = "ListTargetsForPolicy"
	opListTargetsForSecurityProfile         = "ListTargetsForSecurityProfile"
	opListThingGroupsForThing               = "ListThingGroupsForThing"
	opListThingPrincipalsV2                 = "ListThingPrincipalsV2"
	opListThingRegistrationTaskReports      = "ListThingRegistrationTaskReports"
	opListThingRegistrationTasks            = "ListThingRegistrationTasks"
	opListThingsInBillingGroup              = "ListThingsInBillingGroup"
	opListV2LoggingLevels                   = "ListV2LoggingLevels"
	opListViolationEvents                   = "ListViolationEvents"
	opPutVerificationStateOnViolation       = "PutVerificationStateOnViolation"
	opRegisterCACertificate                 = "RegisterCACertificate"
	opRegisterThing                         = "RegisterThing"
	opRejectCertificateTransfer             = "RejectCertificateTransfer"
	opRemoveThingFromBillingGroup           = "RemoveThingFromBillingGroup"
	opSearchIndex                           = "SearchIndex"
	opSetDefaultAuthorizer                  = "SetDefaultAuthorizer"
	opSetLoggingOptions                     = "SetLoggingOptions"
	opSetV2LoggingLevel                     = "SetV2LoggingLevel"
	opSetV2LoggingOptions                   = "SetV2LoggingOptions"
	opStartAuditMitigationActionsTask       = "StartAuditMitigationActionsTask"
	opStartDetectMitigationActionsTask      = "StartDetectMitigationActionsTask"
	opStartOnDemandAuditTask                = "StartOnDemandAuditTask"
	opStartThingRegistrationTask            = "StartThingRegistrationTask"
	opStopThingRegistrationTask             = "StopThingRegistrationTask"
	opTagResource                           = "TagResource"
	opTestAuthorization                     = "TestAuthorization"
	opTestInvokeAuthorizer                  = "TestInvokeAuthorizer"
	opTransferCertificate                   = "TransferCertificate"
	opUntagResource                         = "UntagResource"
	opUpdateAccountAuditConfiguration       = "UpdateAccountAuditConfiguration"
	opUpdateAuditSuppression                = "UpdateAuditSuppression"
	opUpdateAuthorizer                      = "UpdateAuthorizer"
	opUpdateBillingGroup                    = "UpdateBillingGroup"
	opUpdateCACertificate                   = "UpdateCACertificate"
	opUpdateCommand                         = "UpdateCommand"
	opUpdateCustomMetric                    = "UpdateCustomMetric"
	opUpdateDimension                       = "UpdateDimension"
	opUpdateDomainConfiguration             = "UpdateDomainConfiguration"
	opUpdateDynamicThingGroup               = "UpdateDynamicThingGroup"
	opUpdateEncryptionConfiguration         = "UpdateEncryptionConfiguration"
	opUpdateEventConfigurations             = "UpdateEventConfigurations"
	opUpdateFleetMetric                     = "UpdateFleetMetric"
	opUpdateIndexingConfiguration           = "UpdateIndexingConfiguration"
	opUpdateJob                             = "UpdateJob"
	opUpdateMitigationAction                = "UpdateMitigationAction"
	opUpdatePackage                         = "UpdatePackage"
	opUpdatePackageConfiguration            = "UpdatePackageConfiguration"
	opUpdatePackageVersion                  = "UpdatePackageVersion"
	opUpdateProvisioningTemplate            = "UpdateProvisioningTemplate"
	opUpdateRoleAlias                       = "UpdateRoleAlias"
	opUpdateScheduledAudit                  = "UpdateScheduledAudit"
	opUpdateSecurityProfile                 = "UpdateSecurityProfile"
	opUpdateStream                          = "UpdateStream"
	opUpdateThingGroupsForThing             = "UpdateThingGroupsForThing"
	opUpdateThingType                       = "UpdateThingType"
	opValidateSecurityProfileBehaviors      = "ValidateSecurityProfileBehaviors"
)

// allStubOps returns remaining stub operation names.
//
//nolint:funlen // large but mechanical list of SDK operation names
func allStubOps() []string {
	return []string{
		opCancelCertificateTransfer,
		opCancelDetectMitigationActionsTask,
		opClearDefaultAuthorizer,
		opConfirmTopicRuleDestination,
		opCreateAuditSuppression,
		opCreateCommand,
		opCreateCustomMetric,
		opCreateDimension,
		opCreateDynamicThingGroup,
		opCreateFleetMetric,
		opCreateKeysAndCertificate,
		opCreateOTAUpdate,
		opCreatePackage,
		opCreatePackageVersion,
		opCreateProvisioningClaim,
		opCreateStream,
		opDeleteAccountAuditConfiguration,
		opDeleteAuditSuppression,
		opDeleteCACertificate,
		opDeleteCommand,
		opDeleteCommandExecution,
		opDeleteCustomMetric,
		opDeleteDimension,
		opDeleteDynamicThingGroup,
		opDeleteFleetMetric,
		opDeleteOTAUpdate,
		opDeletePackage,
		opDeletePackageVersion,
		opDeleteRegistrationCode,
		opDeleteStream,
		opDeleteV2LoggingLevel,
		opDescribeAccountAuditConfiguration,
		opDescribeAuditFinding,
		opDescribeAuditMitigationActionsTask,
		opDescribeAuditSuppression,
		opDescribeAuditTask,
		opDescribeCACertificate,
		opDescribeCustomMetric,
		opDescribeDefaultAuthorizer,
		opDescribeDetectMitigationActionsTask,
		opDescribeDimension,
		opDescribeEncryptionConfiguration,
		opDescribeEventConfigurations,
		opDescribeFleetMetric,
		opDescribeIndex,
		opDescribeManagedJobTemplate,
		opDescribeProvisioningTemplateVersion,
		opDescribeStream,
		opDescribeThingRegistrationTask,
		opDetachPrincipalPolicy,
		opDetachSecurityProfile,
		opDetachThingPrincipal,
		opDisassociateSbomFromPackageVersion,
		opGetBehaviorModelTrainingSummaries,
		opGetBucketsAggregation,
		opGetCardinality,
		opGetCommand,
		opGetCommandExecution,
		opGetEffectivePolicies,
		opGetIndexingConfiguration,
		opGetLoggingOptions,
		opGetOTAUpdate,
		opGetPackage,
		opGetPackageConfiguration,
		opGetPackageVersion,
		opGetPercentiles,
		opGetRegistrationCode,
		opGetStatistics,
		opGetThingConnectivityData,
		opGetV2LoggingOptions,
		opListActiveViolations,
		opListAuditFindings,
		opListAuditMitigationActionsExecutions,
		opListAuditMitigationActionsTasks,
		opListAuditSuppressions,
		opListAuditTasks,
		opListCACertificates,
		opListCertificatesByCA,
		opListCommandExecutions,
		opListCommands,
		opListCustomMetrics,
		opListDetectMitigationActionsExecutions,
		opListDetectMitigationActionsTasks,
		opListDimensions,
		opListFleetMetrics,
		opListIndices,
		opListJobExecutionsForJob,
		opListJobExecutionsForThing,
		opListManagedJobTemplates,
		opListMetricValues,
		opListOTAUpdates,
		opListOutgoingCertificates,
		opListPackageVersions,
		opListPackages,
		opListPolicyPrincipals,
		opListPrincipalPolicies,
		opListPrincipalThings,
		opListPrincipalThingsV2,
		opListRelatedResourcesForAuditFinding,
		opListSbomValidationResults,
		opListSecurityProfilesForTarget,
		opListStreams,
		opListTagsForResource,
		opListTargetsForPolicy,
		opListTargetsForSecurityProfile,
		opListThingGroupsForThing,
		opListThingPrincipalsV2,
		opListThingRegistrationTaskReports,
		opListThingRegistrationTasks,
		opListThingsInBillingGroup,
		opListV2LoggingLevels,
		opListViolationEvents,
		opPutVerificationStateOnViolation,
		opRegisterCACertificate,
		opRegisterThing,
		opRejectCertificateTransfer,
		opRemoveThingFromBillingGroup,
		opSearchIndex,
		opSetDefaultAuthorizer,
		opSetLoggingOptions,
		opSetV2LoggingLevel,
		opSetV2LoggingOptions,
		opStartAuditMitigationActionsTask,
		opStartDetectMitigationActionsTask,
		opStartOnDemandAuditTask,
		opStartThingRegistrationTask,
		opStopThingRegistrationTask,
		opTagResource,
		opTestAuthorization,
		opTestInvokeAuthorizer,
		opTransferCertificate,
		opUntagResource,
		opUpdateAccountAuditConfiguration,
		opUpdateAuditSuppression,
		opUpdateCACertificate,
		opUpdateCommand,
		opUpdateCustomMetric,
		opUpdateDimension,
		opUpdateDynamicThingGroup,
		opUpdateEncryptionConfiguration,
		opUpdateEventConfigurations,
		opUpdateFleetMetric,
		opUpdateIndexingConfiguration,
		opUpdatePackage,
		opUpdatePackageConfiguration,
		opUpdatePackageVersion,
		opUpdateStream,
		opUpdateThingGroupsForThing,
		opUpdateThingType,
		opValidateSecurityProfileBehaviors,
	}
}

// dispatchStubOp returns (true, err) if op is a known stub operation.
//
//nolint:funlen // routes across many resource types
func (h *Handler) dispatchStubOp(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCancelCertificateTransfer,
		opCancelDetectMitigationActionsTask,
		opClearDefaultAuthorizer,
		opConfirmTopicRuleDestination,
		opCreateAuditSuppression,
		opCreateCommand,
		opCreateCustomMetric,
		opCreateDimension,
		opCreateDynamicThingGroup,
		opCreateFleetMetric,
		opCreateKeysAndCertificate,
		opCreateOTAUpdate,
		opCreatePackage,
		opCreatePackageVersion,
		opCreateProvisioningClaim,
		opCreateStream,
		opDeleteAccountAuditConfiguration,
		opDeleteAuditSuppression,
		opDeleteCACertificate,
		opDeleteCommand,
		opDeleteCommandExecution,
		opDeleteCustomMetric,
		opDeleteDimension,
		opDeleteDynamicThingGroup,
		opDeleteFleetMetric,
		opDeleteOTAUpdate,
		opDeletePackage,
		opDeletePackageVersion,
		opDeleteRegistrationCode,
		opDeleteStream,
		opDeleteV2LoggingLevel,
		opDescribeAccountAuditConfiguration,
		opDescribeAuditFinding,
		opDescribeAuditMitigationActionsTask,
		opDescribeAuditSuppression,
		opDescribeAuditTask,
		opDescribeCACertificate,
		opDescribeCustomMetric,
		opDescribeDefaultAuthorizer,
		opDescribeDetectMitigationActionsTask,
		opDescribeDimension,
		opDescribeEncryptionConfiguration,
		opDescribeEventConfigurations,
		opDescribeFleetMetric,
		opDescribeIndex,
		opDescribeManagedJobTemplate,
		opDescribeProvisioningTemplateVersion,
		opDescribeStream,
		opDescribeThingRegistrationTask,
		opDetachPrincipalPolicy,
		opDetachSecurityProfile,
		opDetachThingPrincipal,
		opDisassociateSbomFromPackageVersion,
		opGetBehaviorModelTrainingSummaries,
		opGetBucketsAggregation,
		opGetCardinality,
		opGetCommand,
		opGetCommandExecution,
		opGetEffectivePolicies,
		opGetIndexingConfiguration,
		opGetLoggingOptions,
		opGetOTAUpdate,
		opGetPackage,
		opGetPackageConfiguration,
		opGetPackageVersion,
		opGetPercentiles,
		opGetRegistrationCode,
		opGetStatistics,
		opGetThingConnectivityData,
		opGetV2LoggingOptions,
		opListActiveViolations,
		opListAuditFindings,
		opListAuditMitigationActionsExecutions,
		opListAuditMitigationActionsTasks,
		opListAuditSuppressions,
		opListAuditTasks,
		opListCACertificates,
		opListCertificatesByCA,
		opListCommandExecutions,
		opListCommands,
		opListCustomMetrics,
		opListDetectMitigationActionsExecutions,
		opListDetectMitigationActionsTasks,
		opListDimensions,
		opListFleetMetrics,
		opListIndices,
		opListJobExecutionsForJob,
		opListJobExecutionsForThing,
		opListManagedJobTemplates,
		opListMetricValues,
		opListOTAUpdates,
		opListOutgoingCertificates,
		opListPackageVersions,
		opListPackages,
		opListPolicyPrincipals,
		opListPrincipalPolicies,
		opListPrincipalThings,
		opListPrincipalThingsV2,
		opListRelatedResourcesForAuditFinding,
		opListSbomValidationResults,
		opListSecurityProfilesForTarget,
		opListStreams,
		opListTagsForResource,
		opListTargetsForPolicy,
		opListTargetsForSecurityProfile,
		opListThingGroupsForThing,
		opListThingPrincipalsV2,
		opListThingRegistrationTaskReports,
		opListThingRegistrationTasks,
		opListThingsInBillingGroup,
		opListV2LoggingLevels,
		opListViolationEvents,
		opPutVerificationStateOnViolation,
		opRegisterCACertificate,
		opRegisterThing,
		opRejectCertificateTransfer,
		opRemoveThingFromBillingGroup,
		opSearchIndex,
		opSetDefaultAuthorizer,
		opSetLoggingOptions,
		opSetV2LoggingLevel,
		opSetV2LoggingOptions,
		opStartAuditMitigationActionsTask,
		opStartDetectMitigationActionsTask,
		opStartOnDemandAuditTask,
		opStartThingRegistrationTask,
		opStopThingRegistrationTask,
		opTagResource,
		opTestAuthorization,
		opTestInvokeAuthorizer,
		opTransferCertificate,
		opUntagResource,
		opUpdateAccountAuditConfiguration,
		opUpdateAuditSuppression,
		opUpdateCACertificate,
		opUpdateCommand,
		opUpdateCustomMetric,
		opUpdateDimension,
		opUpdateDynamicThingGroup,
		opUpdateEncryptionConfiguration,
		opUpdateEventConfigurations,
		opUpdateFleetMetric,
		opUpdateIndexingConfiguration,
		opUpdatePackage,
		opUpdatePackageConfiguration,
		opUpdatePackageVersion,
		opUpdateStream,
		opUpdateThingGroupsForThing,
		opUpdateThingType,
		opValidateSecurityProfileBehaviors:
		return true, h.handleStub(c, op)
	}

	return false, nil
}
