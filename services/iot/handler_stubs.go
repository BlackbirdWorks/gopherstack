package iot

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// handleStub returns 501 Not Implemented for stub operations.
func (h *Handler) handleStub(c *echo.Context, operation string) error {
	return c.String(http.StatusNotImplemented, operation+" not implemented")
}

// Stub operation name constants — all 152 ops from sdk_completeness_test.go notImplemented list.
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
	opCreateCertificateFromCsr              = "CreateCertificateFromCsr"
	opCreateCertificateProvider             = "CreateCertificateProvider"
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
	opCreatePolicyVersion                   = "CreatePolicyVersion"
	opCreateProvisioningClaim               = "CreateProvisioningClaim"
	opCreateProvisioningTemplate            = "CreateProvisioningTemplate"
	opCreateProvisioningTemplateVersion     = "CreateProvisioningTemplateVersion"
	opCreateRoleAlias                       = "CreateRoleAlias"
	opCreateScheduledAudit                  = "CreateScheduledAudit"
	opCreateSecurityProfile                 = "CreateSecurityProfile"
	opCreateStream                          = "CreateStream"
	opCreateThingGroup                      = "CreateThingGroup"
	opCreateThingType                       = "CreateThingType"
	opCreateTopicRuleDestination            = "CreateTopicRuleDestination"
	opDeleteAccountAuditConfiguration       = "DeleteAccountAuditConfiguration"
	opDeleteAuditSuppression                = "DeleteAuditSuppression"
	opDeleteAuthorizer                      = "DeleteAuthorizer"
	opDeleteBillingGroup                    = "DeleteBillingGroup"
	opDeleteCACertificate                   = "DeleteCACertificate"
	opDeleteCertificate                     = "DeleteCertificate"
	opDeleteCertificateProvider             = "DeleteCertificateProvider"
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
	opDeletePolicyVersion                   = "DeletePolicyVersion"
	opDeleteProvisioningTemplate            = "DeleteProvisioningTemplate"
	opDeleteProvisioningTemplateVersion     = "DeleteProvisioningTemplateVersion"
	opDeleteRegistrationCode                = "DeleteRegistrationCode"
	opDeleteRoleAlias                       = "DeleteRoleAlias"
	opDeleteScheduledAudit                  = "DeleteScheduledAudit"
	opDeleteSecurityProfile                 = "DeleteSecurityProfile"
	opDeleteStream                          = "DeleteStream"
	opDeleteThingGroup                      = "DeleteThingGroup"
	opDeleteThingType                       = "DeleteThingType"
	opDeleteTopicRuleDestination            = "DeleteTopicRuleDestination"
	opDeleteV2LoggingLevel                  = "DeleteV2LoggingLevel"
	opDeprecateThingType                    = "DeprecateThingType"
	opDescribeAccountAuditConfiguration     = "DescribeAccountAuditConfiguration"
	opDescribeAuditFinding                  = "DescribeAuditFinding"
	opDescribeAuditMitigationActionsTask    = "DescribeAuditMitigationActionsTask"
	opDescribeAuditSuppression              = "DescribeAuditSuppression"
	opDescribeAuditTask                     = "DescribeAuditTask"
	opDescribeAuthorizer                    = "DescribeAuthorizer"
	opDescribeBillingGroup                  = "DescribeBillingGroup"
	opDescribeCACertificate                 = "DescribeCACertificate"
	opDescribeCertificate                   = "DescribeCertificate"
	opDescribeCertificateProvider           = "DescribeCertificateProvider"
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
	opDescribeThingGroup                    = "DescribeThingGroup"
	opDescribeThingRegistrationTask         = "DescribeThingRegistrationTask"
	opDescribeThingType                     = "DescribeThingType"
	opDetachPolicy                          = "DetachPolicy"
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
	opGetPolicyVersion                      = "GetPolicyVersion"
	opGetRegistrationCode                   = "GetRegistrationCode"
	opGetStatistics                         = "GetStatistics"
	opGetThingConnectivityData              = "GetThingConnectivityData"
	opGetTopicRuleDestination               = "GetTopicRuleDestination"
	opGetV2LoggingOptions                   = "GetV2LoggingOptions"
	opListActiveViolations                  = "ListActiveViolations"
	opListAttachedPolicies                  = "ListAttachedPolicies"
	opListAuditFindings                     = "ListAuditFindings"
	opListAuditMitigationActionsExecutions  = "ListAuditMitigationActionsExecutions"
	opListAuditMitigationActionsTasks       = "ListAuditMitigationActionsTasks"
	opListAuditSuppressions                 = "ListAuditSuppressions"
	opListAuditTasks                        = "ListAuditTasks"
	opListAuthorizers                       = "ListAuthorizers"
	opListBillingGroups                     = "ListBillingGroups"
	opListCACertificates                    = "ListCACertificates"
	opListCertificateProviders              = "ListCertificateProviders"
	opListCertificates                      = "ListCertificates"
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
	opListPolicyVersions                    = "ListPolicyVersions"
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
	opListThingGroups                       = "ListThingGroups"
	opListThingGroupsForThing               = "ListThingGroupsForThing"
	opListThingPrincipalsV2                 = "ListThingPrincipalsV2"
	opListThingRegistrationTaskReports      = "ListThingRegistrationTaskReports"
	opListThingRegistrationTasks            = "ListThingRegistrationTasks"
	opListThingTypes                        = "ListThingTypes"
	opListThingsInBillingGroup              = "ListThingsInBillingGroup"
	opListThingsInThingGroup                = "ListThingsInThingGroup"
	opListTopicRuleDestinations             = "ListTopicRuleDestinations"
	opListV2LoggingLevels                   = "ListV2LoggingLevels"
	opListViolationEvents                   = "ListViolationEvents"
	opPutVerificationStateOnViolation       = "PutVerificationStateOnViolation"
	opRegisterCACertificate                 = "RegisterCACertificate"
	opRegisterCertificate                   = "RegisterCertificate"
	opRegisterCertificateWithoutCA          = "RegisterCertificateWithoutCA"
	opRegisterThing                         = "RegisterThing"
	opRejectCertificateTransfer             = "RejectCertificateTransfer"
	opRemoveThingFromBillingGroup           = "RemoveThingFromBillingGroup"
	opRemoveThingFromThingGroup             = "RemoveThingFromThingGroup"
	opSearchIndex                           = "SearchIndex"
	opSetDefaultAuthorizer                  = "SetDefaultAuthorizer"
	opSetDefaultPolicyVersion               = "SetDefaultPolicyVersion"
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
	opUpdateCertificate                     = "UpdateCertificate"
	opUpdateCertificateProvider             = "UpdateCertificateProvider"
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
	opUpdateThingGroup                      = "UpdateThingGroup"
	opUpdateThingGroupsForThing             = "UpdateThingGroupsForThing"
	opUpdateThingType                       = "UpdateThingType"
	opUpdateTopicRuleDestination            = "UpdateTopicRuleDestination"
	opValidateSecurityProfileBehaviors      = "ValidateSecurityProfileBehaviors"
)

// allStubOps returns all stub operation names (152 missing SDK ops).
//
//nolint:funlen // large but mechanical list of SDK operation names
func allStubOps() []string {
	return []string{
		opCancelCertificateTransfer,
		opCancelDetectMitigationActionsTask,
		opCancelJob,
		opCancelJobExecution,
		opClearDefaultAuthorizer,
		opConfirmTopicRuleDestination,
		opCreateAuditSuppression,
		opCreateAuthorizer,
		opCreateBillingGroup,
		opCreateCertificateFromCsr,
		opCreateCertificateProvider,
		opCreateCommand,
		opCreateCustomMetric,
		opCreateDimension,
		opCreateDomainConfiguration,
		opCreateDynamicThingGroup,
		opCreateFleetMetric,
		opCreateJob,
		opCreateJobTemplate,
		opCreateKeysAndCertificate,
		opCreateMitigationAction,
		opCreateOTAUpdate,
		opCreatePackage,
		opCreatePackageVersion,
		opCreatePolicyVersion,
		opCreateProvisioningClaim,
		opCreateProvisioningTemplate,
		opCreateProvisioningTemplateVersion,
		opCreateRoleAlias,
		opCreateScheduledAudit,
		opCreateSecurityProfile,
		opCreateStream,
		opCreateThingGroup,
		opCreateThingType,
		opCreateTopicRuleDestination,
		opDeleteAccountAuditConfiguration,
		opDeleteAuditSuppression,
		opDeleteAuthorizer,
		opDeleteBillingGroup,
		opDeleteCACertificate,
		opDeleteCertificate,
		opDeleteCertificateProvider,
		opDeleteCommand,
		opDeleteCommandExecution,
		opDeleteCustomMetric,
		opDeleteDimension,
		opDeleteDomainConfiguration,
		opDeleteDynamicThingGroup,
		opDeleteFleetMetric,
		opDeleteJob,
		opDeleteJobExecution,
		opDeleteJobTemplate,
		opDeleteMitigationAction,
		opDeleteOTAUpdate,
		opDeletePackage,
		opDeletePackageVersion,
		opDeletePolicyVersion,
		opDeleteProvisioningTemplate,
		opDeleteProvisioningTemplateVersion,
		opDeleteRegistrationCode,
		opDeleteRoleAlias,
		opDeleteScheduledAudit,
		opDeleteSecurityProfile,
		opDeleteStream,
		opDeleteThingGroup,
		opDeleteThingType,
		opDeleteTopicRuleDestination,
		opDeleteV2LoggingLevel,
		opDeprecateThingType,
		opDescribeAccountAuditConfiguration,
		opDescribeAuditFinding,
		opDescribeAuditMitigationActionsTask,
		opDescribeAuditSuppression,
		opDescribeAuditTask,
		opDescribeAuthorizer,
		opDescribeBillingGroup,
		opDescribeCACertificate,
		opDescribeCertificate,
		opDescribeCertificateProvider,
		opDescribeCustomMetric,
		opDescribeDefaultAuthorizer,
		opDescribeDetectMitigationActionsTask,
		opDescribeDimension,
		opDescribeDomainConfiguration,
		opDescribeEncryptionConfiguration,
		opDescribeEventConfigurations,
		opDescribeFleetMetric,
		opDescribeIndex,
		opDescribeJob,
		opDescribeJobExecution,
		opDescribeJobTemplate,
		opDescribeManagedJobTemplate,
		opDescribeMitigationAction,
		opDescribeProvisioningTemplate,
		opDescribeProvisioningTemplateVersion,
		opDescribeRoleAlias,
		opDescribeScheduledAudit,
		opDescribeSecurityProfile,
		opDescribeStream,
		opDescribeThingGroup,
		opDescribeThingRegistrationTask,
		opDescribeThingType,
		opDetachPolicy,
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
		opGetJobDocument,
		opGetLoggingOptions,
		opGetOTAUpdate,
		opGetPackage,
		opGetPackageConfiguration,
		opGetPackageVersion,
		opGetPercentiles,
		opGetPolicyVersion,
		opGetRegistrationCode,
		opGetStatistics,
		opGetThingConnectivityData,
		opGetTopicRuleDestination,
		opGetV2LoggingOptions,
		opListActiveViolations,
		opListAttachedPolicies,
		opListAuditFindings,
		opListAuditMitigationActionsExecutions,
		opListAuditMitigationActionsTasks,
		opListAuditSuppressions,
		opListAuditTasks,
		opListAuthorizers,
		opListBillingGroups,
		opListCACertificates,
		opListCertificateProviders,
		opListCertificates,
		opListCertificatesByCA,
		opListCommandExecutions,
		opListCommands,
		opListCustomMetrics,
		opListDetectMitigationActionsExecutions,
		opListDetectMitigationActionsTasks,
		opListDimensions,
		opListDomainConfigurations,
		opListFleetMetrics,
		opListIndices,
		opListJobExecutionsForJob,
		opListJobExecutionsForThing,
		opListJobTemplates,
		opListJobs,
		opListManagedJobTemplates,
		opListMetricValues,
		opListMitigationActions,
		opListOTAUpdates,
		opListOutgoingCertificates,
		opListPackageVersions,
		opListPackages,
		opListPolicyPrincipals,
		opListPolicyVersions,
		opListPrincipalPolicies,
		opListPrincipalThings,
		opListPrincipalThingsV2,
		opListProvisioningTemplateVersions,
		opListProvisioningTemplates,
		opListRelatedResourcesForAuditFinding,
		opListRoleAliases,
		opListSbomValidationResults,
		opListScheduledAudits,
		opListSecurityProfiles,
		opListSecurityProfilesForTarget,
		opListStreams,
		opListTagsForResource,
		opListTargetsForPolicy,
		opListTargetsForSecurityProfile,
		opListThingGroups,
		opListThingGroupsForThing,
		opListThingPrincipalsV2,
		opListThingRegistrationTaskReports,
		opListThingRegistrationTasks,
		opListThingTypes,
		opListThingsInBillingGroup,
		opListThingsInThingGroup,
		opListTopicRuleDestinations,
		opListV2LoggingLevels,
		opListViolationEvents,
		opPutVerificationStateOnViolation,
		opRegisterCACertificate,
		opRegisterCertificate,
		opRegisterCertificateWithoutCA,
		opRegisterThing,
		opRejectCertificateTransfer,
		opRemoveThingFromBillingGroup,
		opRemoveThingFromThingGroup,
		opSearchIndex,
		opSetDefaultAuthorizer,
		opSetDefaultPolicyVersion,
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
		opUpdateAuthorizer,
		opUpdateBillingGroup,
		opUpdateCACertificate,
		opUpdateCertificate,
		opUpdateCertificateProvider,
		opUpdateCommand,
		opUpdateCustomMetric,
		opUpdateDimension,
		opUpdateDomainConfiguration,
		opUpdateDynamicThingGroup,
		opUpdateEncryptionConfiguration,
		opUpdateEventConfigurations,
		opUpdateFleetMetric,
		opUpdateIndexingConfiguration,
		opUpdateJob,
		opUpdateMitigationAction,
		opUpdatePackage,
		opUpdatePackageConfiguration,
		opUpdatePackageVersion,
		opUpdateProvisioningTemplate,
		opUpdateRoleAlias,
		opUpdateScheduledAudit,
		opUpdateSecurityProfile,
		opUpdateStream,
		opUpdateThingGroup,
		opUpdateThingGroupsForThing,
		opUpdateThingType,
		opUpdateTopicRuleDestination,
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
		opCancelJob,
		opCancelJobExecution,
		opClearDefaultAuthorizer,
		opConfirmTopicRuleDestination,
		opCreateAuditSuppression,
		opCreateAuthorizer,
		opCreateBillingGroup,
		opCreateCertificateFromCsr,
		opCreateCertificateProvider,
		opCreateCommand,
		opCreateCustomMetric,
		opCreateDimension,
		opCreateDomainConfiguration,
		opCreateDynamicThingGroup,
		opCreateFleetMetric,
		opCreateJob,
		opCreateJobTemplate,
		opCreateKeysAndCertificate,
		opCreateMitigationAction,
		opCreateOTAUpdate,
		opCreatePackage,
		opCreatePackageVersion,
		opCreatePolicyVersion,
		opCreateProvisioningClaim,
		opCreateProvisioningTemplate,
		opCreateProvisioningTemplateVersion,
		opCreateRoleAlias,
		opCreateScheduledAudit,
		opCreateSecurityProfile,
		opCreateStream,
		opCreateThingGroup,
		opCreateThingType,
		opCreateTopicRuleDestination,
		opDeleteAccountAuditConfiguration,
		opDeleteAuditSuppression,
		opDeleteAuthorizer,
		opDeleteBillingGroup,
		opDeleteCACertificate,
		opDeleteCertificate,
		opDeleteCertificateProvider,
		opDeleteCommand,
		opDeleteCommandExecution,
		opDeleteCustomMetric,
		opDeleteDimension,
		opDeleteDomainConfiguration,
		opDeleteDynamicThingGroup,
		opDeleteFleetMetric,
		opDeleteJob,
		opDeleteJobExecution,
		opDeleteJobTemplate,
		opDeleteMitigationAction,
		opDeleteOTAUpdate,
		opDeletePackage,
		opDeletePackageVersion,
		opDeletePolicyVersion,
		opDeleteProvisioningTemplate,
		opDeleteProvisioningTemplateVersion,
		opDeleteRegistrationCode,
		opDeleteRoleAlias,
		opDeleteScheduledAudit,
		opDeleteSecurityProfile,
		opDeleteStream,
		opDeleteThingGroup,
		opDeleteThingType,
		opDeleteTopicRuleDestination,
		opDeleteV2LoggingLevel,
		opDeprecateThingType,
		opDescribeAccountAuditConfiguration,
		opDescribeAuditFinding,
		opDescribeAuditMitigationActionsTask,
		opDescribeAuditSuppression,
		opDescribeAuditTask,
		opDescribeAuthorizer,
		opDescribeBillingGroup,
		opDescribeCACertificate,
		opDescribeCertificate,
		opDescribeCertificateProvider,
		opDescribeCustomMetric,
		opDescribeDefaultAuthorizer,
		opDescribeDetectMitigationActionsTask,
		opDescribeDimension,
		opDescribeDomainConfiguration,
		opDescribeEncryptionConfiguration,
		opDescribeEventConfigurations,
		opDescribeFleetMetric,
		opDescribeIndex,
		opDescribeJob,
		opDescribeJobExecution,
		opDescribeJobTemplate,
		opDescribeManagedJobTemplate,
		opDescribeMitigationAction,
		opDescribeProvisioningTemplate,
		opDescribeProvisioningTemplateVersion,
		opDescribeRoleAlias,
		opDescribeScheduledAudit,
		opDescribeSecurityProfile,
		opDescribeStream,
		opDescribeThingGroup,
		opDescribeThingRegistrationTask,
		opDescribeThingType,
		opDetachPolicy,
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
		opGetJobDocument,
		opGetLoggingOptions,
		opGetOTAUpdate,
		opGetPackage,
		opGetPackageConfiguration,
		opGetPackageVersion,
		opGetPercentiles,
		opGetPolicyVersion,
		opGetRegistrationCode,
		opGetStatistics,
		opGetThingConnectivityData,
		opGetTopicRuleDestination,
		opGetV2LoggingOptions,
		opListActiveViolations,
		opListAttachedPolicies,
		opListAuditFindings,
		opListAuditMitigationActionsExecutions,
		opListAuditMitigationActionsTasks,
		opListAuditSuppressions,
		opListAuditTasks,
		opListAuthorizers,
		opListBillingGroups,
		opListCACertificates,
		opListCertificateProviders,
		opListCertificates,
		opListCertificatesByCA,
		opListCommandExecutions,
		opListCommands,
		opListCustomMetrics,
		opListDetectMitigationActionsExecutions,
		opListDetectMitigationActionsTasks,
		opListDimensions,
		opListDomainConfigurations,
		opListFleetMetrics,
		opListIndices,
		opListJobExecutionsForJob,
		opListJobExecutionsForThing,
		opListJobTemplates,
		opListJobs,
		opListManagedJobTemplates,
		opListMetricValues,
		opListMitigationActions,
		opListOTAUpdates,
		opListOutgoingCertificates,
		opListPackageVersions,
		opListPackages,
		opListPolicyPrincipals,
		opListPolicyVersions,
		opListPrincipalPolicies,
		opListPrincipalThings,
		opListPrincipalThingsV2,
		opListProvisioningTemplateVersions,
		opListProvisioningTemplates,
		opListRelatedResourcesForAuditFinding,
		opListRoleAliases,
		opListSbomValidationResults,
		opListScheduledAudits,
		opListSecurityProfiles,
		opListSecurityProfilesForTarget,
		opListStreams,
		opListTagsForResource,
		opListTargetsForPolicy,
		opListTargetsForSecurityProfile,
		opListThingGroups,
		opListThingGroupsForThing,
		opListThingPrincipalsV2,
		opListThingRegistrationTaskReports,
		opListThingRegistrationTasks,
		opListThingTypes,
		opListThingsInBillingGroup,
		opListThingsInThingGroup,
		opListTopicRuleDestinations,
		opListV2LoggingLevels,
		opListViolationEvents,
		opPutVerificationStateOnViolation,
		opRegisterCACertificate,
		opRegisterCertificate,
		opRegisterCertificateWithoutCA,
		opRegisterThing,
		opRejectCertificateTransfer,
		opRemoveThingFromBillingGroup,
		opRemoveThingFromThingGroup,
		opSearchIndex,
		opSetDefaultAuthorizer,
		opSetDefaultPolicyVersion,
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
		opUpdateAuthorizer,
		opUpdateBillingGroup,
		opUpdateCACertificate,
		opUpdateCertificate,
		opUpdateCertificateProvider,
		opUpdateCommand,
		opUpdateCustomMetric,
		opUpdateDimension,
		opUpdateDomainConfiguration,
		opUpdateDynamicThingGroup,
		opUpdateEncryptionConfiguration,
		opUpdateEventConfigurations,
		opUpdateFleetMetric,
		opUpdateIndexingConfiguration,
		opUpdateJob,
		opUpdateMitigationAction,
		opUpdatePackage,
		opUpdatePackageConfiguration,
		opUpdatePackageVersion,
		opUpdateProvisioningTemplate,
		opUpdateRoleAlias,
		opUpdateScheduledAudit,
		opUpdateSecurityProfile,
		opUpdateStream,
		opUpdateThingGroup,
		opUpdateThingGroupsForThing,
		opUpdateThingType,
		opUpdateTopicRuleDestination,
		opValidateSecurityProfileBehaviors:
		return true, h.handleStub(c, op)
	}

	return false, nil
}
