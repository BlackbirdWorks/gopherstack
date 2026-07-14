package iot

const (
	opAcceptCertificateTransfer        = "AcceptCertificateTransfer"
	opAddThingToBillingGroup           = "AddThingToBillingGroup"
	opAddThingToThingGroup             = "AddThingToThingGroup"
	opAssociateSbomWithPackageVersion  = "AssociateSbomWithPackageVersion"
	opAssociateTargetsWithJob          = "AssociateTargetsWithJob"
	opAttachPolicy                     = "AttachPolicy"
	opAttachPrincipalPolicy            = "AttachPrincipalPolicy"
	opAttachSecurityProfile            = "AttachSecurityProfile"
	opAttachThingPrincipal             = "AttachThingPrincipal"
	opCancelAuditMitigationActionsTask = "CancelAuditMitigationActionsTask"
	opCancelAuditTask                  = "CancelAuditTask"
	opCreatePolicy                     = "CreatePolicy"
	opCreateThing                      = "CreateThing"
	opCreateTopicRule                  = "CreateTopicRule"
	opDeletePolicy                     = "DeletePolicy"
	opDeleteThing                      = "DeleteThing"
	opDeleteTopicRule                  = "DeleteTopicRule"
	opDescribeEndpoint                 = "DescribeEndpoint"
	opDescribeThing                    = "DescribeThing"
	opDisableTopicRule                 = "DisableTopicRule"
	opEnableTopicRule                  = "EnableTopicRule"
	opGetPolicy                        = "GetPolicy"
	opGetTopicRule                     = "GetTopicRule"
	opListPolicies                     = "ListPolicies"
	opListThingPrincipals              = "ListThingPrincipals"
	opListThings                       = "ListThings"
	opListTopicRules                   = "ListTopicRules"
	opReplaceTopicRule                 = "ReplaceTopicRule"
	opUpdateThing                      = "UpdateThing"
	opGetThingShadow                   = "GetThingShadow"
	opUpdateThingShadow                = "UpdateThingShadow"
	opDeleteThingShadow                = "DeleteThingShadow"
	opListNamedShadowsForThing         = "ListNamedShadowsForThing"
)

// New operation name constants for stateful implementations.
const (
	opCreateThingType              = "CreateThingType"
	opDescribeThingType            = "DescribeThingType"
	opListThingTypes               = "ListThingTypes"
	opDeprecateThingType           = "DeprecateThingType"
	opDeleteThingType              = "DeleteThingType"
	opCreateThingGroup             = "CreateThingGroup"
	opDescribeThingGroup           = "DescribeThingGroup"
	opListThingGroups              = "ListThingGroups"
	opUpdateThingGroup             = "UpdateThingGroup"
	opDeleteThingGroup             = "DeleteThingGroup"
	opRemoveThingFromThingGroup    = "RemoveThingFromThingGroup"
	opListThingsInThingGroup       = "ListThingsInThingGroup"
	opCreateCertificateFromCsr     = "CreateCertificateFromCsr"
	opRegisterCertificate          = "RegisterCertificate"
	opRegisterCertificateWithoutCA = "RegisterCertificateWithoutCA"
	opDescribeCertificate          = "DescribeCertificate"
	opListCertificates             = "ListCertificates"
	opUpdateCertificate            = "UpdateCertificate"
	opDeleteCertificate            = "DeleteCertificate"
	opDetachPolicy                 = "DetachPolicy"
	opListAttachedPolicies         = "ListAttachedPolicies"
	opCreatePolicyVersion          = "CreatePolicyVersion"
	opGetPolicyVersion             = "GetPolicyVersion"
	opListPolicyVersions           = "ListPolicyVersions"
	opDeletePolicyVersion          = "DeletePolicyVersion"
	opSetDefaultPolicyVersion      = "SetDefaultPolicyVersion"
	opCreateTopicRuleDestination   = "CreateTopicRuleDestination"
	opGetTopicRuleDestination      = "GetTopicRuleDestination"
	opListTopicRuleDestinations    = "ListTopicRuleDestinations"
	opUpdateTopicRuleDestination   = "UpdateTopicRuleDestination"
	opDeleteTopicRuleDestination   = "DeleteTopicRuleDestination"
	opCreateCertificateProvider    = "CreateCertificateProvider"
	opDescribeCertificateProvider  = "DescribeCertificateProvider"
	opListCertificateProviders     = "ListCertificateProviders"
	opUpdateCertificateProvider    = "UpdateCertificateProvider"
	opDeleteCertificateProvider    = "DeleteCertificateProvider"
)

// Stub operation name constants — all ops are now implemented.
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
// All ops previously listed here are now implemented for real — in
// dispatchBatch3Ops (batch3 ops) and the batch4/device-defender/final-ops/
// indexing dispatchers wired into dispatchNewOp. This returns empty so
// GetSupportedOperations uses only the explicit core list in handler.go.
func allStubOps() []string { return nil }

// coreResourceOperationNames lists thing/shadow/thing-type/thing-group/certificate/
// policy/topic-rule-destination/certificate-provider operation names.
func coreResourceOperationNames() []string {
	return []string{
		opAcceptCertificateTransfer,
		opAddThingToBillingGroup,
		opAddThingToThingGroup,
		opAssociateSbomWithPackageVersion,
		opAssociateTargetsWithJob,
		opAttachPolicy,
		opAttachPrincipalPolicy,
		opAttachSecurityProfile,
		opAttachThingPrincipal,
		opCancelAuditMitigationActionsTask,
		opCancelAuditTask,
		opCreatePolicy,
		opCreateThing,
		opCreateTopicRule,
		opDeletePolicy,
		opDeleteThing,
		opDeleteTopicRule,
		opDescribeEndpoint,
		opDescribeThing,
		opDisableTopicRule,
		opEnableTopicRule,
		opGetPolicy,
		opGetTopicRule,
		opListPolicies,
		opListThingPrincipals,
		opListThings,
		opListTopicRules,
		opReplaceTopicRule,
		opUpdateThing,
		// Device Shadows
		opGetThingShadow,
		opUpdateThingShadow,
		opDeleteThingShadow,
		opListNamedShadowsForThing,
		// ThingType
		opCreateThingType,
		opDescribeThingType,
		opListThingTypes,
		opDeprecateThingType,
		opDeleteThingType,
		// ThingGroup
		opCreateThingGroup,
		opDescribeThingGroup,
		opListThingGroups,
		opUpdateThingGroup,
		opDeleteThingGroup,
		opRemoveThingFromThingGroup,
		opListThingsInThingGroup,
		// Certificate
		opCreateCertificateFromCsr,
		opRegisterCertificate,
		opRegisterCertificateWithoutCA,
		opDescribeCertificate,
		opListCertificates,
		opUpdateCertificate,
		opDeleteCertificate,
		// Policy attachment
		opDetachPolicy,
		opListAttachedPolicies,
		// PolicyVersion
		opCreatePolicyVersion,
		opGetPolicyVersion,
		opListPolicyVersions,
		opDeletePolicyVersion,
		opSetDefaultPolicyVersion,
		// TopicRuleDestination
		opCreateTopicRuleDestination,
		opGetTopicRuleDestination,
		opListTopicRuleDestinations,
		opUpdateTopicRuleDestination,
		opDeleteTopicRuleDestination,
		// CertificateProvider
		opCreateCertificateProvider,
		opDescribeCertificateProvider,
		opListCertificateProviders,
		opUpdateCertificateProvider,
		opDeleteCertificateProvider,
	}
}

// extendedResourceOperationNames lists job, provisioning, authorizer, billing-group,
// scheduled-audit, mitigation-action and security-profile operation names.
func extendedResourceOperationNames() []string {
	return []string{
		// Batch 1: Jobs
		opCreateJob,
		opDescribeJob,
		opListJobs,
		opUpdateJob,
		opCancelJob,
		opDeleteJob,
		opGetJobDocument,
		opDescribeJobExecution,
		opCancelJobExecution,
		opDeleteJobExecution,
		// Batch 1: JobTemplates
		opCreateJobTemplate,
		opDescribeJobTemplate,
		opListJobTemplates,
		opDeleteJobTemplate,
		// Batch 1: RoleAliases
		opCreateRoleAlias,
		opDescribeRoleAlias,
		opListRoleAliases,
		opUpdateRoleAlias,
		opDeleteRoleAlias,
		// Batch 1: DomainConfigurations
		opCreateDomainConfiguration,
		opDescribeDomainConfiguration,
		opListDomainConfigurations,
		opUpdateDomainConfiguration,
		opDeleteDomainConfiguration,
		// Batch 1: ProvisioningTemplates
		opCreateProvisioningTemplate,
		opDescribeProvisioningTemplate,
		opListProvisioningTemplates,
		opUpdateProvisioningTemplate,
		opDeleteProvisioningTemplate,
		opCreateProvisioningTemplateVersion,
		opListProvisioningTemplateVersions,
		opDeleteProvisioningTemplateVersion,
		// Batch 1: Authorizers
		opCreateAuthorizer,
		opDescribeAuthorizer,
		opListAuthorizers,
		opUpdateAuthorizer,
		opDeleteAuthorizer,
		// Batch 1: BillingGroups
		opCreateBillingGroup,
		opDescribeBillingGroup,
		opListBillingGroups,
		opUpdateBillingGroup,
		opDeleteBillingGroup,
		// Batch 1: ScheduledAudits
		opCreateScheduledAudit,
		opDescribeScheduledAudit,
		opListScheduledAudits,
		opUpdateScheduledAudit,
		opDeleteScheduledAudit,
		// Batch 1: MitigationActions
		opCreateMitigationAction,
		opDescribeMitigationAction,
		opListMitigationActions,
		opUpdateMitigationAction,
		opDeleteMitigationAction,
		// Batch 1: SecurityProfiles
		opCreateSecurityProfile,
		opDescribeSecurityProfile,
		opListSecurityProfiles,
		opUpdateSecurityProfile,
		opDeleteSecurityProfile,
	}
}

// deviceFleetOperationNames lists CA certificate, stream, fleet/custom metric,
// dimension, tag, audit-config, misc, and fleet-indexing operation names.
func deviceFleetOperationNames() []string {
	return []string{
		// Batch 2: CACertificate
		opRegisterCACertificate,
		opDescribeCACertificate,
		opListCACertificates,
		opUpdateCACertificate,
		opDeleteCACertificate,
		opListCertificatesByCA,
		// Batch 2: Stream
		opCreateStream,
		opDescribeStream,
		opListStreams,
		opUpdateStream,
		opDeleteStream,
		// Batch 2: FleetMetric
		opCreateFleetMetric,
		opDescribeFleetMetric,
		opListFleetMetrics,
		opUpdateFleetMetric,
		opDeleteFleetMetric,
		// Batch 2: CustomMetric
		opCreateCustomMetric,
		opDescribeCustomMetric,
		opListCustomMetrics,
		opUpdateCustomMetric,
		opDeleteCustomMetric,
		// Batch 2: Dimension
		opCreateDimension,
		opDescribeDimension,
		opListDimensions,
		opUpdateDimension,
		opDeleteDimension,
		// Batch 2: Tags
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		// Batch 2: Audit
		opDescribeAccountAuditConfiguration,
		opUpdateAccountAuditConfiguration,
		opStartOnDemandAuditTask,
		opDescribeAuditTask,
		opListAuditTasks,
		// Batch 2: Misc
		opDetachThingPrincipal,
		opCancelCertificateTransfer,
		opGetRegistrationCode,
		opDeleteRegistrationCode,
		opListThingGroupsForThing,
		opListThingsInBillingGroup,
		opRemoveThingFromBillingGroup,
		opListPrincipalPolicies,
		opListPolicyPrincipals,
		opListTargetsForPolicy,
		opListPrincipalThings,
		opListPrincipalThingsV2,
		opGetEffectivePolicies,
		opSetDefaultAuthorizer,
		opClearDefaultAuthorizer,
		opDescribeDefaultAuthorizer,
		opListJobExecutionsForJob,
		opListJobExecutionsForThing,
		// Fleet indexing / search / aggregation
		opUpdateIndexingConfiguration,
		opGetIndexingConfiguration,
		opListIndices,
		opDescribeIndex,
		opSearchIndex,
		opGetCardinality,
		opGetPercentiles,
		opGetStatistics,
		opGetBucketsAggregation,
	}
}

// thingRegistrationAndDeviceDefenderOperationNames lists batch-4 thing registration,
// device defender, and final-ops operation names.
func thingRegistrationAndDeviceDefenderOperationNames() []string {
	return []string{
		// Batch 4: RegisterThing / bulk thing registration tasks
		opRegisterThing,
		opStartThingRegistrationTask,
		opStopThingRegistrationTask,
		opListThingRegistrationTasks,
		opDescribeThingRegistrationTask,
		opListThingRegistrationTaskReports,
		// Batch 4: ThingType/ThingGroup updates, typed principals, managed job templates
		opUpdateThingType,
		opUpdateThingGroupsForThing,
		opListThingPrincipalsV2,
		opDescribeManagedJobTemplate,
		opListManagedJobTemplates,
		// Device Defender: audit + detect mitigation-action tasks, violations.
		opStartAuditMitigationActionsTask,
		opDescribeAuditMitigationActionsTask,
		opListAuditMitigationActionsTasks,
		opListAuditMitigationActionsExecutions,
		opStartDetectMitigationActionsTask,
		opDescribeDetectMitigationActionsTask,
		opListDetectMitigationActionsTasks,
		opListDetectMitigationActionsExecutions,
		opCancelDetectMitigationActionsTask,
		opListActiveViolations,
		opListViolationEvents,
		opPutVerificationStateOnViolation,
		opListRelatedResourcesForAuditFinding,
		opDeleteAccountAuditConfiguration,
		// Final stub batch: implemented against real backend state (see
		// handler_final_ops.go / dispatchFinalOps).
		opDescribeEncryptionConfiguration,
		opUpdateEncryptionConfiguration,
		opTestAuthorization,
		opTestInvokeAuthorizer,
		opDetachPrincipalPolicy,
		opConfirmTopicRuleDestination,
		opDeleteCommandExecution,
		opGetBehaviorModelTrainingSummaries,
		opListOutgoingCertificates,
		opDisassociateSbomFromPackageVersion,
		opListSbomValidationResults,
		opListMetricValues,
		opGetThingConnectivityData,
		opDescribeProvisioningTemplateVersion,
		opValidateSecurityProfileBehaviors,
	}
}

// extendedOperationNames lists OTA update, package, audit suppression/finding,
// logging, certificate-claim, event-config, dynamic-thing-group and command
// operation names.
func extendedOperationNames() []string {
	return []string{
		opCreateOTAUpdate, opGetOTAUpdate, opDeleteOTAUpdate, opListOTAUpdates,
		opCreatePackage, opGetPackage, opUpdatePackage, opDeletePackage, opListPackages,
		opCreatePackageVersion, opGetPackageVersion, opUpdatePackageVersion,
		opDeletePackageVersion, opListPackageVersions,
		opGetPackageConfiguration, opUpdatePackageConfiguration,
		opCreateAuditSuppression, opDescribeAuditSuppression, opUpdateAuditSuppression,
		opDeleteAuditSuppression, opListAuditSuppressions,
		opDescribeAuditFinding, opListAuditFindings,
		opGetV2LoggingOptions, opSetV2LoggingOptions, opSetV2LoggingLevel,
		opDeleteV2LoggingLevel, opListV2LoggingLevels,
		opGetLoggingOptions, opSetLoggingOptions,
		opCreateProvisioningClaim,
		opCreateKeysAndCertificate,
		opTransferCertificate, opRejectCertificateTransfer,
		opDescribeEventConfigurations, opUpdateEventConfigurations,
		opDetachSecurityProfile, opListTargetsForSecurityProfile, opListSecurityProfilesForTarget,
		opCreateDynamicThingGroup, opDeleteDynamicThingGroup, opUpdateDynamicThingGroup,
		opCreateCommand, opGetCommand, opUpdateCommand, opDeleteCommand, opListCommands,
		opGetCommandExecution, opListCommandExecutions,
	}
}
