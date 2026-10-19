---
service: connect
sdk_module: aws-sdk-go-v2/service/connect@v1.205.0
last_audit_commit: 88c2622c4
last_audit_date: 2026-10-18
overall: B            # 141 of 402 SDK operations are routed and backed by real state, verified through the SDK client in-package; the remaining 261 are unrouted and listed in items_still_open.
ops:
  AssociateApprovedOrigin: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateBot: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateInstanceStorageConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "storage type is checked per resource type; S3 bucket existence is checked against the S3 emulator when wired"}
  AssociateLambdaFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "function existence checked against the Lambda emulator"}
  AssociateLexBot: {wire: ok, errors: ok, state: ok, persist: ok, note: "shape-checked only: the emulator has no Lex service"}
  AssociatePhoneNumberContactFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateQueueQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateRoutingProfileQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateSecurityKey: {wire: ok, errors: ok, state: ok, persist: ok}
  ClaimPhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok, note: "numbers come from a deterministic pool (US, CA, GB, DE, AU, JP, FR; DID and TOLL_FREE); claimed numbers leave the pool, released numbers return"}
  CreateAgentStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateContactFlow: {wire: ok, errors: ok, state: ok, persist: ok, note: "content is validated structurally (JSON, Version, unique Identifiers, StartAction and every transition target resolve); invalid content raises InvalidContactFlowException with problems"}
  CreateContactFlowModule: {wire: ok, errors: ok, state: ok, persist: ok, note: "same validation as flows; ClientToken replays"}
  CreateHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClientToken replays; CONNECT_MANAGED/SAML need InstanceAlias (unique), EXISTING_DIRECTORY needs DirectoryId; seeds Basic Hours, BasicQueue, Basic Routing Profile, Admin/Agent/CallCenterManager/QualityAnalyst security profiles and Available/Offline agent statuses; CREATION_IN_PROGRESS until the lifecycle delay (1s default) passes"}
  CreateIntegrationAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "EVENT and APPLICATION register an association in AppIntegrations (and its EventBridge rule); other types store the ARN"}
  CreatePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateQuickConnect: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRoutingProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUseCase: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "Password required exactly when Amazon Connect manages identities; the password is validated and not stored"}
  CreateUserHierarchyGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContactFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContactFlowModule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades to every child resource and releases AppIntegrations associations"}
  DeleteIntegrationAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteQuickConnect: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRoutingProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUseCase: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUserHierarchyGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAgentStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContact: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContactFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContactFlowModule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstanceAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstanceStorageConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeQuickConnect: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRoutingProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUserHierarchyGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUserHierarchyStructure: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateApprovedOrigin: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateBot: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateInstanceStorageConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateLambdaFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateLexBot: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociatePhoneNumberContactFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateQueueQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateRoutingProfileQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateSecurityKey: {wire: ok, errors: ok, state: ok, persist: ok}
  GetContactAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCurrentMetricData: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived from stored contacts, users and routing profiles: CONTACTS_IN_QUEUE, OLDEST_CONTACT_AGE, CONTACTS_SCHEDULED, AGENTS_ONLINE/AVAILABLE/ON_CALL/STAFFED/ON_CONTACT, SLOTS_ACTIVE/AVAILABLE; other metrics are refused"}
  GetCurrentUserData: {wire: ok, errors: ok, state: ok, persist: ok, note: "status, slots and CONNECTED contacts per agent; filters Agents, Queues, RoutingProfiles, UserHierarchyGroups"}
  GetMetricData: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived from stored contacts: CONTACTS_QUEUED/HANDLED/ABANDONED/HANDLED_OUTBOUND, API_CONTACTS_HANDLED, QUEUED_TIME (AVG, MAX); other metrics are refused"}
  ListAgentStatuses: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApprovedOrigins: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBots: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContactFlowModules: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContactFlows: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHoursOfOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstanceAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstanceStorageConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIntegrationAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLambdaFunctions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLexBots: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPhoneNumbers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPhoneNumbersV2: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPrompts: {wire: ok, errors: ok, state: ok, persist: ok}
  ListQueueQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  ListQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  ListQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRoutingProfileQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRoutingProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityKeys: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityProfilePermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUseCases: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUserHierarchyGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  PutUserStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  ReleasePhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchAvailablePhoneNumbers: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchContacts: {wire: ok, errors: ok, state: ok, persist: ok, note: "time range plus AgentIds, Channels, InitiationMethods, QueueIds, Name and SearchableContactAttributes criteria; other criteria are refused with InvalidRequestException"}
  StartChatContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "creates a CHAT contact with ParticipantId and ParticipantToken; PersistentChat continues an ended chat"}
  StartOutboundVoiceContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "creates a real contact record: instance must allow outbound calls, flow published, source number claimed by the instance (or taken from the queue caller ID), queue enabled; ClientToken replays"}
  StartTaskContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "creates a TASK contact, ScheduledTime within 6 days; TaskTemplateId is refused (task templates are not implemented)"}
  StopContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "sets DisconnectTimestamp and DisconnectReason; an ended contact answers ContactNotFoundException"}
  TagContact: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagContact: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "assigning UserInfo connects the contact to the agent, QueueInfo enqueues it; both feed the metrics"}
  UpdateContactAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowContent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowModuleContent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowModuleMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateInstanceAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateInstanceStorageConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePhoneNumberMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQueueHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQueueMaxContacts: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQueueName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQueueOutboundCallerConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQueueOutboundEmailConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQueueStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQuickConnectConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQuickConnectName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoutingProfileAgentAvailabilityTimer: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoutingProfileConcurrency: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoutingProfileDefaultOutboundQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoutingProfileName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoutingProfileQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserHierarchy: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserHierarchyGroupName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserHierarchyStructure: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserIdentityInfo: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserPhoneConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserRoutingProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserSecurityProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Instance: {status: ok, note: "Attributes default to the console defaults and INBOUND_CALLS/OUTBOUND_CALLS drive the instance flags; approved origins (https, max 100), Lambda functions, Lex bots (V1 and V2), security keys (max 2) and storage configs live on the instance record."}
  Users: {status: ok, note: "Username unique per instance (case-insensitive); routing profile, security profiles and hierarchy group must exist; status set through PutUserStatus."}
  RoutingProfile: {status: ok, note: "Queue configs and manual-assignment queue configs are separate lists; the default outbound queue cannot be disassociated; delete is refused while users or the default flag apply."}
  Queue: {status: ok, note: "Delete refused while a routing profile or quick connect references the queue; AGENT queues are not modeled so ListQueues with AGENT is empty."}
  QuickConnect: {status: ok, note: "Config shape checked per type; USER and QUEUE configs must reference existing resources."}
  HoursOfOperation: {status: ok, note: "Config day and times validated, TimeZone must be a valid IANA zone; delete refused while a queue uses it."}
  Hierarchy: {status: ok, note: "Levels must be defined top-down before groups can be created at that depth; a group needs the level above it."}
  ContactFlow: {status: ok, note: "Delete refused while a phone number or a queue outbound flow references the flow."}
  PhoneNumber: {status: ok, note: "Pool-based; UpdatePhoneNumber to another instance drops the flow association."}
  Contact: {status: ok, note: "Lifecycle is derived from timestamps: queued (QueueInfo, no AgentInfo), connected (AgentInfo), ended (DisconnectTimestamp)."}
  Tags: {status: ok, note: "Any resource ARN of the routed families, instances, contacts, phone numbers and use cases."}
gaps: []
items_still_open:
  - "Search*: SearchAgentStatuses SearchContactEvaluations SearchContactFlowModules SearchContactFlows SearchDataTables SearchEmailAddresses SearchEvaluationForms SearchHoursOfOperationOverrides SearchHoursOfOperations SearchMetrics SearchNotifications SearchPredefinedAttributes SearchPrompts SearchQueues SearchQuickConnects SearchResourceTags SearchRoutingProfiles SearchRules SearchSecurityProfiles SearchTestCases SearchUserHierarchyGroups SearchUsers SearchViews SearchVocabularies SearchWorkspaceAssociations SearchWorkspaces are not routed"
  - "EvaluationForms: ActivateEvaluationForm CreateEvaluationForm DeactivateEvaluationForm DeleteContactEvaluation DeleteEvaluationForm DescribeContactEvaluation DescribeEvaluationForm GetEvaluationFormValidation ListContactEvaluations ListEvaluationFormAIVersions ListEvaluationFormVersions ListEvaluationForms StartContactEvaluation StartEvaluationFormValidation SubmitContactEvaluation UpdateContactEvaluation UpdateEvaluationForm are not routed"
  - "DataTables: BatchCreateDataTableValue BatchDeleteDataTableValue BatchDescribeDataTableValue BatchUpdateDataTableValue CreateDataTable CreateDataTableAttribute DeleteDataTable DeleteDataTableAttribute DescribeDataTable DescribeDataTableAttribute EvaluateDataTableValues ListDataTableAttributes ListDataTablePrimaryValues ListDataTableValues ListDataTables UpdateDataTableAttribute UpdateDataTableMetadata UpdateDataTablePrimaryValues are not routed"
  - "Views: CreateView CreateViewVersion DeleteView DeleteViewVersion DescribeView ListViewVersions ListViews UpdateViewContent UpdateViewMetadata are not routed"
  - "Workspaces: AssociateWorkspace CreateWorkspace CreateWorkspacePage DeleteWorkspace DeleteWorkspaceMedia DeleteWorkspacePage DescribeWorkspace DisassociateWorkspace ImportWorkspaceMedia ListWorkspaceMedia ListWorkspacePages ListWorkspaces UpdateWorkspaceMetadata UpdateWorkspacePage UpdateWorkspaceTheme UpdateWorkspaceVisibility are not routed"
  - "TestCases: CreateTestCase DeleteTestCase DescribeTestCase GetTestCaseExecutionSummary ListTestCaseExecutionRecords ListTestCaseExecutions ListTestCases StartTestCaseExecution StopTestCaseExecution UpdateTestCase are not routed"
  - "Rules: CreateRule DeleteRule DescribeRule ListRules UpdateRule are not routed"
  - "Email: AssociateEmailAddressAlias AssociateQueueEmailAddresses CreateEmailAddress DeleteEmailAddress DescribeEmailAddress DisassociateEmailAddressAlias DisassociateQueueEmailAddresses ListQueueEmailAddresses SendOutboundEmail StartEmailContact StartOutboundEmailContact UpdateEmailAddressMetadata are not routed"
  - "TaskTemplates: CreateTaskTemplate DeleteTaskTemplate GetTaskTemplate ListTaskTemplates UpdateContactTaskTemplate UpdateTaskTemplate are not routed"
  - "PredefinedAttributes: CreatePredefinedAttribute DeletePredefinedAttribute DescribePredefinedAttribute ListPredefinedAttributes UpdatePredefinedAttribute are not routed"
  - "Notifications: CreateNotification CreatePushNotificationRegistration DeleteNotification DeletePushNotificationRegistration DescribeNotification ListNotifications ListUserNotifications SendOutboundWebNotification UpdateNotificationContent UpdateUserNotificationStatus are not routed"
  - "Metrics definitions: CreateMetric DeleteMetric DescribeMetric GetContactMetrics ListMetrics UpdateMetricContent UpdateMetricMetadata are not routed"
  - "HoursOfOperationOverrides: AssociateHoursOfOperations CreateHoursOfOperationOverride DeleteHoursOfOperationOverride DescribeHoursOfOperationOverride DisassociateHoursOfOperations GetEffectiveHoursOfOperations ListChildHoursOfOperations ListHoursOfOperationOverrides UpdateHoursOfOperationOverride are not routed"
  - "Vocabularies: AssociateDefaultVocabulary CreateVocabulary DeleteVocabulary DescribeVocabulary ListDefaultVocabularies are not routed"
  - "TrafficDistribution: AssociateTrafficDistributionGroupUser CreateTrafficDistributionGroup DeleteTrafficDistributionGroup DescribeTrafficDistributionGroup DisassociateTrafficDistributionGroupUser GetTrafficDistribution ListTrafficDistributionGroupUsers ListTrafficDistributionGroups UpdateTrafficDistribution are not routed"
  - "FlowAssociations: AssociateFlow BatchGetFlowAssociation DisassociateFlow GetFlowAssociation ListFlowAssociations are not routed"
  - "AttachedFiles: BatchGetAttachedFileMetadata CompleteAttachedFileUpload CreateAttachedFile DeleteAttachedFile DescribeAttachedFilesConfiguration GetAttachedFile GetPromptFile ListAttachedFilesConfigurations StartAttachedFileUpload UpdateAttachedFilesConfiguration are not routed"
  - "Analytics: AssociateAnalyticsDataSet BatchAssociateAnalyticsDataSet BatchDisassociateAnalyticsDataSet DisassociateAnalyticsDataSet ListAnalyticsDataAssociations ListAnalyticsDataLakeDataSets StartContactConversationalAnalyticsJob are not routed"
  - "FlowVersionsAndAliases: CreateContactFlowAlias CreateContactFlowModuleAlias CreateContactFlowModuleVersion CreateContactFlowVersion DeleteContactFlowAlias DeleteContactFlowModuleAlias DeleteContactFlowModuleVersion DeleteContactFlowVersion DescribeContactFlowAlias DescribeContactFlowModuleAlias ListContactFlowAliases ListContactFlowModuleAliases ListContactFlowModuleVersions ListContactFlowVersions UpdateContactFlowAlias UpdateContactFlowModuleAlias are not routed"
  - "SecurityProfile extras: AssociateSecurityProfiles DisassociateSecurityProfiles ListEntitySecurityProfiles ListSecurityProfileAIAgents ListSecurityProfileApplications ListSecurityProfileFlowModules are not routed"
  - "UserProficiencies and contacts of users: AssociateUserProficiencies DisassociateUserProficiencies DismissUserContact ListUserProficiencies UpdateUserConfig UpdateUserProficiencies are not routed"
  - "Contact media and call control: MonitorContact PauseContact ResumeContact ResumeContactRecording StartAssistantContact StartContactMediaProcessing StartContactRecording StartContactStreaming StartScreenSharing StartWebRTCContact StopContactMediaProcessing StopContactRecording StopContactStreaming SuspendContactRecording TransferContact are not routed"
  - "Contact data and participants: AssociateContactWithUser BatchPutContact CreateAuthCode CreateContact CreateParticipant CreatePersistentContactAssociation DeleteContactData DeleteSession GetFederationToken ListAssociatedContacts ListContactReferences ListRealtimeContactAnalysisSegmentsV2 SendChatIntegrationEvent StartOutboundChatContact UpdateContactRoutingData UpdateContactSchedule UpdateParticipantAuthentication UpdateParticipantRoleConfig are not routed"
  - "Instance extras: GetCrossRegionRouting ReplicateInstance UpdateCrossRegionRouting are not routed"
  - "PhoneNumber import: ImportPhoneNumber are not routed"
  - "Routing profile and queue extras: ListRoutingProfileManualAssignmentQueues are not routed"
  - "Extraction/Push/Auth: CreateExtractionDefinition DeleteExtractionDefinition DescribeAuthenticationProfile DescribeExtractionDefinition ListAuthenticationProfiles ListExtractionDefinitions UpdateAuthenticationProfile UpdateExtractionDefinition are not routed"
  - "Other: GetMetricDataV2 are not routed"
  - "Contact.Recordings, QualityMetrics, ChatMetrics, DisconnectDetails and the other contact analytics members are never populated"
  - "ListQueues QueueTypes=AGENT: per-agent queues are not modeled"
  - "CreateInstance quota (2 per Region) is not enforced"
  - "ResourceGroupsTaggingAPI and CloudFormation do not expose or provision Connect resources"
  - "Default contact flows are not provisioned with a new instance"
  - "Lex bot associations are shape-checked only: no Lex service exists in the emulator"
structural_gaps:
  - "No telephony or media plane: outbound calls are never placed, no audio flows, and contacts only change state through StopContact and UpdateContact because no agent softphone or customer endpoint exists."
  - "Phone numbers come from a fixed pool of fictional numbers: the carrier inventory behind SearchAvailablePhoneNumbers cannot exist in an emulator."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; instance status transitions are lazy deadlines read under the backend lock, no goroutines or timers."}
---

## Notes

Protocol is REST-JSON (SigV4 signing name `connect`); every operation shape was read from the pinned SDK schemas and types. Timestamps are epoch-seconds numbers (`awstime.EpochTime`). The route matcher claims every request signed for `connect` under a Connect URI prefix (mirrored in `internal/routegates/path_gates.txt`) so an unrouted operation answers `UnknownOperationException` instead of falling through to S3.

Emulator choices the SDK does not document:

- Resource IDs are UUIDs; instance, queue, user ARNs follow `arn:aws:connect:<region>:<account>:instance/<id>[/queue/<id>]`.
- Instance state is one global namespace keyed by ID: a resource created through one Region is visible from another (the ARN keeps the creating Region).
- Name uniqueness (queues, users, profiles, flows, ...) is case-insensitive per instance and answers DuplicateResourceException.
- Metrics are computed on read from contacts, users, routing profiles and agent statuses; nothing is sampled or stored.
- Cross-service checks (Lambda, S3 buckets and prompt objects, AppIntegrations, EventBridge) run through `internal/app/connect_wiring.go` and are skipped when a service is wired standalone.
- Credentials do not matter: the account in ARNs comes from the request metadata, falling back to the configured account.
