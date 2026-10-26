---
service: connect
sdk_module: aws-sdk-go-v2/service/connect@v1.205.0
last_audit_commit: 88c2622c4
last_audit_date: 2026-10-18
overall: B            # 296 of 402 SDK operations are routed and backed by real state, verified through the SDK client in-package; the remaining 106 are unrouted and listed in items_still_open.
ops:
  ActivateEvaluationForm: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateApprovedOrigin: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateBot: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateDefaultVocabulary: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateEmailAddressAlias: {wire: ok, errors: ok, state: ok, persist: ok, note: "the alias must exist and neither be an alias nor have aliases; the primary must not itself be an alias"}
  AssociateFlow: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceIds are opaque; VOICE_PHONE_NUMBER associations are read from the claimed phone numbers"}
  AssociateHoursOfOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateInstanceStorageConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "storage type is checked per resource type; S3 bucket existence is checked against the S3 emulator when wired"}
  AssociateLambdaFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "function existence checked against the Lambda emulator"}
  AssociateLexBot: {wire: ok, errors: ok, state: ok, persist: ok, note: "shape-checked only: the emulator has no Lex service"}
  AssociatePhoneNumberContactFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateQueueEmailAddresses: {wire: ok, errors: ok, state: ok, persist: ok, note: "up to 49 additional addresses per queue; re-associating one is a no-op"}
  AssociateQueueQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateRoutingProfileQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateSecurityKey: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateSecurityProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateTrafficDistributionGroupUser: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateUserProficiencies: {wire: ok, errors: ok, state: ok, persist: ok, note: "attribute must be a predefined attribute, value-checked when validation on association is enabled; level 1.0 to 5.0 in 0.5 steps"}
  AssociateWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "users and routing profiles only (checked against the instance); per-resource results in SuccessfulList/FailedList; re-associating is a no-op"}
  BatchCreateDataTableValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "values for primary attributes are not creatable: they are the record key; each item succeeds or fails alone; values are validated against type and validation"}
  BatchDeleteDataTableValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "a record with no values left disappears"}
  BatchDescribeDataTableValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "a primary attribute describes the record key"}
  BatchGetFlowAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchUpdateDataTableValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "lock versions are checked at the table ValueLockLevel only"}
  ClaimPhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok, note: "numbers come from a deterministic pool (US, CA, GB, DE, AU, JP, FR; DID and TOLL_FREE); claimed numbers leave the pool, released numbers return"}
  Contact: {status: ok, note: "Lifecycle is derived from timestamps: queued (QueueInfo, no AgentInfo), connected (AgentInfo), ended (DisconnectTimestamp)."}
  ContactFlow: {status: ok, note: "Delete refused while a phone number or a queue outbound flow references the flow."}
  CreateAgentStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateContactFlow: {wire: ok, errors: ok, state: ok, persist: ok, note: "content is validated structurally (JSON, Version, unique Identifiers, StartAction and every transition target resolve); invalid content raises InvalidContactFlowException with problems"}
  CreateContactFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateContactFlowModule: {wire: ok, errors: ok, state: ok, persist: ok, note: "same validation as flows; ClientToken replays"}
  CreateContactFlowModuleAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateContactFlowModuleVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateContactFlowVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "snapshots a PUBLISHED flow; version 1 is taken when the flow is created"}
  CreateDataTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "names unique per instance case-insensitively, 100 tables per instance; tags join the tag model"}
  CreateDataTableAttribute: {wire: ok, errors: ok, state: ok, persist: ok, note: "a new primary attribute gives existing records the empty value of its type"}
  CreateEmailAddress: {wire: ok, errors: ok, state: ok, persist: ok, note: "addresses are unique per instance case-insensitively and must be plain addr-specs; ClientToken replays"}
  CreateEvaluationForm: {wire: ok, errors: ok, state: ok, persist: ok, note: "items are stored as given after structural checks (unique RefIds, titles, question types and properties); AsDraft=false activates version 1"}
  CreateHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHoursOfOperationOverride: {wire: ok, errors: ok, state: ok, persist: ok, note: "dates are YYYY-MM-DD, STANDARD overrides need Config; names unique per schedule"}
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClientToken replays; CONNECT_MANAGED/SAML need InstanceAlias (unique), EXISTING_DIRECTORY needs DirectoryId; seeds Basic Hours, BasicQueue, Basic Routing Profile, Admin/Agent/CallCenterManager/QualityAnalyst security profiles and Available/Offline agent statuses; CREATION_IN_PROGRESS until the lifecycle delay (1s default) passes"}
  CreateIntegrationAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "EVENT and APPLICATION register an association in AppIntegrations (and its EventBridge rule); other types store the ARN"}
  CreatePredefinedAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateQuickConnect: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRoutingProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "actions are stored as given after each ActionType is matched to its definition member; CREATE_TASK flows must exist; TriggerEventSource names are checked, integration association required for Zendesk/Salesforce sources; duplicate names raise ResourceConflictException"}
  CreateSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTaskTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "field names unique, SINGLE_SELECT needs options, constraints and defaults must name fields, flows must exist; StartTaskContact resolves the template"}
  CreateTrafficDistributionGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATION_IN_PROGRESS until the lifecycle delay; the instance is not required to be replicated"}
  CreateUseCase: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "Password required exactly when Amazon Connect manages identities; the password is validated and not stored"}
  CreateUserHierarchyGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateView: {wire: ok, errors: ok, state: ok, persist: ok, note: "template must be JSON; InputSchema is derived from the $.Name references of the template; PUBLISHED creates a numbered version"}
  CreateViewVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "content is checked as a tab-separated Transcribe table; invalid content ends in CREATION_FAILED after the lifecycle delay, valid content in ACTIVE"}
  CreateWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "names unique per instance case-insensitively; the theme is stored as sent after a Light/Dark object check; new workspaces start ASSIGNED"}
  CreateWorkspacePage: {wire: ok, errors: ok, state: ok, persist: ok, note: "the view ARN must resolve to a view of the instance; InputData must be a JSON object; custom pages cannot start with aws: or connect:"}
  DeactivateEvaluationForm: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContactFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContactFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContactFlowModule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContactFlowModuleAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContactFlowModuleVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContactFlowVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "DataTableId accepts the ID or ARN, with an optional $LATEST or $SAVED alias"}
  DeleteDataTableAttribute: {wire: ok, errors: ok, state: ok, persist: ok, note: "dropping a primary attribute fails with ResourceConflictException if two records would collide"}
  DeleteEmailAddress: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceInUseException while the address is an alias, has aliases, or is associated with or the default outbound address of a queue"}
  DeleteEvaluationForm: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHoursOfOperationOverride: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades to every child resource and releases AppIntegrations associations"}
  DeleteIntegrationAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePredefinedAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteQuickConnect: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRoutingProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTaskTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrafficDistributionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUseCase: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUserHierarchyGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteView: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteViewVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVocabulary: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "pages, media and assignments go with it"}
  DeleteWorkspaceMedia: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWorkspacePage: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAgentStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContact: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContactFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContactFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContactFlowModule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContactFlowModuleAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDataTable: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDataTableAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEmailAddress: {wire: ok, errors: ok, state: ok, persist: ok, note: "timestamps are ISO 8601 strings"}
  DescribeEvaluationForm: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeHoursOfOperationOverride: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstanceAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstanceStorageConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePredefinedAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePrompt: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeQuickConnect: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRoutingProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrafficDistributionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUserHierarchyGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUserHierarchyStructure: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeView: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVocabulary: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWorkspace: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateApprovedOrigin: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateBot: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateEmailAddressAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateHoursOfOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateInstanceStorageConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateLambdaFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateLexBot: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociatePhoneNumberContactFlow: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateQueueEmailAddresses: {wire: ok, errors: ok, state: ok, persist: ok, note: "an address that is not associated answers ResourceNotFoundException"}
  DisassociateQueueQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateRoutingProfileQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateSecurityKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateSecurityProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateTrafficDistributionGroupUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateUserProficiencies: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources that are not associated are reported in FailedList"}
  EvaluateDataTableValues: {wire: ok, errors: ok, state: ok, persist: ok, note: "values are never expressions here, so evaluation returns the stored value; no access audit is kept"}
  EvaluationForm: {status: ok, note: "Versions live inside one form record; DRAFT versions edit in place, locked ones need CreateNewVersion; one ACTIVE version at a time."}
  FlowVersions: {status: ok, note: "Flows and flow modules snapshot on CreateContactFlowVersion; aliases point at a version and block its deletion."}
  GetContactAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCurrentMetricData: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived from stored contacts, users and routing profiles: CONTACTS_IN_QUEUE, OLDEST_CONTACT_AGE, CONTACTS_SCHEDULED, AGENTS_ONLINE/AVAILABLE/ON_CALL/STAFFED/ON_CONTACT, SLOTS_ACTIVE/AVAILABLE; other metrics are refused"}
  GetCurrentUserData: {wire: ok, errors: ok, state: ok, persist: ok, note: "status, slots and CONNECTED contacts per agent; filters Agents, Queues, RoutingProfiles, UserHierarchyGroups"}
  GetEffectiveHoursOfOperations: {wire: ok, errors: ok, state: ok, persist: ok, note: "resolved from the base schedule and overrides over at most 92 days; recurrence patterns are stored but not expanded"}
  GetEvaluationFormValidation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFlowAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMetricData: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived from stored contacts: CONTACTS_QUEUED/HANDLED/ABANDONED/HANDLED_OUTBOUND, API_CONTACTS_HANDLED, QUEUED_TIME (AVG, MAX); other metrics are refused"}
  GetTaskTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrafficDistribution: {wire: ok, errors: ok, state: ok, persist: ok}
  Hierarchy: {status: ok, note: "Levels must be defined top-down before groups can be created at that depth; a group needs the level above it."}
  HoursOfOperation: {status: ok, note: "Config day and times validated, TimeZone must be a valid IANA zone; delete refused while a queue uses it."}
  ImportWorkspaceMedia: {wire: ok, errors: ok, state: ok, persist: ok, note: "MediaSource is an http(s) URL or base64 text and is stored as given (nothing is fetched); one asset per media type"}
  Instance: {status: ok, note: "Attributes default to the console defaults and INBOUND_CALLS/OUTBOUND_CALLS drive the instance flags; approved origins (https, max 100), Lambda functions, Lex bots (V1 and V2), security keys (max 2) and storage configs live on the instance record."}
  ListAgentStatuses: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApprovedOrigins: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBots: {wire: ok, errors: ok, state: ok, persist: ok}
  ListChildHoursOfOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContactFlowAliases: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContactFlowModuleAliases: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContactFlowModuleVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContactFlowModules: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContactFlowVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContactFlows: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataTableAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataTablePrimaryValues: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataTableValues: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataTables: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDefaultVocabularies: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEntitySecurityProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEvaluationFormVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEvaluationForms: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFlowAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHoursOfOperationOverrides: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHoursOfOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstanceAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstanceStorageConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIntegrationAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLambdaFunctions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLexBots: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPhoneNumbers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPhoneNumbersV2: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPredefinedAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPrompts: {wire: ok, errors: ok, state: ok, persist: ok}
  ListQueueEmailAddresses: {wire: ok, errors: ok, state: ok, persist: ok, note: "the queue default outbound address comes first with IsDefaultOutboundEmail"}
  ListQueueQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  ListQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  ListQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRoutingProfileQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRoutingProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRules: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityKeys: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityProfileAIAgents: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityProfileApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityProfileFlowModules: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityProfilePermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTaskTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficDistributionGroupUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrafficDistributionGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUseCases: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUserHierarchyGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUserProficiencies: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListViewVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListViews: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkspaceMedia: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkspacePages: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok}
  PhoneNumber: {status: ok, note: "Pool-based; UpdatePhoneNumber to another instance drops the flow association."}
  PutUserStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  Queue: {status: ok, note: "Delete refused while a routing profile or quick connect references the queue; AGENT queues are not modeled so ListQueues with AGENT is empty."}
  QuickConnect: {status: ok, note: "Config shape checked per type; USER and QUEUE configs must reference existing resources."}
  ReleasePhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok}
  RoutingProfile: {status: ok, note: "Queue configs and manual-assignment queue configs are separate lists; the default outbound queue cannot be disassociated; delete is refused while users or the default flag apply."}
  Rules: {status: ok, note: "Actions kept as JSON after type and definition checks."}
  Search: {status: ok, note: "One criteria/filter engine (searchengine.go) shared by all Search ops; unsupported criteria members are refused rather than ignored."}
  SearchAgentStatuses: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchAvailablePhoneNumbers: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchContactFlowModules: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchContactFlows: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchContacts: {wire: ok, errors: ok, state: ok, persist: ok, note: "time range plus AgentIds, Channels, InitiationMethods, QueueIds, Name and SearchableContactAttributes criteria; other criteria are refused with InvalidRequestException"}
  SearchDataTables: {wire: ok, errors: ok, state: ok, persist: ok, note: "name and description string conditions plus tag filters"}
  SearchEmailAddresses: {wire: ok, errors: ok, state: ok, persist: ok, note: "EmailAddress, DisplayName and Description string conditions plus tag filters"}
  SearchEvaluationForms: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchHoursOfOperationOverrides: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchHoursOfOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchPredefinedAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchPrompts: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchQueues: {wire: ok, errors: ok, state: ok, persist: ok, note: "string, and/or, queue type, state/status/type and view conditions plus tag and attribute filters are evaluated; ComparisonType is case-insensitive"}
  SearchQuickConnects: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchResourceTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "distinct key/value pairs of the taggable resources of the instance"}
  SearchRoutingProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchRules: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchSecurityProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchUserHierarchyGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchViews: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchVocabularies: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchWorkspaceAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "WorkspaceId, ResourceId, ResourceType and ResourceName string conditions; tags are the workspace tags"}
  SearchWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "name, description, title and visibility string conditions plus tag filters"}
  StartChatContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "creates a CHAT contact with ParticipantId and ParticipantToken; PersistentChat continues an ended chat"}
  StartEvaluationFormValidation: {wire: ok, errors: ok, state: ok, persist: ok, note: "findings come from the same structural walk used on create; IN_PROGRESS until the lifecycle delay"}
  StartOutboundVoiceContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "creates a real contact record: instance must allow outbound calls, flow published, source number claimed by the instance (or taken from the queue caller ID), queue enabled; ClientToken replays"}
  StartTaskContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "creates a TASK contact, ScheduledTime within 6 days; TaskTemplateId is refused (task templates are not implemented)"}
  StopContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "sets DisconnectTimestamp and DisconnectReason; an ended contact answers ContactNotFoundException"}
  TagContact: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  Tags: {status: ok, note: "Any resource ARN of the routed families, instances, contacts, phone numbers and use cases."}
  TrafficDistribution: {status: ok, note: "Groups are global by ID; users must exist in the owning instance."}
  UntagContact: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "assigning UserInfo connects the contact to the agent, QueueInfo enqueues it; both feed the metrics"}
  UpdateContactAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowContent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowModuleAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowModuleContent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowModuleMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactFlowName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactTaskTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataTableAttribute: {wire: ok, errors: ok, state: ok, persist: ok, note: "full replace as AWS specifies; changing Primary re-keys records and fails on collisions"}
  UpdateDataTableMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "omitted members keep their value"}
  UpdateDataTablePrimaryValues: {wire: ok, errors: ok, state: ok, persist: ok, note: "values follow the record to its new key"}
  UpdateEmailAddressMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "omitted members keep their value"}
  UpdateEvaluationForm: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHoursOfOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHoursOfOperationOverride: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateInstanceAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateInstanceStorageConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePhoneNumberMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePredefinedAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
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
  UpdateRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTaskTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTrafficDistribution: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "channels are checked and unique per list"}
  UpdateUserHierarchy: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserHierarchyGroupName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserHierarchyStructure: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserIdentityInfo: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserPhoneConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserProficiencies: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserRoutingProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserSecurityProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateViewContent: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateViewMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateWorkspaceMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "omitted members keep their value"}
  UpdateWorkspacePage: {wire: ok, errors: ok, state: ok, persist: ok, note: "NewPage renames the page"}
  UpdateWorkspaceTheme: {wire: ok, errors: ok, state: ok, persist: ok, note: "replaces the whole theme"}
  UpdateWorkspaceVisibility: {wire: ok, errors: ok, state: ok, persist: ok}
  Users: {status: ok, note: "Username unique per instance (case-insensitive); routing profile, security profiles and hierarchy group must exist; status set through PutUserStatus."}
families:
gaps: []
items_still_open:
  - "Search*: SearchContactEvaluations SearchMetrics SearchNotifications SearchTestCases are not routed"
  - "EvaluationForms (contact evaluations and AI form versions): DeleteContactEvaluation DescribeContactEvaluation ListContactEvaluations ListEvaluationFormAIVersions StartContactEvaluation SubmitContactEvaluation UpdateContactEvaluation are not routed"
  - "TestCases: CreateTestCase DeleteTestCase DescribeTestCase GetTestCaseExecutionSummary ListTestCaseExecutionRecords ListTestCaseExecutions ListTestCases StartTestCaseExecution StopTestCaseExecution UpdateTestCase are not routed"
  - "Email: SendOutboundEmail StartEmailContact StartOutboundEmailContact are not routed"
  - "Notifications: CreateNotification CreatePushNotificationRegistration DeleteNotification DeletePushNotificationRegistration DescribeNotification ListNotifications ListUserNotifications SendOutboundWebNotification UpdateNotificationContent UpdateUserNotificationStatus are not routed"
  - "Metrics definitions: CreateMetric DeleteMetric DescribeMetric GetContactMetrics ListMetrics UpdateMetricContent UpdateMetricMetadata are not routed"
  - "AttachedFiles: BatchGetAttachedFileMetadata CompleteAttachedFileUpload CreateAttachedFile DeleteAttachedFile DescribeAttachedFilesConfiguration GetAttachedFile GetPromptFile ListAttachedFilesConfigurations StartAttachedFileUpload UpdateAttachedFilesConfiguration are not routed"
  - "Analytics: AssociateAnalyticsDataSet BatchAssociateAnalyticsDataSet BatchDisassociateAnalyticsDataSet DisassociateAnalyticsDataSet ListAnalyticsDataAssociations ListAnalyticsDataLakeDataSets StartContactConversationalAnalyticsJob are not routed"
  - "UserProficiencies and contacts of users: DismissUserContact are not routed"
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
  - "CloudFormation: AWS::Connect::AWSManagedView, ContactFlowVersion, ContactFlowModuleVersion, ContactFlowModuleAlias, DataLakeAssociation, DataTableRecord, Metric, Notification, TestCase and UserHierarchyStructure are not provisioned; Pages, Media and Associations of Workspace, HoursOfOperationOverrides, ParentHoursOfOperations and ChildHoursOfOperations of HoursOfOperation, ManualAssignmentQueueConfigs of RoutingProfile, AdditionalEmailAddresses of Queue and SourcePhoneNumberArn of PhoneNumber are rejected"
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
- Data tables: lock versions are opaque UUIDs. Updates honor them only at the table's `ValueLockLevel` (DATA_TABLE, PRIMARY_VALUE, ATTRIBUTE, VALUE compare the matching member; NONE ignores them) and an unset member never conflicts. Batch value operations fail per item; `UpdateDataTablePrimaryValues` answers `ConditionalOperationFailedException`. List values (`TEXT_LIST`, `NUMBER_LIST`) are JSON arrays; `MinValues`/`MaxValues` bound their length. A `Validation` field of 0 means unset (the SDK cannot tell 0 from absent). No version aliases beyond `$LATEST`/`$SAVED` exist and both read the live table.
- `CreateQueue` and `UpdateQueueOutboundEmailConfig` now require the outbound email address to exist in the instance (ResourceNotFoundException).
- Workspaces: the theme and media are stored exactly as sent; the emulator does not render them or fetch media URLs. `Visibility` of a new workspace is ASSIGNED.
