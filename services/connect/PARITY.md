---
service: connect
sdk_module: aws-sdk-go-v2/service/connect@v1.205.0
last_audit_commit: 88c2622c4
last_audit_date: 2026-10-18
overall: B            # 384 of 402 SDK operations are routed and backed by real state, verified through the SDK client in-package; the remaining 18 are unrouted and listed in items_still_open.
ops:
  AssociateContactWithUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "the contact must be active and queued (QueueInfo, no agent); the user is an ID or ARN; sets AgentInfo. Shape: schemas.go AssociateContactWithUser; docs API_AssociateContactWithUser"}
  BatchPutContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-25 items, each succeeds or fails alone with FailureReasonCode (MISSING_CUSTOMER_ENDPOINT, INVALID_CUSTOMER_ENDPOINT, INVALID_SYSTEM_ENDPOINT, MISSING_QUEUE_ID_AND_SYSTEM_ENDPOINT, INVALID_QUEUE, INVALID_ATTRIBUTE_KEY, INVALID_OUTBOUND_STRATEGY, MISSING_CAMPAIGN, IDEMPOTENCY_EXCEPTION for a repeated RequestIdentifier); creates OUTBOUND voice contacts; the outbound-campaigns-only principal restriction is not enforced; item-level reason precedence is unverified"}
  CreateContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "VOICE (TRANSFER), EMAIL (OUTBOUND, AGENT_REPLY, FLOW) and TASK (API) only, per API_CreateContact; InitiateAs COMPLETED is TASK-only; UserInfo required for EMAIL OUTBOUND/AGENT_REPLY and CONNECTED_TO_USER; ExpiryDurationInMinutes is stored as the connect:ContactExpiry segment attribute and is not enforced by a timer"}
  CreateParticipant: {wire: ok, errors: ok, state: ok, persist: ok, note: "active chat contacts only, role CUSTOM_BOT only (API_CreateParticipant); WebRTC voice does not exist in the emulator; credentials expiry is 25h after creation (unverified)"}
  CreatePersistentContactAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FROM_SEGMENT uses the ended source chat contact, ENTIRE_PAST_SESSION the most recently ended chat contact of the session whose initial contact is SourceContactId; sets PreviousContactId on the current contact"}
  MonitorContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "needs an active voice/chat contact connected to an agent; creates a MONITOR contact for the user (RelatedContactId = monitored contact, which is unverified) that ends when the monitored contact ends; capabilities SILENT_MONITOR/BARGE are stored"}
  PauseContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "active TASK contacts only (API_PauseContact); ConflictException when already paused; feeds LastPausedTimestamp, TotalPauseCount, TotalPauseDurationInSeconds"}
  ResumeContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException when not paused; stopping a paused contact accounts the open pause"}
  ResumeContactRecording: {wire: ok, errors: ok, state: ok, persist: ok, note: "suspended -> recording; ContactRecordingType defaults to AGENT"}
  SendOutboundEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "CAMPAIGN traffic only with SourceCampaign.CampaignId (API_SendOutboundEmail); From must be an instance email address; RAW messages only (TEMPLATE needs Wisdom message templates, not emulated); recorded on the From address and delivered through SES when wired (best effort: a rejected send is recorded as FAILED, like an asynchronous bounce)"}
  StartContactMediaProcessing: {wire: ok, errors: ok, state: ok, persist: ok, note: "active chat contacts; ProcessorArn is a Lambda ARN checked against the Lambda emulator when wired; a second start is rejected. No messages flow through the processor (no chat message plane)"}
  StartContactRecording: {wire: ok, errors: ok, state: ok, persist: ok, note: "active voice contacts, InitialContactId must match; a one-time action per recording type (AGENT from VoiceRecordingTrack, IVR from IvrRecordingTrack); adds a Contact.Recordings entry (Location from the CALL_RECORDINGS S3 storage config when set); no audio is captured"}
  StartContactStreaming: {wire: ok, errors: ok, state: ok, persist: ok, note: "active chat contacts, MESSAGE_STREAMING instance attribute enabled, SNS topic ARN; returns a StreamingId. No chat messages are published (no message plane)"}
  StartEmailContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "DestinationEmailAddress must be an instance email address; RAW message with text/plain or text/html; at most 50 attachments; RelatedContactId copies and overlays attributes; creates an INBOUND EMAIL contact with customer/system email endpoints and AdditionalEmailRecipients"}
  StartOutboundEmailContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "contact must be an active OUTBOUND or AGENT_REPLY EMAIL contact that has not sent yet; From must be an instance address (defaults to the contact system endpoint); RAW only; delivered through SES when wired, best effort"}
  StopContactMediaProcessing: {wire: ok, errors: ok, state: ok, persist: ok}
  StopContactRecording: {wire: ok, errors: ok, state: ok, persist: ok, note: "recording or suspended -> stopped and not restartable; ending the contact stops open recordings"}
  StopContactStreaming: {wire: ok, errors: ok, state: ok, persist: ok}
  SuspendContactRecording: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRoutingProfileManualAssignmentQueues: {wire: ok, errors: ok, state: ok, persist: ok, note: "reads the profile manual assignment configs; also fixes Create/AssociateRoutingProfileQueues rejecting ManualAssignmentQueueConfigs for lacking Priority/Delay, members the SDK type does not have"}
  CreateExtractionDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "Name 1-127 unique per instance (DuplicateResourceException), ExtractionConfiguration.PromptHint required, NotFoundBehavior.Behavior USE_DEFAULT_VALUE|OMIT (enum: types/enums.go NotFoundBehaviorType); LastUpdatedBy is the account root ARN since the caller identity is unknown; ClientToken replays; tags join the tag model. Name, hint and tag limits beyond the enum are unverified (the SDK carries no length traits)"}
  DescribeExtractionDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  ListExtractionDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "sorted by name; MaxResults 1-100 is unverified"}
  UpdateExtractionDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "replaces Name and ExtractionConfiguration, Display only when sent"}
  DeleteExtractionDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAuthenticationProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "every instance has a default profile (IsDefault) created on first access; its name, description and 600-minute MaxSessionDuration are emulator defaults (unverified)"}
  ListAuthenticationProfiles: {wire: ok, errors: ok, state: ok, persist: ok, note: "sorted by name; only the default profile exists because the SDK has no CreateAuthenticationProfile"}
  UpdateAuthenticationProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "partial update; AllowedIps/BlockedIps must be CIDR ranges (at most 100, unverified), session durations positive minutes, names unique; the SDK carries no further constraints"}
  ListAssociatedContacts: {wire: ok, errors: ok, state: ok, persist: ok, note: "every contact of the instance sharing the InitialContactId of the given contact, oldest first; the contact-tree definition is from the SDK docs (api_op_ListAssociatedContacts.go), MaxResults range unverified"}
  UpdateContactSchedule: {wire: ok, errors: ok, state: ok, persist: ok, note: "active TASK contacts that already have a ScheduledTimestamp (InvalidRequestException otherwise, code unverified); the time must be future and within 6 days (api_op_UpdateContactSchedule.go)"}
  DeleteContactData: {wire: ok, errors: ok, state: ok, persist: ok, note: "terminated contacts only; CUSTOMER_ENDPOINT clears CustomerEndpoint, ADDITIONAL_EMAIL_RECIPIENTS and EMAIL_SUBJECT (EMAIL channel only) clear the recipients and the stored message subjects; the error codes for active contacts and wrong channels are unverified"}
  ListContactReferences: {wire: ok, errors: ok, state: ok, persist: ok, note: "references stored by StartTaskContact/UpdateContact filtered by the referenceTypes query, one union member per reference named after its type (schemas.go ReferenceSummary); NextToken is never returned"}
  CreateMetric: {wire: ok, errors: ok, state: ok, persist: ok, note: "customer-managed metric definitions per API_CreateMetric: name unique case-insensitively, Unit INTEGER|DOUBLE|PERCENT|SECONDS, 1-5 components with unique aliases, each naming exactly one of MetricId/MetricName, every alias used in the formula and every formula identifier an alias or function call; component metric names and filters are NOT checked against the AWS-managed metric catalog (not published); Status defaults to SAVED and Category to CUSTOM (both unverified); ClientToken replays; tags join the tag model"}
  GetMetricDataV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "computed from stored contacts; metrics CONTACTS_CREATED, CONTACTS_QUEUED, CONTACTS_HANDLED, CONTACTS_ABANDONED, ABANDONMENT_RATE, AVG_HANDLE_TIME, AVG_CONTACT_DURATION, AVG_QUEUE_ANSWER_TIME, AVG_ABANDON_TIME, MAX_QUEUED_TIME and SERVICE_LEVEL (Threshold LT/LTE, 1-604800); filters and groupings QUEUE, ROUTING_PROFILE, AGENT, CHANNEL, INITIATION_METHOD, DISCONNECT_REASON (NOT_EXISTS StringCondition supported); metric filters INITIATION_METHOD/DISCONNECT_REASON per metric; IntervalPeriod FIFTEEN_MIN..TOTAL with TimeZone and the 3/35 day spans; any other metric, filter key or grouping, and computing an existing custom MetricId, answer InvalidParameterException; AGENT_ONLINE_TIME and the other agent-state metrics are unmodeled (no status history). Shape: schemas.go GetMetricDataV2; docs API_GetMetricDataV2. Unverified: metric formulas (handle time = connect to disconnect), interval buckets start at StartTime rather than clock boundaries, Dimensions keys QUEUE/QUEUE_ARN style, contacts without a value for a grouped dimension form no group, last interval includes EndTime"}
  GetContactMetrics: {wire: ok, errors: ok, state: ok, persist: ok, note: "POSITION_IN_QUEUE is 1 + earlier-enqueued contacts still waiting in the queue; ESTIMATED_WAIT_TIME is that position times the queue mean enqueue-to-agent time of contacts that reached an agent (0 without history), an emulator estimator since the AWS model is not published; contacts that are not waiting in a queue answer InvalidRequestException (unverified), unknown contacts ResourceNotFoundException"}
  StartAttachedFileUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "FileUseCaseType ATTACHMENT only; the resource is a contact of the instance (EMAIL, TASK or CHAT channel) or a Cases ARN (shape-checked, cases are not modeled); extension and size are checked against the scope policy (AttachmentScope = EMAIL, CHAT, CASE, TASK); returns a PROCESSING file and a pre-signed URL on the emulator's own /attached-files-content endpoint (HMAC-signed, UrlExpiryInSeconds 5-300, default 300) instead of an S3 URL; ClientToken replays; tags join the tag model. Shape: schemas.go StartAttachedFileUpload; docs API_StartAttachedFileUpload. Unverified: the 5-300 second range, the empty HeadersToInclude map, the file ARN format"}
  CompleteAttachedFileUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "PROCESSING -> APPROVED once content of exactly FileSizeInBytes was PUT to the upload URL (no virus scan), else InvalidRequestException (unverified code); the resource ARN must match the file's"}
  CreateAttachedFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "VOICE_RECORDING only, onto a completed voice contact; FileSourceUri must be an s3:// object that exists in the in-process S3 (needs the S3 wiring, else InvalidRequestException); the object is copied synchronously so the file is APPROVED at once (AWS reports PROCESSING until its copy finishes, unverified); file name is the key basename; ClientToken replays"}
  GetAttachedFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "DownloadUrlMetadata only once APPROVED, signed URL on the emulator content endpoint; unknown file or mismatched AssociatedResourceArn is ResourceNotFoundException"}
  BatchGetAttachedFileMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-100 ids; missing files come back in Errors with ErrorCode ResourceNotFoundException (the AWS error code strings are unverified)"}
  DeleteAttachedFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "removes the metadata and the stored content"}
  DescribeAttachedFilesConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "never-updated scopes answer the emulator defaults (20 MiB, csv doc docx gif jpeg jpg pdf png ppt pptx tif tiff txt wav xls xlsx), which are unverified; LastModifiedTime only after an update"}
  ListAttachedFilesConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "one summary per scope EMAIL, CHAT, CASE, TASK"}
  UpdateAttachedFilesConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaximumSizeLimitInBytes 1-104857600, extensions 1-10 alphanumeric, hyphen or underscore characters (case-folded, no dot); either member may be sent alone"}
  CreateNotification: {wire: ok, errors: ok, state: ok, persist: ok, note: "Content 1+ locales of 1-250 characters; Recipients 1-200 user ARNs of the instance (must exist) or the instance ARN; Priority HIGH or LOW (default LOW, unverified); ExpiresAt must be in the future and defaults to a week (docs); PredefinedNotificationId (deprecated) becomes the ID and conflicts raise DuplicateResourceException; ClientToken replays; tags join the tag model. Shape: schemas.go CreateNotification; docs API_CreateNotification"}
  DescribeNotification: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateNotificationContent: {wire: ok, errors: ok, state: ok, persist: ok, note: "replaces the whole content map (up to 500 characters per locale, per the SDK docs) and refreshes LastModifiedTime/Region"}
  DeleteNotification: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNotifications: {wire: ok, errors: ok, state: ok, persist: ok, note: "newest first; MaxResults 1-100"}
  SearchNotifications: {wire: ok, errors: ok, state: ok, persist: ok, note: "StringCondition fields NotificationId/Id and MessageContent/Content (all locales joined; the real field names are unverified), tag filter, And/Or conditions"}
  ListUserNotifications: {wire: ok, errors: ok, state: ok, persist: ok, note: "unexpired notifications addressed to the user directly or through the instance ARN; Source is always CUSTOMER (no rules or system notifications exist); status defaults to UNREAD; MaxResults 1-1000"}
  UpdateUserNotificationStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "READ, UNREAD or HIDDEN for a recipient; the x-amz-last-modified-time/region replication headers are not read (the REST framework does not expose request headers)"}
  CreatePushNotificationRegistration: {wire: ok, errors: ok, state: ok, persist: ok, note: "active chat contact of the instance, ParticipantRole CUSTOMER only, DeviceType GCM|APNS|APNS_SANDBOX, PinpointAppArn shape-checked as a mobiletargeting apps ARN (existence in the Pinpoint emulator is not checked); ClientToken replays"}
  DeletePushNotificationRegistration: {wire: ok, errors: ok, state: ok, persist: ok, note: "the contactId query must be the contact the registration was created for, else ResourceNotFoundException"}
  CreateAuthCode: {wire: ok, errors: ok, state: ok, persist: ok, note: "Scope.EntityType CUSTOMER_PROFILE only (the one SDK enum value), at most 10 security profiles that must exist; SessionInactivityDurationMinutes 1440-20160, MaxSessionDurationMinutes 1440-43200 (default 400 days) and not below the inactivity window; returns a random 256-bit URL-safe code and the id of a recorded session. Nothing consumes the code (no federation endpoint exists), sessions end only through DeleteSession and do not expire. Shape: schemas.go CreateAuthCode; docs API_CreateAuthCode"}
  DeleteSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "removes a session recorded by CreateAuthCode; unknown ids raise ResourceNotFoundException"}
  CreateTestCase: {wire: ok, errors: ok, state: ok, persist: ok, note: "a SAVED or PUBLISHED version; Content, InitializationData (JSON) and an optional EntryPoint (VOICE_CALL or CHAT with its parameters; the FlowId must be a flow of the instance) are stored; PUBLISHED validates that Content is a JSON object and requires an EntryPoint, SAVED does no content validation (SDK docs); names unique per instance case-insensitively (unverified); TestCaseSha256 is the SHA-256 of Content (unverified); the x-amz-resource-id and replication headers are not read. Shape: schemas.go CreateTestCase; docs API_CreateTestCase"}
  DescribeTestCase: {wire: ok, errors: ok, state: ok, persist: ok, note: "Status selects the version; by default the published one when it exists, else the saved one (SDK docs)"}
  UpdateTestCase: {wire: ok, errors: ok, state: ok, persist: ok, note: "partial update of the version named by Status (default SAVED, seeded from the other version when missing); renames apply to the test case; same validation as create"}
  DeleteTestCase: {wire: ok, errors: ok, state: ok, persist: ok, note: "removes both versions"}
  ListTestCases: {wire: ok, errors: ok, state: ok, persist: ok, note: "sorted by name; Status is the published status when published; MaxResults 1-1000"}
  SearchTestCases: {wire: ok, errors: ok, state: ok, persist: ok, note: "StringCondition on name and description, StatusCondition, tag filter, And/Or conditions"}
  StartContactEvaluation: {wire: ok, errors: ok, state: ok, persist: ok, note: "the contact must exist in the instance (ContactNotFoundException) and the form must have an ACTIVE version, which the draft evaluation pins (InvalidRequestException otherwise, code unverified); AutoEvaluationConfiguration.Enabled is recorded but no automatic evaluation runs; ClientToken replays; tags join the tag model. Shape: schemas.go StartContactEvaluation; docs API_StartContactEvaluation"}
  UpdateContactEvaluation: {wire: ok, errors: ok, state: ok, persist: ok, note: "DRAFT evaluations only (submitted ones answer InvalidRequestException); answers merge by question RefId and are type-checked against the pinned form (SINGLESELECT option RefId in StringValue, MULTISELECT RefIds in StringValues, NUMERIC within the question range, DATETIME DateTimeValue, TEXT StringValue; NotApplicable always accepted), notes must belong to questions; UpdatedBy sets EvaluatorArn"}
  SubmitContactEvaluation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same answer handling as update, then scores and locks the evaluation as SUBMITTED. Score (emulator approximation, unverified): with an ENABLED ScoringStrategy, EarnedPoints/MaxBasePoint sum the chosen and best option scores of answered scored questions (SINGLESELECT and NUMERIC options), NotApplicable answers are skipped, Percentage = earned/max, an AutomaticFail option zeroes the percentage; section scores use the same rule per enclosing section; question and section weights, PerformanceCategory, required-answer checks and review flows are not applied"}
  DescribeContactEvaluation: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns the evaluation and the pinned form version content; ContactAgentId is the contact's current agent"}
  ListContactEvaluations: {wire: ok, errors: ok, state: ok, persist: ok, note: "evaluations of the contact newest first; NextToken is always empty, as the SDK docs say"}
  DeleteContactEvaluation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMetric: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMetric: {wire: ok, errors: ok, state: ok, persist: ok, note: "only customer-managed metrics exist; AWS-managed metric IDs answer ResourceNotFoundException; CreatedUser/LastModifiedUser are not returned (the caller identity is unknown)"}
  ListMetrics: {wire: ok, errors: ok, state: ok, persist: ok, note: "type=AWS_MANAGED lists nothing (catalog not modeled); CUSTOMER_MANAGED or no filter lists the customer-managed metrics sorted by name"}
  SearchMetrics: {wire: ok, errors: ok, state: ok, persist: ok, note: "string conditions on name and description, tag filter; boolean conditions are not applicable"}
  UpdateMetricContent: {wire: ok, errors: ok, state: ok, persist: ok, note: "partial update of calculation, unit and trend indicator; no versioning ($LATEST/$SAVED ARN qualifiers are not modeled)"}
  UpdateMetricMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
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
  DescribeContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "ContactAssociationId is the InitialContactId (the root of the contact tree, API_Contact); TaskTemplateInfo (Arn, Name) comes from the task template; ContactEvaluations is keyed by FormId (API_ContactEvaluation) with the latest evaluation of each form, IN_PROGRESS for a draft and COMPLETE with EndTimestamp once submitted (deleted evaluations are removed, so DELETED never appears)"}
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
  DescribeQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "also describes a user's agent queue: enabled, no name or hours (the shape of an agent queue is not documented)"}
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
  GetCurrentUserData: {wire: ok, errors: ok, state: ok, persist: ok, note: "status, slots and CONNECTED contacts per agent (ContactFilter.ContactStates also selects ENDED: contacts the agent handled that ended and were not dismissed, StateStartTimestamp = DisconnectTimestamp; MISSED, ERROR and REJECTED never occur); filters Agents, Queues, RoutingProfiles, UserHierarchyGroups"}
  DismissUserContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "API_DismissUserContact allows MISSED, ERROR, ENDED or REJECTED contacts; the emulator only has ENDED (a disconnected contact of that agent), anything else (still connected, other agent's contact, already dismissed) is InvalidRequestException with message texts unverified; unknown user or contact is ResourceNotFoundException; the dismissal is stored on the contact"}
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
  ListQueues: {wire: ok, errors: ok, state: ok, persist: ok, note: "STANDARD queues plus one AGENT queue per user (concepts-queues-standard-and-agent: created automatically with the agent); the agent queue id and ARN are derived from the user id and the summary carries no Name, which the docs do not specify for agent queues; no QueueTypes lists both"}
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
  - "Search*: SearchContactEvaluations is not routed (the number, boolean, date-time and decimal condition fields and the ContactEvaluationAttributeFilter are not published)"
  - "EvaluationForms: ListEvaluationFormAIVersions is not routed: the AI version catalog is service-owned and API_ListEvaluationFormAIVersions shows only one sample response (AGENT_EVALUATION_2025-11-12 and 2025-09-01), so a catalog would be fabricated; contact evaluations are never auto-evaluated, calibrated, reviewed or acknowledged, and their scoring is an approximation"
  - "TestCases: executions are recorded (Start/Stop/List/ListRecords/GetSummary) but never leave INITIATED on their own, because the emulator has no flow test runner: no PASSED/FAILED outcome, observations or ObservationSummary are ever produced"
  - "Notifications: SendOutboundWebNotification is not routed (web widget delivery has no read-back state in the emulator); push registrations are stored but no push is ever sent; notifications from rules or the system are not generated"
  - "Metrics definitions: the AWS-managed metric catalog is not modeled (ListMetrics type=AWS_MANAGED is empty)"
  - "AttachedFiles: uploads and downloads use an emulator endpoint (/attached-files-content) rather than S3 pre-signed URLs, with no virus scan; Cases are not modeled so Cases ARNs are only shape-checked"
  - "Analytics: ListAnalyticsDataLakeDataSets and StartContactConversationalAnalyticsJob are not routed (the dataset catalog is not published; the job needs Contact Lens); associations are recorded without validating DataSetId against a catalog and without a RAM emulator"
  - "Contact media and call control: StartAssistantContact StartScreenSharing StartWebRTCContact TransferContact are not routed (no media plane)"
  - "Contact data and participants: GetFederationToken ListRealtimeContactAnalysisSegmentsV2 SendChatIntegrationEvent StartOutboundChatContact UpdateParticipantAuthentication are not routed (no chat message plane, no caller identity for federation); UpdateParticipantRoleConfig timers are stored on the contact but nothing enforces them (no chat message plane); QueuePriority/QueueTimeAdjustmentSeconds/RoutingCriteria are stored but no routing engine reorders queues from them"
  - "Instance extras: GetCrossRegionRouting UpdateCrossRegionRouting ReplicateInstance are not routed: they need Global Resiliency instance replicas, which the emulator does not model"
  - "GetMetricDataV2: only the documented subset of metrics is computed (agent-state, flow, campaign, evaluation, case, AI and Contact Lens metrics are not modeled); existing custom metric formulas are not evaluated"
  - "PhoneNumber import: ImportPhoneNumber needs an AWS End User Messaging phone-number inventory that the emulator does not have; a number could only be fabricated"
  - "Contact expiry (ExpiryDurationInMinutes / connect:ContactExpiry), SES-wired email delivery status readback and TEMPLATE outbound email (Wisdom message templates) are not modeled"
  - "Contact.QualityMetrics, DisconnectDetails, Customer, CustomerVoiceActivity, ConnectedToSystemTimestamp, NextContacts (the NextContactEntry types are not documented), ContactDetails, WisdomInfo, GlobalResiliencyMetadata, Cases and the other contact analytics members are never populated (ContactAssociationId, TaskTemplateInfo, ContactEvaluations, QueuePriority, QueueTimeAdjustmentSeconds and RoutingCriteria are); Recordings (start/stop times, no audio), ChatMetrics (conversation close time and MultiParty only: there is no chat message plane for message counts) and the pause fields are populated"
  - "CreateInstance quota: API_CreateInstance documents only a 30-day create/delete limit and ServiceQuotaExceededException (402), not the per-Region count; no instance limit is enforced"
  - "CloudFormation: AWS::Connect::AWSManagedView is not provisioned (no page for it exists in the CloudFormation resource reference, so its properties and attributes cannot be verified) and AWS::Connect::DataLakeAssociation is not provisioned (it is a RAM share of an analytics dataset and the analytics association operations above are unrouted); AWS::Connect::Metric GetAtt CreationMethod, EffectiveTime, Filters, Groupings, PrimaryEventSource, PrimaryEventSourceEffectiveTimestampType, RefreshRate and SupportedStats have no backing state; Pages, Media and Associations of Workspace, HoursOfOperationOverrides, ParentHoursOfOperations and ChildHoursOfOperations of HoursOfOperation, ManualAssignmentQueueConfigs of RoutingProfile, AdditionalEmailAddresses of Queue and SourcePhoneNumberArn of PhoneNumber are rejected"
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
- CloudFormation (docs: AWS CloudFormation Template Reference, AWS::Connect::*): ContactFlowVersion and ContactFlowModuleVersion Ref `<ARN>:<version>` with GetAtt Version and the content SHA-256; ContactFlowModuleAlias Ref `<instance ID>|<module ID>|<alias ID>` (the pages state no Ref) with GetAtt AliasId and ContactFlowModuleAliasARN; DataTableRecord Ref `<instance ID>|<table ID>|<record ID>` with GetAtt RecordId, an update replaces the record's primary and other values (values left out are removed, an emulator reading of the declarative resource); Notification Ref/Arn the notification ARN, Content keys EnUS...ZhTW map to the LocaleCode values (en_US...zh_TW), ExpiresAt is read as epoch seconds (the pattern is digits only); TestCase and Metric Ref the ARN (the pages state no Ref); UserHierarchyStructure Ref and GetAtt UserHierarchyStructureArn are the instance ARN (the API has no identifier for the structure) and deleting it leaves the levels, since no delete operation exists.

## 2026-11-13 open-item re-check

API_ListEvaluationFormAIVersions re-read: ContactInteractionType (AGENT|AUTOMATED|CUSTOMER) is required, but the only catalog data is one AGENT sample (AGENT_EVALUATION_2025-11-12 Active, 2025-09-01 Deprecated); AUTOMATED/CUSTOMER versions are not published, so the op stays unrouted.

## 2026-11-13 parity pass

- UpdateContactRoutingData: body shape and the QueuePriority-or-QueueTimeAdjustmentSeconds rule from API_UpdateContactRoutingData; first step ACTIVE, others INACTIVE (types.RoutingCriteriaStepStatus).
- UpdateParticipantRoleConfig: timers 2-480 minutes or Unset (API_ParticipantTimerValue), chat contacts only.
- Test case executions: API_StartTestCaseExecution (published test case only), API_StopTestCaseExecution; list query names from the SDK schemas (startTime/endTime epoch milliseconds).
- Analytics associations: API_AssociateAnalyticsDataSet and API_BatchDisassociateAnalyticsDataSet; ResourceShareStatus ACTIVE and repeated associations returning the existing share are unverified.
- GetPromptFile: signed URL on the emulator content endpoint serving the prompt WAV from S3 (5 minute expiry unverified).
