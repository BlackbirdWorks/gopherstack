package cleanrooms

const (
	statusActive    = "ACTIVE"
	errCodeNotFound = "ResourceNotFoundException"
	errMsgNotFound  = "not found"

	protectedQueryStatusSuccess = "SUCCESS"
	protectedJobStatusSuccess   = "SUCCESS"
	mockDurationMillis          = 100

	// statusCancelled is shared by ProtectedQueryStatus, ProtectedJobStatus,
	// and ChangeRequestStatus -- all three real AWS enums use the literal
	// string "CANCELLED".
	statusCancelled = "CANCELLED"
	// changeRequestActionCancel is the ChangeRequestAction value shared by
	// both PENDING and APPROVED transitions in changeRequestNextStatus.
	changeRequestActionCancel = "CANCEL"

	// ChangeSpecificationType enum values (types.ChangeSpecificationTypeMember/
	// -Collaboration).
	changeSpecTypeMember        = "MEMBER"
	changeSpecTypeCollaboration = "COLLABORATION"

	// IntermediateTableStatus values (types.IntermediateTableStatus).
	// BASE_TABLE_REMOVED and RETENTION_PERIOD_EXPIRED are real enum values
	// this backend never assigns (they require, respectively, tracking base
	// table lifecycle across other members' configured tables and a
	// time-based expiry sweep -- neither is modeled, see PARITY.md).
	itStatusCreated                  = "CREATED"
	itStatusPopulateStarted          = "POPULATE_STARTED"
	itStatusPopulateSuccess          = "POPULATE_SUCCESS"
	itStatusPopulateFailed           = "POPULATE_FAILED"
	itStatusDisallowedByDataProvider = "DISALLOWED_BY_DATA_PROVIDER"

	// IntermediateTableVersionStatus values (types.IntermediateTableVersionStatus).
	itVersionStatusPopulateStarted = "POPULATE_STARTED"
	itVersionStatusPopulateSuccess = "POPULATE_SUCCESS"
	itVersionStatusPopulateFailed  = "POPULATE_FAILED"

	// analysisTypeQuery is the sole real value of
	// types.PopulateIntermediateTableAnalysisType.
	analysisTypeQuery = "QUERY"

	// privacyBudgetAutoRefreshNone is the PrivacyBudgetTemplateAutoRefresh
	// value CreatePrivacyBudgetTemplate defaults to when the (optional-on-input,
	// required-on-output) autoRefresh field is unspecified -- the only other
	// valid enum value besides "CALENDAR_MONTH".
	privacyBudgetAutoRefreshNone = "NONE"

	// secondsPerDay converts IntermediateTable.RetentionInDays into an
	// epoch-seconds expirationTime offset.
	secondsPerDay = 86400
)

type MemberSpec struct {
	PaymentConfig map[string]any `json:"paymentConfiguration,omitempty"`
	AccountID     string         `json:"accountId"`
	DisplayName   string         `json:"displayName"`
	Abilities     []string       `json:"memberAbilities"`
}

// MemberSummary is the wire shape returned by ListMembers. Verified against
// aws-sdk-go-v2/service/cleanrooms@v1.45.6's
// awsRestjson1_deserializeDocumentMemberSummary: real keys are abilities,
// accountId, createTime, displayName, membershipArn, membershipId,
// mlAbilities (not modeled -- ML abilities are out of scope for this
// emulator), paymentConfiguration, status, updateTime.
type MemberSummary struct {
	PaymentConfig map[string]any `json:"paymentConfiguration"`
	AccountID     string         `json:"accountId"`
	DisplayName   string         `json:"displayName"`
	Status        string         `json:"status"`
	MembershipArn string         `json:"membershipArn,omitempty"`
	MembershipID  string         `json:"membershipId,omitempty"`
	Abilities     []string       `json:"abilities"`
	CreateTime    float64        `json:"createTime,omitempty"`
	UpdateTime    float64        `json:"updateTime,omitempty"`
}

// Collaboration is the wire shape returned by CreateCollaboration/
// GetCollaboration/UpdateCollaboration. Verified against
// awsRestjson1_deserializeDocumentCollaboration: real keys are
// allowedResultRegions, analyticsEngine, arn, autoApprovedChangeTypes,
// createTime, creatorAccountId, creatorDisplayName, dataEncryptionMetadata,
// description, id, isMetricsEnabled, jobLogStatus, membershipArn,
// membershipId, memberStatus, name, queryLogStatus, updateTime.
//
// There is NO "collaborationIdentifier", "memberAbilities", "members", or
// "tags" key on this shape in the real API -- members come only from
// ListMembers, tags only from ListTagsForResource, and
// "collaborationIdentifier" is exclusively a *request* parameter name (used
// by Get/Update/Delete/etc, see the handler_*.go request DTOs), never a
// response field. CollaborationIdentifier/MemberAbilities/Members/Tags are
// kept as Go-only bookkeeping fields (json:"-") since they back real
// internal behavior (composite-key derivation, ListMembers/DeleteMember
// backing store, tagsByArn population at create time) -- only their
// wire presence was invented.
type Collaboration struct {
	Tags                    map[string]string `json:"-"`
	MemberStatus            string            `json:"memberStatus"`
	MembershipArn           string            `json:"membershipArn,omitempty"`
	Arn                     string            `json:"arn"`
	Name                    string            `json:"name"`
	Description             string            `json:"description,omitempty"`
	CreatorAccountID        string            `json:"creatorAccountId"`
	ID                      string            `json:"id"`
	CreatorDisplayName      string            `json:"creatorDisplayName"`
	QueryLogStatus          string            `json:"queryLogStatus,omitempty"`
	CollaborationIdentifier string            `json:"-"`
	MembershipID            string            `json:"membershipId,omitempty"`
	MemberAbilities         []string          `json:"-"`
	Members                 []*MemberSummary  `json:"members,omitempty"`
	AutoApprovedChangeTypes []string          `json:"autoApprovedChangeTypes,omitempty"`
	CreateTime              float64           `json:"createTime,omitempty"`
	UpdateTime              float64           `json:"updateTime,omitempty"`
}

// CollaborationSummary is the wire shape returned by ListCollaborations.
// Verified against awsRestjson1_deserializeDocumentCollaborationSummary:
// real keys are analyticsEngine, arn, createTime, creatorAccountId,
// creatorDisplayName, id, membershipArn, membershipId, memberStatus, name,
// updateTime. No "collaborationIdentifier" key (see Collaboration doc).
type CollaborationSummary struct {
	CollaborationIdentifier string  `json:"-"`
	ID                      string  `json:"id"`
	Arn                     string  `json:"arn"`
	Name                    string  `json:"name"`
	CreatorAccountID        string  `json:"creatorAccountId"`
	CreatorDisplayName      string  `json:"creatorDisplayName"`
	MemberStatus            string  `json:"memberStatus"`
	MembershipArn           string  `json:"membershipArn,omitempty"`
	MembershipID            string  `json:"membershipId,omitempty"`
	CreateTime              float64 `json:"createTime,omitempty"`
	UpdateTime              float64 `json:"updateTime,omitempty"`
}

// Membership is the wire shape for CreateMembership/GetMembership/
// UpdateMembership. Verified against
// awsRestjson1_deserializeDocumentMembership: real keys are arn,
// collaborationArn, collaborationCreatorAccountId,
// collaborationCreatorDisplayName, collaborationId, collaborationName,
// createTime, defaultJobResultConfiguration (not modeled),
// defaultResultConfiguration, id, isMetricsEnabled (not modeled),
// jobLogStatus (not modeled), memberAbilities, mlMemberAbilities (not
// modeled), paymentConfiguration, queryLogStatus, status, updateTime. No
// "membershipIdentifier" or "collaborationIdentifier" key (those are
// request-only parameter names).
type Membership struct {
	DefaultResultConfiguration      map[string]any `json:"defaultResultConfiguration,omitempty"`
	PaymentConfiguration            map[string]any `json:"paymentConfiguration"`
	QueryLogStatus                  string         `json:"queryLogStatus,omitempty"`
	CollaborationIdentifier         string         `json:"-"`
	CollaborationCreatorAccountID   string         `json:"collaborationCreatorAccountId"`
	CollaborationCreatorDisplayName string         `json:"collaborationCreatorDisplayName"`
	MembershipIdentifier            string         `json:"-"`
	Status                          string         `json:"status"`
	CollaborationName               string         `json:"collaborationName"`
	CollaborationArn                string         `json:"collaborationArn"`
	Arn                             string         `json:"arn"`
	CollaborationID                 string         `json:"collaborationId"`
	ID                              string         `json:"id"`
	MemberAbilities                 []string       `json:"memberAbilities"`
	UpdateTime                      float64        `json:"updateTime,omitempty"`
	CreateTime                      float64        `json:"createTime,omitempty"`
}

// MembershipSummary is the wire shape for ListMemberships. Verified against
// awsRestjson1_deserializeDocumentMembershipSummary: same key set as
// Membership minus defaultResultConfiguration/defaultJobResultConfiguration/
// isMetricsEnabled/jobLogStatus.
type MembershipSummary struct {
	PaymentConfiguration            map[string]any `json:"paymentConfiguration"`
	CollaborationName               string         `json:"collaborationName"`
	Arn                             string         `json:"arn"`
	CollaborationIdentifier         string         `json:"-"`
	CollaborationArn                string         `json:"collaborationArn"`
	CollaborationCreatorAccountID   string         `json:"collaborationCreatorAccountId"`
	CollaborationCreatorDisplayName string         `json:"collaborationCreatorDisplayName"`
	MembershipIdentifier            string         `json:"-"`
	Status                          string         `json:"status"`
	ID                              string         `json:"id"`
	CollaborationID                 string         `json:"collaborationId"`
	MemberAbilities                 []string       `json:"memberAbilities"`
	CreateTime                      float64        `json:"createTime,omitempty"`
	UpdateTime                      float64        `json:"updateTime,omitempty"`
}

// ConfiguredTable is the wire shape for CreateConfiguredTable/GetConfiguredTable/
// UpdateConfiguredTable (ConfiguredTableSummary is its List shape). Verified against
// awsRestjson1_deserializeDocumentConfiguredTable(Summary): real keys use
// "id", never "configuredTableIdentifier" (request-parameter-only name).
// selectedAnalysisMethods is not modeled (deferred, see PARITY.md).
type ConfiguredTable struct {
	TableReference            map[string]any    `json:"tableReference,omitempty"`
	Tags                      map[string]string `json:"-"`
	ConfiguredTableIdentifier string            `json:"-"`
	Arn                       string            `json:"arn"`
	Name                      string            `json:"name"`
	Description               string            `json:"description,omitempty"`
	AnalysisMethod            string            `json:"analysisMethod,omitempty"`
	ID                        string            `json:"id"`
	AllowedColumns            []string          `json:"allowedColumns"`
	AnalysisRuleTypes         []string          `json:"analysisRuleTypes"`
	CreateTime                float64           `json:"createTime,omitempty"`
	UpdateTime                float64           `json:"updateTime,omitempty"`
}

type ConfiguredTableSummary struct {
	ConfiguredTableIdentifier string   `json:"-"`
	Arn                       string   `json:"arn"`
	Name                      string   `json:"name"`
	AnalysisMethod            string   `json:"analysisMethod,omitempty"`
	ID                        string   `json:"id"`
	AnalysisRuleTypes         []string `json:"analysisRuleTypes"`
	CreateTime                float64  `json:"createTime,omitempty"`
	UpdateTime                float64  `json:"updateTime,omitempty"`
}

// ConfiguredTableAnalysisRule verified against
// awsRestjson1_deserializeDocumentConfiguredTableAnalysisRule: real keys are
// configuredTableArn, configuredTableId, createTime, policy, type,
// updateTime. No standalone "id" and no "configuredTableIdentifier".
type ConfiguredTableAnalysisRule struct {
	Policy                    map[string]any `json:"policy,omitempty"`
	ConfiguredTableIdentifier string         `json:"-"`
	ConfiguredTableArn        string         `json:"configuredTableArn"`
	Type                      string         `json:"type"`
	ConfiguredTableID         string         `json:"configuredTableId"`
	CreateTime                float64        `json:"createTime,omitempty"`
	UpdateTime                float64        `json:"updateTime,omitempty"`
}

// ConfiguredTableAssociation is the wire shape for CreateConfiguredTableAssociation/
// Get/UpdateConfiguredTableAssociation (Summary is its List shape). Verified against
// awsRestjson1_deserializeDocumentConfiguredTableAssociation(Summary): real
// keys use "id"/"configuredTableId"/"membershipId", never the "*Identifier"
// forms (request-parameter-only names). Summary additionally has no
// configuredTableArn/description/roleArn (full-resource-only fields).
type ConfiguredTableAssociation struct {
	Tags                                 map[string]string `json:"-"`
	RoleArn                              string            `json:"roleArn,omitempty"`
	Name                                 string            `json:"name"`
	MembershipArn                        string            `json:"membershipArn"`
	ConfiguredTableIdentifier            string            `json:"-"`
	ConfiguredTableArn                   string            `json:"configuredTableArn"`
	ConfiguredTableAssociationIdentifier string            `json:"-"`
	MembershipIdentifier                 string            `json:"-"`
	ConfiguredTableID                    string            `json:"configuredTableId"`
	Description                          string            `json:"description,omitempty"`
	MembershipID                         string            `json:"membershipId"`
	Arn                                  string            `json:"arn"`
	ID                                   string            `json:"id"`
	AnalysisRuleTypes                    []string          `json:"analysisRuleTypes"`
	UpdateTime                           float64           `json:"updateTime,omitempty"`
	CreateTime                           float64           `json:"createTime,omitempty"`
}

type ConfiguredTableAssociationSummary struct {
	ConfiguredTableAssociationIdentifier string  `json:"-"`
	Arn                                  string  `json:"arn"`
	MembershipIdentifier                 string  `json:"-"`
	MembershipArn                        string  `json:"membershipArn"`
	ConfiguredTableIdentifier            string  `json:"-"`
	Name                                 string  `json:"name"`
	ID                                   string  `json:"id"`
	MembershipID                         string  `json:"membershipId"`
	ConfiguredTableID                    string  `json:"configuredTableId"`
	CreateTime                           float64 `json:"createTime,omitempty"`
	UpdateTime                           float64 `json:"updateTime,omitempty"`
}

// ConfiguredTableAssociationAnalysisRule verified against
// awsRestjson1_deserializeDocumentConfiguredTableAssociationAnalysisRule:
// real keys are configuredTableAssociationArn, configuredTableAssociationId
// (NOT "...Identifier"), createTime, membershipIdentifier (this one IS
// spelled "membershipIdentifier" in the real API -- confirmed against the
// deserializer, an intentional asymmetry vs every other type in this
// service), policy, type, updateTime. There is no "membershipArn" key for
// this specific nested shape (it was previously fabricated here).
type ConfiguredTableAssociationAnalysisRule struct {
	Policy                               map[string]any `json:"policy,omitempty"`
	ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationId"`
	ConfiguredTableAssociationArn        string         `json:"configuredTableAssociationArn"`
	MembershipIdentifier                 string         `json:"membershipIdentifier"`
	MembershipArn                        string         `json:"-"`
	Type                                 string         `json:"type"`
	CreateTime                           float64        `json:"createTime,omitempty"`
	UpdateTime                           float64        `json:"updateTime,omitempty"`
}

// AnalysisTemplate is the wire shape for CreateAnalysisTemplate/Get/
// UpdateAnalysisTemplate (Summary is its List shape). Verified against
// awsRestjson1_deserializeDocumentAnalysisTemplate(Summary): real keys use
// "id"/"collaborationId"/"membershipId", never the "*Identifier" forms
// (request-parameter-only names). errorMessageConfiguration,
// sourceMetadata, syntheticDataParameters, validations (full-resource) and
// isSyntheticData (summary) are not modeled (deferred, see PARITY.md).
type AnalysisTemplate struct {
	Source                     map[string]any    `json:"source,omitempty"`
	Tags                       map[string]string `json:"-"`
	Schema                     map[string]any    `json:"schema,omitempty"`
	AnalysisTemplateIdentifier string            `json:"-"`
	Format                     string            `json:"format,omitempty"`
	MembershipArn              string            `json:"membershipArn"`
	Name                       string            `json:"name"`
	Description                string            `json:"description,omitempty"`
	CollaborationIdentifier    string            `json:"-"`
	CollaborationArn           string            `json:"collaborationArn"`
	MembershipIdentifier       string            `json:"-"`
	Arn                        string            `json:"arn"`
	CollaborationID            string            `json:"collaborationId"`
	MembershipID               string            `json:"membershipId"`
	ID                         string            `json:"id"`
	AnalysisParameters         []map[string]any  `json:"analysisParameters,omitempty"`
	UpdateTime                 float64           `json:"updateTime,omitempty"`
	CreateTime                 float64           `json:"createTime,omitempty"`
}

type AnalysisTemplateSummary struct {
	AnalysisTemplateIdentifier string  `json:"-"`
	Arn                        string  `json:"arn"`
	CollaborationArn           string  `json:"collaborationArn"`
	CollaborationIdentifier    string  `json:"-"`
	MembershipIdentifier       string  `json:"-"`
	MembershipArn              string  `json:"membershipArn"`
	Name                       string  `json:"name"`
	ID                         string  `json:"id"`
	MembershipID               string  `json:"membershipId"`
	CollaborationID            string  `json:"collaborationId"`
	CreateTime                 float64 `json:"createTime,omitempty"`
	UpdateTime                 float64 `json:"updateTime,omitempty"`
}

// CollaborationAnalysisTemplateSummary is the wire shape for
// ListCollaborationAnalysisTemplates (types.CollaborationAnalysisTemplateSummary).
// Unlike AnalysisTemplateSummary it carries creatorAccountId, not
// membershipArn/membershipId -- there is no single membership at
// collaboration scope.
type CollaborationAnalysisTemplateSummary struct {
	Arn              string  `json:"arn"`
	CollaborationArn string  `json:"collaborationArn"`
	CollaborationID  string  `json:"collaborationId"`
	CreatorAccountID string  `json:"creatorAccountId"`
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	CreateTime       float64 `json:"createTime,omitempty"`
	UpdateTime       float64 `json:"updateTime,omitempty"`
}

type BatchError struct {
	Arn     string `json:"arn,omitempty"`
	Name    string `json:"name,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Schema is the wire shape for GetSchema (SchemaSummary is its List shape).
// Verified against
// awsRestjson1_deserializeDocumentSchema(Summary): real keys use
// "collaborationId", never "collaborationIdentifier" (request-parameter-only
// name). resourceArn, schemaStatusDetails, schemaTypeProperties,
// selectedAnalysisMethods are not modeled (deferred, see PARITY.md; there is
// no Create path for schemas in this backend at all, so these fields are
// never populated regardless).
type Schema struct {
	CollaborationArn        string           `json:"collaborationArn"`
	CollaborationIdentifier string           `json:"-"`
	CollaborationID         string           `json:"collaborationId"`
	CreatorAccountID        string           `json:"creatorAccountId"`
	Name                    string           `json:"name"`
	Type                    string           `json:"type"`
	AnalysisMethod          string           `json:"analysisMethod,omitempty"`
	Columns                 []map[string]any `json:"columns,omitempty"`
	PartitionKeys           []map[string]any `json:"partitionKeys,omitempty"`
	AnalysisRuleTypes       []string         `json:"analysisRuleTypes,omitempty"`
	CreateTime              float64          `json:"createTime,omitempty"`
	UpdateTime              float64          `json:"updateTime,omitempty"`
}

type SchemaSummary struct {
	CollaborationArn        string   `json:"collaborationArn"`
	CollaborationIdentifier string   `json:"-"`
	CollaborationID         string   `json:"collaborationId"`
	CreatorAccountID        string   `json:"creatorAccountId"`
	Name                    string   `json:"name"`
	Type                    string   `json:"type"`
	AnalysisMethod          string   `json:"analysisMethod,omitempty"`
	AnalysisRuleTypes       []string `json:"analysisRuleTypes,omitempty"`
	CreateTime              float64  `json:"createTime,omitempty"`
	UpdateTime              float64  `json:"updateTime,omitempty"`
}

// SchemaAnalysisRule is a simplified stand-in: the real GetSchemaAnalysisRule
// response wraps a types.AnalysisRule union (a deeper shape keyed by
// analysis-rule "type", distinct from the ConfiguredTable/
// ConfiguredTableAssociation analysis-rule shapes), which this backend does
// not model precisely (deferred, see PARITY.md). Since schemas are never
// created in this backend (no Create path exists, see Schema doc), this
// type's wire shape is currently unreachable in practice either way.
type SchemaAnalysisRule struct {
	Policy                  map[string]any `json:"policy,omitempty"`
	CollaborationArn        string         `json:"collaborationArn"`
	CollaborationIdentifier string         `json:"-"`
	CollaborationID         string         `json:"collaborationId"`
	Name                    string         `json:"name"`
	Type                    string         `json:"type"`
	CreateTime              float64        `json:"createTime,omitempty"`
	UpdateTime              float64        `json:"updateTime,omitempty"`
}

// ProtectedQuery is the wire shape for StartProtectedQuery/GetProtectedQuery
// (Summary is its List shape). Verified against
// awsRestjson1_deserializeDocumentProtectedQuery(Summary): real keys use
// "id"/"membershipId", never "membershipIdentifier" (request-parameter-only
// name). differentialPrivacy, queryComputePayerAccountId (both),
// receiverConfigurations (summary) are not modeled (deferred, see
// PARITY.md).
type ProtectedQuery struct {
	SQLParameters        map[string]any `json:"sqlParameters,omitempty"`
	ResultConfiguration  map[string]any `json:"resultConfiguration,omitempty"`
	ComputeConfiguration map[string]any `json:"computeConfiguration,omitempty"`
	Statistics           map[string]any `json:"statistics,omitempty"`
	Result               map[string]any `json:"result,omitempty"`
	Error                map[string]any `json:"error,omitempty"`
	ID                   string         `json:"id"`
	MembershipIdentifier string         `json:"-"`
	MembershipArn        string         `json:"membershipArn"`
	Status               string         `json:"status"`
	MembershipID         string         `json:"membershipId"`
	CreateTime           float64        `json:"createTime,omitempty"`
}

type ProtectedQuerySummary struct {
	ID                   string  `json:"id"`
	MembershipIdentifier string  `json:"-"`
	MembershipArn        string  `json:"membershipArn"`
	Status               string  `json:"status"`
	MembershipID         string  `json:"membershipId"`
	CreateTime           float64 `json:"createTime,omitempty"`
}

// ProtectedJob is the wire shape for StartProtectedJob/GetProtectedJob
// (Summary is its List shape). Verified against
// awsRestjson1_deserializeDocumentProtectedJob(Summary): same pattern as
// ProtectedQuery. jobComputePayerAccountId (both),
// receiverConfigurations (summary) are not modeled (deferred).
type ProtectedJob struct {
	JobParameters        map[string]any `json:"jobParameters,omitempty"`
	ResultConfiguration  map[string]any `json:"resultConfiguration,omitempty"`
	Statistics           map[string]any `json:"statistics,omitempty"`
	Result               map[string]any `json:"result,omitempty"`
	Error                map[string]any `json:"error,omitempty"`
	ID                   string         `json:"id"`
	MembershipIdentifier string         `json:"-"`
	MembershipArn        string         `json:"membershipArn"`
	Status               string         `json:"status"`
	Type                 string         `json:"type"`
	MembershipID         string         `json:"membershipId"`
	CreateTime           float64        `json:"createTime,omitempty"`
}

type ProtectedJobSummary struct {
	ID                   string  `json:"id"`
	MembershipIdentifier string  `json:"-"`
	MembershipArn        string  `json:"membershipArn"`
	Status               string  `json:"status"`
	Type                 string  `json:"type"`
	MembershipID         string  `json:"membershipId"`
	CreateTime           float64 `json:"createTime,omitempty"`
}

// PrivacyBudgetTemplate is the wire shape for CreatePrivacyBudgetTemplate/Get/
// UpdatePrivacyBudgetTemplate (Summary is its List shape). Verified against
// awsRestjson1_deserializeDocumentPrivacyBudgetTemplate(Summary): real keys
// use "id"/"collaborationId"/"membershipId", never the "*Identifier" forms.
//
// AutoRefresh is required on this output shape but optional on
// CreatePrivacyBudgetTemplateInput -- a real client can create a template
// without specifying it (gopherstack-r80d). CreatePrivacyBudgetTemplate
// defaults an unspecified value to "NONE", the only other valid enum value
// besides "CALENDAR_MONTH" and the natural off-state for an opt-in refresh
// schedule -- not fabricated data, since there is no third possibility.
type PrivacyBudgetTemplate struct {
	Parameters                      map[string]any    `json:"parameters,omitempty"`
	Tags                            map[string]string `json:"-"`
	MembershipArn                   string            `json:"membershipArn"`
	Arn                             string            `json:"arn"`
	CollaborationArn                string            `json:"collaborationArn"`
	CollaborationIdentifier         string            `json:"-"`
	PrivacyBudgetTemplateIdentifier string            `json:"-"`
	MembershipIdentifier            string            `json:"-"`
	PrivacyBudgetType               string            `json:"privacyBudgetType"`
	AutoRefresh                     string            `json:"autoRefresh"`
	ID                              string            `json:"id"`
	MembershipID                    string            `json:"membershipId"`
	CollaborationID                 string            `json:"collaborationId"`
	CreateTime                      float64           `json:"createTime,omitempty"`
	UpdateTime                      float64           `json:"updateTime,omitempty"`
}

type PrivacyBudgetTemplateSummary struct {
	PrivacyBudgetTemplateIdentifier string  `json:"-"`
	Arn                             string  `json:"arn"`
	CollaborationArn                string  `json:"collaborationArn"`
	CollaborationIdentifier         string  `json:"-"`
	MembershipArn                   string  `json:"membershipArn"`
	MembershipIdentifier            string  `json:"-"`
	PrivacyBudgetType               string  `json:"privacyBudgetType"`
	ID                              string  `json:"id"`
	MembershipID                    string  `json:"membershipId"`
	CollaborationID                 string  `json:"collaborationId"`
	CreateTime                      float64 `json:"createTime,omitempty"`
	UpdateTime                      float64 `json:"updateTime,omitempty"`
}

// CollaborationPrivacyBudgetTemplateSummary is the wire shape for
// ListCollaborationPrivacyBudgetTemplates
// (types.CollaborationPrivacyBudgetTemplateSummary). Carries
// creatorAccountId, not membershipArn/membershipId.
type CollaborationPrivacyBudgetTemplateSummary struct {
	Arn               string  `json:"arn"`
	CollaborationArn  string  `json:"collaborationArn"`
	CollaborationID   string  `json:"collaborationId"`
	CreatorAccountID  string  `json:"creatorAccountId"`
	PrivacyBudgetType string  `json:"privacyBudgetType"`
	ID                string  `json:"id"`
	CreateTime        float64 `json:"createTime,omitempty"`
	UpdateTime        float64 `json:"updateTime,omitempty"`
}

// PrivacyBudget is the wire shape returned by ListPrivacyBudgets/
// ListCollaborationPrivacyBudgets (real AWS name: PrivacyBudgetSummary).
// Verified against awsRestjson1_deserializeDocumentPrivacyBudgetSummary: real
// keys are budget, collaborationArn, collaborationId, createTime, id,
// membershipArn, membershipId, privacyBudgetTemplateArn,
// privacyBudgetTemplateId, type, updateTime.
//
// FIXED (bd gopherstack-kiqa): this struct previously mislabeled
// PrivacyBudgetType's wire key as "privacyBudgetType" (the real key is
// "type") and additionally emitted invented duplicate
// collaborationIdentifier/membershipIdentifier/privacyBudgetTemplateIdentifier
// keys alongside the correctly-named .../Id fields -- the exact systemic
// invented-*Identifier bug class fixed everywhere else in this service
// (see CollaborationChangeRequest/Collaboration/etc doc comments), missed
// here. createTime/updateTime (both real, required) were entirely absent.
// A real SDK client would have received a "type"-less, extra-keyed,
// timestamp-less summary -- wire-incompatible on the required Type field.
type PrivacyBudget struct {
	Budget                          map[string]any `json:"budget,omitempty"`
	ID                              string         `json:"id"`
	PrivacyBudgetTemplateArn        string         `json:"privacyBudgetTemplateArn"`
	PrivacyBudgetTemplateIdentifier string         `json:"-"`
	CollaborationArn                string         `json:"collaborationArn"`
	CollaborationIdentifier         string         `json:"-"`
	MembershipArn                   string         `json:"membershipArn"`
	MembershipIdentifier            string         `json:"-"`
	PrivacyBudgetType               string         `json:"type"`
	MembershipID                    string         `json:"membershipId"`
	CollaborationID                 string         `json:"collaborationId"`
	PrivacyBudgetTemplateID         string         `json:"privacyBudgetTemplateId"`
	CreateTime                      float64        `json:"createTime,omitempty"`
	UpdateTime                      float64        `json:"updateTime,omitempty"`
}

// CollaborationPrivacyBudgetSummary is the wire shape for
// ListCollaborationPrivacyBudgets (types.CollaborationPrivacyBudgetSummary).
// Carries creatorAccountId, not membershipArn/membershipId.
type CollaborationPrivacyBudgetSummary struct {
	Budget                   map[string]any `json:"budget,omitempty"`
	ID                       string         `json:"id"`
	PrivacyBudgetTemplateArn string         `json:"privacyBudgetTemplateArn"`
	PrivacyBudgetTemplateID  string         `json:"privacyBudgetTemplateId"`
	CollaborationArn         string         `json:"collaborationArn"`
	CollaborationID          string         `json:"collaborationId"`
	CreatorAccountID         string         `json:"creatorAccountId"`
	PrivacyBudgetType        string         `json:"type"`
	CreateTime               float64        `json:"createTime,omitempty"`
	UpdateTime               float64        `json:"updateTime,omitempty"`
}

// IDMappingTable is the wire shape for CreateIdMappingTable/GetIdMappingTable
// (Summary is its List shape). Verified against
// awsRestjson1_deserializeDocumentIdMappingTable(Summary): real keys use
// "id"/"collaborationId"/"membershipId", never the "*Identifier" forms.
// Summary DOES include inputReferenceConfig (unlike some other Summary
// shapes in this service) but not inputReferenceProperties.
type IDMappingTable struct {
	InputReferenceConfig     map[string]any    `json:"inputReferenceConfig,omitempty"`
	Tags                     map[string]string `json:"-"`
	InputReferenceProperties map[string]any    `json:"inputReferenceProperties,omitempty"`
	IDMappingTableIdentifier string            `json:"-"`
	KmsKeyArn                string            `json:"kmsKeyArn,omitempty"`
	MembershipIdentifier     string            `json:"-"`
	Name                     string            `json:"name"`
	Description              string            `json:"description,omitempty"`
	CollaborationIdentifier  string            `json:"-"`
	CollaborationArn         string            `json:"collaborationArn"`
	MembershipArn            string            `json:"membershipArn"`
	Arn                      string            `json:"arn"`
	CollaborationID          string            `json:"collaborationId"`
	MembershipID             string            `json:"membershipId"`
	ID                       string            `json:"id"`
	UpdateTime               float64           `json:"updateTime,omitempty"`
	CreateTime               float64           `json:"createTime,omitempty"`
}

type IDMappingTableSummary struct {
	InputReferenceConfig     map[string]any `json:"inputReferenceConfig,omitempty"`
	IDMappingTableIdentifier string         `json:"-"`
	Arn                      string         `json:"arn"`
	CollaborationArn         string         `json:"collaborationArn"`
	CollaborationIdentifier  string         `json:"-"`
	MembershipArn            string         `json:"membershipArn"`
	MembershipIdentifier     string         `json:"-"`
	Name                     string         `json:"name"`
	ID                       string         `json:"id"`
	MembershipID             string         `json:"membershipId"`
	CollaborationID          string         `json:"collaborationId"`
	CreateTime               float64        `json:"createTime,omitempty"`
	UpdateTime               float64        `json:"updateTime,omitempty"`
}

// IDNamespaceAssociation is the wire shape for CreateIdNamespaceAssociation/Get/
// UpdateIdNamespaceAssociation (Summary is its List shape). Verified against
// awsRestjson1_deserializeDocumentIdNamespaceAssociation(Summary): real keys
// use "id"/"collaborationId"/"membershipId", never the "*Identifier" forms.
// Summary includes inputReferenceConfig AND inputReferenceProperties (only
// idMappingConfig is full-resource-only).
type IDNamespaceAssociation struct {
	InputReferenceConfig             map[string]any    `json:"inputReferenceConfig,omitempty"`
	Tags                             map[string]string `json:"-"`
	IDMappingConfig                  map[string]any    `json:"idMappingConfig,omitempty"`
	InputReferenceProperties         map[string]any    `json:"inputReferenceProperties,omitempty"`
	MembershipArn                    string            `json:"membershipArn"`
	MembershipIdentifier             string            `json:"-"`
	Name                             string            `json:"name"`
	Description                      string            `json:"description,omitempty"`
	CollaborationIdentifier          string            `json:"-"`
	IDNamespaceAssociationIdentifier string            `json:"-"`
	CollaborationArn                 string            `json:"collaborationArn"`
	Arn                              string            `json:"arn"`
	ID                               string            `json:"id"`
	MembershipID                     string            `json:"membershipId"`
	CollaborationID                  string            `json:"collaborationId"`
	CreateTime                       float64           `json:"createTime,omitempty"`
	UpdateTime                       float64           `json:"updateTime,omitempty"`
}

type IDNamespaceAssociationSummary struct {
	InputReferenceConfig             map[string]any `json:"inputReferenceConfig,omitempty"`
	InputReferenceProperties         map[string]any `json:"inputReferenceProperties,omitempty"`
	IDNamespaceAssociationIdentifier string         `json:"-"`
	Arn                              string         `json:"arn"`
	CollaborationArn                 string         `json:"collaborationArn"`
	CollaborationIdentifier          string         `json:"-"`
	MembershipArn                    string         `json:"membershipArn"`
	MembershipIdentifier             string         `json:"-"`
	Name                             string         `json:"name"`
	ID                               string         `json:"id"`
	MembershipID                     string         `json:"membershipId"`
	CollaborationID                  string         `json:"collaborationId"`
	CreateTime                       float64        `json:"createTime,omitempty"`
	UpdateTime                       float64        `json:"updateTime,omitempty"`
}

// CollaborationIDNamespaceAssociationSummary is the wire shape for
// ListCollaborationIdNamespaceAssociations
// (types.CollaborationIdNamespaceAssociationSummary). Carries
// creatorAccountId, not membershipArn/membershipId.
type CollaborationIDNamespaceAssociationSummary struct {
	InputReferenceConfig     map[string]any `json:"inputReferenceConfig,omitempty"`
	InputReferenceProperties map[string]any `json:"inputReferenceProperties,omitempty"`
	Arn                      string         `json:"arn"`
	CollaborationArn         string         `json:"collaborationArn"`
	CollaborationID          string         `json:"collaborationId"`
	CreatorAccountID         string         `json:"creatorAccountId"`
	Name                     string         `json:"name"`
	ID                       string         `json:"id"`
	CreateTime               float64        `json:"createTime,omitempty"`
	UpdateTime               float64        `json:"updateTime,omitempty"`
}

// ConfiguredAudienceModelAssociation is the wire shape for
// CreateConfiguredAudienceModelAssociation (Summary is its List shape). Verified against
// awsRestjson1_deserializeDocumentConfiguredAudienceModelAssociation
// (Summary): real keys use "id"/"collaborationId"/"membershipId", never the
// "*Identifier" forms. Summary lacks only manageResourcePolicies vs the
// full resource.
type ConfiguredAudienceModelAssociation struct {
	Tags                                         map[string]string `json:"-"`
	Description                                  string            `json:"description,omitempty"`
	ConfiguredAudienceModelArn                   string            `json:"configuredAudienceModelArn"`
	CollaborationIdentifier                      string            `json:"-"`
	MembershipArn                                string            `json:"membershipArn"`
	MembershipIdentifier                         string            `json:"-"`
	ConfiguredAudienceModelAssociationIdentifier string            `json:"-"`
	CollaborationArn                             string            `json:"collaborationArn"`
	CollaborationID                              string            `json:"collaborationId"`
	Name                                         string            `json:"name"`
	MembershipID                                 string            `json:"membershipId"`
	Arn                                          string            `json:"arn"`
	ID                                           string            `json:"id"`
	CreateTime                                   float64           `json:"createTime,omitempty"`
	UpdateTime                                   float64           `json:"updateTime,omitempty"`
	ManageResourcePolicies                       bool              `json:"manageResourcePolicies"`
}

// ConfiguredAudienceModelAssociationSummary is the wire shape for
// ListConfiguredAudienceModelAssociations. ConfiguredAudienceModelArn:
// FIXED (bd gopherstack-r80d): required per
// awsRestjson1_deserializeDocumentConfiguredAudienceModelAssociationSummary
// but this struct never carried it, even though the full resource always
// stores it -- flagged and left unfixed by gopherstack-dv4s's 2026-08-14
// pass ("opposite bug direction from the over-wide leaks this pass
// targeted... recorded rather than folded into this pass's leak fix").
type ConfiguredAudienceModelAssociationSummary struct {
	ConfiguredAudienceModelAssociationIdentifier string  `json:"-"`
	Arn                                          string  `json:"arn"`
	CollaborationArn                             string  `json:"collaborationArn"`
	CollaborationIdentifier                      string  `json:"-"`
	ConfiguredAudienceModelArn                   string  `json:"configuredAudienceModelArn"`
	MembershipArn                                string  `json:"membershipArn"`
	MembershipIdentifier                         string  `json:"-"`
	Name                                         string  `json:"name"`
	ID                                           string  `json:"id"`
	MembershipID                                 string  `json:"membershipId"`
	CollaborationID                              string  `json:"collaborationId"`
	CreateTime                                   float64 `json:"createTime,omitempty"`
	UpdateTime                                   float64 `json:"updateTime,omitempty"`
}

// CollaborationConfiguredAudienceModelAssociationSummary is the wire shape
// for ListCollaborationConfiguredAudienceModelAssociations
// (types.CollaborationConfiguredAudienceModelAssociationSummary). Carries
// creatorAccountId, not membershipArn/membershipId.
type CollaborationConfiguredAudienceModelAssociationSummary struct {
	Arn              string  `json:"arn"`
	CollaborationArn string  `json:"collaborationArn"`
	CollaborationID  string  `json:"collaborationId"`
	CreatorAccountID string  `json:"creatorAccountId"`
	Name             string  `json:"name"`
	ID               string  `json:"id"`
	CreateTime       float64 `json:"createTime,omitempty"`
	UpdateTime       float64 `json:"updateTime,omitempty"`
}

// MemberChangeSpecification is the MEMBER-typed ChangeSpecification union member.
// Verified against awsRestjson1_deserializeDocumentMemberChangeSpecification:
// real keys are accountId, displayName, memberAbilities (mlMemberAbilities/
// paymentConfiguration are real but not modeled, see PARITY.md).
type MemberChangeSpecification struct {
	AccountID       string   `json:"accountId"`
	DisplayName     string   `json:"displayName,omitempty"`
	MemberAbilities []string `json:"memberAbilities"`
}

// CollaborationChangeSpecification is the COLLABORATION-typed ChangeSpecification
// union member. Verified against
// awsRestjson1_deserializeDocumentCollaborationChangeSpecification: the only real
// key is autoApprovedChangeTypes.
type CollaborationChangeSpecification struct {
	AutoApprovedChangeTypes []string `json:"autoApprovedChangeTypes,omitempty"`
}

// ChangeSpecification is the typed union for a Change's specification details
// (types.ChangeSpecification: ChangeSpecificationMemberMember |
// ChangeSpecificationMemberCollaboration, confirmed against
// awsRestjson1_deserializeDocumentChangeSpecification's "member"/"collaboration"
// keys). Exactly one of Member/Collaboration is set, matching SpecificationType.
type ChangeSpecification struct {
	Member        *MemberChangeSpecification        `json:"member,omitempty"`
	Collaboration *CollaborationChangeSpecification `json:"collaboration,omitempty"`
}

// Change is a single entry in CollaborationChangeRequest.Changes / a
// CreateCollaborationChangeRequestInput.Changes element. Verified against
// awsRestjson1_deserializeDocumentChange: real keys are specification,
// specificationType, types.
type Change struct {
	Specification     ChangeSpecification `json:"specification"`
	SpecificationType string              `json:"specificationType"`
	Types             []string            `json:"types"`
}

// CollaborationChangeRequest verified against
// awsRestjson1_deserializeDocumentCollaborationChangeRequest: real keys are
// approvals, changes, collaborationId, createTime, id, isAutoApproved,
// status, updateTime. There is NO "changeRequestIdentifier",
// "collaborationIdentifier", "collaborationArn", "type", or "details" key
// in the real API. Changes is a typed union (see Change/ChangeSpecification),
// field-diffed against ChangeInput/Change/ChangeSpecification/
// MemberChangeSpecification/CollaborationChangeSpecification.
// ChangeRequestIdentifier/CollaborationArn are kept as Go-only bookkeeping
// (json:"-").
type CollaborationChangeRequest struct {
	Approvals               map[string]any `json:"approvals,omitempty"`
	CollaborationArn        string         `json:"-"`
	ChangeRequestIdentifier string         `json:"-"`
	CollaborationIdentifier string         `json:"-"`
	CollaborationID         string         `json:"collaborationId"`
	Status                  string         `json:"status"`
	ID                      string         `json:"id"`
	Changes                 []Change       `json:"changes,omitempty"`
	CreateTime              float64        `json:"createTime,omitempty"`
	UpdateTime              float64        `json:"updateTime,omitempty"`
	IsAutoApproved          bool           `json:"isAutoApproved"`
}

// IntermediateTable is the wire shape for CreateIntermediateTable/
// GetIntermediateTable/UpdateIntermediateTable (Summary is its List shape).
// Verified against aws-sdk-go-v2/service/cleanrooms@v1.48.0's
// awsRestjson1_deserializeDocumentIntermediateTable: real keys are
// analysisRuleTypes, arn, childResources, collaborationArn, collaborationId,
// createTime, description, id, intermediateTableVersion, kmsKeyArn,
// membershipArn, membershipId, name, populationAnalysisConfiguration,
// retentionInDays, schema, status, statusReason, tableDependencies,
// updateTime. No "*Identifier" forms -- those are request-parameter-only
// names, matching every other resource in this service.
//
// childResources, schema, and tableDependencies are real optional fields
// this backend does not model and are deliberately omitted from this struct
// (never fabricated with fake values, see PARITY.md): schema requires
// actually executing the stored populationAnalysisConfiguration query to
// learn real column types, which this emulator cannot do; childResources/
// tableDependencies require a full base-table-dependency graph across other
// members' configured tables that this backend does not build.
type IntermediateTable struct {
	PopulationAnalysisConfiguration map[string]any    `json:"populationAnalysisConfiguration,omitempty"`
	IntermediateTableVersion        map[string]any    `json:"intermediateTableVersion,omitempty"`
	Tags                            map[string]string `json:"-"`
	CollaborationArn                string            `json:"collaborationArn"`
	MembershipArn                   string            `json:"membershipArn"`
	Arn                             string            `json:"arn"`
	CollaborationID                 string            `json:"collaborationId"`
	MembershipID                    string            `json:"membershipId"`
	ID                              string            `json:"id"`
	Name                            string            `json:"name"`
	Description                     string            `json:"description,omitempty"`
	KmsKeyArn                       string            `json:"kmsKeyArn,omitempty"`
	Status                          string            `json:"status"`
	StatusReason                    string            `json:"statusReason,omitempty"`
	AnalysisRuleTypes               []string          `json:"analysisRuleTypes,omitempty"`
	RetentionInDays                 int32             `json:"retentionInDays,omitempty"`
	CreateTime                      float64           `json:"createTime,omitempty"`
	UpdateTime                      float64           `json:"updateTime,omitempty"`
}

// IntermediateTableSummary is the wire shape for ListIntermediateTables
// items. Verified against
// awsRestjson1_deserializeDocumentIntermediateTableSummary: real keys are
// analysisRuleTypes, arn, collaborationArn, collaborationId, createTime,
// description, id, membershipArn, membershipId, name, retentionInDays,
// status, updateTime -- a strict subset of IntermediateTable (no
// childResources, intermediateTableVersion, kmsKeyArn,
// populationAnalysisConfiguration, schema, statusReason,
// tableDependencies).
type IntermediateTableSummary struct {
	CollaborationArn  string   `json:"collaborationArn"`
	MembershipArn     string   `json:"membershipArn"`
	Arn               string   `json:"arn"`
	CollaborationID   string   `json:"collaborationId"`
	MembershipID      string   `json:"membershipId"`
	ID                string   `json:"id"`
	Name              string   `json:"name"`
	Description       string   `json:"description,omitempty"`
	Status            string   `json:"status"`
	AnalysisRuleTypes []string `json:"analysisRuleTypes,omitempty"`
	RetentionInDays   int32    `json:"retentionInDays,omitempty"`
	CreateTime        float64  `json:"createTime,omitempty"`
	UpdateTime        float64  `json:"updateTime,omitempty"`
}

// IntermediateTableAnalysisRule verified against
// awsRestjson1_deserializeDocumentIntermediateTableAnalysisRule: real keys
// are analysisRulePolicy, analysisRuleType, createTime, intermediateTableArn,
// intermediateTableIdentifier, updateTime. Note that the real output key
// really is "intermediateTableIdentifier" (NOT "intermediateTableId") --
// confirmed directly against the deserializer, the same kind of intentional
// asymmetry already documented on
// ConfiguredTableAssociationAnalysisRule.MembershipIdentifier. Unlike
// ConfiguredTableAnalysisRule, IntermediateTableAnalysisRule is NOT the same
// SDK union: types.IntermediateTableAnalysisRulePolicy
// (isIntermediateTableAnalysisRulePolicy) is a distinct Go interface from
// types.AnalysisRulePolicy (isAnalysisRulePolicy) used by
// ConfiguredTableAnalysisRule, even though their content shape happens to be
// structurally identical ({"v1": {"custom": {...}}}). This backend models
// both the same way regardless (a generic map[string]any pass-through, this
// service's established convention for every policy/union field), so no
// code is literally shared -- only the pass-through *strategy* is reused.
type IntermediateTableAnalysisRule struct {
	AnalysisRulePolicy          map[string]any `json:"analysisRulePolicy,omitempty"`
	IntermediateTableArn        string         `json:"intermediateTableArn"`
	IntermediateTableIdentifier string         `json:"intermediateTableIdentifier"`
	AnalysisRuleType            string         `json:"analysisRuleType"`
	CreateTime                  float64        `json:"createTime,omitempty"`
	UpdateTime                  float64        `json:"updateTime,omitempty"`
}

// IntermediateTableVersionSummary is the wire shape for
// ListIntermediateTableVersions items. Verified against
// awsRestjson1_deserializeDocumentIntermediateTableVersionSummary: real keys
// are analysisId, analysisType, createTime, expirationTime, kmsKeyArn,
// status, tableId, versionId. MembershipID is Go-only bookkeeping (json:"-")
// used to look up the underlying ProtectedQuery this version's population
// run created (see PopulateIntermediateTable/advanceIntermediateTablesLocked
// in intermediate_tables.go) -- there is no membershipId key on this real
// type. Parameters is also Go-only bookkeeping: the real "parameters" key
// belongs to the nested types.IntermediateTableActiveVersion (surfaced as
// IntermediateTable.IntermediateTableVersion once population succeeds), not
// to this Summary type, so it is carried here only to round-trip through to
// that map when advanceIntermediateTablesLocked resolves the version.
type IntermediateTableVersionSummary struct {
	Parameters     map[string]string `json:"-"`
	MembershipID   string            `json:"-"`
	AnalysisID     string            `json:"analysisId"`
	AnalysisType   string            `json:"analysisType"`
	Status         string            `json:"status"`
	TableID        string            `json:"tableId"`
	VersionID      string            `json:"versionId"`
	KmsKeyArn      string            `json:"kmsKeyArn,omitempty"`
	CreateTime     float64           `json:"createTime,omitempty"`
	ExpirationTime float64           `json:"expirationTime,omitempty"`
}

// PopulateIntermediateTableOutput is the (unwrapped -- no envelope key,
// confirmed against awsRestjson1_deserializeOpDocumentPopulateIntermediateTableOutput)
// response shape for PopulateIntermediateTable: real keys are analysisId,
// analysisType, versionId.
type PopulateIntermediateTableOutput struct {
	AnalysisID   string `json:"analysisId"`
	AnalysisType string `json:"analysisType"`
	VersionID    string `json:"versionId"`
}
