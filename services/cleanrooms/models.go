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
	CollaborationIdentifier string            `json:"-"`
	ID                      string            `json:"id"`
	Arn                     string            `json:"arn"`
	Name                    string            `json:"name"`
	Description             string            `json:"description,omitempty"`
	CreatorAccountID        string            `json:"creatorAccountId"`
	CreatorDisplayName      string            `json:"creatorDisplayName"`
	MemberStatus            string            `json:"memberStatus"`
	QueryLogStatus          string            `json:"queryLogStatus,omitempty"`
	// MembershipArn/MembershipID are the caller's own membership within this
	// collaboration -- real AWS auto-creates a membership for the creator at
	// CreateCollaboration time (see InMemoryBackend.CreateCollaboration).
	MembershipArn   string   `json:"membershipArn,omitempty"`
	MembershipID    string   `json:"membershipId,omitempty"`
	MemberAbilities []string `json:"-"`
	// Members is the real backing store for ListMembers/DeleteMember (see
	// InMemoryBackend.ListMembers/DeleteMember) and has no separate
	// persisted representation the way tagsByArn does for Tags, so unlike
	// every other invented field cleaned up in this pass it keeps a real
	// (non "-") json tag: store.Table's Snapshot/Restore round-trips
	// through this exact tag, and a json:"-" here would silently lose every
	// collaboration's member list across a restart. A real AWS SDK/
	// Terraform client tolerates the extra "members" key on
	// Create/Get/UpdateCollaboration responses (every deserializer in this
	// service ends its field switch with a default case that discards
	// unrecognized keys), so this is a deliberate, documented exception,
	// not a silent wire violation -- moving Members to its own
	// store.Table (like tagsByArn) to fully remove it from the wire is
	// deferred, see PARITY.md.
	Members    []*MemberSummary `json:"members,omitempty"`
	CreateTime float64          `json:"createTime,omitempty"`
	UpdateTime float64          `json:"updateTime,omitempty"`
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
	MemberAbilities                 []string       `json:"memberAbilities,omitempty"`
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
	MemberAbilities                 []string       `json:"memberAbilities,omitempty"`
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
	AllowedColumns            []string          `json:"allowedColumns,omitempty"`
	AnalysisRuleTypes         []string          `json:"analysisRuleTypes,omitempty"`
	CreateTime                float64           `json:"createTime,omitempty"`
	UpdateTime                float64           `json:"updateTime,omitempty"`
}

type ConfiguredTableSummary struct {
	ConfiguredTableIdentifier string   `json:"-"`
	Arn                       string   `json:"arn"`
	Name                      string   `json:"name"`
	AnalysisMethod            string   `json:"analysisMethod,omitempty"`
	ID                        string   `json:"id"`
	AnalysisRuleTypes         []string `json:"analysisRuleTypes,omitempty"`
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
	AnalysisRuleTypes                    []string          `json:"analysisRuleTypes,omitempty"`
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
	AutoRefresh                     string            `json:"autoRefresh,omitempty"`
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

type PrivacyBudget struct {
	Budget                          map[string]any `json:"budget,omitempty"`
	ID                              string         `json:"id"`
	PrivacyBudgetTemplateArn        string         `json:"privacyBudgetTemplateArn"`
	PrivacyBudgetTemplateIdentifier string         `json:"privacyBudgetTemplateIdentifier"`
	CollaborationArn                string         `json:"collaborationArn"`
	CollaborationIdentifier         string         `json:"collaborationIdentifier"`
	MembershipArn                   string         `json:"membershipArn"`
	MembershipIdentifier            string         `json:"membershipIdentifier"`
	PrivacyBudgetType               string         `json:"privacyBudgetType"`
	MembershipID                    string         `json:"membershipId"`
	CollaborationID                 string         `json:"collaborationId"`
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

type ConfiguredAudienceModelAssociationSummary struct {
	ConfiguredAudienceModelAssociationIdentifier string  `json:"-"`
	Arn                                          string  `json:"arn"`
	CollaborationArn                             string  `json:"collaborationArn"`
	CollaborationIdentifier                      string  `json:"-"`
	MembershipArn                                string  `json:"membershipArn"`
	MembershipIdentifier                         string  `json:"-"`
	Name                                         string  `json:"name"`
	ID                                           string  `json:"id"`
	MembershipID                                 string  `json:"membershipId"`
	CollaborationID                              string  `json:"collaborationId"`
	CreateTime                                   float64 `json:"createTime,omitempty"`
	UpdateTime                                   float64 `json:"updateTime,omitempty"`
}

// CollaborationChangeRequest verified against
// awsRestjson1_deserializeDocumentCollaborationChangeRequest: real keys are
// approvals, changes, collaborationId, createTime, id, isAutoApproved,
// status, updateTime. There is NO "changeRequestIdentifier",
// "collaborationIdentifier", "collaborationArn", "type", or "details" key
// in the real API -- CreateCollaborationChangeRequestInput takes a
// `changes` array of {specification, specificationType} objects (not a
// free-form "type"+"details" pair), which this backend stores as a
// generic []map[string]any pass-through, matching the convention used for
// other complex nested unions in this service (Policy, TableReference,
// etc). ChangeRequestIdentifier/CollaborationArn/Type are kept as Go-only
// bookkeeping (json:"-").
type CollaborationChangeRequest struct {
	Details                 map[string]any   `json:"-"`
	Approvals               map[string]any   `json:"approvals,omitempty"`
	CollaborationArn        string           `json:"-"`
	ChangeRequestIdentifier string           `json:"-"`
	CollaborationIdentifier string           `json:"-"`
	CollaborationID         string           `json:"collaborationId"`
	Status                  string           `json:"status"`
	Type                    string           `json:"-"`
	ID                      string           `json:"id"`
	Changes                 []map[string]any `json:"changes,omitempty"`
	CreateTime              float64          `json:"createTime,omitempty"`
	UpdateTime              float64          `json:"updateTime,omitempty"`
	IsAutoApproved          bool             `json:"isAutoApproved"`
}
