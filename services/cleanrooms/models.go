package cleanrooms

const (
	statusActive    = "ACTIVE"
	errCodeNotFound = "ResourceNotFoundException"
	errMsgNotFound  = "not found"

	protectedQueryStatusSuccess = "SUCCESS"
	protectedJobStatusSuccess   = "SUCCESS"
	mockDurationMillis          = 100
)

type MemberSpec struct {
	PaymentConfig map[string]any `json:"paymentConfiguration,omitempty"`
	AccountID     string         `json:"accountId"`
	DisplayName   string         `json:"displayName"`
	Abilities     []string       `json:"memberAbilities"`
}

type MemberSummary struct {
	AccountID   string   `json:"accountId"`
	DisplayName string   `json:"displayName"`
	Status      string   `json:"status"`
	Abilities   []string `json:"abilities"`
	CreateTime  float64  `json:"createTime,omitempty"`
	UpdateTime  float64  `json:"updateTime,omitempty"`
}

type Collaboration struct {
	Tags                    map[string]string `json:"tags,omitempty"`
	CollaborationIdentifier string            `json:"collaborationIdentifier"`
	// ID mirrors CollaborationIdentifier under the canonical AWS API key ("id").
	// The AWS SDK/Terraform provider read this resource via the "id" field, so it
	// must be emitted in addition to the legacy "collaborationIdentifier" key.
	ID                 string           `json:"id"`
	Arn                string           `json:"arn"`
	Name               string           `json:"name"`
	Description        string           `json:"description,omitempty"`
	CreatorAccountID   string           `json:"creatorAccountId"`
	CreatorDisplayName string           `json:"creatorDisplayName"`
	MemberStatus       string           `json:"memberStatus"`
	QueryLogStatus     string           `json:"queryLogStatus,omitempty"`
	MemberAbilities    []string         `json:"memberAbilities,omitempty"`
	Members            []*MemberSummary `json:"members,omitempty"`
	CreateTime         float64          `json:"createTime,omitempty"`
	UpdateTime         float64          `json:"updateTime,omitempty"`
}

type CollaborationSummary struct {
	CollaborationIdentifier string `json:"collaborationIdentifier"`
	// ID mirrors CollaborationIdentifier under the canonical AWS API key ("id").
	ID                 string  `json:"id"`
	Arn                string  `json:"arn"`
	Name               string  `json:"name"`
	CreatorAccountID   string  `json:"creatorAccountId"`
	CreatorDisplayName string  `json:"creatorDisplayName"`
	MemberStatus       string  `json:"memberStatus"`
	CreateTime         float64 `json:"createTime,omitempty"`
	UpdateTime         float64 `json:"updateTime,omitempty"`
}

type Membership struct {
	DefaultResultConfiguration      map[string]any `json:"defaultResultConfiguration,omitempty"`
	PaymentConfiguration            map[string]any `json:"paymentConfiguration,omitempty"`
	QueryLogStatus                  string         `json:"queryLogStatus,omitempty"`
	CollaborationIdentifier         string         `json:"collaborationIdentifier"`
	CollaborationCreatorAccountID   string         `json:"collaborationCreatorAccountId"`
	CollaborationCreatorDisplayName string         `json:"collaborationCreatorDisplayName"`
	MembershipIdentifier            string         `json:"membershipIdentifier"`
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

type MembershipSummary struct {
	CollaborationName               string   `json:"collaborationName"`
	Arn                             string   `json:"arn"`
	CollaborationIdentifier         string   `json:"collaborationIdentifier"`
	CollaborationArn                string   `json:"collaborationArn"`
	CollaborationCreatorAccountID   string   `json:"collaborationCreatorAccountId"`
	CollaborationCreatorDisplayName string   `json:"collaborationCreatorDisplayName"`
	MembershipIdentifier            string   `json:"membershipIdentifier"`
	Status                          string   `json:"status"`
	ID                              string   `json:"id"`
	CollaborationID                 string   `json:"collaborationId"`
	MemberAbilities                 []string `json:"memberAbilities,omitempty"`
	CreateTime                      float64  `json:"createTime,omitempty"`
	UpdateTime                      float64  `json:"updateTime,omitempty"`
}

type ConfiguredTable struct {
	TableReference            map[string]any    `json:"tableReference,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
	ConfiguredTableIdentifier string            `json:"configuredTableIdentifier"`
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
	ConfiguredTableIdentifier string   `json:"configuredTableIdentifier"`
	Arn                       string   `json:"arn"`
	Name                      string   `json:"name"`
	AnalysisMethod            string   `json:"analysisMethod,omitempty"`
	ID                        string   `json:"id"`
	AnalysisRuleTypes         []string `json:"analysisRuleTypes,omitempty"`
	CreateTime                float64  `json:"createTime,omitempty"`
	UpdateTime                float64  `json:"updateTime,omitempty"`
}

type ConfiguredTableAnalysisRule struct {
	Policy                    map[string]any `json:"policy,omitempty"`
	ConfiguredTableIdentifier string         `json:"configuredTableIdentifier"`
	ConfiguredTableArn        string         `json:"configuredTableArn"`
	Type                      string         `json:"type"`
	ConfiguredTableID         string         `json:"configuredTableId"`
	CreateTime                float64        `json:"createTime,omitempty"`
	UpdateTime                float64        `json:"updateTime,omitempty"`
}

type ConfiguredTableAssociation struct {
	Tags                                 map[string]string `json:"tags,omitempty"`
	RoleArn                              string            `json:"roleArn,omitempty"`
	Name                                 string            `json:"name"`
	MembershipArn                        string            `json:"membershipArn"`
	ConfiguredTableIdentifier            string            `json:"configuredTableIdentifier"`
	ConfiguredTableArn                   string            `json:"configuredTableArn"`
	ConfiguredTableAssociationIdentifier string            `json:"configuredTableAssociationIdentifier"`
	MembershipIdentifier                 string            `json:"membershipIdentifier"`
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
	ConfiguredTableAssociationIdentifier string  `json:"configuredTableAssociationIdentifier"`
	Arn                                  string  `json:"arn"`
	MembershipIdentifier                 string  `json:"membershipIdentifier"`
	MembershipArn                        string  `json:"membershipArn"`
	ConfiguredTableIdentifier            string  `json:"configuredTableIdentifier"`
	Name                                 string  `json:"name"`
	ID                                   string  `json:"id"`
	MembershipID                         string  `json:"membershipId"`
	ConfiguredTableID                    string  `json:"configuredTableId"`
	CreateTime                           float64 `json:"createTime,omitempty"`
	UpdateTime                           float64 `json:"updateTime,omitempty"`
}

type ConfiguredTableAssociationAnalysisRule struct {
	Policy                               map[string]any `json:"policy,omitempty"`
	ConfiguredTableAssociationIdentifier string         `json:"configuredTableAssociationIdentifier"`
	ConfiguredTableAssociationArn        string         `json:"configuredTableAssociationArn"`
	MembershipIdentifier                 string         `json:"membershipIdentifier"`
	MembershipArn                        string         `json:"membershipArn"`
	Type                                 string         `json:"type"`
	CreateTime                           float64        `json:"createTime,omitempty"`
	UpdateTime                           float64        `json:"updateTime,omitempty"`
}

type AnalysisTemplate struct {
	Source                     map[string]any    `json:"source,omitempty"`
	Tags                       map[string]string `json:"tags,omitempty"`
	Schema                     map[string]any    `json:"schema,omitempty"`
	AnalysisTemplateIdentifier string            `json:"analysisTemplateIdentifier"`
	Format                     string            `json:"format,omitempty"`
	MembershipArn              string            `json:"membershipArn"`
	Name                       string            `json:"name"`
	Description                string            `json:"description,omitempty"`
	CollaborationIdentifier    string            `json:"collaborationIdentifier"`
	CollaborationArn           string            `json:"collaborationArn"`
	MembershipIdentifier       string            `json:"membershipIdentifier"`
	Arn                        string            `json:"arn"`
	CollaborationID            string            `json:"collaborationId"`
	MembershipID               string            `json:"membershipId"`
	ID                         string            `json:"id"`
	AnalysisParameters         []map[string]any  `json:"analysisParameters,omitempty"`
	UpdateTime                 float64           `json:"updateTime,omitempty"`
	CreateTime                 float64           `json:"createTime,omitempty"`
}

type AnalysisTemplateSummary struct {
	AnalysisTemplateIdentifier string  `json:"analysisTemplateIdentifier"`
	Arn                        string  `json:"arn"`
	CollaborationArn           string  `json:"collaborationArn"`
	CollaborationIdentifier    string  `json:"collaborationIdentifier"`
	MembershipIdentifier       string  `json:"membershipIdentifier"`
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

type Schema struct {
	CollaborationArn        string           `json:"collaborationArn"`
	CollaborationIdentifier string           `json:"collaborationIdentifier"`
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
	CollaborationIdentifier string   `json:"collaborationIdentifier"`
	CreatorAccountID        string   `json:"creatorAccountId"`
	Name                    string   `json:"name"`
	Type                    string   `json:"type"`
	AnalysisMethod          string   `json:"analysisMethod,omitempty"`
	AnalysisRuleTypes       []string `json:"analysisRuleTypes,omitempty"`
	CreateTime              float64  `json:"createTime,omitempty"`
	UpdateTime              float64  `json:"updateTime,omitempty"`
}

type SchemaAnalysisRule struct {
	Policy                  map[string]any `json:"policy,omitempty"`
	CollaborationArn        string         `json:"collaborationArn"`
	CollaborationIdentifier string         `json:"collaborationIdentifier"`
	Name                    string         `json:"name"`
	Type                    string         `json:"type"`
	CreateTime              float64        `json:"createTime,omitempty"`
	UpdateTime              float64        `json:"updateTime,omitempty"`
}

type ProtectedQuery struct {
	SQLParameters        map[string]any `json:"sqlParameters,omitempty"`
	ResultConfiguration  map[string]any `json:"resultConfiguration,omitempty"`
	ComputeConfiguration map[string]any `json:"computeConfiguration,omitempty"`
	Statistics           map[string]any `json:"statistics,omitempty"`
	Result               map[string]any `json:"result,omitempty"`
	Error                map[string]any `json:"error,omitempty"`
	ID                   string         `json:"id"`
	MembershipIdentifier string         `json:"membershipIdentifier"`
	MembershipArn        string         `json:"membershipArn"`
	Status               string         `json:"status"`
	MembershipID         string         `json:"membershipId"`
	CreateTime           float64        `json:"createTime,omitempty"`
}

type ProtectedQuerySummary struct {
	ID                   string  `json:"id"`
	MembershipIdentifier string  `json:"membershipIdentifier"`
	MembershipArn        string  `json:"membershipArn"`
	Status               string  `json:"status"`
	MembershipID         string  `json:"membershipId"`
	CreateTime           float64 `json:"createTime,omitempty"`
}

type ProtectedJob struct {
	JobParameters        map[string]any `json:"jobParameters,omitempty"`
	ResultConfiguration  map[string]any `json:"resultConfiguration,omitempty"`
	Statistics           map[string]any `json:"statistics,omitempty"`
	Result               map[string]any `json:"result,omitempty"`
	Error                map[string]any `json:"error,omitempty"`
	ID                   string         `json:"id"`
	MembershipIdentifier string         `json:"membershipIdentifier"`
	MembershipArn        string         `json:"membershipArn"`
	Status               string         `json:"status"`
	Type                 string         `json:"type"`
	MembershipID         string         `json:"membershipId"`
	CreateTime           float64        `json:"createTime,omitempty"`
}

type ProtectedJobSummary struct {
	ID                   string  `json:"id"`
	MembershipIdentifier string  `json:"membershipIdentifier"`
	MembershipArn        string  `json:"membershipArn"`
	Status               string  `json:"status"`
	Type                 string  `json:"type"`
	MembershipID         string  `json:"membershipId"`
	CreateTime           float64 `json:"createTime,omitempty"`
}

type PrivacyBudgetTemplate struct {
	Parameters                      map[string]any    `json:"parameters,omitempty"`
	Tags                            map[string]string `json:"tags,omitempty"`
	MembershipArn                   string            `json:"membershipArn"`
	Arn                             string            `json:"arn"`
	CollaborationArn                string            `json:"collaborationArn"`
	CollaborationIdentifier         string            `json:"collaborationIdentifier"`
	PrivacyBudgetTemplateIdentifier string            `json:"privacyBudgetTemplateIdentifier"`
	MembershipIdentifier            string            `json:"membershipIdentifier"`
	PrivacyBudgetType               string            `json:"privacyBudgetType"`
	AutoRefresh                     string            `json:"autoRefresh,omitempty"`
	ID                              string            `json:"id"`
	MembershipID                    string            `json:"membershipId"`
	CollaborationID                 string            `json:"collaborationId"`
	CreateTime                      float64           `json:"createTime,omitempty"`
	UpdateTime                      float64           `json:"updateTime,omitempty"`
}

type PrivacyBudgetTemplateSummary struct {
	PrivacyBudgetTemplateIdentifier string  `json:"privacyBudgetTemplateIdentifier"`
	Arn                             string  `json:"arn"`
	CollaborationArn                string  `json:"collaborationArn"`
	CollaborationIdentifier         string  `json:"collaborationIdentifier"`
	MembershipArn                   string  `json:"membershipArn"`
	MembershipIdentifier            string  `json:"membershipIdentifier"`
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

type IDMappingTable struct {
	InputReferenceConfig     map[string]any    `json:"inputReferenceConfig,omitempty"`
	Tags                     map[string]string `json:"tags,omitempty"`
	InputReferenceProperties map[string]any    `json:"inputReferenceProperties,omitempty"`
	IDMappingTableIdentifier string            `json:"idMappingTableIdentifier"`
	KmsKeyArn                string            `json:"kmsKeyArn,omitempty"`
	MembershipIdentifier     string            `json:"membershipIdentifier"`
	Name                     string            `json:"name"`
	Description              string            `json:"description,omitempty"`
	CollaborationIdentifier  string            `json:"collaborationIdentifier"`
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
	IDMappingTableIdentifier string  `json:"idMappingTableIdentifier"`
	Arn                      string  `json:"arn"`
	CollaborationArn         string  `json:"collaborationArn"`
	CollaborationIdentifier  string  `json:"collaborationIdentifier"`
	MembershipArn            string  `json:"membershipArn"`
	MembershipIdentifier     string  `json:"membershipIdentifier"`
	Name                     string  `json:"name"`
	ID                       string  `json:"id"`
	MembershipID             string  `json:"membershipId"`
	CollaborationID          string  `json:"collaborationId"`
	CreateTime               float64 `json:"createTime,omitempty"`
	UpdateTime               float64 `json:"updateTime,omitempty"`
}

type IDNamespaceAssociation struct {
	InputReferenceConfig             map[string]any    `json:"inputReferenceConfig,omitempty"`
	Tags                             map[string]string `json:"tags,omitempty"`
	IDMappingConfig                  map[string]any    `json:"idMappingConfig,omitempty"`
	InputReferenceProperties         map[string]any    `json:"inputReferenceProperties,omitempty"`
	MembershipArn                    string            `json:"membershipArn"`
	MembershipIdentifier             string            `json:"membershipIdentifier"`
	Name                             string            `json:"name"`
	Description                      string            `json:"description,omitempty"`
	CollaborationIdentifier          string            `json:"collaborationIdentifier"`
	IDNamespaceAssociationIdentifier string            `json:"idNamespaceAssociationIdentifier"`
	CollaborationArn                 string            `json:"collaborationArn"`
	Arn                              string            `json:"arn"`
	ID                               string            `json:"id"`
	MembershipID                     string            `json:"membershipId"`
	CollaborationID                  string            `json:"collaborationId"`
	CreateTime                       float64           `json:"createTime,omitempty"`
	UpdateTime                       float64           `json:"updateTime,omitempty"`
}

type IDNamespaceAssociationSummary struct {
	IDNamespaceAssociationIdentifier string  `json:"idNamespaceAssociationIdentifier"`
	Arn                              string  `json:"arn"`
	CollaborationArn                 string  `json:"collaborationArn"`
	CollaborationIdentifier          string  `json:"collaborationIdentifier"`
	MembershipArn                    string  `json:"membershipArn"`
	MembershipIdentifier             string  `json:"membershipIdentifier"`
	Name                             string  `json:"name"`
	ID                               string  `json:"id"`
	MembershipID                     string  `json:"membershipId"`
	CollaborationID                  string  `json:"collaborationId"`
	CreateTime                       float64 `json:"createTime,omitempty"`
	UpdateTime                       float64 `json:"updateTime,omitempty"`
}

type ConfiguredAudienceModelAssociation struct {
	Tags                                         map[string]string `json:"tags,omitempty"`
	Description                                  string            `json:"description,omitempty"`
	ConfiguredAudienceModelArn                   string            `json:"configuredAudienceModelArn"`
	CollaborationIdentifier                      string            `json:"collaborationIdentifier"`
	MembershipArn                                string            `json:"membershipArn"`
	MembershipIdentifier                         string            `json:"membershipIdentifier"`
	ConfiguredAudienceModelAssociationIdentifier string            `json:"configuredAudienceModelAssociationIdentifier"`
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
	ConfiguredAudienceModelAssociationIdentifier string  `json:"configuredAudienceModelAssociationIdentifier"`
	Arn                                          string  `json:"arn"`
	CollaborationArn                             string  `json:"collaborationArn"`
	CollaborationIdentifier                      string  `json:"collaborationIdentifier"`
	MembershipArn                                string  `json:"membershipArn"`
	MembershipIdentifier                         string  `json:"membershipIdentifier"`
	Name                                         string  `json:"name"`
	ID                                           string  `json:"id"`
	MembershipID                                 string  `json:"membershipId"`
	CollaborationID                              string  `json:"collaborationId"`
	CreateTime                                   float64 `json:"createTime,omitempty"`
	UpdateTime                                   float64 `json:"updateTime,omitempty"`
}

type CollaborationChangeRequest struct {
	Details                 map[string]any `json:"details,omitempty"`
	ChangeRequestIdentifier string         `json:"changeRequestIdentifier"`
	CollaborationIdentifier string         `json:"collaborationIdentifier"`
	CollaborationArn        string         `json:"collaborationArn"`
	Status                  string         `json:"status"`
	Type                    string         `json:"type"`
	CreateTime              float64        `json:"createTime,omitempty"`
	UpdateTime              float64        `json:"updateTime,omitempty"`
}
