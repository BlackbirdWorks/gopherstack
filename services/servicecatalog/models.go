package servicecatalog

import "time"

const (
	productTypeCFN      = "CLOUD_FORMATION_TEMPLATE"
	productTypeExternal = "EXTERNAL"
	productTypeTFOpen   = "TERRAFORM_OPEN_SOURCE"
	productTypeTFCloud  = "TERRAFORM_CLOUD"
	artifactTypeCFN     = "CLOUD_FORMATION_TEMPLATE"

	statusAvailable = "AVAILABLE"
	statusCreating  = "CREATING"

	ppUnderChange = "UNDER_CHANGE"
	ppTainted     = "TAINTED"
	ppError       = "ERROR"

	recCreated    = "CREATED"
	recInProgress = "IN_PROGRESS"
	recSucceeded  = "SUCCEEDED"
	recFailed     = "FAILED"

	orderDescending   = "DESCENDING"
	orderAscending    = "ASCENDING"
	filterOwner       = "Owner"
	filterProductType = "ProductType"
	kindPortfolio     = "Portfolio"
	kindProduct       = "Product"
	queryStatus       = "status"

	recProvision      = "PROVISION_PRODUCT"
	recUpdate         = "UPDATE_PROVISIONED_PRODUCT"
	recTerminate      = "TERMINATE_PROVISIONED_PRODUCT"
	recImport         = "IMPORT_PROVISIONED_PRODUCT"
	recProperties     = "UPDATE_PROVISIONED_PRODUCT_PROPERTIES"
	recServiceAct     = "EXECUTE_PROFILE_SERVICE_ACTION"
	ppTypeCFNStack    = "CFN_STACK"
	ppTypeCFNStackSet = "CFN_STACKSET"

	constraintLaunch   = "LAUNCH"
	constraintNotify   = "NOTIFICATION"
	constraintTemplate = "TEMPLATE"
	constraintStackSet = "STACKSET"
	constraintResource = "RESOURCE_UPDATE"

	shareAccount        = "ACCOUNT"
	principalIAMPattern = "IAM_PATTERN"
	shareOrg            = "ORGANIZATION"
	shareOU             = "ORGANIZATIONAL_UNIT"
	shareMember         = "ORGANIZATION_MEMBER_ACCOUNT"
	shareImported       = "IMPORTED"
	shareViaOrgs        = "AWS_ORGANIZATIONS"
	shareViaSC          = "AWS_SERVICECATALOG"
	shareCompleted      = "COMPLETED"
)

// Portfolio is a collection of products.
type Portfolio struct {
	CreatedTime  time.Time         `json:"createdTime"`
	Tags         map[string]string `json:"tags,omitempty"`
	ID           string            `json:"id"`
	ARN          string            `json:"arn"`
	AccountID    string            `json:"accountID"`
	Region       string            `json:"region"`
	DisplayName  string            `json:"displayName"`
	Description  string            `json:"description,omitempty"`
	ProviderName string            `json:"providerName"`
}

// SourceConnection links a product to a CodeStar repository.
type SourceConnection struct {
	Type          string `json:"type"`
	ConnectionArn string `json:"connectionArn,omitempty"`
	Repository    string `json:"repository,omitempty"`
	Branch        string `json:"branch,omitempty"`
	ArtifactPath  string `json:"artifactPath,omitempty"`
}

// Product is a Service Catalog product.
type Product struct {
	CreatedTime        time.Time         `json:"createdTime"`
	Tags               map[string]string `json:"tags,omitempty"`
	SourceConnection   *SourceConnection `json:"sourceConnection,omitempty"`
	ID                 string            `json:"id"`
	ViewID             string            `json:"viewID"`
	ARN                string            `json:"arn"`
	AccountID          string            `json:"accountID"`
	Region             string            `json:"region"`
	Name               string            `json:"name"`
	Owner              string            `json:"owner"`
	Description        string            `json:"description,omitempty"`
	Distributor        string            `json:"distributor,omitempty"`
	SupportDescription string            `json:"supportDescription,omitempty"`
	SupportEmail       string            `json:"supportEmail,omitempty"`
	SupportURL         string            `json:"supportUrl,omitempty"`
	Type               string            `json:"type"`
	Status             string            `json:"status"`
	SourceProductID    string            `json:"sourceProductID,omitempty"`
}

// Artifact is a provisioning artifact (product version).
type Artifact struct {
	CreatedTime               time.Time         `json:"createdTime"`
	Info                      map[string]string `json:"info,omitempty"`
	ID                        string            `json:"id"`
	ProductID                 string            `json:"productID"`
	Name                      string            `json:"name"`
	Description               string            `json:"description,omitempty"`
	Type                      string            `json:"type"`
	Guidance                  string            `json:"guidance"`
	SourceRevision            string            `json:"sourceRevision,omitempty"`
	Inactive                  bool              `json:"inactive,omitempty"`
	DisableTemplateValidation bool              `json:"disableTemplateValidation,omitempty"`
}

// Association links a portfolio to a product.
type Association struct {
	Key         string `json:"key"`
	PortfolioID string `json:"portfolioID"`
	ProductID   string `json:"productID"`
}

// Principal is a principal associated with a portfolio by one account.
type Principal struct {
	Key           string `json:"key"`
	PortfolioID   string `json:"portfolioID"`
	AccountID     string `json:"accountID"`
	PrincipalARN  string `json:"principalARN"`
	PrincipalType string `json:"principalType"`
}

// Constraint restricts or configures how a product is launched from a portfolio.
type Constraint struct {
	ID          string `json:"id"`
	AccountID   string `json:"accountID"`
	Region      string `json:"region"`
	PortfolioID string `json:"portfolioID"`
	ProductID   string `json:"productID"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	Parameters  string `json:"parameters"`
}

// TagOption is a key-value pair offered to end users at launch.
type TagOption struct {
	ID        string `json:"id"`
	AccountID string `json:"accountID"`
	Region    string `json:"region"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Active    bool   `json:"active"`
}

// TagOptionLink associates a tag option with a portfolio or product.
type TagOptionLink struct {
	Key         string `json:"key"`
	TagOptionID string `json:"tagOptionID"`
	ResourceID  string `json:"resourceID"`
}

// BudgetLink associates a budget with a portfolio or product.
type BudgetLink struct {
	Key        string `json:"key"`
	BudgetName string `json:"budgetName"`
	ResourceID string `json:"resourceID"`
}

// ServiceAction is an SSM-automation-backed action on provisioned products.
type ServiceAction struct {
	Definition  map[string]string `json:"definition"`
	ID          string            `json:"id"`
	AccountID   string            `json:"accountID"`
	Region      string            `json:"region"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
}

// ServiceActionLink associates a service action with a provisioning artifact.
type ServiceActionLink struct {
	Key        string `json:"key"`
	ProductID  string `json:"productID"`
	ArtifactID string `json:"artifactID"`
	ActionID   string `json:"actionID"`
}

// Share is a portfolio share to an account or organization node.
type Share struct {
	CreatedTime      time.Time `json:"createdTime"`
	Token            string    `json:"token,omitempty"`
	Key              string    `json:"key"`
	PortfolioID      string    `json:"portfolioID"`
	OwnerAccount     string    `json:"ownerAccount"`
	Region           string    `json:"region"`
	Type             string    `json:"type"`
	Recipient        string    `json:"recipient"`
	NodeType         string    `json:"nodeType,omitempty"`
	SuccessfulShares []string  `json:"successfulShares,omitempty"`
	SharePrincipals  bool      `json:"sharePrincipals"`
	ShareTagOptions  bool      `json:"shareTagOptions"`
	Accepted         bool      `json:"accepted"`
}

// Output is a CloudFormation stack output captured on a provisioned product or record.
type Output struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	Description string `json:"description,omitempty"`
}

// ProvisionedProduct is a launched product (one CloudFormation stack).
type ProvisionedProduct struct {
	CreatedTime                        time.Time         `json:"createdTime"`
	Tags                               map[string]string `json:"tags,omitempty"`
	Parameters                         map[string]string `json:"parameters,omitempty"`
	ID                                 string            `json:"id"`
	ARN                                string            `json:"arn"`
	AccountID                          string            `json:"accountID"`
	Region                             string            `json:"region"`
	Name                               string            `json:"name"`
	Type                               string            `json:"type"`
	Status                             string            `json:"status"`
	StatusMessage                      string            `json:"statusMessage,omitempty"`
	ProductID                          string            `json:"productID"`
	ArtifactID                         string            `json:"artifactID"`
	PathID                             string            `json:"pathID,omitempty"`
	StackName                          string            `json:"stackName,omitempty"`
	StackID                            string            `json:"stackID,omitempty"`
	LaunchRoleArn                      string            `json:"launchRoleArn,omitempty"`
	IdempotencyToken                   string            `json:"idempotencyToken,omitempty"`
	LastRecordID                       string            `json:"lastRecordID"`
	LastProvisioningRecordID           string            `json:"lastProvisioningRecordID"`
	LastSuccessfulProvisioningRecordID string            `json:"lastSuccessfulProvisioningRecordID,omitempty"`
	UserArn                            string            `json:"userArn,omitempty"`
	Owner                              string            `json:"owner,omitempty"`
	Outputs                            []Output          `json:"outputs,omitempty"`
}

// RecordError is an error attached to a failed record.
type RecordError struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

// Record tracks one provisioning operation.
type Record struct {
	CreatedTime            time.Time         `json:"createdTime"`
	SettleAt               time.Time         `json:"settleAt"`
	Tags                   map[string]string `json:"tags,omitempty"`
	ID                     string            `json:"id"`
	AccountID              string            `json:"accountID"`
	Region                 string            `json:"region"`
	Type                   string            `json:"type"`
	FinalStatus            string            `json:"finalStatus"`
	ProvisionedProductID   string            `json:"provisionedProductID"`
	ProvisionedProductName string            `json:"provisionedProductName"`
	ProductID              string            `json:"productID"`
	ArtifactID             string            `json:"artifactID"`
	PathID                 string            `json:"pathID,omitempty"`
	LaunchRoleArn          string            `json:"launchRoleArn,omitempty"`
	WorkflowToken          string            `json:"workflowToken,omitempty"`
	PPType                 string            `json:"provisionedProductType,omitempty"`
	Errors                 []RecordError     `json:"errors,omitempty"`
	Outputs                []Output          `json:"outputs,omitempty"`
	AwaitingEngine         bool              `json:"awaitingEngine,omitempty"`
}

// CopyOperation tracks a CopyProduct request.
type CopyOperation struct {
	SettleAt        time.Time `json:"settleAt"`
	Token           string    `json:"token"`
	AccountID       string    `json:"accountID"`
	TargetProductID string    `json:"targetProductID"`
	StatusDetail    string    `json:"statusDetail,omitempty"`
	FinalStatus     string    `json:"finalStatus"`
}

// Plan is a provisioned product plan backed by a CloudFormation change set.
type Plan struct {
	CreatedTime            time.Time         `json:"createdTime"`
	UpdatedTime            time.Time         `json:"updatedTime"`
	Tags                   map[string]string `json:"tags,omitempty"`
	Parameters             map[string]string `json:"parameters,omitempty"`
	ID                     string            `json:"id"`
	AccountID              string            `json:"accountID"`
	Region                 string            `json:"region"`
	Name                   string            `json:"name"`
	Type                   string            `json:"type"`
	Status                 string            `json:"status"`
	StatusMessage          string            `json:"statusMessage,omitempty"`
	ProductID              string            `json:"productID"`
	ArtifactID             string            `json:"artifactID"`
	PathID                 string            `json:"pathID,omitempty"`
	ProvisionedProductID   string            `json:"provisionedProductID"`
	ProvisionedProductName string            `json:"provisionedProductName"`
	ChangeSetName          string            `json:"changeSetName"`
	StackName              string            `json:"stackName"`
	NotificationArns       []string          `json:"notificationArns,omitempty"`
	Changes                []PlanChange      `json:"changes,omitempty"`
	IsUpdate               bool              `json:"isUpdate"`
}

// PlanChange is one resource change of a plan.
type PlanChange struct {
	Action            string `json:"action"`
	LogicalResourceID string `json:"logicalResourceID"`
	PhysicalID        string `json:"physicalID,omitempty"`
	ResourceType      string `json:"resourceType"`
	Replacement       string `json:"replacement,omitempty"`
}

// AccountState holds per-account Service Catalog settings.
type AccountState struct {
	AccountID           string `json:"accountID"`
	OrganizationsAccess bool   `json:"organizationsAccess"`
}
