package cloudformation

import "time"

// Stack represents a CloudFormation stack.
type Stack struct {
	RollbackConfiguration    *RollbackConfiguration `xml:"RollbackConfiguration,omitempty"    json:"rollbackConfiguration,omitempty"`
	CreationTime             time.Time              `xml:"CreationTime"                       json:"creationTime"`
	LastUpdatedTime          *time.Time             `xml:"LastUpdatedTime,omitempty"          json:"lastUpdatedTime,omitempty"`
	DeletionTime             *time.Time             `xml:"DeletionTime,omitempty"             json:"deletionTime,omitempty"`
	StackID                  string                 `xml:"StackId"                            json:"stackID"`
	StackName                string                 `xml:"StackName"                          json:"stackName"`
	Description              string                 `xml:"Description,omitempty"              json:"description,omitempty"`
	StackStatus              string                 `xml:"StackStatus"                        json:"stackStatus"`
	StackStatusReason        string                 `xml:"StackStatusReason,omitempty"        json:"stackStatusReason,omitempty"`
	RoleARN                  string                 `xml:"RoleARN,omitempty"                  json:"roleARN,omitempty"`
	TemplateBody             string                 `xml:"-"                                  json:"templateBody,omitempty"`
	Parameters               []Parameter            `xml:"Parameters>member,omitempty"        json:"parameters,omitempty"`
	Outputs                  []Output               `xml:"Outputs>member,omitempty"           json:"outputs,omitempty"`
	Tags                     []Tag                  `xml:"Tags>member,omitempty"              json:"tags,omitempty"`
	Capabilities             []string               `xml:"Capabilities>member,omitempty"      json:"capabilities,omitempty"`
	NotificationARNs         []string               `xml:"NotificationARNs>member,omitempty"  json:"notificationARNs,omitempty"`
	TimeoutInMinutes         int                    `xml:"TimeoutInMinutes,omitempty"         json:"timeoutInMinutes,omitempty"`
	EnableTerminationProtection bool                `xml:"EnableTerminationProtection"        json:"enableTerminationProtection"`
	DisableRollback          bool                   `xml:"DisableRollback,omitempty"          json:"disableRollback,omitempty"`
	ParentID                 string                 `xml:"ParentId,omitempty"                 json:"parentID,omitempty"`
	RootID                   string                 `xml:"RootId,omitempty"                   json:"rootID,omitempty"`
}

// RollbackConfiguration holds rollback trigger configuration for a stack.
type RollbackConfiguration struct {
	RollbackTriggers          []RollbackTrigger `xml:"RollbackTriggers>member,omitempty" json:"rollbackTriggers,omitempty"`
	MonitoringTimeInMinutes   int               `xml:"MonitoringTimeInMinutes,omitempty" json:"monitoringTimeInMinutes,omitempty"`
}

// RollbackTrigger defines a CloudWatch alarm ARN used as a rollback trigger.
type RollbackTrigger struct {
	ARN  string `xml:"Arn"  json:"arn"`
	Type string `xml:"Type" json:"type"`
}

// Parameter is a CloudFormation stack parameter.
type Parameter struct {
	ParameterKey     string `xml:"ParameterKey"               json:"parameterKey"`
	ParameterValue   string `xml:"ParameterValue,omitempty"   json:"parameterValue,omitempty"`
	ResolvedValue    string `xml:"ResolvedValue,omitempty"    json:"resolvedValue,omitempty"`
	UsePreviousValue bool   `xml:"UsePreviousValue,omitempty" json:"usePreviousValue,omitempty"`
	NoEcho           bool   `xml:"-"                          json:"noEcho,omitempty"`
}

// Output is a CloudFormation stack output.
type Output struct {
	OutputKey   string `xml:"OutputKey"             json:"outputKey"`
	OutputValue string `xml:"OutputValue"           json:"outputValue"`
	Description string `xml:"Description,omitempty" json:"description,omitempty"`
	ExportName  string `xml:"ExportName,omitempty"  json:"exportName,omitempty"`
}

// Export represents a cross-stack export (from ListExports).
type Export struct {
	ExportingStackID string `xml:"ExportingStackId" json:"exportingStackID"`
	Name             string `xml:"Name"             json:"name"`
	Value            string `xml:"Value"            json:"value"`
}

// StackResourceSummary is a brief summary of a resource within a stack (for ListStackResources).
type StackResourceSummary struct {
	Timestamp            time.Time `xml:"LastUpdatedTimestamp"           json:"timestamp"`
	LogicalResourceID    string    `xml:"LogicalResourceId"              json:"logicalResourceID"`
	PhysicalResourceID   string    `xml:"PhysicalResourceId,omitempty"   json:"physicalResourceID,omitempty"`
	ResourceType         string    `xml:"ResourceType"                   json:"resourceType"`
	ResourceStatus       string    `xml:"ResourceStatus"                 json:"resourceStatus"`
	ResourceStatusReason string    `xml:"ResourceStatusReason,omitempty" json:"resourceStatusReason,omitempty"`
}

// Tag is a CloudFormation resource tag.
type Tag struct {
	Key   string `xml:"Key"   json:"key"`
	Value string `xml:"Value" json:"value"`
}

// StackSummary is a brief summary of a stack for ListStacks.
type StackSummary struct {
	CreationTime time.Time  `xml:"CreationTime"           json:"creationTime"`
	DeletionTime *time.Time `xml:"DeletionTime,omitempty" json:"deletionTime,omitempty"`
	StackID      string     `xml:"StackId"                json:"stackID"`
	StackName    string     `xml:"StackName"              json:"stackName"`
	StackStatus  string     `xml:"StackStatus"            json:"stackStatus"`
}

// StackEvent is a single event in a stack's history.
type StackEvent struct {
	Timestamp            time.Time `xml:"Timestamp"                      json:"timestamp"`
	EventID              string    `xml:"EventId"                        json:"eventID"`
	StackID              string    `xml:"StackId"                        json:"stackID"`
	StackName            string    `xml:"StackName"                      json:"stackName"`
	LogicalResourceID    string    `xml:"LogicalResourceId"              json:"logicalResourceID"`
	PhysicalResourceID   string    `xml:"PhysicalResourceId,omitempty"   json:"physicalResourceID,omitempty"`
	ResourceType         string    `xml:"ResourceType"                   json:"resourceType"`
	ResourceStatus       string    `xml:"ResourceStatus"                 json:"resourceStatus"`
	ResourceStatusReason string    `xml:"ResourceStatusReason,omitempty" json:"resourceStatusReason,omitempty"`
}

// StackResource represents a resource within a stack.
type StackResource struct {
	Timestamp  time.Time      `json:"timestamp"`
	Properties map[string]any `json:"properties,omitempty"`
	LogicalID  string         `json:"logicalID"`
	PhysicalID string         `json:"physicalID"`
	Type       string         `json:"type"`
	Status     string         `json:"status"`
	StackID    string         `json:"stackID"`
	StackName  string         `json:"stackName"`
}

// ChangeSet represents a CloudFormation change set.
type ChangeSet struct {
	ChangeSetID   string      `xml:"ChangeSetId"            json:"changeSetID"`
	ChangeSetName string      `xml:"ChangeSetName"          json:"changeSetName"`
	StackID       string      `xml:"StackId"                json:"stackID"`
	StackName     string      `xml:"StackName"              json:"stackName"`
	Status        string      `xml:"Status"                 json:"status"`
	StatusReason  string      `xml:"StatusReason,omitempty" json:"statusReason,omitempty"`
	CreationTime  time.Time   `xml:"CreationTime"           json:"creationTime"`
	Description   string      `xml:"Description,omitempty"  json:"description,omitempty"`
	TemplateBody  string      `xml:"-"                      json:"templateBody,omitempty"`
	Parameters    []Parameter `xml:"-"                      json:"parameters,omitempty"`
	Changes       []Change    `xml:"-"                      json:"changes,omitempty"`
}

// ChangeSetSummary is a brief summary of a change set.
type ChangeSetSummary struct {
	ChangeSetID   string    `xml:"ChangeSetId"`
	ChangeSetName string    `xml:"ChangeSetName"`
	StackID       string    `xml:"StackId"`
	StackName     string    `xml:"StackName"`
	Status        string    `xml:"Status"`
	CreationTime  time.Time `xml:"CreationTime"`
	Description   string    `xml:"Description,omitempty"`
}

// Change represents a single change in a change set.
type Change struct {
	Type           string         `xml:"Type"           json:"type"`
	ResourceChange ResourceChange `xml:"ResourceChange" json:"resourceChange"`
}

// ResourceChange describes a resource-level change.
type ResourceChange struct {
	Action       string `xml:"Action"            json:"action"`
	LogicalID    string `xml:"LogicalResourceId" json:"logicalID"`
	ResourceType string `xml:"ResourceType"      json:"resourceType"`
}

// DriftDetectionStatus holds the status of a stack drift detection operation.
type DriftDetectionStatus struct {
	Timestamp                 time.Time `xml:"Timestamp"                       json:"timestamp"`
	StackID                   string    `xml:"StackId"                         json:"stackID"`
	StackDriftDetectionID     string    `xml:"StackDriftDetectionId"           json:"stackDriftDetectionID"`
	StackDriftStatus          string    `xml:"StackDriftStatus"                json:"stackDriftStatus"`
	DetectionStatus           string    `xml:"DetectionStatus"                 json:"detectionStatus"`
	DetectionStatusReason     string    `xml:"DetectionStatusReason,omitempty" json:"detectionStatusReason,omitempty"`
	DriftedStackResourceCount int       `xml:"DriftedStackResourceCount"       json:"driftedStackResourceCount"`
}

// StackResourceDrift holds drift information for a single stack resource.
type StackResourceDrift struct {
	Timestamp                time.Time `xml:"Timestamp"                    json:"timestamp"`
	StackID                  string    `xml:"StackId"                      json:"stackID"`
	LogicalResourceID        string    `xml:"LogicalResourceId"            json:"logicalResourceID"`
	PhysicalResourceID       string    `xml:"PhysicalResourceId,omitempty" json:"physicalResourceID,omitempty"`
	ResourceType             string    `xml:"ResourceType"                 json:"resourceType"`
	StackResourceDriftStatus string    `xml:"StackResourceDriftStatus"     json:"stackResourceDriftStatus"`
}

// ParameterDeclaration describes a parameter declared in a CloudFormation template.
type ParameterDeclaration struct {
	ParameterKey             string   `xml:"ParameterKey"                       json:"parameterKey"`
	ParameterType            string   `xml:"ParameterType"                      json:"parameterType"`
	DefaultValue             string   `xml:"DefaultValue,omitempty"             json:"defaultValue,omitempty"`
	Description              string   `xml:"Description,omitempty"              json:"description,omitempty"`
	AllowedValues            []string `xml:"AllowedValues>member,omitempty"     json:"allowedValues,omitempty"`
	ConstraintDescription    string   `xml:"ConstraintDescription,omitempty"    json:"constraintDescription,omitempty"`
	AllowedPattern           string   `xml:"AllowedPattern,omitempty"           json:"allowedPattern,omitempty"`
	NoEcho                   bool     `xml:"NoEcho,omitempty"                   json:"noEcho,omitempty"`
}

// TemplateSummary holds summary information about a CloudFormation template.
type TemplateSummary struct {
	Description   string                 `xml:"Description,omitempty"          json:"description,omitempty"`
	Parameters    []ParameterDeclaration `xml:"Parameters>member,omitempty"    json:"parameters,omitempty"`
	ResourceTypes []string               `xml:"ResourceTypes>member,omitempty" json:"resourceTypes,omitempty"`
}

// AccountLimit holds a single CloudFormation account limit.
type AccountLimit struct {
	Name  string `xml:"Name"  json:"name"`
	Value int    `xml:"Value" json:"value"`
}

// StackSet represents a CloudFormation StackSet.
type StackSet struct {
	StackSetID   string `xml:"StackSetId"            json:"stackSetID"`
	StackSetName string `xml:"StackSetName"          json:"stackSetName"`
	Description  string `xml:"Description,omitempty" json:"description,omitempty"`
	Status       string `xml:"Status"                json:"status"`
	TemplateBody string `xml:"-"                     json:"templateBody,omitempty"`
}

// StackSetSummary is a brief summary of a StackSet.
type StackSetSummary struct {
	StackSetID   string `xml:"StackSetId"`
	StackSetName string `xml:"StackSetName"`
	Status       string `xml:"Status"`
	Description  string `xml:"Description,omitempty"`
}

// StackInstance represents an instance of a StackSet in a specific account/region.
type StackInstance struct {
	StackSetID   string `xml:"StackSetId,omitempty"   json:"stackSetID,omitempty"`
	StackSetName string `xml:"StackSetName,omitempty" json:"stackSetName,omitempty"`
	StackID      string `xml:"StackId,omitempty"      json:"stackID,omitempty"`
	Account      string `xml:"Account,omitempty"      json:"account,omitempty"`
	Region       string `xml:"Region,omitempty"       json:"region,omitempty"`
	Status       string `xml:"Status,omitempty"       json:"status,omitempty"`
}

// GeneratedTemplate holds a CloudFormation generated template.
type GeneratedTemplate struct {
	GeneratedTemplateID   string `xml:"GeneratedTemplateId,omitempty"   json:"generatedTemplateID,omitempty"`
	GeneratedTemplateName string `xml:"GeneratedTemplateName,omitempty" json:"generatedTemplateName,omitempty"`
	Status                string `xml:"Status,omitempty"                json:"status,omitempty"`
	TemplateBody          string `xml:"-"                               json:"templateBody,omitempty"`
}

// ResourceScan holds the status of a resource scan.
type ResourceScan struct {
	ResourceScanID      string  `xml:"ResourceScanId,omitempty"      json:"resourceScanID,omitempty"`
	Status              string  `xml:"Status,omitempty"              json:"status,omitempty"`
	PercentageCompleted float64 `xml:"PercentageCompleted,omitempty" json:"percentageCompleted,omitempty"`
}

// TypeSummary holds a brief summary of a CloudFormation type.
type TypeSummary struct {
	TypeName    string `xml:"TypeName,omitempty"`
	TypeArn     string `xml:"TypeArn,omitempty"`
	Type        string `xml:"Type,omitempty"`
	Visibility  string `xml:"Visibility,omitempty"`
	Description string `xml:"Description,omitempty"`
}

// StackSetOperation represents a StackSet operation (create/update/delete instances, etc.).
type StackSetOperation struct {
	CreatedAt    time.Time
	OperationID  string
	StackSetName string
	Action       string // CREATE_INSTANCES / UPDATE_INSTANCES / DELETE_INSTANCES / UPDATE / DETECT_DRIFT / IMPORT
	Status       string // RUNNING / SUCCEEDED / STOPPED / STOPPING / FAILED
}

// RegisteredType holds registration info for a CloudFormation type.
type RegisteredType struct {
	TypeArn        string
	TypeName       string
	Type           string // RESOURCE / MODULE / HOOK
	VersionID      string
	DefaultVersion string
	Status         string // COMPLETE / IN_PROGRESS / FAILED / DEPRECATED
	Configuration  string
	IsActivated    bool
	IsPublished    bool
}

// TypeRegistrationRecord holds the state of a type registration request.
type TypeRegistrationRecord struct {
	Token    string
	TypeName string
	TypeArn  string
	Status   string // COMPLETE / IN_PROGRESS / FAILED
}

// Publisher holds publisher registration info.
type Publisher struct {
	PublisherID   string
	ConnectionArn string
	Status        string // VERIFIED / UNVERIFIED
}

// StackRefactor holds info about a stack refactor operation.
type StackRefactor struct {
	RefactorID       string
	Description      string
	Status           string // CREATE_IN_PROGRESS / CREATE_COMPLETE / EXECUTE_IN_PROGRESS / EXECUTE_COMPLETE
	StackDefinitions []string
}

// HookResult holds the result of a CloudFormation hook invocation.
type HookResult struct {
	Token      string
	HookStatus string // IN_PROGRESS / SUCCEEDED / FAILED / SKIPPED
	ErrorCode  string
}

// SignalRecord holds a single resource signal.
type SignalRecord struct {
	UniqueID string
	Status   string
}
