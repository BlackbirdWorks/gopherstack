package cloudformation

import "time"

// Stack represents a CloudFormation stack.
type Stack struct {
	CreationTime      time.Time   `xml:"CreationTime"                json:"creationTime"`
	LastUpdatedTime   *time.Time  `xml:"LastUpdatedTime,omitempty"   json:"lastUpdatedTime,omitempty"`
	DeletionTime      *time.Time  `xml:"DeletionTime,omitempty"      json:"deletionTime,omitempty"`
	StackID           string      `xml:"StackId"                     json:"stackID"`
	StackName         string      `xml:"StackName"                   json:"stackName"`
	Description       string      `xml:"Description,omitempty"       json:"description,omitempty"`
	StackStatus       string      `xml:"StackStatus"                 json:"stackStatus"`
	StackStatusReason string      `xml:"StackStatusReason,omitempty" json:"stackStatusReason,omitempty"`
	TemplateBody      string      `xml:"-"                           json:"templateBody,omitempty"`
	Parameters        []Parameter `xml:"Parameters>member,omitempty" json:"parameters,omitempty"`
	Outputs           []Output    `xml:"Outputs>member,omitempty"    json:"outputs,omitempty"`
	Tags              []Tag       `xml:"Tags>member,omitempty"       json:"tags,omitempty"`
}

// Parameter is a CloudFormation stack parameter.
type Parameter struct {
	ParameterKey   string `xml:"ParameterKey"   json:"parameterKey"`
	ParameterValue string `xml:"ParameterValue" json:"parameterValue"`
}

// Output is a CloudFormation stack output.
type Output struct {
	OutputKey   string `xml:"OutputKey"             json:"outputKey"`
	OutputValue string `xml:"OutputValue"           json:"outputValue"`
	Description string `xml:"Description,omitempty" json:"description,omitempty"`
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
	Properties map[string]any `json:"properties,omitempty"`
	LogicalID  string         `json:"logicalID"`
	PhysicalID string         `json:"physicalID"`
	Type       string         `json:"type"`
	Status     string         `json:"status"`
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
