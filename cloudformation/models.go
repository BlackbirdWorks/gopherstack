package cloudformation

import "time"

// Stack represents a CloudFormation stack.
type Stack struct {
	CreationTime      time.Time   `xml:"CreationTime"`
	LastUpdatedTime   *time.Time  `xml:"LastUpdatedTime,omitempty"`
	DeletionTime      *time.Time  `xml:"DeletionTime,omitempty"`
	StackID           string      `xml:"StackId"`
	StackName         string      `xml:"StackName"`
	Description       string      `xml:"Description,omitempty"`
	StackStatus       string      `xml:"StackStatus"`
	StackStatusReason string      `xml:"StackStatusReason,omitempty"`
	TemplateBody      string      `xml:"-"`
	Parameters        []Parameter `xml:"Parameters>member,omitempty"`
	Outputs           []Output    `xml:"Outputs>member,omitempty"`
	Tags              []Tag       `xml:"Tags>member,omitempty"`
}

// Parameter is a CloudFormation stack parameter.
type Parameter struct {
	ParameterKey   string `xml:"ParameterKey"`
	ParameterValue string `xml:"ParameterValue"`
}

// Output is a CloudFormation stack output.
type Output struct {
	OutputKey   string `xml:"OutputKey"`
	OutputValue string `xml:"OutputValue"`
	Description string `xml:"Description,omitempty"`
}

// Tag is a CloudFormation resource tag.
type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// StackSummary is a brief summary of a stack for ListStacks.
type StackSummary struct {
	CreationTime time.Time  `xml:"CreationTime"`
	DeletionTime *time.Time `xml:"DeletionTime,omitempty"`
	StackID      string     `xml:"StackId"`
	StackName    string     `xml:"StackName"`
	StackStatus  string     `xml:"StackStatus"`
}

// StackEvent is a single event in a stack's history.
type StackEvent struct {
	Timestamp            time.Time `xml:"Timestamp"`
	EventID              string    `xml:"EventId"`
	StackID              string    `xml:"StackId"`
	StackName            string    `xml:"StackName"`
	LogicalResourceID    string    `xml:"LogicalResourceId"`
	PhysicalResourceID   string    `xml:"PhysicalResourceId,omitempty"`
	ResourceType         string    `xml:"ResourceType"`
	ResourceStatus       string    `xml:"ResourceStatus"`
	ResourceStatusReason string    `xml:"ResourceStatusReason,omitempty"`
}

// StackResource represents a resource within a stack.
type StackResource struct {
	Properties map[string]any
	LogicalID  string
	PhysicalID string
	Type       string
	Status     string
}

// ChangeSet represents a CloudFormation change set.
type ChangeSet struct {
	ChangeSetID   string      `xml:"ChangeSetId"`
	ChangeSetName string      `xml:"ChangeSetName"`
	StackID       string      `xml:"StackId"`
	StackName     string      `xml:"StackName"`
	Status        string      `xml:"Status"`
	StatusReason  string      `xml:"StatusReason,omitempty"`
	CreationTime  time.Time   `xml:"CreationTime"`
	Description   string      `xml:"Description,omitempty"`
	TemplateBody  string      `xml:"-"`
	Parameters    []Parameter `xml:"-"`
	Changes       []Change    `xml:"-"`
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
	Type           string         `xml:"Type"`
	ResourceChange ResourceChange `xml:"ResourceChange"`
}

// ResourceChange describes a resource-level change.
type ResourceChange struct {
	Action       string `xml:"Action"`
	LogicalID    string `xml:"LogicalResourceId"`
	ResourceType string `xml:"ResourceType"`
}
