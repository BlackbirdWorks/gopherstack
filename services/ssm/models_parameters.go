package ssm

import (
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type putParameterValidated struct {
	dataType string
	tier     string
}

// LabelParameterVersionInput is the request payload.
type LabelParameterVersionInput struct {
	Name             string   `json:"Name"`
	Labels           []string `json:"Labels"`
	ParameterVersion int64    `json:"ParameterVersion,omitempty"`
}

// LabelParameterVersionOutput is the response payload.
type LabelParameterVersionOutput struct{}

// UnlabelParameterVersionInput is the request payload.
type UnlabelParameterVersionInput struct {
	Name             string   `json:"Name"`
	Labels           []string `json:"Labels"`
	ParameterVersion int64    `json:"ParameterVersion,omitempty"`
}

// UnlabelParameterVersionOutput is the response payload.
type UnlabelParameterVersionOutput struct{}
type ParameterInlinePolicy struct {
	PolicyText   string `json:"PolicyText"`
	PolicyType   string `json:"PolicyType"`
	PolicyStatus string `json:"PolicyStatus"`
}

// Parameter represents a single SSM Parameter.
type Parameter struct {
	Name           string     `json:"Name"`
	Type           string     `json:"Type"`
	Value          string     `json:"Value"`
	Tags           *tags.Tags `json:"Tags,omitempty"`
	Description    string     `json:"Description,omitempty"`
	KeyID          string     `json:"KeyId,omitempty"`
	Tier           string     `json:"Tier,omitempty"`
	AllowedPattern string     `json:"AllowedPattern,omitempty"`
	DataType       string     `json:"DataType,omitempty"`
	Policies       string     `json:"Policies,omitempty"`
	// ARN is the Amazon Resource Name of the parameter. Real AWS SSM returns this
	// field on GetParameter, GetParameters, and GetParametersByPath responses.
	ARN string `json:"ARN,omitempty"`
	// Selector is the version or label selector used to retrieve this parameter,
	// e.g. ":3" or ":prod". Empty when the latest version is returned without a
	// selector. AWS echoes the selector back in the GetParameter response.
	Selector         string  `json:"Selector,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate"`
	Version          int64   `json:"Version"`
}

// PutParameterInput represents the request payload for PutParameter.
type PutParameterInput struct {
	Name           string `json:"Name"`
	Type           string `json:"Type"`
	Value          string `json:"Value"`
	Description    string `json:"Description,omitempty"`
	KeyID          string `json:"KeyId,omitempty"`
	Tier           string `json:"Tier,omitempty"`
	AllowedPattern string `json:"AllowedPattern,omitempty"`
	DataType       string `json:"DataType,omitempty"`
	Policies       string `json:"Policies,omitempty"`
	Overwrite      bool   `json:"Overwrite,omitempty"`
}

// PutParameterOutput represents the response payload for PutParameter.
type PutParameterOutput struct {
	Tier    string `json:"Tier,omitempty"`
	Version int64  `json:"Version"`
}

// GetParameterInput represents the request payload for GetParameter.
type GetParameterInput struct {
	Name           string `json:"Name"`
	WithDecryption bool   `json:"WithDecryption,omitempty"`
}

// GetParameterOutput represents the response payload for GetParameter.
type GetParameterOutput struct {
	Parameter Parameter `json:"Parameter"`
}

// GetParametersInput represents the request payload for GetParameters.
type GetParametersInput struct {
	Names          []string `json:"Names"`
	WithDecryption bool     `json:"WithDecryption,omitempty"`
}

// GetParametersOutput represents the response payload for GetParameters.
type GetParametersOutput struct {
	Parameters        []Parameter `json:"Parameters"`
	InvalidParameters []string    `json:"InvalidParameters"`
}

// DeleteParameterInput represents the request payload for DeleteParameter.
type DeleteParameterInput struct {
	Name string `json:"Name"`
}

// DeleteParameterOutput represents the response payload for DeleteParameter.
type DeleteParameterOutput struct{}

// DeleteParametersInput represents the request payload for DeleteParameters.
type DeleteParametersInput struct {
	Names []string `json:"Names"`
}

// DeleteParametersOutput represents the response payload for DeleteParameters.
type DeleteParametersOutput struct {
	DeletedParameters []string `json:"DeletedParameters"`
	InvalidParameters []string `json:"InvalidParameters"`
}

// ParameterHistory represents a historical version of a parameter.
type ParameterHistory struct {
	Name             string   `json:"Name"`
	Type             string   `json:"Type"`
	Value            string   `json:"Value"`
	KeyID            string   `json:"KeyId,omitempty"`
	Tier             string   `json:"Tier,omitempty"`
	AllowedPattern   string   `json:"AllowedPattern,omitempty"`
	DataType         string   `json:"DataType,omitempty"`
	Description      string   `json:"Description,omitempty"`
	Labels           []string `json:"Labels,omitempty"`
	LastModifiedDate float64  `json:"LastModifiedDate"`
	Version          int64    `json:"Version"`
}

// GetParameterHistoryInput represents the request payload for GetParameterHistory.
type GetParameterHistoryInput struct {
	Name           string `json:"Name"`
	MaxResults     *int64 `json:"MaxResults,omitempty"` // 0 to 50, defaults to 50
	NextToken      string `json:"NextToken,omitempty"`
	WithDecryption bool   `json:"WithDecryption,omitempty"`
}

// GetParameterHistoryOutput represents the response payload for GetParameterHistory.
type GetParameterHistoryOutput struct {
	NextToken  string             `json:"NextToken,omitempty"`
	Parameters []ParameterHistory `json:"Parameters"`
}

// ParameterFilter is a filter criterion for parameter queries.
type ParameterFilter struct {
	// Key is the filter key: Name, Type, KeyId, etc.
	Key string `json:"Key"`
	// Option is the comparison operator: Equals, BeginsWith, Contains.
	Option string `json:"Option,omitempty"`
	// Values contains the values to match against.
	Values []string `json:"Values"`
}

// GetParametersByPathInput is the request payload for GetParametersByPath.
type GetParametersByPathInput struct {
	MaxResults       *int64            `json:"MaxResults,omitempty"`
	Path             string            `json:"Path"`
	NextToken        string            `json:"NextToken,omitempty"`
	ParameterFilters []ParameterFilter `json:"ParameterFilters,omitempty"`
	WithDecryption   bool              `json:"WithDecryption,omitempty"`
	Recursive        bool              `json:"Recursive,omitempty"`
}

// GetParametersByPathOutput is the response payload for GetParametersByPath.
type GetParametersByPathOutput struct {
	NextToken  string      `json:"NextToken,omitempty"`
	Parameters []Parameter `json:"Parameters"`
}

// ParameterMetadata contains parameter metadata without the parameter value.
type ParameterMetadata struct {
	Name             string  `json:"Name"`
	Type             string  `json:"Type"`
	Description      string  `json:"Description,omitempty"`
	KeyID            string  `json:"KeyId,omitempty"`
	Tier             string  `json:"Tier,omitempty"`
	AllowedPattern   string  `json:"AllowedPattern,omitempty"`
	DataType         string  `json:"DataType,omitempty"`
	Policies         string  `json:"Policies,omitempty"`
	ARN              string  `json:"ARN,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate"`
	Version          int64   `json:"Version"`
}

// DescribeParametersInput is the request payload for DescribeParameters.
type DescribeParametersInput struct {
	MaxResults       *int64            `json:"MaxResults,omitempty"`
	NextToken        string            `json:"NextToken,omitempty"`
	ParameterFilters []ParameterFilter `json:"ParameterFilters,omitempty"`
}

// DescribeParametersOutput is the response payload for DescribeParameters.
type DescribeParametersOutput struct {
	NextToken  string              `json:"NextToken,omitempty"`
	Parameters []ParameterMetadata `json:"Parameters"`
}

// LabelParameterVersionOutputFull extends the empty stub.
type LabelParameterVersionOutputFull struct {
	InvalidLabels []string `json:"InvalidLabels"`
	// ParameterVersion is the version of the parameter the labels were attached
	// to. AWS returns this so callers know which version a label-without-version
	// request resolved to.
	ParameterVersion int64 `json:"ParameterVersion"`
}

// UnlabelParameterVersionOutputFull extends the empty stub.
type UnlabelParameterVersionOutputFull struct {
	InvalidLabels []string `json:"InvalidLabels"`
	RemovedLabels []string `json:"RemovedLabels"`
}
