package ssm

import "time"

// Parameter represents a single SSM Parameter.
type Parameter struct {
	Name             string            `json:"Name"`
	Type             string            `json:"Type"`
	Value            string            `json:"Value"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Description      string            `json:"Description,omitempty"`
	Version          int64             `json:"Version"`
	LastModifiedDate float64           `json:"LastModifiedDate"`
}

// PutParameterInput represents the request payload for PutParameter.
type PutParameterInput struct {
	Name        string `json:"Name"`
	Type        string `json:"Type"`
	Value       string `json:"Value"`
	Description string `json:"Description,omitempty"`
	Overwrite   bool   `json:"Overwrite,omitempty"`
}

// PutParameterOutput represents the response payload for PutParameter.
type PutParameterOutput struct {
	Version int64 `json:"Version"`
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
	Labels           []string `json:"Labels,omitempty"`
	Version          int64    `json:"Version"`
	LastModifiedDate float64  `json:"LastModifiedDate"`
}

// GetParameterHistoryInput represents the request payload for GetParameterHistory.
type GetParameterHistoryInput struct {
	Name       string `json:"Name"`
	MaxResults *int64 `json:"MaxResults,omitempty"` // 0 to 50, defaults to 50
	NextToken  string `json:"NextToken,omitempty"`
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
	Version          int64   `json:"Version"`
	LastModifiedDate float64 `json:"LastModifiedDate"`
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

// Tag represents a key/value tag pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// AddTagsToResourceInput is the request payload for AddTagsToResource.
type AddTagsToResourceInput struct {
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
	Tags         []Tag  `json:"Tags"`
}

// RemoveTagsFromResourceInput is the request payload for RemoveTagsFromResource.
type RemoveTagsFromResourceInput struct {
	ResourceType string   `json:"ResourceType"`
	ResourceID   string   `json:"ResourceId"`
	TagKeys      []string `json:"TagKeys"`
}

// ListTagsForResourceInput is the request payload for ListTagsForResource.
type ListTagsForResourceInput struct {
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
}

// ListTagsForResourceOutput is the response payload for ListTagsForResource.
type ListTagsForResourceOutput struct {
	TagList []Tag `json:"TagList"`
}

// ErrorResponse represents an SSM wire error.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// UnixTimeFloat returns a unix timestamp float required by some AWS SDKs.
const nanoToSeconds = 1e9

func UnixTimeFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / nanoToSeconds
}
