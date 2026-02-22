package ssm

import "time"

// Parameter represents a single SSM Parameter.
type Parameter struct {
	Name             string  `json:"Name"`
	Type             string  `json:"Type"`
	Value            string  `json:"Value"`
	Description      string  `json:"Description,omitempty"`
	Version          int64   `json:"Version"`
	LastModifiedDate float64 `json:"LastModifiedDate"`
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
type GetParametersByPathInput struct { //nolint:govet // field order matches AWS API
	// Path is the parameter path prefix to search under.
	Path string `json:"Path"`
	// WithDecryption controls whether SecureString values are decrypted.
	WithDecryption bool `json:"WithDecryption,omitempty"`
	// Recursive enables traversal beyond the first level.
	Recursive bool `json:"Recursive,omitempty"`
	// MaxResults limits the number of results (1–10, default 10).
	MaxResults *int64 `json:"MaxResults,omitempty"`
	// NextToken is the pagination cursor from a previous call.
	NextToken string `json:"NextToken,omitempty"`
	// ParameterFilters contains additional filter criteria.
	ParameterFilters []ParameterFilter `json:"ParameterFilters,omitempty"`
}

// GetParametersByPathOutput is the response payload for GetParametersByPath.
type GetParametersByPathOutput struct { //nolint:govet // field order matches AWS API
	// Parameters contains the matching parameter list.
	Parameters []Parameter `json:"Parameters"`
	// NextToken is the pagination cursor for the next page, empty if last page.
	NextToken string `json:"NextToken,omitempty"`
}

// ParameterMetadata contains parameter metadata without the parameter value.
type ParameterMetadata struct { //nolint:govet // field order matches AWS API
	// Name is the parameter name.
	Name string `json:"Name"`
	// Type is the parameter type (String, StringList, SecureString).
	Type string `json:"Type"`
	// Version is the current version number.
	Version int64 `json:"Version"`
	// LastModifiedDate is the Unix timestamp of the last modification.
	LastModifiedDate float64 `json:"LastModifiedDate"`
	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`
}

// DescribeParametersInput is the request payload for DescribeParameters.
type DescribeParametersInput struct { //nolint:govet // field order matches AWS API
	// ParameterFilters contains optional filter criteria.
	ParameterFilters []ParameterFilter `json:"ParameterFilters,omitempty"`
	// MaxResults limits the number of results (1–50, default 50).
	MaxResults *int64 `json:"MaxResults,omitempty"`
	// NextToken is the pagination cursor from a previous call.
	NextToken string `json:"NextToken,omitempty"`
}

// DescribeParametersOutput is the response payload for DescribeParameters.
type DescribeParametersOutput struct { //nolint:govet // field order matches AWS API
	// Parameters contains the parameter metadata list.
	Parameters []ParameterMetadata `json:"Parameters"`
	// NextToken is the pagination cursor for the next page, empty if last page.
	NextToken string `json:"NextToken,omitempty"`
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
