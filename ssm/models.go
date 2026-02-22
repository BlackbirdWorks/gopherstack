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
