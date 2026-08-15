package ssm

// DeleteResourcePolicyOutput is the response for DeleteResourcePolicy.
type DeleteResourcePolicyOutput struct{}

// DeleteResourcePolicyInput is the request for DeleteResourcePolicy.
type DeleteResourcePolicyInput struct {
	ResourceARN string `json:"ResourceArn"`
	PolicyID    string `json:"PolicyId"`
	PolicyHash  string `json:"PolicyHash"`
}

// GetResourcePoliciesInput is the request payload.
type GetResourcePoliciesInput struct {
	ResourceARN string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
	MaxResults  int32  `json:"MaxResults"`
}

// GetResourcePoliciesOutput is the response payload.
type GetResourcePoliciesOutput struct{}

// PutResourcePolicyInput is the request payload.
type PutResourcePolicyInput struct {
	ResourceARN string `json:"ResourceArn"`
	Policy      string `json:"Policy"`
	PolicyID    string `json:"PolicyId"`
	PolicyHash  string `json:"PolicyHash"`
}

// PutResourcePolicyOutput is the response payload.
type PutResourcePolicyOutput struct{}

// ResourcePolicy stores a policy document attached to an SSM resource.
type ResourcePolicy struct {
	PolicyID   string `json:"PolicyId"`
	PolicyHash string `json:"PolicyHash"`
	Policy     string `json:"Policy"`
}

// GetResourcePoliciesOutputFull extends the empty stub output.
type GetResourcePoliciesOutputFull struct {
	NextToken string           `json:"NextToken,omitempty"`
	Policies  []ResourcePolicy `json:"Policies"`
}

// PutResourcePolicyOutputFull extends the empty stub.
type PutResourcePolicyOutputFull struct {
	PolicyID   string `json:"PolicyId"`
	PolicyHash string `json:"PolicyHash"`
}
