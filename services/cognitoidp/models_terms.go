package cognitoidp

// Terms stores the terms and conditions text for a user pool.
type Terms struct {
	UserPoolID string `json:"userPoolID,omitempty"`
	Text       string `json:"text,omitempty"`
}

type termsType struct {
	DefaultTermsAndConditions string `json:"DefaultTermsAndConditions,omitempty"`
}

type createTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type createTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

type deleteTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type deleteTermsOutput struct{}

type describeTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type describeTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

type listTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type listTermsOutput struct {
	Terms []termsType `json:"Terms"`
}

type updateTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type updateTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}
