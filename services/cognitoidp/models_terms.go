package cognitoidp

import "time"

// TermsEnforcementNone is the only value AWS currently accepts for TermsEnforcementType
// (cognitoidentityprovider@1.67.4 types/enums.go:1049 -- "reserved for future use").
const TermsEnforcementNone = "NONE"

// TermsSourceLink is the only value AWS currently accepts for TermsSourceType
// (types/enums.go:1066 -- "reserved for future use").
const TermsSourceLink = "LINK"

// Terms is a terms-of-use or privacy-policy document bound to an app client.
type Terms struct {
	CreatedAt      time.Time         `json:"createdAt"`
	LastModifiedAt time.Time         `json:"lastModifiedAt"`
	Links          map[string]string `json:"links,omitempty"`
	TermsID        string            `json:"termsID,omitempty"`
	UserPoolID     string            `json:"userPoolID,omitempty"`
	ClientID       string            `json:"clientID,omitempty"`
	TermsName      string            `json:"termsName,omitempty"`
	Enforcement    string            `json:"enforcement,omitempty"`
	TermsSource    string            `json:"termsSource,omitempty"`
}

type createTermsInput struct {
	Links       map[string]string `json:"Links,omitempty"`
	ClientID    string            `json:"ClientId,omitempty"`
	Enforcement string            `json:"Enforcement,omitempty"`
	TermsName   string            `json:"TermsName,omitempty"`
	TermsSource string            `json:"TermsSource,omitempty"`
	UserPoolID  string            `json:"UserPoolId,omitempty"`
}

// termsType mirrors types.TermsType (cognitoidentityprovider@1.67.4 types/types.go:2225).
// Links is required on the wire even when empty -- toTermsType never leaves it nil.
type termsType struct {
	Links            map[string]string `json:"Links"`
	ClientID         string            `json:"ClientId,omitempty"`
	TermsID          string            `json:"TermsId,omitempty"`
	UserPoolID       string            `json:"UserPoolId,omitempty"`
	TermsName        string            `json:"TermsName,omitempty"`
	Enforcement      string            `json:"Enforcement,omitempty"`
	TermsSource      string            `json:"TermsSource,omitempty"`
	CreationDate     float64           `json:"CreationDate,omitempty"`
	LastModifiedDate float64           `json:"LastModifiedDate,omitempty"`
}

type createTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

type deleteTermsInput struct {
	TermsID    string `json:"TermsId,omitempty"`
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type deleteTermsOutput struct{}

type describeTermsInput struct {
	TermsID    string `json:"TermsId,omitempty"`
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type describeTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

type listTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

// termsDescriptionType mirrors types.TermsDescriptionType (types/types.go:2188) --
// unlike termsType it has no ClientId/Links/TermsSource/UserPoolId.
type termsDescriptionType struct {
	TermsID          string  `json:"TermsId,omitempty"`
	TermsName        string  `json:"TermsName,omitempty"`
	Enforcement      string  `json:"Enforcement,omitempty"`
	CreationDate     float64 `json:"CreationDate,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
}

type listTermsOutput struct {
	NextToken string                 `json:"NextToken,omitempty"`
	Terms     []termsDescriptionType `json:"Terms"`
}

type updateTermsInput struct {
	Links       map[string]string `json:"Links,omitempty"`
	TermsID     string            `json:"TermsId,omitempty"`
	UserPoolID  string            `json:"UserPoolId,omitempty"`
	Enforcement string            `json:"Enforcement,omitempty"`
	TermsName   string            `json:"TermsName,omitempty"`
	TermsSource string            `json:"TermsSource,omitempty"`
}

type updateTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}
