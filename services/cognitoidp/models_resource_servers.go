package cognitoidp

// ResourceServerScope defines a single OAuth scope on a resource server.
type ResourceServerScope struct {
	ScopeName        string `json:"ScopeName,omitempty"`
	ScopeDescription string `json:"ScopeDescription,omitempty"`
}

// ResourceServer represents an OAuth 2.0 resource server registered to a user pool.
type ResourceServer struct {
	UserPoolID string                `json:"UserPoolId,omitempty"`
	Identifier string                `json:"Identifier,omitempty"`
	Name       string                `json:"Name,omitempty"`
	Scopes     []ResourceServerScope `json:"Scopes,omitempty"`
}

// ScopeName and ScopeDescription are both required per scope element, and the
// real SDK's client-side validator only null-checks the pointer, not its
// content -- a real client can send an empty-string scope name/description,
// so both must round-trip even when empty, never omitted.
type resourceServerScopeType struct {
	ScopeName        string `json:"ScopeName"`
	ScopeDescription string `json:"ScopeDescription"`
}

type resourceServerAccurateType struct {
	UserPoolID string                    `json:"UserPoolId,omitempty"`
	Identifier string                    `json:"Identifier,omitempty"`
	Name       string                    `json:"Name,omitempty"`
	Scopes     []resourceServerScopeType `json:"Scopes,omitempty"`
}

type createResourceServerAccurateInput struct {
	UserPoolID string                    `json:"UserPoolId,omitempty"`
	Identifier string                    `json:"Identifier,omitempty"`
	Name       string                    `json:"Name,omitempty"`
	Scopes     []resourceServerScopeType `json:"Scopes,omitempty"`
}

type createResourceServerAccurateOutput struct {
	ResourceServer resourceServerAccurateType `json:"ResourceServer"`
}

type describeResourceServerAccurateInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Identifier string `json:"Identifier,omitempty"`
}

type describeResourceServerAccurateOutput struct {
	ResourceServer resourceServerAccurateType `json:"ResourceServer"`
}

type listResourceServersAccurateInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listResourceServersAccurateOutput struct {
	NextToken       string                       `json:"NextToken,omitempty"`
	ResourceServers []resourceServerAccurateType `json:"ResourceServers,omitempty"`
}

type updateResourceServerAccurateInput struct {
	UserPoolID string                    `json:"UserPoolId,omitempty"`
	Identifier string                    `json:"Identifier,omitempty"`
	Name       string                    `json:"Name,omitempty"`
	Scopes     []resourceServerScopeType `json:"Scopes,omitempty"`
}

type updateResourceServerAccurateOutput struct {
	ResourceServer resourceServerAccurateType `json:"ResourceServer"`
}

type deleteResourceServerAccurateInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Identifier string `json:"Identifier,omitempty"`
}

type deleteResourceServerAccurateOutput struct{}
