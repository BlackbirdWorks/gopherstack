package iam

import "encoding/xml"

// AttachedPolicy is a simplified representation of an attached managed policy.
type AttachedPolicy struct {
	PolicyName string `json:"policyName,omitempty"`
	PolicyArn  string `json:"policyArn,omitempty"`
}

// InlinePolicyEntry is an inline policy name/document pair used in AccountAuthorizationDetails.
type InlinePolicyEntry struct {
	PolicyName     string `json:"policyName,omitempty"`
	PolicyDocument string `json:"policyDocument,omitempty"`
}

// UserDetail holds user data and all associated policies for GetAccountAuthorizationDetails.
type UserDetail struct {
	User

	AttachedPolicies []AttachedPolicy    `json:"attachedPolicies,omitempty"`
	InlinePolicies   []InlinePolicyEntry `json:"inlinePolicies,omitempty"`
	GroupNames       []string            `json:"groupNames,omitempty"`
}

// GroupDetail holds group data and all associated policies for GetAccountAuthorizationDetails.
type GroupDetail struct {
	Group

	AttachedPolicies []AttachedPolicy    `json:"attachedPolicies,omitempty"`
	InlinePolicies   []InlinePolicyEntry `json:"inlinePolicies,omitempty"`
}

// RoleDetail holds role data and all associated policies for GetAccountAuthorizationDetails.
type RoleDetail struct {
	Role

	AttachedPolicies []AttachedPolicy    `json:"attachedPolicies,omitempty"`
	InlinePolicies   []InlinePolicyEntry `json:"inlinePolicies,omitempty"`
	InstanceProfiles []InstanceProfile   `json:"instanceProfiles,omitempty"`
}

// AccountSummary holds summary counts for GetAccountSummary.
type AccountSummary struct {
	Users                      int `json:"users,omitempty"`
	Groups                     int `json:"groups,omitempty"`
	Roles                      int `json:"roles,omitempty"`
	Policies                   int `json:"policies,omitempty"`
	InstanceProfiles           int `json:"instanceProfiles,omitempty"`
	AccessKeysPerUser          int `json:"accessKeysPerUser,omitempty"`
	ActiveAccessKeys           int `json:"activeAccessKeys,omitempty"`
	AttachedPolicies           int `json:"attachedPolicies,omitempty"`
	AccountAliases             int `json:"accountAliases,omitempty"`
	OIDCProviders              int `json:"oidcProviders,omitempty"`
	SAMLProviders              int `json:"samlProviders,omitempty"`
	MFADevices                 int `json:"mfaDevices,omitempty"`
	GlobalEndpointTokenVersion int `json:"globalEndpointTokenVersion,omitempty"`
}

// AccountAuthorizationDetails is the full IAM entity dump returned by GetAccountAuthorizationDetails.
type AccountAuthorizationDetails struct {
	Users    []UserDetail  `json:"users,omitempty"`
	Groups   []GroupDetail `json:"groups,omitempty"`
	Roles    []RoleDetail  `json:"roles,omitempty"`
	Policies []Policy      `json:"policies,omitempty"`
}

// SimulationResult is the outcome of evaluating a single action/resource pair.
type SimulationResult struct {
	EvalDecisionDetails          map[string]string `json:"evalDecisionDetails,omitempty"`
	AllowedByPermissionsBoundary *bool             `json:"allowedByPermissionsBoundary,omitempty"`
	ActionName                   string            `json:"actionName,omitempty"`
	ResourceName                 string            `json:"resourceName,omitempty"`
	Decision                     string            `json:"decision,omitempty"`
}

// namedPolicyDoc pairs a policy source ID with its JSON document.
type namedPolicyDoc struct {
	// SourceID is the ARN for managed policies or the inline policy name.
	SourceID string `json:"sourceID,omitempty"`
	Doc      string `json:"doc,omitempty"`
}

// InlinePolicyEntryXML is an inline policy name/document pair in GetAccountAuthorizationDetails.
type InlinePolicyEntryXML struct {
	PolicyName     string `xml:"PolicyName"`
	PolicyDocument string `xml:"PolicyDocument"`
}

// UserDetailXML is the per-user element in GetAccountAuthorizationDetails.
type UserDetailXML struct {
	Path                    string                 `xml:"Path"`
	UserName                string                 `xml:"UserName"`
	UserID                  string                 `xml:"UserId"`
	Arn                     string                 `xml:"Arn"`
	CreateDate              string                 `xml:"CreateDate"`
	UserPolicyList          []InlinePolicyEntryXML `xml:"UserPolicyList>member"`
	AttachedManagedPolicies []AttachedPolicyXML    `xml:"AttachedManagedPolicies>member"`
	GroupList               []string               `xml:"GroupList>member"`
}

// GroupDetailXML is the per-group element in GetAccountAuthorizationDetails.
type GroupDetailXML struct {
	Path                    string                 `xml:"Path"`
	GroupName               string                 `xml:"GroupName"`
	GroupID                 string                 `xml:"GroupId"`
	Arn                     string                 `xml:"Arn"`
	CreateDate              string                 `xml:"CreateDate"`
	GroupPolicyList         []InlinePolicyEntryXML `xml:"GroupPolicyList>member"`
	AttachedManagedPolicies []AttachedPolicyXML    `xml:"AttachedManagedPolicies>member"`
}

// RoleDetailXML is the per-role element in GetAccountAuthorizationDetails.
type RoleDetailXML struct {
	Path                     string                 `xml:"Path"`
	RoleName                 string                 `xml:"RoleName"`
	RoleID                   string                 `xml:"RoleId"`
	Arn                      string                 `xml:"Arn"`
	CreateDate               string                 `xml:"CreateDate"`
	AssumeRolePolicyDocument string                 `xml:"AssumeRolePolicyDocument"`
	RolePolicyList           []InlinePolicyEntryXML `xml:"RolePolicyList>member"`
	AttachedManagedPolicies  []AttachedPolicyXML    `xml:"AttachedManagedPolicies>member"`
	InstanceProfileList      []InstanceProfileXML   `xml:"InstanceProfileList>member"`
}

// ManagedPolicyDetailXML is the per-policy element in GetAccountAuthorizationDetails.
type ManagedPolicyDetailXML struct {
	PolicyName        string             `xml:"PolicyName"`
	PolicyID          string             `xml:"PolicyId"`
	Arn               string             `xml:"Arn"`
	Path              string             `xml:"Path"`
	CreateDate        string             `xml:"CreateDate"`
	PolicyVersionList []PolicyVersionXML `xml:"PolicyVersionList>member"`
}

// GetAccountAuthorizationDetailsResponse is the XML response for GetAccountAuthorizationDetails.
type GetAccountAuthorizationDetailsResponse struct {
	XMLName                              xml.Name                             `xml:"GetAccountAuthorizationDetailsResponse"` //nolint:lll // long XML element name
	Xmlns                                string                               `xml:"xmlns,attr"`
	ResponseMetadata                     ResponseMetadata                     `xml:"ResponseMetadata"`
	GetAccountAuthorizationDetailsResult GetAccountAuthorizationDetailsResult `xml:"GetAccountAuthorizationDetailsResult"`
}

// GetAccountAuthorizationDetailsResult contains all IAM entity details.
type GetAccountAuthorizationDetailsResult struct {
	Marker          string                   `xml:"Marker,omitempty"`
	UserDetailList  []UserDetailXML          `xml:"UserDetailList>member"`
	GroupDetailList []GroupDetailXML         `xml:"GroupDetailList>member"`
	RoleDetailList  []RoleDetailXML          `xml:"RoleDetailList>member"`
	Policies        []ManagedPolicyDetailXML `xml:"Policies>member"`
	IsTruncated     bool                     `xml:"IsTruncated"`
}

// EvalDecisionDetailEntry is a single entry in the EvalDecisionDetails map.
type EvalDecisionDetailEntry struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

// PermBoundaryDecisionXML carries the boundary evaluation outcome.
type PermBoundaryDecisionXML struct {
	AllowedByPermissionsBoundary bool `xml:"AllowedByPermissionsBoundary"`
}

// SimulationEvalResultXML is a single evaluation result in SimulatePrincipalPolicy.
type SimulationEvalResultXML struct {
	// PermissionsBoundaryDecisionDetail is present when the principal has a permissions boundary.
	PermissionsBoundaryDecisionDetail *PermBoundaryDecisionXML  `xml:"PermissionsBoundaryDecisionDetail,omitempty"`
	EvalActionName                    string                    `xml:"EvalActionName"`
	EvalResourceName                  string                    `xml:"EvalResourceName"`
	EvalDecision                      string                    `xml:"EvalDecision"`
	EvalDecisionDetails               []EvalDecisionDetailEntry `xml:"EvalDecisionDetails>entry,omitempty"`
}

// SimulatePrincipalPolicyResponse is the XML response for SimulatePrincipalPolicy.
type SimulatePrincipalPolicyResponse struct {
	XMLName                       xml.Name                      `xml:"SimulatePrincipalPolicyResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
	SimulatePrincipalPolicyResult SimulatePrincipalPolicyResult `xml:"SimulatePrincipalPolicyResult"`
}

// SimulatePrincipalPolicyResult contains all evaluation results.
type SimulatePrincipalPolicyResult struct {
	EvaluationResults []SimulationEvalResultXML `xml:"EvaluationResults>member"`
	IsTruncated       bool                      `xml:"IsTruncated"`
}

// GenerateCredentialReportResponse is the XML response for GenerateCredentialReport.
type GenerateCredentialReportResponse struct {
	XMLName                        xml.Name                       `xml:"GenerateCredentialReportResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	GenerateCredentialReportResult GenerateCredentialReportResult `xml:"GenerateCredentialReportResult"`
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
}

// GenerateCredentialReportResult contains the credential report generation state.
type GenerateCredentialReportResult struct {
	State       string `xml:"State"`
	Description string `xml:"Description,omitempty"`
}

// GetCredentialReportResponse is the XML response for GetCredentialReport.
type GetCredentialReportResponse struct {
	XMLName                   xml.Name                  `xml:"GetCredentialReportResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	GetCredentialReportResult GetCredentialReportResult `xml:"GetCredentialReportResult"`
	ResponseMetadata          ResponseMetadata          `xml:"ResponseMetadata"`
}

// GetCredentialReportResult contains the credential report content.
type GetCredentialReportResult struct {
	Content       string `xml:"Content"`
	ReportFormat  string `xml:"ReportFormat"`
	GeneratedTime string `xml:"GeneratedTime"`
}
