// Package organizations provides an in-memory stub for the AWS Organizations API.
package organizations

import "time"

// ----------------------------------------
// Domain models
// ----------------------------------------

// Organization represents an AWS organization.
type Organization struct {
	ID                 string
	ARN                string
	FeatureSet         string
	MasterAccountID    string
	MasterAccountARN   string
	MasterAccountEmail string
}

// Account represents an AWS account in an organization.
type Account struct {
	JoinedAt     time.Time
	ID           string
	ARN          string
	Name         string
	Email        string
	Status       string
	JoinedMethod string
}

// Root represents the root container in an organization.
type Root struct {
	ID          string
	ARN         string
	Name        string
	PolicyTypes []PolicyTypeSummary
}

// PolicyTypeSummary holds policy type enablement state for a root.
type PolicyTypeSummary struct {
	Type   string
	Status string
}

// OrganizationalUnit represents an OU in the organization hierarchy.
type OrganizationalUnit struct {
	ID       string
	ARN      string
	Name     string
	ParentID string
}

// Policy represents an Organizations policy.
type Policy struct {
	Content       string
	PolicySummary PolicySummary
}

// PolicySummary holds metadata about a policy.
type PolicySummary struct {
	ID          string
	ARN         string
	Name        string
	Description string
	Type        string
	AwsManaged  bool
}

// PolicyTargetSummary describes a target of a policy attachment.
type PolicyTargetSummary struct {
	TargetID string
	ARN      string
	Name     string
	Type     string
}

// Tag is a key-value pair attached to an Organizations resource.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// EnabledServicePrincipal holds a service principal that has been enabled for AWS service access.
type EnabledServicePrincipal struct {
	DateEnabled      time.Time `json:"DateEnabled"`
	ServicePrincipal string    `json:"ServicePrincipal"`
}

// DelegatedAdmin holds a delegated administrator registration.
type DelegatedAdmin struct {
	AccountID        string    `json:"Id"`
	ARN              string    `json:"Arn"`
	Name             string    `json:"Name"`
	Email            string    `json:"Email"`
	Status           string    `json:"Status"`
	JoinedMethod     string    `json:"JoinedMethod"`
	JoinedAt         time.Time `json:"JoinedTimestamp"`
	DelegationTime   time.Time `json:"DelegationEnabledDate"`
	ServicePrincipal string    `json:"-"`
}

// CreateAccountStatus is the result of a CreateAccount request.
type CreateAccountStatus struct {
	ID                 string    `json:"Id"`
	AccountID          string    `json:"AccountId,omitempty"`
	AccountName        string    `json:"AccountName"`
	State              string    `json:"State"`
	RequestedTimestamp time.Time `json:"RequestedTimestamp"`
	CompletedTimestamp time.Time `json:"CompletedTimestamp"`
	FailureReason      string    `json:"FailureReason,omitempty"`
}

// ----------------------------------------
// JSON request/response types
// ----------------------------------------

// -- Organization --

type createOrganizationRequest struct {
	FeatureSet string `json:"FeatureSet"`
}

type organizationObject struct {
	ID                 string `json:"Id"`
	ARN                string `json:"Arn"`
	FeatureSet         string `json:"FeatureSet"`
	MasterAccountID    string `json:"MasterAccountId"`
	MasterAccountARN   string `json:"MasterAccountArn"`
	MasterAccountEmail string `json:"MasterAccountEmail"`
}

type createOrganizationResponse struct {
	Organization organizationObject `json:"Organization"`
}

type describeOrganizationResponse struct {
	Organization organizationObject `json:"Organization"`
}

// -- Account --

type createAccountRequest struct {
	AccountName            string `json:"AccountName"`
	Email                  string `json:"Email"`
	IamUserAccessToBilling string `json:"IamUserAccessToBilling,omitempty"`
	RoleName               string `json:"RoleName,omitempty"`
	Tags                   []Tag  `json:"Tags,omitempty"`
}

type createAccountResponse struct {
	CreateAccountStatus CreateAccountStatus `json:"CreateAccountStatus"`
}

type describeCreateAccountStatusRequest struct {
	CreateAccountRequestID string `json:"CreateAccountRequestId"`
}

type describeCreateAccountStatusResponse struct {
	CreateAccountStatus CreateAccountStatus `json:"CreateAccountStatus"`
}

type describeAccountRequest struct {
	AccountID string `json:"AccountId"`
}

type accountObject struct {
	JoinedAt     time.Time `json:"JoinedTimestamp"`
	ID           string    `json:"Id"`
	ARN          string    `json:"Arn"`
	Name         string    `json:"Name"`
	Email        string    `json:"Email"`
	Status       string    `json:"Status"`
	JoinedMethod string    `json:"JoinedMethod"`
}

type describeAccountResponse struct {
	Account accountObject `json:"Account"`
}

type listAccountsResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Accounts  []accountObject `json:"Accounts"`
}

type removeAccountFromOrganizationRequest struct {
	AccountID string `json:"AccountId"`
}

type moveAccountRequest struct {
	AccountID           string `json:"AccountId"`
	SourceParentID      string `json:"SourceParentId"`
	DestinationParentID string `json:"DestinationParentId"`
}

type listAccountsForParentRequest struct {
	ParentID  string `json:"ParentId"`
	NextToken string `json:"NextToken,omitempty"`
}

type listAccountsForParentResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Accounts  []accountObject `json:"Accounts"`
}

// -- Root --

type rootObject struct {
	ID          string             `json:"Id"`
	ARN         string             `json:"Arn"`
	Name        string             `json:"Name"`
	PolicyTypes []policyTypeObject `json:"PolicyTypes"`
}

type policyTypeObject struct {
	Type   string `json:"Type"`
	Status string `json:"Status"`
}

type listRootsResponse struct {
	NextToken string       `json:"NextToken,omitempty"`
	Roots     []rootObject `json:"Roots"`
}

// -- OU --

type createOrganizationalUnitRequest struct {
	ParentID string `json:"ParentId"`
	Name     string `json:"Name"`
	Tags     []Tag  `json:"Tags,omitempty"`
}

type ouObject struct {
	ID   string `json:"Id"`
	ARN  string `json:"Arn"`
	Name string `json:"Name"`
}

type createOrganizationalUnitResponse struct {
	OrganizationalUnit ouObject `json:"OrganizationalUnit"`
}

type describeOrganizationalUnitRequest struct {
	OrganizationalUnitID string `json:"OrganizationalUnitId"`
}

type describeOrganizationalUnitResponse struct {
	OrganizationalUnit ouObject `json:"OrganizationalUnit"`
}

type deleteOrganizationalUnitRequest struct {
	OrganizationalUnitID string `json:"OrganizationalUnitId"`
}

type updateOrganizationalUnitRequest struct {
	OrganizationalUnitID string `json:"OrganizationalUnitId"`
	Name                 string `json:"Name"`
}

type updateOrganizationalUnitResponse struct {
	OrganizationalUnit ouObject `json:"OrganizationalUnit"`
}

type listOrganizationalUnitsForParentRequest struct {
	ParentID  string `json:"ParentId"`
	NextToken string `json:"NextToken,omitempty"`
}

type listOrganizationalUnitsForParentResponse struct {
	NextToken           string     `json:"NextToken,omitempty"`
	OrganizationalUnits []ouObject `json:"OrganizationalUnits"`
}

type listParentsRequest struct {
	ChildID string `json:"ChildId"`
}

type ParentSummary struct {
	ID   string `json:"Id"`
	Type string `json:"Type"`
}

type listParentsResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Parents   []ParentSummary `json:"Parents"`
}

type listChildrenRequest struct {
	ParentID  string `json:"ParentId"`
	ChildType string `json:"ChildType"`
	NextToken string `json:"NextToken,omitempty"`
}

type ChildSummary struct {
	ID   string `json:"Id"`
	Type string `json:"Type"`
}

type listChildrenResponse struct {
	NextToken string         `json:"NextToken,omitempty"`
	Children  []ChildSummary `json:"Children"`
}

// -- Policy --

type createPolicyRequest struct {
	Name        string `json:"Name"`
	Description string `json:"Description"`
	Content     string `json:"Content"`
	Type        string `json:"Type"`
	Tags        []Tag  `json:"Tags,omitempty"`
}

type policySummaryObject struct {
	ID          string `json:"Id"`
	ARN         string `json:"Arn"`
	Name        string `json:"Name"`
	Description string `json:"Description"`
	Type        string `json:"Type"`
	AwsManaged  bool   `json:"AwsManaged"`
}

type policyObject struct {
	Content       string              `json:"Content"`
	PolicySummary policySummaryObject `json:"PolicySummary"`
}

type createPolicyResponse struct {
	Policy policyObject `json:"Policy"`
}

type describePolicyRequest struct {
	PolicyID string `json:"PolicyId"`
}

type describePolicyResponse struct {
	Policy policyObject `json:"Policy"`
}

type updatePolicyRequest struct {
	PolicyID    string `json:"PolicyId"`
	Name        string `json:"Name,omitempty"`
	Description string `json:"Description,omitempty"`
	Content     string `json:"Content,omitempty"`
}

type updatePolicyResponse struct {
	Policy policyObject `json:"Policy"`
}

type deletePolicyRequest struct {
	PolicyID string `json:"PolicyId"`
}

type listPoliciesRequest struct {
	Filter    string `json:"Filter"`
	NextToken string `json:"NextToken,omitempty"`
}

type listPoliciesResponse struct {
	NextToken string                `json:"NextToken,omitempty"`
	Policies  []policySummaryObject `json:"Policies"`
}

type attachPolicyRequest struct {
	PolicyID string `json:"PolicyId"`
	TargetID string `json:"TargetId"`
}

type detachPolicyRequest struct {
	PolicyID string `json:"PolicyId"`
	TargetID string `json:"TargetId"`
}

type listPoliciesForTargetRequest struct {
	TargetID  string `json:"TargetId"`
	Filter    string `json:"Filter"`
	NextToken string `json:"NextToken,omitempty"`
}

type listPoliciesForTargetResponse struct {
	NextToken string                `json:"NextToken,omitempty"`
	Policies  []policySummaryObject `json:"Policies"`
}

type listTargetsForPolicyRequest struct {
	PolicyID  string `json:"PolicyId"`
	NextToken string `json:"NextToken,omitempty"`
}

type policyTargetObject struct {
	TargetID string `json:"TargetId"`
	ARN      string `json:"Arn"`
	Name     string `json:"Name"`
	Type     string `json:"Type"`
}

type listTargetsForPolicyResponse struct {
	NextToken string               `json:"NextToken,omitempty"`
	Targets   []policyTargetObject `json:"Targets"`
}

type enablePolicyTypeRequest struct {
	RootID     string `json:"RootId"`
	PolicyType string `json:"PolicyType"`
}

type enablePolicyTypeResponse struct {
	Root rootObject `json:"Root"`
}

type disablePolicyTypeRequest struct {
	RootID     string `json:"RootId"`
	PolicyType string `json:"PolicyType"`
}

type disablePolicyTypeResponse struct {
	Root rootObject `json:"Root"`
}

// -- Tags --

type tagResourceRequest struct {
	ResourceID string `json:"ResourceId"`
	Tags       []Tag  `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceID string   `json:"ResourceId"`
	TagKeys    []string `json:"TagKeys"`
}

type listTagsForResourceRequest struct {
	ResourceID string `json:"ResourceId"`
}

type listTagsForResourceResponse struct {
	NextToken string `json:"NextToken,omitempty"`
	Tags      []Tag  `json:"Tags"`
}

// -- Service Access --

type enableAWSServiceAccessRequest struct {
	ServicePrincipal string `json:"ServicePrincipal"`
}

type disableAWSServiceAccessRequest struct {
	ServicePrincipal string `json:"ServicePrincipal"`
}

type enabledServicePrincipalObject struct {
	DateEnabled      time.Time `json:"DateEnabled"`
	ServicePrincipal string    `json:"ServicePrincipal"`
}

type listAWSServiceAccessResponse struct {
	NextToken                string                          `json:"NextToken,omitempty"`
	EnabledServicePrincipals []enabledServicePrincipalObject `json:"EnabledServicePrincipals"`
}

// -- Delegated Admin --

type registerDelegatedAdministratorRequest struct {
	AccountID        string `json:"AccountId"`
	ServicePrincipal string `json:"ServicePrincipal"`
}

type deregisterDelegatedAdministratorRequest struct {
	AccountID        string `json:"AccountId"`
	ServicePrincipal string `json:"ServicePrincipal"`
}

type listDelegatedAdministratorsRequest struct {
	ServicePrincipal string `json:"ServicePrincipal,omitempty"`
	NextToken        string `json:"NextToken,omitempty"`
}

type delegatedAdminObject struct {
	JoinedAt       time.Time `json:"JoinedTimestamp"`
	DelegationTime time.Time `json:"DelegationEnabledDate"`
	ID             string    `json:"Id"`
	ARN            string    `json:"Arn"`
	Name           string    `json:"Name"`
	Email          string    `json:"Email"`
	Status         string    `json:"Status"`
	JoinedMethod   string    `json:"JoinedMethod"`
}

type listDelegatedAdministratorsResponse struct {
	NextToken               string                 `json:"NextToken,omitempty"`
	DelegatedAdministrators []delegatedAdminObject `json:"DelegatedAdministrators"`
}
