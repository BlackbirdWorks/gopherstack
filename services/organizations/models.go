// Package organizations provides an in-memory stub for the AWS Organizations API.
package organizations

import "time"

// epochSeconds returns t as Unix epoch seconds (float64).
// The AWS SDK Go v2 deserializes JSON timestamps as float64 epoch seconds,
// so all timestamp fields in JSON response types must use this type.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// ----------------------------------------
// Domain models
// ----------------------------------------

// Organization represents an AWS organization.
type Organization struct {
	ID                   string              `json:"id"`
	ARN                  string              `json:"arn"`
	FeatureSet           string              `json:"featureSet"`
	MasterAccountID      string              `json:"masterAccountID"`
	MasterAccountARN     string              `json:"masterAccountARN"`
	MasterAccountEmail   string              `json:"masterAccountEmail"`
	AvailablePolicyTypes []PolicyTypeSummary `json:"availablePolicyTypes,omitempty"`
}

// Account represents an AWS account in an organization.
type Account struct {
	JoinedAt               time.Time `json:"joinedAt"`
	ID                     string    `json:"id"`
	ARN                    string    `json:"arn"`
	Name                   string    `json:"name"`
	Email                  string    `json:"email"`
	Status                 string    `json:"status"`
	JoinedMethod           string    `json:"joinedMethod"`
	RoleName               string    `json:"roleName,omitempty"`
	IamUserAccessToBilling string    `json:"iamUserAccessToBilling,omitempty"`
}

// Root represents the root container in an organization.
type Root struct {
	ID          string              `json:"id"`
	ARN         string              `json:"arn"`
	Name        string              `json:"name"`
	PolicyTypes []PolicyTypeSummary `json:"policyTypes,omitempty"`
}

// PolicyTypeSummary holds policy type enablement state for a root.
type PolicyTypeSummary struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

// OrganizationalUnit represents an OU in the organization hierarchy.
type OrganizationalUnit struct {
	ID       string `json:"id"`
	ARN      string `json:"arn"`
	Name     string `json:"name"`
	ParentID string `json:"parentID"`
}

// Policy represents an Organizations policy.
type Policy struct {
	Content       string        `json:"content"`
	PolicySummary PolicySummary `json:"policySummary"`
}

// PolicySummary holds metadata about a policy.
type PolicySummary struct {
	ID          string `json:"id"`
	ARN         string `json:"arn"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
	AwsManaged  bool   `json:"awsManaged"`
}

// PolicyTargetSummary describes a target of a policy attachment.
type PolicyTargetSummary struct {
	TargetID string `json:"targetID"`
	ARN      string `json:"arn"`
	Name     string `json:"name"`
	Type     string `json:"type"`
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
	ID                 string  `json:"Id"`
	AccountID          string  `json:"AccountId,omitempty"`
	GovCloudAccountID  string  `json:"GovCloudAccountId,omitempty"`
	AccountName        string  `json:"AccountName"`
	State              string  `json:"State"`
	FailureReason      string  `json:"FailureReason,omitempty"`
	RequestedTimestamp float64 `json:"RequestedTimestamp"`
	CompletedTimestamp float64 `json:"CompletedTimestamp"`
}

// Handshake represents an AWS Organizations handshake invitation or transfer.
type Handshake struct {
	RequestedTimestamp  time.Time           `json:"requestedTimestamp"`
	ExpirationTimestamp time.Time           `json:"expirationTimestamp"`
	ID                  string              `json:"id"`
	ARN                 string              `json:"arn"`
	Action              string              `json:"action"`
	State               string              `json:"state"`
	Parties             []HandshakeParty    `json:"parties"`
	Resources           []HandshakeResource `json:"resources"`
}

// HandshakeParty is a participant in a handshake.
type HandshakeParty struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// HandshakeResource holds a resource associated with a handshake.
type HandshakeResource struct {
	Type      string              `json:"type"`
	Value     string              `json:"value"`
	Resources []HandshakeResource `json:"resources,omitempty"`
}

// ResourcePolicy represents the organization resource-based policy.
type ResourcePolicy struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	Content string `json:"content"`
}

// DelegatedService holds a service principal for which an account is a delegated administrator.
type DelegatedService struct {
	DelegationEnabledDate time.Time `json:"DelegationEnabledDate"`
	ServicePrincipal      string    `json:"ServicePrincipal"`
}

// EffectivePolicy represents the aggregated effective policy for a target.
type EffectivePolicy struct {
	LastUpdatedTimestamp time.Time `json:"lastUpdatedTimestamp"`
	PolicyContent        string    `json:"policyContent"`
	PolicyID             string    `json:"policyId"`
	PolicyType           string    `json:"policyType"`
	TargetID             string    `json:"targetId"`
}

// ----------------------------------------
// JSON request/response types
// ----------------------------------------

// -- Organization --

type createOrganizationRequest struct {
	FeatureSet string `json:"FeatureSet"`
}

type organizationObject struct {
	ID                   string             `json:"Id"`
	ARN                  string             `json:"Arn"`
	FeatureSet           string             `json:"FeatureSet"`
	MasterAccountID      string             `json:"MasterAccountId"`
	MasterAccountARN     string             `json:"MasterAccountArn"`
	MasterAccountEmail   string             `json:"MasterAccountEmail"`
	AvailablePolicyTypes []policyTypeObject `json:"AvailablePolicyTypes,omitempty"`
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
	ID                     string  `json:"Id"`
	ARN                    string  `json:"Arn"`
	Name                   string  `json:"Name"`
	Email                  string  `json:"Email"`
	Status                 string  `json:"Status"`
	JoinedMethod           string  `json:"JoinedMethod"`
	RoleName               string  `json:"RoleName,omitempty"`
	IamUserAccessToBilling string  `json:"IamUserAccessToBilling,omitempty"`
	JoinedAt               float64 `json:"JoinedTimestamp"`
}

type describeAccountResponse struct {
	Account accountObject `json:"Account"`
}

type listAccountsRequest struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
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
	ParentID   string `json:"ParentId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
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
	ParentID   string `json:"ParentId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
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
	ParentID   string `json:"ParentId"`
	ChildType  string `json:"ChildType"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
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
	Filter     string `json:"Filter"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
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
	ServicePrincipal string  `json:"ServicePrincipal"`
	DateEnabled      float64 `json:"DateEnabled"`
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
	ID             string  `json:"Id"`
	ARN            string  `json:"Arn"`
	Name           string  `json:"Name"`
	Email          string  `json:"Email"`
	Status         string  `json:"Status"`
	JoinedMethod   string  `json:"JoinedMethod"`
	JoinedAt       float64 `json:"JoinedTimestamp"`
	DelegationTime float64 `json:"DelegationEnabledDate"`
}

type listDelegatedAdministratorsResponse struct {
	NextToken               string                 `json:"NextToken,omitempty"`
	DelegatedAdministrators []delegatedAdminObject `json:"DelegatedAdministrators"`
}

// -- Handshake --

type acceptHandshakeRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type cancelHandshakeRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type declineHandshakeRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type describeHandshakeRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type describeResponsibilityTransferRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type handshakePartyObject struct {
	ID   string `json:"Id"`
	Type string `json:"Type"`
}

type handshakeResourceObject struct {
	Type      string                    `json:"Type"`
	Value     string                    `json:"Value"`
	Resources []handshakeResourceObject `json:"Resources,omitempty"`
}

type handshakeObject struct {
	ID                  string                    `json:"Id"`
	ARN                 string                    `json:"Arn"`
	Action              string                    `json:"Action"`
	State               string                    `json:"State"`
	Parties             []handshakePartyObject    `json:"Parties"`
	Resources           []handshakeResourceObject `json:"Resources"`
	RequestedTimestamp  float64                   `json:"RequestedTimestamp"`
	ExpirationTimestamp float64                   `json:"ExpirationTimestamp"`
}

type acceptHandshakeResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

type cancelHandshakeResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

type declineHandshakeResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

type describeHandshakeResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

type describeResponsibilityTransferResponse struct {
	HandshakeDetails handshakeObject `json:"HandshakeDetails"`
}

// -- CloseAccount --

type closeAccountRequest struct {
	AccountID string `json:"AccountId"`
}

// -- CreateGovCloudAccount --

type createGovCloudAccountRequest struct {
	AccountName            string `json:"AccountName"`
	Email                  string `json:"Email"`
	IamUserAccessToBilling string `json:"IamUserAccessToBilling,omitempty"`
	RoleName               string `json:"RoleName,omitempty"`
	Tags                   []Tag  `json:"Tags,omitempty"`
}

type createGovCloudAccountResponse struct {
	CreateAccountStatus CreateAccountStatus `json:"CreateAccountStatus"`
}

// -- ResourcePolicy --

type resourcePolicySummaryObject struct {
	ARN string `json:"Arn"`
	ID  string `json:"Id"`
}

type resourcePolicyObject struct {
	Content               string                      `json:"Content"`
	ResourcePolicySummary resourcePolicySummaryObject `json:"ResourcePolicySummary"`
}

type describeResourcePolicyResponse struct {
	ResourcePolicy resourcePolicyObject `json:"ResourcePolicy"`
}

// -- EffectivePolicy --

type describeEffectivePolicyRequest struct {
	PolicyType string `json:"PolicyType"`
	TargetID   string `json:"TargetId,omitempty"`
}

type effectivePolicyObject struct {
	PolicyContent        string  `json:"PolicyContent"`
	PolicyID             string  `json:"PolicyId"`
	PolicyType           string  `json:"PolicyType"`
	TargetID             string  `json:"TargetId"`
	LastUpdatedTimestamp float64 `json:"LastUpdatedTimestamp"`
}

type describeEffectivePolicyResponse struct {
	EffectivePolicy effectivePolicyObject `json:"EffectivePolicy"`
}

// -- EnableAllFeatures --

type enableAllFeaturesResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

// -- ListHandshakes filter --

type handshakeFilter struct {
	ActionType string `json:"ActionType,omitempty"`
}

type listHandshakesFilterRequest struct {
	Filter     handshakeFilter `json:"Filter"`
	NextToken  string          `json:"NextToken,omitempty"`
	MaxResults int             `json:"MaxResults,omitempty"`
}

// -- InviteAccountToOrganization --

type inviteAccountToOrganizationRequest struct {
	Target HandshakeParty `json:"Target"`
	Notes  string         `json:"Notes,omitempty"`
}

type inviteAccountToOrganizationResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}

// -- LeaveOrganization --
// (no request body; no response body)

// -- ListHandshakesForAccount --

type listHandshakesForAccountResponse struct {
	NextToken  string            `json:"NextToken,omitempty"`
	Handshakes []handshakeObject `json:"Handshakes"`
}

// -- ListHandshakesForOrganization --

type listHandshakesForOrganizationResponse struct {
	NextToken  string            `json:"NextToken,omitempty"`
	Handshakes []handshakeObject `json:"Handshakes"`
}

// -- ListCreateAccountStatus --

type listCreateAccountStatusRequest struct {
	NextToken string   `json:"NextToken,omitempty"`
	States    []string `json:"States,omitempty"`
}

type listCreateAccountStatusResponse struct {
	NextToken             string                `json:"NextToken,omitempty"`
	CreateAccountStatuses []CreateAccountStatus `json:"CreateAccountStatuses"`
}

// -- ListDelegatedServicesForAccount --

type listDelegatedServicesForAccountRequest struct {
	AccountID string `json:"AccountId"`
	NextToken string `json:"NextToken,omitempty"`
}

type delegatedServiceObject struct {
	ServicePrincipal      string  `json:"ServicePrincipal"`
	DelegationEnabledDate float64 `json:"DelegationEnabledDate"`
}

type listDelegatedServicesForAccountResponse struct {
	NextToken         string                   `json:"NextToken,omitempty"`
	DelegatedServices []delegatedServiceObject `json:"DelegatedServices"`
}

// -- ListEffectivePolicyValidationErrors --

type listEffectivePolicyValidationErrorsRequest struct {
	PolicyType string `json:"PolicyType"`
	TargetID   string `json:"TargetId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listEffectivePolicyValidationErrorsResponse struct {
	NextToken        string `json:"NextToken,omitempty"`
	ValidationErrors []any  `json:"ValidationErrors"`
}

// -- ListAccountsWithInvalidEffectivePolicy --

type listAccountsWithInvalidEffectivePolicyRequest struct {
	PolicyType string `json:"PolicyType"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listAccountsWithInvalidEffectivePolicyResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Accounts  []accountObject `json:"Accounts"`
}

// -- ListInboundResponsibilityTransfers --

type listInboundResponsibilityTransfersResponse struct {
	NextToken               string            `json:"NextToken,omitempty"`
	ResponsibilityTransfers []handshakeObject `json:"ResponsibilityTransfers"`
}

// -- ListOutboundResponsibilityTransfers --

type listOutboundResponsibilityTransfersResponse struct {
	NextToken               string            `json:"NextToken,omitempty"`
	ResponsibilityTransfers []handshakeObject `json:"ResponsibilityTransfers"`
}

// -- TerminateResponsibilityTransfer --

type terminateResponsibilityTransferRequest struct {
	HandshakeID string `json:"HandshakeId"`
}

type terminateResponsibilityTransferResponse struct {
	HandshakeDetails handshakeObject `json:"HandshakeDetails"`
}

// -- UpdateResponsibilityTransfer --

type updateResponsibilityTransferRequest struct {
	HandshakeID string `json:"HandshakeId"`
	Action      string `json:"Action"`
}

type updateResponsibilityTransferResponse struct {
	HandshakeDetails handshakeObject `json:"HandshakeDetails"`
}

// -- InviteOrganizationToTransferResponsibility --

type inviteOrganizationToTransferResponsibilityRequest struct {
	Target HandshakeParty `json:"Target"`
	Notes  string         `json:"Notes,omitempty"`
}

type inviteOrganizationToTransferResponsibilityResponse struct {
	Handshake handshakeObject `json:"Handshake"`
}
