package iam

import (
	"encoding/xml"
	"time"
)

// ---- Account Aliases ----

// CreateAccountAliasResponse is the XML response for CreateAccountAlias.
type CreateAccountAliasResponse struct {
	XMLName          xml.Name         `xml:"CreateAccountAliasResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListAccountAliasesResult contains the list of account aliases.
type ListAccountAliasesResult struct {
	AccountAliases []string `xml:"AccountAliases>member"`
	IsTruncated    bool     `xml:"IsTruncated"`
}

// ListAccountAliasesResponse is the XML response for ListAccountAliases.
type ListAccountAliasesResponse struct {
	XMLName                  xml.Name                 `xml:"ListAccountAliasesResponse"`
	Xmlns                    string                   `xml:"xmlns,attr"`
	ResponseMetadata         ResponseMetadata         `xml:"ResponseMetadata"`
	ListAccountAliasesResult ListAccountAliasesResult `xml:"ListAccountAliasesResult"`
}

// DeleteAccountAliasResponse is the XML response for DeleteAccountAlias.
type DeleteAccountAliasResponse struct {
	XMLName          xml.Name         `xml:"DeleteAccountAliasResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Account Password Policy ----

// PasswordPolicy represents the IAM account password policy.
type PasswordPolicy struct {
	MinimumPasswordLength      int  `json:"MinimumPasswordLength,omitempty"`
	MaxPasswordAge             int  `json:"MaxPasswordAge,omitempty"`
	PasswordReusePrevention    int  `json:"PasswordReusePrevention,omitempty"`
	RequireUppercaseCharacters bool `json:"RequireUppercaseCharacters,omitempty"`
	RequireLowercaseCharacters bool `json:"RequireLowercaseCharacters,omitempty"`
	RequireNumbers             bool `json:"RequireNumbers,omitempty"`
	RequireSymbols             bool `json:"RequireSymbols,omitempty"`
	AllowUsersToChangePassword bool `json:"AllowUsersToChangePassword,omitempty"`
	HardExpiry                 bool `json:"HardExpiry,omitempty"`
}

// PasswordPolicyXML is the XML representation of the account password policy.
type PasswordPolicyXML struct {
	MinimumPasswordLength      int  `xml:"MinimumPasswordLength"`
	MaxPasswordAge             int  `xml:"MaxPasswordAge,omitempty"`
	PasswordReusePrevention    int  `xml:"PasswordReusePrevention,omitempty"`
	RequireUppercaseCharacters bool `xml:"RequireUppercaseCharacters"`
	RequireLowercaseCharacters bool `xml:"RequireLowercaseCharacters"`
	RequireNumbers             bool `xml:"RequireNumbers"`
	RequireSymbols             bool `xml:"RequireSymbols"`
	AllowUsersToChangePassword bool `xml:"AllowUsersToChangePassword"`
	HardExpiry                 bool `xml:"HardExpiry"`
	ExpirePasswords            bool `xml:"ExpirePasswords"`
}

// GetAccountPasswordPolicyResult contains the password policy.
type GetAccountPasswordPolicyResult struct {
	PasswordPolicy PasswordPolicyXML `xml:"PasswordPolicy"`
}

// GetAccountPasswordPolicyResponse is the XML response for GetAccountPasswordPolicy.
type GetAccountPasswordPolicyResponse struct {
	XMLName                        xml.Name                       `xml:"GetAccountPasswordPolicyResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
	GetAccountPasswordPolicyResult GetAccountPasswordPolicyResult `xml:"GetAccountPasswordPolicyResult"`
}

// UpdateAccountPasswordPolicyResponse is the XML response for UpdateAccountPasswordPolicy.
type UpdateAccountPasswordPolicyResponse struct {
	XMLName          xml.Name         `xml:"UpdateAccountPasswordPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DeleteAccountPasswordPolicyResponse is the XML response for DeleteAccountPasswordPolicy.
type DeleteAccountPasswordPolicyResponse struct {
	XMLName          xml.Name         `xml:"DeleteAccountPasswordPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Outbound Web Identity Federation ----

// EnableOutboundWebIdentityFederationResult contains the account's issuer URL.
type EnableOutboundWebIdentityFederationResult struct {
	IssuerIdentifier string `xml:"IssuerIdentifier"`
}

// EnableOutboundWebIdentityFederationResponse is the XML response for
// EnableOutboundWebIdentityFederation. The Result field is named just
// "Result" (rather than repeating the full, very long operation name) purely
// to keep the struct's source lines under the linter's line-length limit --
// the `xml:"EnableOutboundWebIdentityFederationResult"` tag, not the Go field
// name, is what determines the wire element name.
type EnableOutboundWebIdentityFederationResponse struct {
	XMLName          xml.Name                                  `xml:"EnableOutboundWebIdentityFederationResponse"`
	Xmlns            string                                    `xml:"xmlns,attr"`
	Result           EnableOutboundWebIdentityFederationResult `xml:"EnableOutboundWebIdentityFederationResult"`
	ResponseMetadata ResponseMetadata                          `xml:"ResponseMetadata"`
}

// DisableOutboundWebIdentityFederationResponse is the XML response for
// DisableOutboundWebIdentityFederation.
type DisableOutboundWebIdentityFederationResponse struct {
	XMLName          xml.Name         `xml:"DisableOutboundWebIdentityFederationResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetOutboundWebIdentityFederationInfoResult contains the account's issuer
// URL and current enabled/disabled status.
type GetOutboundWebIdentityFederationInfoResult struct {
	IssuerIdentifier  string `xml:"IssuerIdentifier"`
	JwtVendingEnabled bool   `xml:"JwtVendingEnabled"`
}

// GetOutboundWebIdentityFederationInfoResponse is the XML response for
// GetOutboundWebIdentityFederationInfo. See EnableOutboundWebIdentityFederationResponse's
// doc comment for why the Result field is named just "Result".
type GetOutboundWebIdentityFederationInfoResponse struct {
	XMLName          xml.Name                                   `xml:"GetOutboundWebIdentityFederationInfoResponse"`
	Xmlns            string                                     `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata                           `xml:"ResponseMetadata"`
	Result           GetOutboundWebIdentityFederationInfoResult `xml:"GetOutboundWebIdentityFederationInfoResult"`
}

// ---- Change Password ----

// ChangePasswordResponse is the XML response for ChangePassword.
type ChangePasswordResponse struct {
	XMLName          xml.Name         `xml:"ChangePasswordResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Delegation Request types ----

// DelegationPolicyParameter is one entry of a delegation request's
// Permissions.Parameters (aws-sdk-go-v2/service/iam/types.PolicyParameter).
type DelegationPolicyParameter struct {
	Name   string   `json:"Name,omitempty"`
	Type   string   `json:"Type,omitempty"`
	Values []string `json:"Values,omitempty"`
}

// DelegationRequest represents an IAM delegation request. GetHumanReadableSummary
// and GetDelegationRequest are the only readers of this state (see PARITY.md);
// gopherstack does not fabricate the LLM-generated summary itself.
type DelegationRequest struct {
	CreateDate           time.Time                   `json:"CreateDate"`
	DelegationID         string                      `json:"DelegationId,omitempty"`
	RedirectURL          string                      `json:"RedirectUrl,omitempty"`
	Status               string                      `json:"Status,omitempty"`
	Description          string                      `json:"Description,omitempty"`
	NotificationChannel  string                      `json:"NotificationChannel,omitempty"`
	RequestorWorkflowID  string                      `json:"RequestorWorkflowId,omitempty"`
	TargetAccountID      string                      `json:"TargetAccountId,omitempty"`
	RequestMessage       string                      `json:"RequestMessage,omitempty"`
	PolicyTemplateArn    string                      `json:"PolicyTemplateArn,omitempty"`
	Notes                string                      `json:"Notes,omitempty"`
	PermissionParameters []DelegationPolicyParameter `json:"PermissionParameters,omitempty"`
	SessionDuration      int32                       `json:"SessionDuration,omitempty"`
	OnlySendByOwner      bool                        `json:"OnlySendByOwner,omitempty"`
}

// CreateDelegationRequestInput is the parsed, validated form of
// CreateDelegationRequest's request parameters, passed to the backend.
type CreateDelegationRequestInput struct {
	Description          string
	NotificationChannel  string
	RequestorWorkflowID  string
	OwnerAccountID       string
	RedirectURL          string
	RequestMessage       string
	PolicyTemplateArn    string
	PermissionParameters []DelegationPolicyParameter
	SessionDuration      int32
	OnlySendByOwner      bool
}

// CreateDelegationRequestResult mirrors CreateDelegationRequestOutput's flat
// ConsoleDeepLink/DelegationRequestId shape (api_op_CreateDelegationRequest.go) --
// not a nested DelegationRequest object.
type CreateDelegationRequestResult struct {
	ConsoleDeepLink     string `xml:"ConsoleDeepLink"`
	DelegationRequestID string `xml:"DelegationRequestId"`
}

// CreateDelegationRequestResponse is the XML response for CreateDelegationRequest.
type CreateDelegationRequestResponse struct {
	XMLName                       xml.Name                      `xml:"CreateDelegationRequestResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	CreateDelegationRequestResult CreateDelegationRequestResult `xml:"CreateDelegationRequestResult"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
}

// GetHumanReadableSummaryResult mirrors GetHumanReadableSummaryOutput's flat
// Locale/SummaryContent/SummaryState shape (api_op_GetHumanReadableSummary.go).
// gopherstack never generates SummaryContent -- see PARITY.md for why.
type GetHumanReadableSummaryResult struct {
	Locale         string `xml:"Locale"`
	SummaryContent string `xml:"SummaryContent"`
	SummaryState   string `xml:"SummaryState"`
}

// GetHumanReadableSummaryResponse is the XML response for GetHumanReadableSummary.
type GetHumanReadableSummaryResponse struct {
	XMLName                       xml.Name                      `xml:"GetHumanReadableSummaryResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	GetHumanReadableSummaryResult GetHumanReadableSummaryResult `xml:"GetHumanReadableSummaryResult"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
}

// AcceptDelegationRequestResponse is the XML response for AcceptDelegationRequest.
type AcceptDelegationRequestResponse struct {
	XMLName          xml.Name         `xml:"AcceptDelegationRequestResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// AssociateDelegationRequestResponse is the XML response for AssociateDelegationRequest.
type AssociateDelegationRequestResponse struct {
	XMLName          xml.Name         `xml:"AssociateDelegationRequestResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// listDelegationRequestsResult contains the (always-empty, mock) delegation request list.
type listDelegationRequestsResult struct {
	DelegationRequests []string `xml:"DelegationRequests>member"`
	IsTruncated        bool     `xml:"IsTruncated"`
}

// listDelegationRequestsResponse is the XML response for ListDelegationRequests.
type listDelegationRequestsResponse struct {
	XMLName                      xml.Name                     `xml:"ListDelegationRequestsResponse"`
	Xmlns                        string                       `xml:"xmlns,attr"`
	ResponseMetadata             ResponseMetadata             `xml:"ResponseMetadata"`
	ListDelegationRequestsResult listDelegationRequestsResult `xml:"ListDelegationRequestsResult"`
}

// ---- Organizations ----

// listOrganizationsFeaturesResult contains the (always-empty, mock)
// organizations features list. Real ListOrganizationsFeaturesOutput's members
// are "EnabledFeatures" and "OrganizationId", not "OrganizationFeatures"/
// "RootId" (iam@v1.58.1 deserializers.go:
// awsAwsquery_deserializeOpDocumentListOrganizationsFeaturesOutput).
type listOrganizationsFeaturesResult struct {
	OrganizationID  string   `xml:"OrganizationId,omitempty"`
	EnabledFeatures []string `xml:"EnabledFeatures>member"`
}

// listOrganizationsFeaturesResponse is the XML response for ListOrganizationsFeatures.
type listOrganizationsFeaturesResponse struct {
	XMLName                         xml.Name                        `xml:"ListOrganizationsFeaturesResponse"`
	Xmlns                           string                          `xml:"xmlns,attr"`
	ResponseMetadata                ResponseMetadata                `xml:"ResponseMetadata"`
	ListOrganizationsFeaturesResult listOrganizationsFeaturesResult `xml:"ListOrganizationsFeaturesResult"`
}

// generateOrgAccessReportResult contains the generated organizations access report job ID.
type generateOrgAccessReportResult struct {
	JobID string `xml:"JobId"`
}

// generateOrganizationsAccessReportResponse is the XML response for GenerateOrganizationsAccessReport.
type generateOrganizationsAccessReportResponse struct {
	XMLName                                 xml.Name                      `xml:"GenerateOrganizationsAccessReportResponse"`
	Xmlns                                   string                        `xml:"xmlns,attr"`
	GenerateOrganizationsAccessReportResult generateOrgAccessReportResult `xml:"GenerateOrganizationsAccessReportResult"`
	ResponseMetadata                        ResponseMetadata              `xml:"ResponseMetadata"`
}

// getOrgAccessReportResult contains the status of an organizations access report job.
type getOrgAccessReportResult struct {
	JobStatus                   string   `xml:"JobStatus"`
	JobCreationDate             string   `xml:"JobCreationDate"`
	AccessDetails               []string `xml:"AccessDetails>member"`
	IsTruncated                 bool     `xml:"IsTruncated"`
	NumberOfServicesAccessible  int      `xml:"NumberOfServicesAccessible"`
	NumberOfServicesNotAccessed int      `xml:"NumberOfServicesNotAccessed"`
}

// getOrganizationsAccessReportResponse is the XML response for GetOrganizationsAccessReport.
type getOrganizationsAccessReportResponse struct {
	XMLName                            xml.Name                 `xml:"GetOrganizationsAccessReportResponse"`
	Xmlns                              string                   `xml:"xmlns,attr"`
	ResponseMetadata                   ResponseMetadata         `xml:"ResponseMetadata"`
	GetOrganizationsAccessReportResult getOrgAccessReportResult `xml:"GetOrganizationsAccessReportResult"`
}

// listPGSAResult contains the (always-empty, mock) policies-granting-service-access list.
// PoliciesGrantingServiceAccess is []string, not the real
// []types.PolicyGrantingServiceAccess struct list, because the list is
// always empty (see the handler's validation-only note) — an empty child
// element serializes identically either way. Revisit the element type if
// this op ever grows real emulation.
type listPGSAResult struct {
	PoliciesGrantingServiceAccess []string `xml:"PoliciesGrantingServiceAccess>member"`
	IsTruncated                   bool     `xml:"IsTruncated"`
}

// listPoliciesGrantingServiceAccessResponse is the XML response for ListPoliciesGrantingServiceAccess.
type listPoliciesGrantingServiceAccessResponse struct {
	XMLName                                 xml.Name         `xml:"ListPoliciesGrantingServiceAccessResponse"`
	Xmlns                                   string           `xml:"xmlns,attr"`
	ResponseMetadata                        ResponseMetadata `xml:"ResponseMetadata"`
	ListPoliciesGrantingServiceAccessResult listPGSAResult   `xml:"ListPoliciesGrantingServiceAccessResult"`
}
