package iam

import "encoding/xml"

// ---- Access Key Last Used ----

// AccessKeyLastUsed contains information about the last time an access key was used.
type AccessKeyLastUsed struct {
	UserName     string `json:"UserName,omitempty"`
	AccessKeyID  string `json:"AccessKeyId,omitempty"`
	LastUsedDate string `json:"LastUsedDate,omitempty"`
	ServiceName  string `json:"ServiceName,omitempty"`
	Region       string `json:"Region,omitempty"`
}

// AccessKeyLastUsedXML is the XML representation for GetAccessKeyLastUsed response.
type AccessKeyLastUsedXML struct {
	LastUsedDate string `xml:"LastUsedDate"`
	ServiceName  string `xml:"ServiceName"`
	Region       string `xml:"Region"`
}

// GetAccessKeyLastUsedResult contains the access key last used details.
type GetAccessKeyLastUsedResult struct {
	UserName          string               `xml:"UserName"`
	AccessKeyLastUsed AccessKeyLastUsedXML `xml:"AccessKeyLastUsed"`
}

// GetAccessKeyLastUsedResponse is the XML response for GetAccessKeyLastUsed.
type GetAccessKeyLastUsedResponse struct {
	XMLName                    xml.Name                   `xml:"GetAccessKeyLastUsedResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           ResponseMetadata           `xml:"ResponseMetadata"`
	GetAccessKeyLastUsedResult GetAccessKeyLastUsedResult `xml:"GetAccessKeyLastUsedResult"`
}

// UpdateAccessKeyResponse is the XML response for UpdateAccessKey.
type UpdateAccessKeyResponse struct {
	XMLName          xml.Name         `xml:"UpdateAccessKeyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Account Aliases ----

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

// ---- Groups For User ----

// ListGroupsForUserXML holds a single group entry returned by ListGroupsForUser.
type ListGroupsForUserXML struct {
	GroupName  string `xml:"GroupName"`
	GroupID    string `xml:"GroupId"`
	Arn        string `xml:"Arn"`
	Path       string `xml:"Path"`
	CreateDate string `xml:"CreateDate"`
}

// ListGroupsForUserResult contains the list of groups.
type ListGroupsForUserResult struct {
	Groups      []ListGroupsForUserXML `xml:"Groups>member"`
	IsTruncated bool                   `xml:"IsTruncated"`
}

// ListGroupsForUserResponse is the XML response for ListGroupsForUser.
type ListGroupsForUserResponse struct {
	XMLName                 xml.Name                `xml:"ListGroupsForUserResponse"`
	Xmlns                   string                  `xml:"xmlns,attr"`
	ResponseMetadata        ResponseMetadata        `xml:"ResponseMetadata"`
	ListGroupsForUserResult ListGroupsForUserResult `xml:"ListGroupsForUserResult"`
}

// ---- Virtual MFA Devices ----

// ListVirtualMFADevicesResult contains the list of virtual MFA devices.
type ListVirtualMFADevicesResult struct {
	VirtualMFADevices []VirtualMFADeviceXML `xml:"VirtualMFADevices>member"`
	IsTruncated       bool                  `xml:"IsTruncated"`
}

// ListVirtualMFADevicesResponse is the XML response for ListVirtualMFADevices.
type ListVirtualMFADevicesResponse struct {
	XMLName                     xml.Name                    `xml:"ListVirtualMFADevicesResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	ResponseMetadata            ResponseMetadata            `xml:"ResponseMetadata"`
	ListVirtualMFADevicesResult ListVirtualMFADevicesResult `xml:"ListVirtualMFADevicesResult"`
}

// DeleteVirtualMFADeviceResponse is the XML response for DeleteVirtualMFADevice.
type DeleteVirtualMFADeviceResponse struct {
	XMLName          xml.Name         `xml:"DeleteVirtualMFADeviceResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Policy Version Management ----

// SetDefaultPolicyVersionResponse is the XML response for SetDefaultPolicyVersion.
type SetDefaultPolicyVersionResponse struct {
	XMLName          xml.Name         `xml:"SetDefaultPolicyVersionResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DeletePolicyVersionResponse is the XML response for DeletePolicyVersion.
type DeletePolicyVersionResponse struct {
	XMLName          xml.Name         `xml:"DeletePolicyVersionResponse"`
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

// ---- List Entities For Policy ----

// PolicyEntityUser is a user attached to a managed policy.
type PolicyEntityUser struct {
	UserName string `xml:"UserName"`
}

// PolicyEntityGroup is a group attached to a managed policy.
type PolicyEntityGroup struct {
	GroupName string `xml:"GroupName"`
}

// PolicyEntityRole is a role attached to a managed policy.
type PolicyEntityRole struct {
	RoleName string `xml:"RoleName"`
}

// PolicyEntities is the collection of entities attached to a managed policy.
type PolicyEntities struct {
	PolicyUsers  []PolicyEntityUser  `json:"PolicyUsers,omitempty"`
	PolicyGroups []PolicyEntityGroup `json:"PolicyGroups,omitempty"`
	PolicyRoles  []PolicyEntityRole  `json:"PolicyRoles,omitempty"`
}

// ListEntitiesForPolicyResult contains the policy entity lists.
type ListEntitiesForPolicyResult struct {
	PolicyUsers  []PolicyEntityUser  `xml:"PolicyUsers>member"`
	PolicyGroups []PolicyEntityGroup `xml:"PolicyGroups>member"`
	PolicyRoles  []PolicyEntityRole  `xml:"PolicyRoles>member"`
	IsTruncated  bool                `xml:"IsTruncated"`
}

// ListEntitiesForPolicyResponse is the XML response for ListEntitiesForPolicy.
type ListEntitiesForPolicyResponse struct {
	XMLName                     xml.Name                    `xml:"ListEntitiesForPolicyResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	ResponseMetadata            ResponseMetadata            `xml:"ResponseMetadata"`
	ListEntitiesForPolicyResult ListEntitiesForPolicyResult `xml:"ListEntitiesForPolicyResult"`
}

// ---- Service-Specific Credentials List / Delete ----

// ServiceSpecificCredentialMetadataXML is the XML representation of credential metadata.
type ServiceSpecificCredentialMetadataXML struct {
	UserName                    string `xml:"UserName"`
	ServiceName                 string `xml:"ServiceName"`
	ServiceUserName             string `xml:"ServiceUserName"`
	ServiceSpecificCredentialID string `xml:"ServiceSpecificCredentialId"`
	Status                      string `xml:"Status"`
	CreateDate                  string `xml:"CreateDate"`
}

// ListServiceSpecificCredentialsResult contains the list of credentials.
type ListServiceSpecificCredentialsResult struct {
	ServiceSpecificCredentials []ServiceSpecificCredentialMetadataXML `xml:"ServiceSpecificCredentials>member"`
}

// ListServiceSpecificCredentialsResponse is the XML response for ListServiceSpecificCredentials.
type ListServiceSpecificCredentialsResponse struct {
	XMLName xml.Name                             `xml:"ListServiceSpecificCredentialsResponse"`
	Xmlns   string                               `xml:"xmlns,attr"`
	Meta    ResponseMetadata                     `xml:"ResponseMetadata"`
	Result  ListServiceSpecificCredentialsResult `xml:"ListServiceSpecificCredentialsResult"`
}

// DeleteServiceSpecificCredentialResponse is the XML response for DeleteServiceSpecificCredential.
type DeleteServiceSpecificCredentialResponse struct {
	XMLName          xml.Name         `xml:"DeleteServiceSpecificCredentialResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Update operations ----

// UpdateUserResponse is the XML response for UpdateUser.
type UpdateUserResponse struct {
	XMLName          xml.Name         `xml:"UpdateUserResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// UpdateRoleResult wraps the updated role.
type UpdateRoleResult struct {
	Role RoleXML `xml:"Role"`
}

// UpdateRoleResponse is the XML response for UpdateRole.
type UpdateRoleResponse struct {
	XMLName          xml.Name         `xml:"UpdateRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	UpdateRoleResult UpdateRoleResult `xml:"UpdateRoleResult"`
}

// UpdateRoleDescriptionResult wraps the updated role.
type UpdateRoleDescriptionResult struct {
	Role RoleXML `xml:"Role"`
}

// UpdateRoleDescriptionResponse is the XML response for UpdateRoleDescription.
type UpdateRoleDescriptionResponse struct {
	XMLName                     xml.Name                    `xml:"UpdateRoleDescriptionResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	ResponseMetadata            ResponseMetadata            `xml:"ResponseMetadata"`
	UpdateRoleDescriptionResult UpdateRoleDescriptionResult `xml:"UpdateRoleDescriptionResult"`
}

// UpdateGroupResponse is the XML response for UpdateGroup.
type UpdateGroupResponse struct {
	XMLName          xml.Name         `xml:"UpdateGroupResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- OIDC Provider Client ID Removal ----

// RemoveClientIDFromOpenIDConnectProviderResponse is the XML response for RemoveClientIDFromOpenIDConnectProvider.
type RemoveClientIDFromOpenIDConnectProviderResponse struct {
	XMLName          xml.Name         `xml:"RemoveClientIDFromOpenIDConnectProviderResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- GetInstanceProfile ----

// GetInstanceProfileResult wraps the instance profile.
type GetInstanceProfileResult struct {
	InstanceProfile InstanceProfileXML `xml:"InstanceProfile"`
}

// GetInstanceProfileResponse is the XML response for GetInstanceProfile.
type GetInstanceProfileResponse struct {
	XMLName                  xml.Name                 `xml:"GetInstanceProfileResponse"`
	Xmlns                    string                   `xml:"xmlns,attr"`
	ResponseMetadata         ResponseMetadata         `xml:"ResponseMetadata"`
	GetInstanceProfileResult GetInstanceProfileResult `xml:"GetInstanceProfileResult"`
}

// ---- ListInstanceProfilesForRole ----

// ListInstanceProfilesForRoleResult contains the instance profile list.
type ListInstanceProfilesForRoleResult struct {
	InstanceProfiles []InstanceProfileXML `xml:"InstanceProfiles>member"`
	IsTruncated      bool                 `xml:"IsTruncated"`
}

// ListInstanceProfilesForRoleResponse is the XML response for ListInstanceProfilesForRole.
type ListInstanceProfilesForRoleResponse struct {
	XMLName                           xml.Name                          `xml:"ListInstanceProfilesForRoleResponse"`
	Xmlns                             string                            `xml:"xmlns,attr"`
	ResponseMetadata                  ResponseMetadata                  `xml:"ResponseMetadata"`
	ListInstanceProfilesForRoleResult ListInstanceProfilesForRoleResult `xml:"ListInstanceProfilesForRoleResult"`
}

// ---- SimulateCustomPolicy ----

// SimulateCustomPolicyResult contains all evaluation results for SimulateCustomPolicy.
type SimulateCustomPolicyResult struct {
	EvaluationResults []SimulationEvalResultXML `xml:"EvaluationResults>member"`
	IsTruncated       bool                      `xml:"IsTruncated"`
}

// SimulateCustomPolicyResponse is the XML response for SimulateCustomPolicy.
type SimulateCustomPolicyResponse struct {
	XMLName                    xml.Name                   `xml:"SimulateCustomPolicyResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           ResponseMetadata           `xml:"ResponseMetadata"`
	SimulateCustomPolicyResult SimulateCustomPolicyResult `xml:"SimulateCustomPolicyResult"`
}

// ---- GetServiceLinkedRoleDeletionStatus ----

// GetServiceLinkedRoleDeletionStatusResult contains the deletion status.
type GetServiceLinkedRoleDeletionStatusResult struct {
	Status string `xml:"Status"`
}

// GetServiceLinkedRoleDeletionStatusResponse is the XML response for GetServiceLinkedRoleDeletionStatus.
type GetServiceLinkedRoleDeletionStatusResponse struct {
	XMLName          xml.Name         `xml:"GetServiceLinkedRoleDeletionStatusResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	// GetServiceLinkedRoleDeletionStatusResult mirrors the AWS API field name.
	GetServiceLinkedRoleDeletionStatusResult GetServiceLinkedRoleDeletionStatusResult `xml:"GetServiceLinkedRoleDeletionStatusResult"` //nolint:lll // AWS contract
}
