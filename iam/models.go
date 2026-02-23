package iam

import (
	"encoding/xml"
	"time"
)

// iamXMLNS is the IAM XML namespace used in all responses.
const iamXMLNS = "https://iam.amazonaws.com/doc/2010-05-08/"

// IAMAccountID is the dummy AWS account ID used in ARNs.
const IAMAccountID = "000000000000"

// User represents an IAM user resource.
type User struct {
	CreateDate time.Time `json:"CreateDate"`
	UserName   string    `json:"UserName"`
	UserID     string    `json:"UserId"`
	Arn        string    `json:"Arn"`
	Path       string    `json:"Path"`
}

// Role represents an IAM role resource.
type Role struct {
	CreateDate               time.Time `json:"CreateDate"`
	RoleName                 string    `json:"RoleName"`
	RoleID                   string    `json:"RoleId"`
	Arn                      string    `json:"Arn"`
	Path                     string    `json:"Path"`
	AssumeRolePolicyDocument string    `json:"AssumeRolePolicyDocument"`
}

// Policy represents an IAM managed policy resource.
type Policy struct {
	CreateDate     time.Time `json:"CreateDate"`
	PolicyName     string    `json:"PolicyName"`
	PolicyID       string    `json:"PolicyId"`
	Arn            string    `json:"Arn"`
	Path           string    `json:"Path"`
	PolicyDocument string    `json:"PolicyDocument"`
}

// Group represents an IAM group resource.
type Group struct {
	CreateDate time.Time `json:"CreateDate"`
	GroupName  string    `json:"GroupName"`
	GroupID    string    `json:"GroupId"`
	Arn        string    `json:"Arn"`
	Path       string    `json:"Path"`
}

// AccessKey represents an IAM access key for a user.
type AccessKey struct {
	CreateDate      time.Time `json:"CreateDate"`
	AccessKeyID     string    `json:"AccessKeyId"`
	SecretAccessKey string    `json:"SecretAccessKey"`
	UserName        string    `json:"UserName"`
	Status          string    `json:"Status"`
}

// InstanceProfile represents an IAM instance profile.
type InstanceProfile struct {
	CreateDate          time.Time `json:"CreateDate"`
	InstanceProfileName string    `json:"InstanceProfileName"`
	InstanceProfileID   string    `json:"InstanceProfileId"`
	Arn                 string    `json:"Arn"`
	Path                string    `json:"Path"`
	Roles               []string  `json:"Roles"`
}

// ---- XML response types ----

// ResponseMetadata is embedded in all IAM XML responses.
type ResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// ErrorResponse is the IAM XML error envelope.
type ErrorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	Error     IAMError `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

// IAMError (APIError) contains the IAM error code, message, and type.
//
//nolint:revive // Stuttering intentional: iam.IAMError would stutter, but IAMError is clearer than APIError.
type IAMError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

// ---- User XML responses ----

// UserXML is the XML representation of an IAM User.
type UserXML struct {
	Path       string `xml:"Path"`
	UserName   string `xml:"UserName"`
	UserID     string `xml:"UserId"`
	Arn        string `xml:"Arn"`
	CreateDate string `xml:"CreateDate"`
}

// CreateUserResponse is the XML response for CreateUser.
type CreateUserResponse struct {
	XMLName          xml.Name         `xml:"CreateUserResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	CreateUserResult CreateUserResult `xml:"CreateUserResult"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// CreateUserResult wraps the created user.
type CreateUserResult struct {
	User UserXML `xml:"User"`
}

// GetUserResponse is the XML response for GetUser.
type GetUserResponse struct {
	XMLName          xml.Name         `xml:"GetUserResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	GetUserResult    GetUserResult    `xml:"GetUserResult"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetUserResult wraps a single user.
type GetUserResult struct {
	User UserXML `xml:"User"`
}

// DeleteUserResponse is the XML response for DeleteUser.
type DeleteUserResponse struct {
	XMLName          xml.Name         `xml:"DeleteUserResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListUsersResponse is the XML response for ListUsers.
type ListUsersResponse struct {
	XMLName          xml.Name         `xml:"ListUsersResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	ListUsersResult  ListUsersResult  `xml:"ListUsersResult"`
}

// ListUsersResult contains the list of users.
type ListUsersResult struct {
	Users       []UserXML `xml:"Users>member"`
	IsTruncated bool      `xml:"IsTruncated"`
}

// ---- Role XML responses ----

// RoleXML is the XML representation of an IAM Role.
type RoleXML struct {
	Path                     string `xml:"Path"`
	RoleName                 string `xml:"RoleName"`
	RoleID                   string `xml:"RoleId"`
	Arn                      string `xml:"Arn"`
	CreateDate               string `xml:"CreateDate"`
	AssumeRolePolicyDocument string `xml:"AssumeRolePolicyDocument"`
}

// CreateRoleResponse is the XML response for CreateRole.
type CreateRoleResponse struct {
	XMLName          xml.Name         `xml:"CreateRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	CreateRoleResult CreateRoleResult `xml:"CreateRoleResult"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// CreateRoleResult wraps the created role.
type CreateRoleResult struct {
	Role RoleXML `xml:"Role"`
}

// GetRoleResponse is the XML response for GetRole.
type GetRoleResponse struct {
	XMLName          xml.Name         `xml:"GetRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	GetRoleResult    GetRoleResult    `xml:"GetRoleResult"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetRoleResult wraps a single role.
type GetRoleResult struct {
	Role RoleXML `xml:"Role"`
}

// DeleteRoleResponse is the XML response for DeleteRole.
type DeleteRoleResponse struct {
	XMLName          xml.Name         `xml:"DeleteRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListRolesResponse is the XML response for ListRoles.
type ListRolesResponse struct {
	XMLName          xml.Name         `xml:"ListRolesResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	ListRolesResult  ListRolesResult  `xml:"ListRolesResult"`
}

// ListRolesResult contains the list of roles.
type ListRolesResult struct {
	Roles       []RoleXML `xml:"Roles>member"`
	IsTruncated bool      `xml:"IsTruncated"`
}

// ---- Policy XML responses ----

// PolicyXML is the XML representation of an IAM Policy.
type PolicyXML struct {
	PolicyName string `xml:"PolicyName"`
	PolicyID   string `xml:"PolicyId"`
	Arn        string `xml:"Arn"`
	Path       string `xml:"Path"`
	CreateDate string `xml:"CreateDate"`
}

// CreatePolicyResponse is the XML response for CreatePolicy.
type CreatePolicyResponse struct {
	XMLName            xml.Name           `xml:"CreatePolicyResponse"`
	Xmlns              string             `xml:"xmlns,attr"`
	CreatePolicyResult CreatePolicyResult `xml:"CreatePolicyResult"`
	ResponseMetadata   ResponseMetadata   `xml:"ResponseMetadata"`
}

// CreatePolicyResult wraps the created policy.
type CreatePolicyResult struct {
	Policy PolicyXML `xml:"Policy"`
}

// DeletePolicyResponse is the XML response for DeletePolicy.
type DeletePolicyResponse struct {
	XMLName          xml.Name         `xml:"DeletePolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListPoliciesResponse is the XML response for ListPolicies.
type ListPoliciesResponse struct {
	XMLName            xml.Name           `xml:"ListPoliciesResponse"`
	Xmlns              string             `xml:"xmlns,attr"`
	ResponseMetadata   ResponseMetadata   `xml:"ResponseMetadata"`
	ListPoliciesResult ListPoliciesResult `xml:"ListPoliciesResult"`
}

// ListPoliciesResult contains the list of policies.
type ListPoliciesResult struct {
	Policies    []PolicyXML `xml:"Policies>member"`
	IsTruncated bool        `xml:"IsTruncated"`
}

// AttachUserPolicyResponse is the XML response for AttachUserPolicy.
type AttachUserPolicyResponse struct {
	XMLName          xml.Name         `xml:"AttachUserPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// AttachRolePolicyResponse is the XML response for AttachRolePolicy.
type AttachRolePolicyResponse struct {
	XMLName          xml.Name         `xml:"AttachRolePolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Group XML responses ----

// GroupXML is the XML representation of an IAM Group.
type GroupXML struct {
	Path       string `xml:"Path"`
	GroupName  string `xml:"GroupName"`
	GroupID    string `xml:"GroupId"`
	Arn        string `xml:"Arn"`
	CreateDate string `xml:"CreateDate"`
}

// CreateGroupResponse is the XML response for CreateGroup.
type CreateGroupResponse struct {
	XMLName           xml.Name          `xml:"CreateGroupResponse"`
	Xmlns             string            `xml:"xmlns,attr"`
	CreateGroupResult CreateGroupResult `xml:"CreateGroupResult"`
	ResponseMetadata  ResponseMetadata  `xml:"ResponseMetadata"`
}

// CreateGroupResult wraps the created group.
type CreateGroupResult struct {
	Group GroupXML `xml:"Group"`
}

// DeleteGroupResponse is the XML response for DeleteGroup.
type DeleteGroupResponse struct {
	XMLName          xml.Name         `xml:"DeleteGroupResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// AddUserToGroupResponse is the XML response for AddUserToGroup.
type AddUserToGroupResponse struct {
	XMLName          xml.Name         `xml:"AddUserToGroupResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Access Key XML responses ----

// AccessKeyXML is the XML representation of an IAM AccessKey.
type AccessKeyXML struct {
	AccessKeyID     string `xml:"AccessKeyId"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	UserName        string `xml:"UserName"`
	Status          string `xml:"Status"`
	CreateDate      string `xml:"CreateDate"`
}

// AccessKeyMetadataXML is the XML representation of IAM AccessKey metadata (no secret).
type AccessKeyMetadataXML struct {
	AccessKeyID string `xml:"AccessKeyId"`
	UserName    string `xml:"UserName"`
	Status      string `xml:"Status"`
	CreateDate  string `xml:"CreateDate"`
}

// CreateAccessKeyResponse is the XML response for CreateAccessKey.
type CreateAccessKeyResponse struct {
	XMLName               xml.Name              `xml:"CreateAccessKeyResponse"`
	Xmlns                 string                `xml:"xmlns,attr"`
	CreateAccessKeyResult CreateAccessKeyResult `xml:"CreateAccessKeyResult"`
	ResponseMetadata      ResponseMetadata      `xml:"ResponseMetadata"`
}

// CreateAccessKeyResult wraps the created access key.
type CreateAccessKeyResult struct {
	AccessKey AccessKeyXML `xml:"AccessKey"`
}

// DeleteAccessKeyResponse is the XML response for DeleteAccessKey.
type DeleteAccessKeyResponse struct {
	XMLName          xml.Name         `xml:"DeleteAccessKeyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListAccessKeysResponse is the XML response for ListAccessKeys.
type ListAccessKeysResponse struct {
	XMLName              xml.Name             `xml:"ListAccessKeysResponse"`
	Xmlns                string               `xml:"xmlns,attr"`
	ResponseMetadata     ResponseMetadata     `xml:"ResponseMetadata"`
	ListAccessKeysResult ListAccessKeysResult `xml:"ListAccessKeysResult"`
}

// ListAccessKeysResult contains the list of access key metadata.
type ListAccessKeysResult struct {
	AccessKeyMetadata []AccessKeyMetadataXML `xml:"AccessKeyMetadata>member"`
	IsTruncated       bool                   `xml:"IsTruncated"`
}

// ---- Instance Profile XML responses ----

// InstanceProfileXML is the XML representation of an IAM InstanceProfile.
type InstanceProfileXML struct {
	Path                string `xml:"Path"`
	InstanceProfileName string `xml:"InstanceProfileName"`
	InstanceProfileID   string `xml:"InstanceProfileId"`
	Arn                 string `xml:"Arn"`
	CreateDate          string `xml:"CreateDate"`
}

// CreateInstanceProfileResponse is the XML response for CreateInstanceProfile.
type CreateInstanceProfileResponse struct {
	XMLName                     xml.Name                    `xml:"CreateInstanceProfileResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	CreateInstanceProfileResult CreateInstanceProfileResult `xml:"CreateInstanceProfileResult"`
	ResponseMetadata            ResponseMetadata            `xml:"ResponseMetadata"`
}

// CreateInstanceProfileResult wraps the created instance profile.
type CreateInstanceProfileResult struct {
	InstanceProfile InstanceProfileXML `xml:"InstanceProfile"`
}

// DeleteInstanceProfileResponse is the XML response for DeleteInstanceProfile.
type DeleteInstanceProfileResponse struct {
	XMLName          xml.Name         `xml:"DeleteInstanceProfileResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListInstanceProfilesResponse is the XML response for ListInstanceProfiles.
type ListInstanceProfilesResponse struct {
	XMLName                    xml.Name                   `xml:"ListInstanceProfilesResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           ResponseMetadata           `xml:"ResponseMetadata"`
	ListInstanceProfilesResult ListInstanceProfilesResult `xml:"ListInstanceProfilesResult"`
}

// ListInstanceProfilesResult contains the list of instance profiles.
type ListInstanceProfilesResult struct {
	InstanceProfiles []InstanceProfileXML `xml:"InstanceProfiles>member"`
	IsTruncated      bool                 `xml:"IsTruncated"`
}

// isoTime formats a [time.Time] to an ISO 8601 string.
func isoTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

// ---- Attached Policy XML ----

// AttachedPolicyXML is the XML representation of an attached managed policy.
type AttachedPolicyXML struct {
	PolicyName string `xml:"PolicyName"`
	PolicyArn  string `xml:"PolicyArn"`
}

// ListAttachedUserPoliciesResponse is the XML response for ListAttachedUserPolicies.
type ListAttachedUserPoliciesResponse struct {
	XMLName                        xml.Name                       `xml:"ListAttachedUserPoliciesResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
	ListAttachedUserPoliciesResult ListAttachedUserPoliciesResult `xml:"ListAttachedUserPoliciesResult"`
}

// ListAttachedUserPoliciesResult contains the list of attached policies.
type ListAttachedUserPoliciesResult struct {
	AttachedPolicies []AttachedPolicyXML `xml:"AttachedPolicies>member"`
	IsTruncated      bool                `xml:"IsTruncated"`
}

// ListAttachedRolePoliciesResponse is the XML response for ListAttachedRolePolicies.
type ListAttachedRolePoliciesResponse struct {
	XMLName                        xml.Name                       `xml:"ListAttachedRolePoliciesResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
	ListAttachedRolePoliciesResult ListAttachedRolePoliciesResult `xml:"ListAttachedRolePoliciesResult"`
}

// ListAttachedRolePoliciesResult contains the list of attached policies for a role.
type ListAttachedRolePoliciesResult struct {
	AttachedPolicies []AttachedPolicyXML `xml:"AttachedPolicies>member"`
	IsTruncated      bool                `xml:"IsTruncated"`
}

// PolicyVersionXML is the XML representation of a policy version.
type PolicyVersionXML struct {
	Document         string `xml:"Document"`
	VersionID        string `xml:"VersionId"`
	CreateDate       string `xml:"CreateDate"`
	IsDefaultVersion bool   `xml:"IsDefaultVersion"`
}

// GetPolicyResponse is the XML response for GetPolicy.
type GetPolicyResponse struct {
	XMLName          xml.Name         `xml:"GetPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	GetPolicyResult  GetPolicyResult  `xml:"GetPolicyResult"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetPolicyResult contains the policy details.
type GetPolicyResult struct {
	Policy PolicyXML `xml:"Policy"`
}

// GetPolicyVersionResponse is the XML response for GetPolicyVersion.
type GetPolicyVersionResponse struct {
	XMLName                xml.Name               `xml:"GetPolicyVersionResponse"`
	Xmlns                  string                 `xml:"xmlns,attr"`
	ResponseMetadata       ResponseMetadata       `xml:"ResponseMetadata"`
	GetPolicyVersionResult GetPolicyVersionResult `xml:"GetPolicyVersionResult"`
}

// GetPolicyVersionResult contains the policy version details.
type GetPolicyVersionResult struct {
	PolicyVersion PolicyVersionXML `xml:"PolicyVersion"`
}
