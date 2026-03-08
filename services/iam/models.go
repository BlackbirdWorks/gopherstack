package iam

import (
	"encoding/xml"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// iamXMLNS is the IAM XML namespace used in all responses.
const iamXMLNS = "https://iam.amazonaws.com/doc/2010-05-08/"

// IAMAccountID is the dummy AWS account ID used in ARNs.
const IAMAccountID = config.DefaultAccountID

// User represents an IAM user resource.
type User struct {
	CreateDate          time.Time `json:"CreateDate"`
	UserName            string    `json:"UserName"`
	UserID              string    `json:"UserId"`
	Arn                 string    `json:"Arn"`
	Path                string    `json:"Path"`
	PermissionsBoundary string    `json:"PermissionsBoundary,omitempty"`
}

// Role represents an IAM role resource.
type Role struct {
	CreateDate               time.Time `json:"CreateDate"`
	RoleName                 string    `json:"RoleName"`
	RoleID                   string    `json:"RoleId"`
	Arn                      string    `json:"Arn"`
	Path                     string    `json:"Path"`
	AssumeRolePolicyDocument string    `json:"AssumeRolePolicyDocument"`
	PermissionsBoundary      string    `json:"PermissionsBoundary,omitempty"`
	// MaxSessionDuration is the maximum session duration (in seconds) for role credentials.
	// A value of 0 means the default system maximum applies (43200 seconds / 12 hours).
	MaxSessionDuration int32 `json:"MaxSessionDuration,omitempty"`
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

// PermissionsBoundaryXML is the XML representation of a permissions boundary.
type PermissionsBoundaryXML struct {
	PermissionsBoundaryArn  string `xml:"PermissionsBoundaryArn"`
	PermissionsBoundaryType string `xml:"PermissionsBoundaryType"`
}

// UserXML is the XML representation of an IAM User.
type UserXML struct {
	PermissionsBoundary *PermissionsBoundaryXML `xml:"PermissionsBoundary,omitempty"`
	Path                string                  `xml:"Path"`
	UserName            string                  `xml:"UserName"`
	UserID              string                  `xml:"UserId"`
	Arn                 string                  `xml:"Arn"`
	CreateDate          string                  `xml:"CreateDate"`
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
	Marker      string    `xml:"Marker,omitempty"`
	Users       []UserXML `xml:"Users>member"`
	IsTruncated bool      `xml:"IsTruncated"`
}

// ---- Role XML responses ----

// RoleXML is the XML representation of an IAM Role.
type RoleXML struct {
	PermissionsBoundary      *PermissionsBoundaryXML `xml:"PermissionsBoundary,omitempty"`
	Path                     string                  `xml:"Path"`
	RoleName                 string                  `xml:"RoleName"`
	RoleID                   string                  `xml:"RoleId"`
	Arn                      string                  `xml:"Arn"`
	CreateDate               string                  `xml:"CreateDate"`
	AssumeRolePolicyDocument string                  `xml:"AssumeRolePolicyDocument"`
	MaxSessionDuration       int32                   `xml:"MaxSessionDuration,omitempty"`
}

// CreateRoleResponse is the XML response for CreateRole.
type CreateRoleResponse struct {
	XMLName          xml.Name         `xml:"CreateRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	CreateRoleResult CreateRoleResult `xml:"CreateRoleResult"`
}

// CreateRoleResult wraps the created role.
type CreateRoleResult struct {
	Role RoleXML `xml:"Role"`
}

// GetRoleResponse is the XML response for GetRole.
type GetRoleResponse struct {
	XMLName          xml.Name         `xml:"GetRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	GetRoleResult    GetRoleResult    `xml:"GetRoleResult"`
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
	Marker      string    `xml:"Marker,omitempty"`
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
	Marker      string      `xml:"Marker,omitempty"`
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

// DetachRolePolicyResponse is the XML response for DetachRolePolicy.
type DetachRolePolicyResponse struct {
	XMLName          xml.Name         `xml:"DetachRolePolicyResponse"`
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

// ListGroupsResponse is the XML response for ListGroups.
type ListGroupsResponse struct {
	XMLName          xml.Name         `xml:"ListGroupsResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	ListGroupsResult ListGroupsResult `xml:"ListGroupsResult"`
}

// ListGroupsResult contains the list of groups.
type ListGroupsResult struct {
	Marker      string     `xml:"Marker,omitempty"`
	Groups      []GroupXML `xml:"Groups>member"`
	IsTruncated bool       `xml:"IsTruncated"`
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
	Marker            string                 `xml:"Marker,omitempty"`
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
	Marker           string               `xml:"Marker,omitempty"`
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

// ---- Inline Policy XML responses ----

// PutUserPolicyResponse is the XML response for PutUserPolicy.
type PutUserPolicyResponse struct {
	XMLName          xml.Name         `xml:"PutUserPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// PutRolePolicyResponse is the XML response for PutRolePolicy.
type PutRolePolicyResponse struct {
	XMLName          xml.Name         `xml:"PutRolePolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// PutGroupPolicyResponse is the XML response for PutGroupPolicy.
type PutGroupPolicyResponse struct {
	XMLName          xml.Name         `xml:"PutGroupPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetUserPolicyResponse is the XML response for GetUserPolicy.
type GetUserPolicyResponse struct {
	XMLName             xml.Name            `xml:"GetUserPolicyResponse"`
	Xmlns               string              `xml:"xmlns,attr"`
	GetUserPolicyResult GetUserPolicyResult `xml:"GetUserPolicyResult"`
	ResponseMetadata    ResponseMetadata    `xml:"ResponseMetadata"`
}

// GetUserPolicyResult contains the user inline policy details.
type GetUserPolicyResult struct {
	UserName       string `xml:"UserName"`
	PolicyName     string `xml:"PolicyName"`
	PolicyDocument string `xml:"PolicyDocument"`
}

// GetRolePolicyResponse is the XML response for GetRolePolicy.
type GetRolePolicyResponse struct {
	XMLName             xml.Name            `xml:"GetRolePolicyResponse"`
	Xmlns               string              `xml:"xmlns,attr"`
	GetRolePolicyResult GetRolePolicyResult `xml:"GetRolePolicyResult"`
	ResponseMetadata    ResponseMetadata    `xml:"ResponseMetadata"`
}

// GetRolePolicyResult contains the role inline policy details.
type GetRolePolicyResult struct {
	RoleName       string `xml:"RoleName"`
	PolicyName     string `xml:"PolicyName"`
	PolicyDocument string `xml:"PolicyDocument"`
}

// GetGroupPolicyResponse is the XML response for GetGroupPolicy.
type GetGroupPolicyResponse struct {
	XMLName              xml.Name             `xml:"GetGroupPolicyResponse"`
	Xmlns                string               `xml:"xmlns,attr"`
	GetGroupPolicyResult GetGroupPolicyResult `xml:"GetGroupPolicyResult"`
	ResponseMetadata     ResponseMetadata     `xml:"ResponseMetadata"`
}

// GetGroupPolicyResult contains the group inline policy details.
type GetGroupPolicyResult struct {
	GroupName      string `xml:"GroupName"`
	PolicyName     string `xml:"PolicyName"`
	PolicyDocument string `xml:"PolicyDocument"`
}

// DeleteUserPolicyResponse is the XML response for DeleteUserPolicy.
type DeleteUserPolicyResponse struct {
	XMLName          xml.Name         `xml:"DeleteUserPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DeleteRolePolicyResponse is the XML response for DeleteRolePolicy.
type DeleteRolePolicyResponse struct {
	XMLName          xml.Name         `xml:"DeleteRolePolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DeleteGroupPolicyResponse is the XML response for DeleteGroupPolicy.
type DeleteGroupPolicyResponse struct {
	XMLName          xml.Name         `xml:"DeleteGroupPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListUserPoliciesResponse is the XML response for ListUserPolicies.
type ListUserPoliciesResponse struct {
	XMLName                xml.Name               `xml:"ListUserPoliciesResponse"`
	Xmlns                  string                 `xml:"xmlns,attr"`
	ResponseMetadata       ResponseMetadata       `xml:"ResponseMetadata"`
	ListUserPoliciesResult ListUserPoliciesResult `xml:"ListUserPoliciesResult"`
}

// ListUserPoliciesResult contains the list of inline policy names for a user.
type ListUserPoliciesResult struct {
	PolicyNames []string `xml:"PolicyNames>member"`
	IsTruncated bool     `xml:"IsTruncated"`
}

// ListRolePoliciesResponse is the XML response for ListRolePolicies.
type ListRolePoliciesResponse struct {
	XMLName                xml.Name               `xml:"ListRolePoliciesResponse"`
	Xmlns                  string                 `xml:"xmlns,attr"`
	ResponseMetadata       ResponseMetadata       `xml:"ResponseMetadata"`
	ListRolePoliciesResult ListRolePoliciesResult `xml:"ListRolePoliciesResult"`
}

// ListRolePoliciesResult contains the list of inline policy names for a role.
type ListRolePoliciesResult struct {
	PolicyNames []string `xml:"PolicyNames>member"`
	IsTruncated bool     `xml:"IsTruncated"`
}

// ListGroupPoliciesResponse is the XML response for ListGroupPolicies.
type ListGroupPoliciesResponse struct {
	XMLName                 xml.Name                `xml:"ListGroupPoliciesResponse"`
	Xmlns                   string                  `xml:"xmlns,attr"`
	ResponseMetadata        ResponseMetadata        `xml:"ResponseMetadata"`
	ListGroupPoliciesResult ListGroupPoliciesResult `xml:"ListGroupPoliciesResult"`
}

// ListGroupPoliciesResult contains the list of inline policy names for a group.
type ListGroupPoliciesResult struct {
	PolicyNames []string `xml:"PolicyNames>member"`
	IsTruncated bool     `xml:"IsTruncated"`
}

// ---- Permission Boundary XML responses ----

// PutUserPermissionsBoundaryResponse is the XML response for PutUserPermissionsBoundary.
type PutUserPermissionsBoundaryResponse struct {
	XMLName          xml.Name         `xml:"PutUserPermissionsBoundaryResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DeleteUserPermissionsBoundaryResponse is the XML response for DeleteUserPermissionsBoundary.
type DeleteUserPermissionsBoundaryResponse struct {
	XMLName          xml.Name         `xml:"DeleteUserPermissionsBoundaryResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// PutRolePermissionsBoundaryResponse is the XML response for PutRolePermissionsBoundary.
type PutRolePermissionsBoundaryResponse struct {
	XMLName          xml.Name         `xml:"PutRolePermissionsBoundaryResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DeleteRolePermissionsBoundaryResponse is the XML response for DeleteRolePermissionsBoundary.
type DeleteRolePermissionsBoundaryResponse struct {
	XMLName          xml.Name         `xml:"DeleteRolePermissionsBoundaryResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- UpdateAssumeRolePolicy XML response ----

// UpdateAssumeRolePolicyResponse is the XML response for UpdateAssumeRolePolicy.
type UpdateAssumeRolePolicyResponse struct {
	XMLName          xml.Name         `xml:"UpdateAssumeRolePolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- DetachUserPolicy XML response ----

// DetachUserPolicyResponse is the XML response for DetachUserPolicy.
type DetachUserPolicyResponse struct {
	XMLName          xml.Name         `xml:"DetachUserPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Group attached policies XML responses ----

// AttachGroupPolicyResponse is the XML response for AttachGroupPolicy.
type AttachGroupPolicyResponse struct {
	XMLName          xml.Name         `xml:"AttachGroupPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DetachGroupPolicyResponse is the XML response for DetachGroupPolicy.
type DetachGroupPolicyResponse struct {
	XMLName          xml.Name         `xml:"DetachGroupPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ListAttachedGroupPoliciesResponse is the XML response for ListAttachedGroupPolicies.
type ListAttachedGroupPoliciesResponse struct {
	XMLName                         xml.Name                        `xml:"ListAttachedGroupPoliciesResponse"`
	Xmlns                           string                          `xml:"xmlns,attr"`
	ResponseMetadata                ResponseMetadata                `xml:"ResponseMetadata"`
	ListAttachedGroupPoliciesResult ListAttachedGroupPoliciesResult `xml:"ListAttachedGroupPoliciesResult"`
}

// ListAttachedGroupPoliciesResult contains the list of attached policies for a group.
type ListAttachedGroupPoliciesResult struct {
	AttachedPolicies []AttachedPolicyXML `xml:"AttachedPolicies>member"`
	IsTruncated      bool                `xml:"IsTruncated"`
}

// ---- GetAccountAuthorizationDetails XML types ----

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
	UserDetailList  []UserDetailXML          `xml:"UserDetailList>member"`
	GroupDetailList []GroupDetailXML         `xml:"GroupDetailList>member"`
	RoleDetailList  []RoleDetailXML          `xml:"RoleDetailList>member"`
	Policies        []ManagedPolicyDetailXML `xml:"Policies>member"`
	IsTruncated     bool                     `xml:"IsTruncated"`
}

// ---- SimulatePrincipalPolicy XML types ----

// SimulationEvalResultXML is a single evaluation result in SimulatePrincipalPolicy.
type SimulationEvalResultXML struct {
	EvalActionName   string `xml:"EvalActionName"`
	EvalResourceName string `xml:"EvalResourceName"`
	EvalDecision     string `xml:"EvalDecision"`
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

// ---- GenerateCredentialReport / GetCredentialReport XML types ----

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
