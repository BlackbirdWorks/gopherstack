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
	Tags                map[string]string `json:"Tags,omitempty"`
	CreateDate          time.Time         `json:"CreateDate"`
	UserName            string            `json:"UserName,omitempty"`
	UserID              string            `json:"UserId,omitempty"`
	Arn                 string            `json:"Arn,omitempty"`
	Path                string            `json:"Path,omitempty"`
	PermissionsBoundary string            `json:"PermissionsBoundary,omitempty"`
}

// Role represents an IAM role resource.
type Role struct {
	Tags                     map[string]string `json:"Tags,omitempty"`
	CreateDate               time.Time         `json:"CreateDate"`
	RoleName                 string            `json:"RoleName,omitempty"`
	RoleID                   string            `json:"RoleId,omitempty"`
	Arn                      string            `json:"Arn,omitempty"`
	Path                     string            `json:"Path,omitempty"`
	AssumeRolePolicyDocument string            `json:"AssumeRolePolicyDocument,omitempty"`
	PermissionsBoundary      string            `json:"PermissionsBoundary,omitempty"`
	Description              string            `json:"Description,omitempty"`
	// MaxSessionDuration is the maximum session duration (in seconds) for role credentials.
	// A value of 0 means the default system maximum applies (43200 seconds / 12 hours).
	MaxSessionDuration int32 `json:"MaxSessionDuration"`
}

// Policy represents an IAM managed policy resource.
type Policy struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	CreateDate       time.Time         `json:"CreateDate"`
	UpdateDate       time.Time         `json:"UpdateDate"`
	PolicyName       string            `json:"PolicyName,omitempty"`
	PolicyID         string            `json:"PolicyId,omitempty"`
	Arn              string            `json:"Arn,omitempty"`
	Path             string            `json:"Path,omitempty"`
	PolicyDocument   string            `json:"PolicyDocument,omitempty"`
	DefaultVersionID string            `json:"DefaultVersionId,omitempty"`
	AttachmentCount  int               `json:"AttachmentCount,omitempty"`
	IsAttachable     bool              `json:"IsAttachable,omitempty"`
}

// Group represents an IAM group resource.
// Group represents an IAM group. Note: real IAM does not support tagging
// Groups (aws-sdk-go-v2/service/iam/types.Group has no Tags field), so this
// model intentionally has no Tags field either.
type Group struct {
	CreateDate time.Time `json:"CreateDate"`
	GroupName  string    `json:"GroupName,omitempty"`
	GroupID    string    `json:"GroupId,omitempty"`
	Arn        string    `json:"Arn,omitempty"`
	Path       string    `json:"Path,omitempty"`
}

// AccessKey represents an IAM access key for a user.
type AccessKey struct {
	LastUsedDate        *time.Time `json:"LastUsedDate,omitempty"`
	CreateDate          time.Time  `json:"CreateDate"`
	AccessKeyID         string     `json:"AccessKeyId,omitempty"`
	SecretAccessKey     string     `json:"SecretAccessKey,omitempty"`
	UserName            string     `json:"UserName,omitempty"`
	LastUsedRegion      string     `json:"LastUsedRegion,omitempty"`
	LastUsedServiceName string     `json:"LastUsedServiceName,omitempty"`
	Status              string     `json:"Status,omitempty"`
}

// SigningCertificate represents an IAM X.509 signing certificate.
type SigningCertificate struct {
	UploadDate      time.Time `json:"UploadDate"`
	CertificateID   string    `json:"CertificateId,omitempty"`
	UserName        string    `json:"UserName,omitempty"`
	CertificateBody string    `json:"CertificateBody,omitempty"`
	Status          string    `json:"Status,omitempty"`
}

// ServerCertificate represents an IAM server certificate.
type ServerCertificate struct {
	UploadDate            time.Time `json:"UploadDate"`
	ServerCertificateName string    `json:"ServerCertificateName,omitempty"`
	ServerCertificateID   string    `json:"ServerCertificateId,omitempty"`
	Arn                   string    `json:"Arn,omitempty"`
	Path                  string    `json:"Path,omitempty"`
	CertificateBody       string    `json:"CertificateBody,omitempty"`
	CertificateChain      string    `json:"CertificateChain,omitempty"`
}

// InstanceProfile represents an IAM instance profile.
type InstanceProfile struct {
	CreateDate          time.Time `json:"CreateDate"`
	InstanceProfileName string    `json:"InstanceProfileName,omitempty"`
	InstanceProfileID   string    `json:"InstanceProfileId,omitempty"`
	Arn                 string    `json:"Arn,omitempty"`
	Path                string    `json:"Path,omitempty"`
	Roles               []string  `json:"Roles,omitempty"`
}

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

// PermissionsBoundaryXML is the XML representation of a permissions boundary.
type PermissionsBoundaryXML struct {
	PermissionsBoundaryArn  string `xml:"PermissionsBoundaryArn"`
	PermissionsBoundaryType string `xml:"PermissionsBoundaryType"`
}

// TagXML is the XML representation of a single IAM tag.
type TagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// tagsToXML converts a map[string]string to a sorted []TagXML slice for XML marshaling.
func tagsToXML(tags map[string]string) []TagXML {
	if len(tags) == 0 {
		return nil
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	// Sort for deterministic XML output.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}

	out := make([]TagXML, 0, len(keys))
	for _, k := range keys {
		out = append(out, TagXML{Key: k, Value: tags[k]})
	}

	return out
}

// UserXML is the XML representation of an IAM User.
type UserXML struct {
	PermissionsBoundary *PermissionsBoundaryXML `xml:"PermissionsBoundary,omitempty"`
	Path                string                  `xml:"Path"`
	UserName            string                  `xml:"UserName"`
	UserID              string                  `xml:"UserId"`
	Arn                 string                  `xml:"Arn"`
	CreateDate          string                  `xml:"CreateDate"`
	Tags                []TagXML                `xml:"Tags>member,omitempty"`
}

// CreateUserResponse is the XML response for CreateUser.
type CreateUserResponse struct {
	XMLName          xml.Name         `xml:"CreateUserResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	CreateUserResult CreateUserResult `xml:"CreateUserResult"`
}

// CreateUserResult wraps the created user.
type CreateUserResult struct {
	User UserXML `xml:"User"`
}

// GetUserResponse is the XML response for GetUser.
type GetUserResponse struct {
	XMLName          xml.Name         `xml:"GetUserResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	GetUserResult    GetUserResult    `xml:"GetUserResult"`
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

// UpdateUserResponse is the XML response for UpdateUser.
type UpdateUserResponse struct {
	XMLName          xml.Name         `xml:"UpdateUserResponse"`
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

// RoleXML is the XML representation of an IAM Role.
type RoleXML struct {
	PermissionsBoundary      *PermissionsBoundaryXML `xml:"PermissionsBoundary,omitempty"`
	Path                     string                  `xml:"Path"`
	RoleName                 string                  `xml:"RoleName"`
	RoleID                   string                  `xml:"RoleId"`
	Arn                      string                  `xml:"Arn"`
	CreateDate               string                  `xml:"CreateDate"`
	AssumeRolePolicyDocument string                  `xml:"AssumeRolePolicyDocument"`
	Description              string                  `xml:"Description,omitempty"`
	Tags                     []TagXML                `xml:"Tags>member,omitempty"`
	MaxSessionDuration       int32                   `xml:"MaxSessionDuration"`
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

// PolicyXML is the XML representation of an IAM Policy.
type PolicyXML struct {
	PolicyName       string   `xml:"PolicyName"`
	PolicyID         string   `xml:"PolicyId"`
	Arn              string   `xml:"Arn"`
	Path             string   `xml:"Path"`
	CreateDate       string   `xml:"CreateDate"`
	UpdateDate       string   `xml:"UpdateDate"`
	DefaultVersionID string   `xml:"DefaultVersionId"`
	Tags             []TagXML `xml:"Tags>member,omitempty"`
	AttachmentCount  int      `xml:"AttachmentCount"`
	IsAttachable     bool     `xml:"IsAttachable"`
}

// CreatePolicyResponse is the XML response for CreatePolicy.
type CreatePolicyResponse struct {
	XMLName            xml.Name           `xml:"CreatePolicyResponse"`
	Xmlns              string             `xml:"xmlns,attr"`
	ResponseMetadata   ResponseMetadata   `xml:"ResponseMetadata"`
	CreatePolicyResult CreatePolicyResult `xml:"CreatePolicyResult"`
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

// GroupXML is the XML representation of an IAM Group.
// GroupXML is the wire shape for a Group. Note: real IAM's Group type has no
// Tags field (Groups are not a taggable resource type), so none is present here.
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
	ResponseMetadata  ResponseMetadata  `xml:"ResponseMetadata"`
	CreateGroupResult CreateGroupResult `xml:"CreateGroupResult"`
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

// RemoveUserFromGroupResponse is the XML response for RemoveUserFromGroup.
type RemoveUserFromGroupResponse struct {
	XMLName          xml.Name         `xml:"RemoveUserFromGroupResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetGroupResult wraps a single group.
type GetGroupResult struct {
	Group       GroupXML  `xml:"Group"`
	Users       []UserXML `xml:"Users>member"`
	IsTruncated bool      `xml:"IsTruncated"`
}

// GetGroupResponse is the XML response for GetGroup.
type GetGroupResponse struct {
	XMLName          xml.Name         `xml:"GetGroupResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	GetGroupResult   GetGroupResult   `xml:"GetGroupResult"`
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

// InstanceProfileXML is the XML representation of an IAM InstanceProfile.
type InstanceProfileXML struct {
	Path                string    `xml:"Path"`
	InstanceProfileName string    `xml:"InstanceProfileName"`
	InstanceProfileID   string    `xml:"InstanceProfileId"`
	Arn                 string    `xml:"Arn"`
	CreateDate          string    `xml:"CreateDate"`
	Roles               []RoleXML `xml:"Roles>member"`
}

// CreateInstanceProfileResponse is the XML response for CreateInstanceProfile.
type CreateInstanceProfileResponse struct {
	XMLName                     xml.Name                    `xml:"CreateInstanceProfileResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	ResponseMetadata            ResponseMetadata            `xml:"ResponseMetadata"`
	CreateInstanceProfileResult CreateInstanceProfileResult `xml:"CreateInstanceProfileResult"`
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

// AddRoleToInstanceProfileResponse is the XML response for AddRoleToInstanceProfile.
type AddRoleToInstanceProfileResponse struct {
	XMLName          xml.Name         `xml:"AddRoleToInstanceProfileResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// RemoveRoleFromInstanceProfileResponse is the XML response for RemoveRoleFromInstanceProfile.
type RemoveRoleFromInstanceProfileResponse struct {
	XMLName          xml.Name         `xml:"RemoveRoleFromInstanceProfileResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// isoTime formats a [time.Time] to an ISO 8601 string.
func isoTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

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
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	GetPolicyResult  GetPolicyResult  `xml:"GetPolicyResult"`
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

// ListPolicyVersionsResult contains the policy version list.
type ListPolicyVersionsResult struct {
	Versions []PolicyVersionXML `xml:"Versions>member"`
}

// ListPolicyVersionsResponse is the XML response for ListPolicyVersions.
type ListPolicyVersionsResponse struct {
	XMLName                  xml.Name                 `xml:"ListPolicyVersionsResponse"`
	Xmlns                    string                   `xml:"xmlns,attr"`
	ResponseMetadata         ResponseMetadata         `xml:"ResponseMetadata"`
	ListPolicyVersionsResult ListPolicyVersionsResult `xml:"ListPolicyVersionsResult"`
}

// UpdateAssumeRolePolicyResponse is the XML response for UpdateAssumeRolePolicy.
type UpdateAssumeRolePolicyResponse struct {
	XMLName          xml.Name         `xml:"UpdateAssumeRolePolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// DetachUserPolicyResponse is the XML response for DetachUserPolicy.
type DetachUserPolicyResponse struct {
	XMLName          xml.Name         `xml:"DetachUserPolicyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

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
