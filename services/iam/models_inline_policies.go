package iam

import "encoding/xml"

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
