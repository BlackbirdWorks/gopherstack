package iam

import (
	"encoding/xml"
	"time"
)

// ---- Policy Version types ----

// StoredPolicyVersion is the in-memory representation of a managed policy version.
type StoredPolicyVersion struct {
	CreateDate       time.Time `json:"CreateDate"`
	PolicyDocument   string    `json:"PolicyDocument,omitempty"`
	VersionID        string    `json:"VersionId,omitempty"`
	IsDefaultVersion bool      `json:"IsDefaultVersion,omitempty"`
}

// CreatePolicyVersionResult contains the created policy version details.
type CreatePolicyVersionResult struct {
	PolicyVersion PolicyVersionXML `xml:"PolicyVersion"`
}

// CreatePolicyVersionResponse is the XML response for CreatePolicyVersion.
type CreatePolicyVersionResponse struct {
	XMLName                   xml.Name                  `xml:"CreatePolicyVersionResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	ResponseMetadata          ResponseMetadata          `xml:"ResponseMetadata"`
	CreatePolicyVersionResult CreatePolicyVersionResult `xml:"CreatePolicyVersionResult"`
}

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

// ---- GetContextKeys ----

// GetContextKeysResult contains the distinct condition context keys referenced by the supplied policies.
type GetContextKeysResult struct {
	ContextKeyNames []string `xml:"ContextKeyNames>member"`
}

// GetContextKeysResponse is the XML response for GetContextKeysForCustomPolicy
// and GetContextKeysForPrincipalPolicy.
type GetContextKeysResponse struct {
	XMLName              xml.Name             `xml:"GetContextKeysForCustomPolicyResponse"`
	Xmlns                string               `xml:"xmlns,attr"`
	ResponseMetadata     ResponseMetadata     `xml:"ResponseMetadata"`
	GetContextKeysResult GetContextKeysResult `xml:"GetContextKeysForCustomPolicyResult"`
}
