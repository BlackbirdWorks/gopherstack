// Package organizations provides an in-memory stub for the AWS Organizations API.
package organizations

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// epochSeconds returns t as Unix epoch seconds (float64), preserving sub-second
// precision. The AWS SDK Go v2 deserializes JSON timestamps as float64 epoch
// seconds, so all timestamp fields in JSON response types must use this type.
// Delegates to pkgs/awstime.Epoch (see pkgs-catalog.md) rather than
// reimplementing epoch conversion locally.
func epochSeconds(t time.Time) float64 {
	return awstime.Epoch(t)
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

// ParentSummary describes the parent of an account or OU.
type ParentSummary struct {
	ID   string `json:"Id"`
	Type string `json:"Type"`
}

// ChildSummary describes a child (account or OU) of a parent.
type ChildSummary struct {
	ID   string `json:"Id"`
	Type string `json:"Type"`
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
