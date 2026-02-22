package sts

import "encoding/xml"

const (
	// STSNamespace is the XML namespace for STS wire responses.
	STSNamespace = "https://sts.amazonaws.com/doc/2011-06-15/"

	// MockAccountID is the fixed account ID returned by the mock service.
	MockAccountID = "123456789012"

	// MockUserID is the fixed user ID returned by GetCallerIdentity.
	MockUserID = "AKIAIOSFODNN7EXAMPLE" //nolint:gosec // well-known AWS example key, not real credentials

	// MockUserArn is the fixed ARN returned by GetCallerIdentity.
	MockUserArn = "arn:aws:iam::123456789012:root"

	// DefaultDurationSeconds is the default credential lifetime (1 hour).
	DefaultDurationSeconds = 3600

	// MinDurationSeconds is the minimum allowed credential lifetime.
	MinDurationSeconds = 900

	// MaxDurationSeconds is the maximum allowed credential lifetime.
	MaxDurationSeconds = 43200
)

// AssumeRoleInput holds the parameters for an AssumeRole call.
type AssumeRoleInput struct {
	RoleArn         string
	RoleSessionName string
	ExternalID      string
	Policy          string
	DurationSeconds int32
}

// AssumedRoleUser contains the ARN and ID of the resulting assumed-role principal.
type AssumedRoleUser struct {
	Arn           string `xml:"Arn"`
	AssumedRoleID string `xml:"AssumedRoleId"`
}

// Credentials holds a set of temporary AWS security credentials.
type Credentials struct {
	AccessKeyID     string `xml:"AccessKeyId"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	SessionToken    string `xml:"SessionToken"`
	Expiration      string `xml:"Expiration"`
}

// AssumeRoleResult wraps the assumed-role user and credentials.
type AssumeRoleResult struct {
	AssumedRoleUser AssumedRoleUser `xml:"AssumedRoleUser"`
	Credentials     Credentials     `xml:"Credentials"`
}

// ResponseMetadata carries the per-request identifier.
type ResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// AssumeRoleResponse is the top-level XML envelope returned by AssumeRole.
type AssumeRoleResponse struct {
	XMLName          xml.Name         `xml:"AssumeRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	AssumeRoleResult AssumeRoleResult `xml:"AssumeRoleResult"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetCallerIdentityResult carries the caller's account, ARN, and user-ID.
type GetCallerIdentityResult struct {
	Account string `xml:"Account"`
	Arn     string `xml:"Arn"`
	UserID  string `xml:"UserId"`
}

// GetCallerIdentityResponse is the top-level XML envelope returned by GetCallerIdentity.
type GetCallerIdentityResponse struct {
	XMLName                 xml.Name                `xml:"GetCallerIdentityResponse"`
	Xmlns                   string                  `xml:"xmlns,attr"`
	GetCallerIdentityResult GetCallerIdentityResult `xml:"GetCallerIdentityResult"`
	ResponseMetadata        ResponseMetadata        `xml:"ResponseMetadata"`
}

// ErrorDetail carries the STS error code and message.
type ErrorDetail struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// ErrorResponse is the XML error envelope returned on failed STS operations.
type ErrorResponse struct {
	XMLName   xml.Name    `xml:"ErrorResponse"`
	Xmlns     string      `xml:"xmlns,attr"`
	Error     ErrorDetail `xml:"Error"`
	RequestID string      `xml:"RequestId"`
}
