package sts

import "encoding/xml"

const (
	// STSNamespace is the XML namespace for STS wire responses.
	STSNamespace = "https://sts.amazonaws.com/doc/2011-06-15/"

	// MockAccountID is the default mock AWS account ID returned by GetCallerIdentity.
	MockAccountID = "000000000000"

	// MockUserID is the fixed user ID returned by GetCallerIdentity.
	MockUserID = "AKIAIOSFODNN7EXAMPLE" //nolint:gosec // well-known AWS example key, not real credentials

	// MockUserArn is the default ARN returned by GetCallerIdentity.
	MockUserArn = "arn:aws:iam::000000000000:root"

	// DefaultDurationSeconds is the default credential lifetime (1 hour).
	DefaultDurationSeconds = 3600

	// MinDurationSeconds is the minimum allowed credential lifetime.
	MinDurationSeconds = 900

	// MaxDurationSeconds is the maximum allowed credential lifetime.
	MaxDurationSeconds = 43200

	// DefaultSessionTokenDurationSeconds is the default lifetime for GetSessionToken (12 hours).
	DefaultSessionTokenDurationSeconds = 43200

	// MinSessionTokenDurationSeconds is the minimum allowed lifetime (15 minutes).
	MinSessionTokenDurationSeconds = 900
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
	// SessionToken is part of AWS Credentials struct, not a secret being stored
	SessionToken string `xml:"SessionToken"` //nolint:gosec // AWS Credentials field
	Expiration   string `xml:"Expiration"`
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

// GetSessionTokenInput holds the parameters for a GetSessionToken call.
type GetSessionTokenInput struct {
	SerialNumber    string
	TokenCode       string
	DurationSeconds int32
}

// GetSessionTokenResult wraps the credentials.
type GetSessionTokenResult struct {
	Credentials Credentials `xml:"Credentials"`
}

// GetSessionTokenResponse is the top-level XML envelope returned by GetSessionToken.
type GetSessionTokenResponse struct {
	XMLName               xml.Name              `xml:"GetSessionTokenResponse"`
	Xmlns                 string                `xml:"xmlns,attr"`
	GetSessionTokenResult GetSessionTokenResult `xml:"GetSessionTokenResult"`
	ResponseMetadata      ResponseMetadata      `xml:"ResponseMetadata"`
}

// ErrorResponse is the XML error envelope returned on failed STS operations.
type ErrorResponse struct {
	XMLName   xml.Name    `xml:"ErrorResponse"`
	Xmlns     string      `xml:"xmlns,attr"`
	Error     ErrorDetail `xml:"Error"`
	RequestID string      `xml:"RequestId"`
}

// GetAccessKeyInfoResult carries the account for the given access key.
type GetAccessKeyInfoResult struct {
	Account string `xml:"Account"`
}

// GetAccessKeyInfoResponse is the top-level XML envelope returned by GetAccessKeyInfo.
type GetAccessKeyInfoResponse struct {
	XMLName                xml.Name               `xml:"GetAccessKeyInfoResponse"`
	Xmlns                  string                 `xml:"xmlns,attr"`
	GetAccessKeyInfoResult GetAccessKeyInfoResult `xml:"GetAccessKeyInfoResult"`
	ResponseMetadata       ResponseMetadata       `xml:"ResponseMetadata"`
}

// DecodeAuthorizationMessageResult carries the decoded message.
type DecodeAuthorizationMessageResult struct {
	DecodedMessage string `xml:"DecodedMessage"`
}

// DecodeAuthorizationMessageResponse is the top-level XML envelope returned by DecodeAuthorizationMessage.
type DecodeAuthorizationMessageResponse struct {
	XMLName                          xml.Name                         `xml:"DecodeAuthorizationMessageResponse"`
	Xmlns                            string                           `xml:"xmlns,attr"`
	DecodeAuthorizationMessageResult DecodeAuthorizationMessageResult `xml:"DecodeAuthorizationMessageResult"`
	ResponseMetadata                 ResponseMetadata                 `xml:"ResponseMetadata"`
}
