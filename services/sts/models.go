package sts

import (
	"encoding/xml"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

const (
	// STSNamespace is the XML namespace for STS wire responses.
	STSNamespace = "https://sts.amazonaws.com/doc/2011-06-15/"

	// MockAccountID is the default mock AWS account ID returned by GetCallerIdentity.
	MockAccountID = config.DefaultAccountID

	// MockUserID is the fixed user ID returned by GetCallerIdentity.
	MockUserID = "AKIAIOSFODNN7EXAMPLE" //nolint:gosec // well-known AWS example key, not real credentials

	// MockUserArn is the default ARN returned by GetCallerIdentity.
	MockUserArn = "arn:aws:iam::" + config.DefaultAccountID + ":root"

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

	// MaxTagCount is the maximum number of session tags allowed per AssumeRole call.
	MaxTagCount = 50
)

// Tag represents a session tag key-value pair passed to AssumeRole.
type Tag struct {
	Key   string
	Value string
}

// AssumeRoleInput holds the parameters for an AssumeRole call.
type AssumeRoleInput struct {
	RoleArn           string
	RoleSessionName   string
	ExternalID        string
	Policy            string
	SourceIdentity    string
	Tags              []Tag
	TransitiveTagKeys []string
	DurationSeconds   int32
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
	// SourceIdentity is the source identity set when the role was assumed.
	SourceIdentity string `xml:"SourceIdentity,omitempty"`
	// PackedPolicySize is the percentage of session policy size used (informational).
	PackedPolicySize int32 `xml:"PackedPolicySize,omitempty"`
}

// ResponseMetadata carries the per-request identifier.
type ResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// AssumeRoleResponse is the top-level XML envelope returned by AssumeRole.
type AssumeRoleResponse struct {
	XMLName          xml.Name         `xml:"AssumeRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	AssumeRoleResult AssumeRoleResult `xml:"AssumeRoleResult"`
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

// SessionInfo stores metadata about an issued assumed-role session for GetCallerIdentity lookups.
type SessionInfo struct {
	AssumedRoleArn string
	AccountID      string
	SessionName    string
	AccessKeyID    string
	// AssumedRoleID is the AROA-prefixed role ID + session name (e.g. "AROATESTROLEID:session").
	// It is the value returned by GetCallerIdentity as the UserId for assumed-role credentials.
	AssumedRoleID     string
	SourceIdentity    string
	Tags              []Tag
	TransitiveTagKeys []string
	// Expiration is the time at which this session expires and should be evicted.
	Expiration time.Time
}
