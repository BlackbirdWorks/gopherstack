// Package appconfigdata provides an in-memory stub for the
// AWS AppConfigData service, which is used to retrieve deployed
// configuration data for applications at runtime.
package appconfigdata

import "time"

const (
	// maxHistoryEntries is the maximum number of historical versions retained per profile.
	maxHistoryEntries = 50
	// maxContentBytes is the maximum size of configuration content (1 MiB, matching AWS).
	maxContentBytes = 1 * 1024 * 1024
	// minPollIntervalSeconds is the AWS-enforced minimum for RequiredMinimumPollIntervalInSeconds.
	minPollIntervalSeconds = 15
	// maxPollIntervalSeconds is the AWS-enforced maximum for RequiredMinimumPollIntervalInSeconds.
	maxPollIntervalSeconds = 86400
	// maxIdentifierLength is the AWS-enforced maximum length for application, environment,
	// and configuration profile identifiers. Verified against the real service model's
	// "Identifier" shape (min: 1, max: 128) -- NOT 2048, which was never the real bound.
	maxIdentifierLength = 128
	// DefaultSessionTTL is how long a session may be idle before the janitor evicts it.
	// AWS tokens expire after ~24 h; we use 1 h idle TTL with absolute 24 h cap.
	DefaultSessionTTL = 1 * time.Hour
	// sessionAbsoluteMaxTTL is the maximum session lifetime from creation, regardless of activity.
	// AWS tokens are valid for up to 24 hours per the API documentation.
	sessionAbsoluteMaxTTL = 24 * time.Hour
	// tokenGracePeriod is how long a rotated (old) token remains valid for retry idempotency.
	tokenGracePeriod = 5 * time.Minute
	// tokenByteSize is the number of random bytes used per token (32 → 64 hex chars).
	tokenByteSize = 32
	// signingKeySize is the number of bytes used for the HMAC-SHA256 signing key.
	signingKeySize = 32
	// tokenParts is the expected number of parts when splitting a token by its separator.
	tokenParts = 2
	// familyIDSize is the number of random bytes used for a token family identifier.
	familyIDSize = 8
)

// graceEntry holds the response cached for a rotated token during the grace period.
// A client that retries with an old token receives the same response it would have
// gotten on the first successful poll, preventing duplicate-delivery confusion.
//
// Token holds the rotated (old) token this entry is cached under -- the identity
// field the graceTokens table is keyed by (see graceEntryTableKeyFn in
// store_setup.go). Unlike the hidden-identity DTOs in services/ses and
// services/codecommit (which tag their identity field json:"-" because the
// *live* type is also serialized straight to an AWS API response, where that
// field must not leak), graceEntry has no AWS wire representation at all --
// it is never marshaled to anything but a persistence snapshot -- so Token
// carries no json tag and simply round-trips through store.Table's plain
// JSON marshaling like every other field. Every field was promoted from
// unexported to exported so encoding/json (used by store.Table's
// snapshot/restore) can see them at all -- graceEntry itself stays
// unexported and package-private.
type graceEntry struct {
	ExpiresAt    time.Time
	NextToken    string
	ContentType  string
	ContentHash  string
	VersionLabel string
	Token        string
	Content      []byte
}

// ConfigVersion records a historical snapshot of configuration content.
type ConfigVersion struct {
	UpdatedAt     time.Time `json:"updatedAt"`
	Content       string    `json:"content"`
	ContentType   string    `json:"contentType"`
	ContentHash   string    `json:"contentHash"`
	VersionLabel  string    `json:"versionLabel"`
	DeploymentID  string    `json:"deploymentId"`
	VersionNumber int       `json:"versionNumber"`
}

// ConfigurationProfile stores configuration content for an application/environment/profile combination.
type ConfigurationProfile struct {
	UpdatedAt                      time.Time       `json:"updatedAt"`
	ApplicationIdentifier          string          `json:"applicationIdentifier"`
	EnvironmentIdentifier          string          `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string          `json:"configurationProfileIdentifier"`
	Content                        string          `json:"content"`
	ContentType                    string          `json:"contentType"`
	ContentHash                    string          `json:"contentHash"`
	VersionLabel                   string          `json:"versionLabel"`
	DeploymentID                   string          `json:"deploymentId"`
	History                        []ConfigVersion `json:"history"`
	VersionNumber                  int             `json:"versionNumber"`
}

// Session represents an active configuration retrieval session.
type Session struct {
	CreatedAt                      time.Time `json:"createdAt"`
	LastAccessedAt                 time.Time `json:"lastAccessedAt"`
	LastPollAt                     time.Time `json:"lastPollAt"`
	ExpiresAt                      time.Time `json:"expiresAt"`
	Token                          string    `json:"token"`
	TokenFamilyID                  string    `json:"tokenFamilyId"`
	PreviousContentHash            string    `json:"previousContentHash"`
	ApplicationIdentifier          string    `json:"applicationIdentifier"`
	EnvironmentIdentifier          string    `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string    `json:"configurationProfileIdentifier"`
	PollIntervalInSeconds          int       `json:"pollIntervalInSeconds"`
	PollCount                      int       `json:"pollCount"`
}

// SafeSession mirrors Session but truncates the token for safe display in admin UIs.
// The full token is never returned in list responses; only via authenticated audit endpoints.
type SafeSession struct {
	CreatedAt                      time.Time `json:"createdAt"`
	LastAccessedAt                 time.Time `json:"lastAccessedAt"`
	LastPollAt                     time.Time `json:"lastPollAt"`
	ExpiresAt                      time.Time `json:"expiresAt"`
	TokenPrefix                    string    `json:"tokenPrefix"` // first8…last4, e.g. "abcd1234...ef12"
	TokenFamilyID                  string    `json:"tokenFamilyId"`
	ApplicationIdentifier          string    `json:"applicationIdentifier"`
	EnvironmentIdentifier          string    `json:"environmentIdentifier"`
	ConfigurationProfileIdentifier string    `json:"configurationProfileIdentifier"`
	PollIntervalInSeconds          int       `json:"pollIntervalInSeconds"`
	PollCount                      int       `json:"pollCount"`
}

// ServiceStats holds aggregate metrics for the AppConfigData service.
type ServiceStats struct {
	LastSweepAt              time.Time `json:"lastSweepAt"`
	SessionTTL               string    `json:"sessionTtl"`
	JanitorPeriod            string    `json:"janitorPeriod"`
	SessionCount             int       `json:"sessionCount"`
	ProfileCount             int       `json:"profileCount"`
	TotalPollCount           int64     `json:"totalPollCount"`
	TotalPollFailures        int64     `json:"totalPollFailures"`
	ConfigurationChangeCount int64     `json:"configurationChangeCount"`
}

// startSessionRequest is the JSON body for StartConfigurationSession.
type startSessionRequest struct {
	ApplicationIdentifier                string `json:"ApplicationIdentifier"`
	EnvironmentIdentifier                string `json:"EnvironmentIdentifier"`
	ConfigurationProfileIdentifier       string `json:"ConfigurationProfileIdentifier"`
	RequiredMinimumPollIntervalInSeconds int    `json:"RequiredMinimumPollIntervalInSeconds"`
}

// startSessionResponse is the JSON response for StartConfigurationSession.
type startSessionResponse struct {
	InitialConfigurationToken string `json:"InitialConfigurationToken"`
}

// awsErrorBody is the standard AWS REST-JSON error response body.
// The __type field is how the AWS SDK identifies the exception type.
type awsErrorBody struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// awsBadRequestBody is an extended BadRequestException body with Reason and Details.
// AWS populates these for token-related parameter errors so clients can take targeted action.
type awsBadRequestBody struct {
	Details *invalidParamsDetail `json:"Details,omitempty"`
	Type    string               `json:"__type"`
	Message string               `json:"message"`
	Reason  string               `json:"Reason,omitempty"`
}

// invalidParamsDetail wraps a map of parameter name → problem under "InvalidParameters".
type invalidParamsDetail struct {
	InvalidParameters map[string]invalidParamProblem `json:"InvalidParameters"`
}

// invalidParamProblem describes why a specific parameter was rejected.
type invalidParamProblem struct {
	Problem string `json:"Problem"`
}

// awsResourceNotFoundBody is a ResourceNotFoundException response body.
// ResourceType identifies which resource kind was absent; ReferencedBy carries
// the identifiers the caller supplied.
type awsResourceNotFoundBody struct {
	ReferencedBy map[string]string `json:"ReferencedBy,omitempty"`
	Type         string            `json:"__type"`
	Message      string            `json:"message"`
	ResourceType string            `json:"ResourceType,omitempty"`
}
