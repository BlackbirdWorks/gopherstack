package ses

import "errors"

// Errors returned by the SES backend.
//
// ErrTrackingOptionsNotFound / ErrTrackingOptionsExists deliberately carry the
// "Exception"-suffixed wire error codes (TrackingOptionsDoesNotExistException /
// TrackingOptionsAlreadyExistsException) even though every sibling *DoesNotExist
// / *AlreadyExists error in this list omits the suffix -- this asymmetry is not
// a typo, it is what aws-sdk-go-v2/service/ses/types/errors.go's
// TrackingOptions{DoesNotExist,AlreadyExists}Exception.ErrorCode() literally
// returns, confirmed against the SDK's deserializers.go error-code switch
// (case strings.EqualFold("TrackingOptionsDoesNotExistException", errorCode)).
// Sending the unsuffixed form (as this file did before this pass) causes a
// real AWS SDK client's error deserializer to miss the typed-exception match.
var (
	ErrEmailNotFound               = errors.New("EmailNotFound")
	ErrInvalidParameter            = errors.New("InvalidParameterValue")
	ErrInvalidPolicy               = errors.New("InvalidPolicy")
	ErrMessageRejected             = errors.New("MessageRejected")
	ErrTemplateNotFound            = errors.New("TemplateDoesNotExist")
	ErrTemplateExists              = errors.New("AlreadyExists")
	ErrConfigSetNotFound           = errors.New("ConfigurationSetDoesNotExist")
	ErrConfigSetExists             = errors.New("ConfigurationSetAlreadyExists")
	ErrReceiptRuleSetNotFound      = errors.New("RuleSetDoesNotExist")
	ErrReceiptRuleSetExists        = errors.New("AlreadyExists")
	ErrReceiptRuleSetActive        = errors.New("CannotDelete")
	ErrReceiptRuleNotFound         = errors.New("RuleDoesNotExist")
	ErrReceiptRuleExists           = errors.New("AlreadyExists")
	ErrReceiptFilterExists         = errors.New("AlreadyExists")
	ErrEventDestinationNotFound    = errors.New("EventDestinationDoesNotExist")
	ErrEventDestinationExists      = errors.New("EventDestinationAlreadyExists")
	ErrTrackingOptionsNotFound     = errors.New("TrackingOptionsDoesNotExistException")
	ErrTrackingOptionsExists       = errors.New("TrackingOptionsAlreadyExistsException")
	ErrCustomVerifTemplateNotFound = errors.New("CustomVerificationEmailTemplateDoesNotExist")
	ErrCustomVerifTemplateExists   = errors.New("CustomVerificationEmailTemplateAlreadyExists")
	ErrValidation                  = errors.New("ValidationError")
	// ErrAccountSendingPaused is returned by send operations when account-level
	// sending has been paused via UpdateAccountSendingEnabled(false), matching
	// real AWS SES's AccountSendingPausedException.
	ErrAccountSendingPaused = errors.New("AccountSendingPausedException")
	// ErrThrottling is returned by send operations when the simulated
	// per-second MaxSendRate (GetSendQuota) is exceeded. The classic SES
	// query-protocol API has no typed exception for this (confirmed absent
	// from aws-sdk-go-v2/service/ses@v1.37.4/types/errors.go) -- error code
	// "Throttling", message "Maximum sending rate exceeded." per
	// https://docs.aws.amazon.com/ses/latest/dg/manage-sending-quotas.html.
	ErrThrottling = errors.New("Throttling")
	// ErrLimitExceeded is returned by the Create*/Clone*/Update* operations
	// that real AWS SES declares LimitExceededException on (see limits.go)
	// when a hard resource cap is reached. Wire code "LimitExceeded" --
	// LimitExceededException.ErrorCode() (types/errors.go) returns this
	// unsuffixed form when ErrorCodeOverride is nil, the default.
	ErrLimitExceeded = errors.New("LimitExceeded")
	// ErrMailFromDomainNotVerified is returned by the four Send* operations
	// (SendEmail/SendRawEmail/SendTemplatedEmail/SendBulkTemplatedEmail, all
	// confirmed via aws-sdk-go-v2/service/ses@v1.37.4/deserializers.go's own
	// awsAwsquery_deserializeOpError<Op> switch) when the sender identity's
	// MAIL FROM domain status is not Success and BehaviorOnMXFailure is
	// RejectMessage. Wire code carries the "Exception" suffix --
	// MailFromDomainNotVerifiedException.ErrorCode() (types/errors.go)
	// returns the type name verbatim, unlike ErrThrottling/ErrLimitExceeded.
	ErrMailFromDomainNotVerified = errors.New("MailFromDomainNotVerifiedException")
)
