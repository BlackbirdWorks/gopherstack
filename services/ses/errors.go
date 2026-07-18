package ses

import "errors"

// Errors returned by the SES backend.
var (
	ErrIdentityNotFound            = errors.New("IdentityNotFound")
	ErrEmailNotFound               = errors.New("EmailNotFound")
	ErrInvalidParameter            = errors.New("InvalidParameterValue")
	ErrMessageRejected             = errors.New("MessageRejected")
	ErrTemplateNotFound            = errors.New("TemplateDoesNotExist")
	ErrTemplateExists              = errors.New("AlreadyExists")
	ErrConfigSetNotFound           = errors.New("ConfigurationSetDoesNotExist")
	ErrConfigSetExists             = errors.New("ConfigurationSetAlreadyExists")
	ErrReceiptRuleSetNotFound      = errors.New("RuleSetDoesNotExist")
	ErrReceiptRuleSetExists        = errors.New("AlreadyExists")
	ErrReceiptRuleNotFound         = errors.New("RuleDoesNotExist")
	ErrReceiptRuleExists           = errors.New("AlreadyExists")
	ErrReceiptFilterNotFound       = errors.New("FilterDoesNotExist")
	ErrReceiptFilterExists         = errors.New("AlreadyExists")
	ErrEventDestinationNotFound    = errors.New("EventDestinationDoesNotExist")
	ErrEventDestinationExists      = errors.New("EventDestinationAlreadyExists")
	ErrTrackingOptionsNotFound     = errors.New("TrackingOptionsDoesNotExist")
	ErrTrackingOptionsExists       = errors.New("TrackingOptionsAlreadyExists")
	ErrCustomVerifTemplateNotFound = errors.New("CustomVerificationEmailTemplateDoesNotExist")
	ErrCustomVerifTemplateExists   = errors.New("CustomVerificationEmailTemplateAlreadyExists")
	ErrValidation                  = errors.New("ValidationError")
	// ErrAccountSendingPaused is returned by send operations when account-level
	// sending has been paused via UpdateAccountSendingEnabled(false), matching
	// real AWS SES's AccountSendingPausedException.
	ErrAccountSendingPaused = errors.New("AccountSendingPausedException")
)
