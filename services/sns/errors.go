package sns

import (
	"errors"
)

var (
	ErrTopicNotFound                    = errors.New("NotFound")
	ErrTopicAlreadyExists               = errors.New("TopicAlreadyExists")
	ErrSubscriptionNotFound             = errors.New("NotFound")
	ErrPlatformApplicationNotFound      = errors.New("NotFound")
	ErrPlatformApplicationAlreadyExists = errors.New("PlatformApplicationAlreadyExists")
	ErrEndpointNotFound                 = errors.New("NotFound")
	ErrEndpointDisabled                 = errors.New("EndpointDisabled")
	ErrInvalidParameter                 = errors.New("InvalidParameter")
	ErrPhoneNumberNotFound              = errors.New("ResourceNotFound")
	ErrSandboxPhoneAlreadyExists        = errors.New("AlreadyExists")
	ErrPermissionLabelExists            = errors.New("AuthorizationError")
	ErrPermissionLabelNotFound          = errors.New("AuthorizationError")
	ErrSandboxPhoneNotVerified          = errors.New("InvalidParameter")
	// ErrSubscriptionLimitExceeded maps to the SNS "SubscriptionLimitExceeded" error
	// (HTTP 403): the customer already owns the maximum allowed number of
	// subscriptions on the topic.
	ErrSubscriptionLimitExceeded = errors.New("SubscriptionLimitExceeded")
	// ErrFilterPolicyLimitExceeded maps to the SNS "FilterPolicyLimitExceeded" error
	// (HTTP 403): the number of filter policies in the account (or on the topic)
	// exceeds the AWS quota (200/topic, 10,000/account by default).
	ErrFilterPolicyLimitExceeded = errors.New("FilterPolicyLimitExceeded")
	// ErrOptedOut maps to the SNS "OptedOut" error code (see errorCode in handler.go).
	// The sentinel's own message must describe the actual condition, since %w-wrapping
	// embeds it verbatim into the API error message returned to the caller: it
	// previously read "KMSOptInRequired", an unrelated KMS error string that leaked
	// into every opted-out-SMS error message.
	ErrOptedOut   = errors.New("OptedOut")
	ErrHTTPStatus = errors.New("HTTP status")
)
