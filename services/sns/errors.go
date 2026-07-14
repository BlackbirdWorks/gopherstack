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
	// ErrOptedOut maps to the SNS "OptedOut" error code (see errorCode in handler.go).
	// The sentinel's own message must describe the actual condition, since %w-wrapping
	// embeds it verbatim into the API error message returned to the caller: it
	// previously read "KMSOptInRequired", an unrelated KMS error string that leaked
	// into every opted-out-SMS error message.
	ErrOptedOut   = errors.New("OptedOut")
	ErrHTTPStatus = errors.New("HTTP status")
)
