package workmail

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	// ErrOrganizationNotFound is returned when the OrganizationId argument
	// does not refer to an existing organization. Most ops' own error model
	// (workmail@v1.39.4 deserializers.go) declares OrganizationNotFoundException
	// for this and does not declare EntityNotFoundException at all -- verified
	// per-op, not assumed service-wide (gopherstack-6flj/uox6 error-envelope sweep).
	ErrOrganizationNotFound = awserr.New("OrganizationNotFoundException", awserr.ErrNotFound)
	// ErrResourceNotFound is returned for an entity lookup miss on an
	// operation whose own error model declares ResourceNotFoundException
	// for this condition (checked per call site, not assumed).
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrMailDomainNotFound is returned when a mail domain lookup misses on
	// an operation whose own error model declares MailDomainNotFoundException.
	ErrMailDomainNotFound = awserr.New("MailDomainNotFoundException", awserr.ErrNotFound)
	// ErrNameUnavailable is returned when a name is already taken within an
	// organization (CreateAvailabilityConfiguration, CreateGroup,
	// CreateOrganization, CreateResource, CreateUser -- all five model
	// NameAvailabilityException for this).
	ErrNameUnavailable = awserr.New("name is not available", awserr.ErrAlreadyExists)
	// ErrEmailInUse is returned when an email address is already assigned to
	// a different entity (CreateAlias, RegisterToWorkMail -- both model
	// EmailAddressInUseException for this).
	ErrEmailInUse = awserr.New("email address already in use", awserr.ErrAlreadyExists)
	// ErrMailDomainInUse is returned when a mail domain is already
	// registered with the organization (RegisterMailDomain's own error
	// model defines MailDomainInUseException for this).
	ErrMailDomainInUse = awserr.New("mail domain already registered", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	// ErrLimitExceeded is returned when resource limits are hit.
	ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)
	// ErrMailDomainState is returned for domain state issues.
	ErrMailDomainState = awserr.New("MailDomainStateException", awserr.ErrConflict)
	// ErrEntityState is returned when an operation violates entity state constraints.
	ErrEntityState = awserr.New("EntityStateException", awserr.ErrConflict)
)
