package workmail

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	// ErrConflict is returned by CreateImpersonationRole when a role with the
	// same name already exists. CreateImpersonationRole's own error model
	// (workmail@v1.39.4 deserializers.go
	// awsAwsjson11_deserializeOpErrorCreateImpersonationRole) defines no
	// AlreadyExists-shaped exception at all, so no replacement code is
	// invented here; every other "already exists"-style caller uses one of
	// ErrNameUnavailable/ErrEmailInUse/ErrMailDomainInUse below, chosen per
	// the raising op's own model -- workmail has no single generic
	// "EntityAlreadyExistsException" type.
	ErrConflict = awserr.New("EntityAlreadyExistsException", awserr.ErrAlreadyExists)
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
