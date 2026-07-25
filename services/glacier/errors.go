package glacier

import "errors"

// Sentinel errors for Glacier backend operations.
var (
	// ErrVaultNotFound is returned when a vault does not exist.
	ErrVaultNotFound = errors.New("ResourceNotFoundException: Vault not found")
	// ErrArchiveNotFound is returned when an archive does not exist.
	ErrArchiveNotFound = errors.New("ResourceNotFoundException: Archive not found")
	// ErrJobNotFound is returned when a job does not exist.
	ErrJobNotFound = errors.New("ResourceNotFoundException: Job not found")
	// ErrUploadNotFound is returned when a multipart upload does not exist.
	ErrUploadNotFound = errors.New("ResourceNotFoundException: Multipart upload not found")
	// ErrResourceInUse is returned when creating a vault that already exists.
	ErrResourceInUse = errors.New("ResourceInUseException: vault already exists")
	// ErrValidation is returned when an invalid parameter is supplied.
	ErrValidation = errors.New("InvalidParameterValueException: invalid parameter")
	// ErrVaultNotEmpty is returned when deleting a vault that still has archives.
	ErrVaultNotEmpty = errors.New("ConflictException: Vault not empty")
	// ErrLockConflict is returned when a vault lock is already in progress.
	ErrLockConflict = errors.New("InvalidParameterValueException: Vault lock already in progress")
	// ErrLockAlreadyLocked is returned when attempting to initiate a lock on an already-locked vault.
	ErrLockAlreadyLocked = errors.New("InvalidParameterValueException: Vault is already locked")
	// ErrTooManyTags is returned when adding tags would exceed the per-vault limit.
	ErrTooManyTags = errors.New("InvalidParameterValueException: too many tags on vault")
	// ErrProvisionedCapacityLimit is returned when trying to purchase more than 2 capacity units.
	ErrProvisionedCapacityLimit = errors.New("LimitExceededException: maximum 2 provisioned capacity units per account")
	// ErrInvalidTag is returned when a tag key or value fails validation.
	ErrInvalidTag = errors.New("InvalidParameterValueException: invalid tag key or value")
	// ErrMissingParameter is returned when a required parameter is omitted entirely
	// (as opposed to ErrValidation, which covers a parameter that was supplied but is
	// malformed/out-of-range) -- maps to AWS's distinct MissingParameterValueException.
	ErrMissingParameter = errors.New("MissingParameterValueException: required parameter missing")
)

// Handler-level sentinel errors used as wrapping targets to satisfy err113.
var (
	// ErrDescriptionTooLong is returned when an archive description exceeds maxDescriptionLen.
	ErrDescriptionTooLong = errors.New("description too long")
	// ErrDescriptionChar is returned when an archive description contains a non-printable character.
	ErrDescriptionChar = errors.New("description contains invalid character")
	// ErrLimitOutOfRange is returned when a ?limit query param is out of the allowed range.
	ErrLimitOutOfRange = errors.New("limit out of range")
	// ErrInvalidStrategy is returned when a DataRetrievalPolicy strategy is not recognised.
	ErrInvalidStrategy = errors.New("invalid data retrieval strategy")
	// ErrBytesPerHourRequired is returned when BytesPerHour strategy omits the BytesPerHour value.
	ErrBytesPerHourRequired = errors.New(
		"BytesPerHour strategy requires a positive BytesPerHour value",
	)
	// ErrInvalidVaultName is returned when a vault name contains invalid characters.
	ErrInvalidVaultName = errors.New("invalid vault name")
	// ErrJobNotComplete is returned when GetJobOutput is called on an incomplete job.
	ErrJobNotComplete = errors.New("job output is not yet available")
)
