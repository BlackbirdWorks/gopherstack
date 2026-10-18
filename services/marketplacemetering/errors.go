package marketplacemetering

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// Sentinels, one per exception in the pinned SDK's error set (plus ValidationException and DryRunOperation).
var (
	ErrCustomerNotEntitled       = errors.New("customer not entitled")
	ErrDuplicateRequest          = errors.New("duplicate request")
	ErrExpiredToken              = errors.New("expired token")
	ErrIdempotencyConflict       = errors.New("idempotency conflict")
	ErrInternalServiceError      = errors.New("internal service error")
	ErrInvalidCustomerIdentifier = errors.New("invalid customer identifier")
	ErrInvalidProductCode        = errors.New("invalid product code")
	ErrInvalidPublicKeyVersion   = errors.New("invalid public key version")
	ErrInvalidTag                = errors.New("invalid tag")
	ErrInvalidToken              = errors.New("invalid token")
	ErrInvalidUsageAllocations   = errors.New("invalid usage allocations")
	ErrInvalidUsageDimension     = errors.New("invalid usage dimension")
	ErrTimestampOutOfBounds      = errors.New("timestamp out of bounds")
	ErrValidation                = errors.New("validation")
	ErrDryRunOperation           = errors.New("dry run operation")
	errUnknownOperation          = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrCustomerNotEntitled:        apiErr("CustomerNotEntitledException", http.StatusBadRequest),
		ErrDuplicateRequest:           apiErr("DuplicateRequestException", http.StatusBadRequest),
		ErrExpiredToken:               apiErr("ExpiredTokenException", http.StatusBadRequest),
		ErrIdempotencyConflict:        apiErr("IdempotencyConflictException", http.StatusConflict),
		ErrInternalServiceError:       apiErr("InternalServiceErrorException", http.StatusInternalServerError),
		ErrInvalidCustomerIdentifier:  apiErr("InvalidCustomerIdentifierException", http.StatusBadRequest),
		ErrInvalidProductCode:         apiErr("InvalidProductCodeException", http.StatusBadRequest),
		ErrInvalidPublicKeyVersion:    apiErr("InvalidPublicKeyVersionException", http.StatusBadRequest),
		ErrInvalidTag:                 apiErr("InvalidTagException", http.StatusBadRequest),
		ErrInvalidToken:               apiErr("InvalidTokenException", http.StatusBadRequest),
		ErrInvalidUsageAllocations:    apiErr("InvalidUsageAllocationsException", http.StatusBadRequest),
		ErrInvalidUsageDimension:      apiErr("InvalidUsageDimensionException", http.StatusBadRequest),
		ErrTimestampOutOfBounds:       apiErr("TimestampOutOfBoundsException", http.StatusBadRequest),
		ErrValidation:                 apiErr("ValidationException", http.StatusBadRequest),
		ErrDryRunOperation:            apiErr("DryRunOperation", http.StatusBadRequest),
		idempotency.ErrParamsMismatch: apiErr("IdempotencyConflictException", http.StatusConflict),
		errUnknownOperation:           apiErr("ValidationException", http.StatusBadRequest),
	}
}
