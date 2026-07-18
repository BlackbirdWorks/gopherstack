package account

import "errors"

var (
	errNoAlternateContact = errors.New("ResourceNotFoundException: no alternate contact found")
	errNoContactInfo      = errors.New("ResourceNotFoundException: no contact information set")
	errRegionNotFound     = errors.New("ResourceNotFoundException: region not found")
	errRegionNotOptIn     = errors.New("ValidationException: only opt-in regions can be enabled or disabled")
	errNoPendingUpdate    = errors.New("ResourceNotFoundException: no primary email update in progress")
	errInvalidOTP         = errors.New("ValidationException: invalid OTP")
	// errInvalidNextToken is returned when ListRegions receives an undecodable cursor.
	errInvalidNextToken = errors.New("ValidationException: invalid nextToken")
)
