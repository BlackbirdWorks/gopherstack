package account

import "errors"

var (
	errNoAlternateContact = errors.New("ResourceNotFoundException: no alternate contact found")
	errNoContactInfo      = errors.New("ResourceNotFoundException: no contact information set")
	// errRegionNotFound is a ValidationException, not a ResourceNotFoundException:
	// the real EnableRegion/DisableRegion/GetRegionOptStatus operations' modeled
	// error sets (verified against aws-sdk-go-v2/service/account's deserializers.go
	// and the public API reference) contain AccessDeniedException,
	// InternalServerException, TooManyRequestsException, ValidationException, and
	// (EnableRegion/DisableRegion only) ConflictException -- ResourceNotFoundException
	// is not a possible error for any of the three. An unrecognized RegionName is
	// therefore reported the same way as any other invalid input field.
	errRegionNotFound  = errors.New("ValidationException: region not found")
	errRegionNotOptIn  = errors.New("ValidationException: only opt-in regions can be enabled or disabled")
	errNoPendingUpdate = errors.New("ResourceNotFoundException: no primary email update in progress")
	errInvalidOTP      = errors.New("ValidationException: invalid OTP")
	// errInvalidNextToken is returned when ListRegions receives an undecodable cursor.
	errInvalidNextToken = errors.New("ValidationException: invalid nextToken")
	// errNoPrimaryEmailUpdateStatus is returned by GetPrimaryEmailUpdateStatus
	// when no primary email update has ever been started for this account.
	errNoPrimaryEmailUpdateStatus = errors.New(
		"ResourceNotFoundException: no primary email update found for this account",
	)
	// errGovCloudNotLinked is returned by GetGovCloudAccountInformation. Real
	// AWS returns exactly this ResourceNotFoundException/404 for a standard
	// account with no linked GovCloud pair (confirmed against the API
	// reference's documented example response: {"message":"GovCloud Account
	// ID not found for Standard Account - ..."}). This backend models a
	// single, standalone (non-organization-member) account -- see the
	// AccountId-targeting gap in PARITY.md -- so no account it simulates ever
	// has a linked GovCloud pair, and this error always fires.
	errGovCloudNotLinked = errors.New("ResourceNotFoundException: GovCloud Account ID not found for Standard Account")
)
