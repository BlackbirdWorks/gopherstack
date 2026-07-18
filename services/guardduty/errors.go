package guardduty

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrDetectorNotFound is returned when a detector does not exist.
	ErrDetectorNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDetectorAlreadyExists is returned when a detector already exists.
	ErrDetectorAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrFilterNotFound is returned when a filter does not exist.
	ErrFilterNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFilterAlreadyExists is returned when a filter already exists.
	ErrFilterAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrFindingNotFound is returned when a finding does not exist.
	ErrFindingNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrIPSetNotFound is returned when an IP set does not exist.
	ErrIPSetNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrIPSetAlreadyExists is returned when an IP set already exists.
	ErrIPSetAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrThreatIntelSetNotFound is returned when a threat intel set does not exist.
	ErrThreatIntelSetNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrThreatIntelSetAlreadyExists is returned when a threat intel set already exists.
	ErrThreatIntelSetAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

var (
	ErrMemberNotFound                = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrMemberAlreadyExists           = awserr.New(errConflictException, awserr.ErrConflict)
	ErrPublishingDestNotFound        = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrPublishingDestAlreadyExists   = awserr.New(errConflictException, awserr.ErrConflict)
	ErrMalwareScanNotFound           = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrMalwareProtPlanNotFound       = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrMalwareProtPlanAlreadyExists  = awserr.New(errConflictException, awserr.ErrConflict)
	ErrThreatEntitySetNotFound       = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrThreatEntitySetAlreadyExists  = awserr.New(errConflictException, awserr.ErrConflict)
	ErrTrustedEntitySetNotFound      = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrTrustedEntitySetAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
)
