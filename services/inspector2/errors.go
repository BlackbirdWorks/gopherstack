package inspector2

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// AWS error type name constants used to build the sentinel errors below and
// by handler-side error mapping (see handler.go's mapError).
const (
	errResourceNotFound = "ResourceNotFoundException"
	errConflict         = "ConflictException"
	errValidation       = "ValidationException"
)

var (
	// ErrFilterNotFound is returned when a filter does not exist.
	ErrFilterNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFilterAlreadyExists is returned when a filter already exists.
	ErrFilterAlreadyExists = awserr.New(errConflict, awserr.ErrConflict)
	// ErrTagsResourceNotFound is returned when the tagged resource does not exist.
	ErrTagsResourceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)

	// ErrMemberNotFound is returned when a member account is not found.
	ErrMemberNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrMemberAlreadyExists is returned when a member already exists.
	ErrMemberAlreadyExists = awserr.New(errConflict, awserr.ErrConflict)
	// ErrDelegatedAdminNotFound is returned when a delegated admin is not found.
	ErrDelegatedAdminNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDelegatedAdminAlreadyExists is returned on duplicate enable.
	ErrDelegatedAdminAlreadyExists = awserr.New(errConflict, awserr.ErrConflict)
	// ErrCisScanConfigNotFound is returned when a CIS scan config is missing.
	ErrCisScanConfigNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCodeSecurityIntegrationNotFound is returned when a code security integration is missing.
	ErrCodeSecurityIntegrationNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCodeSecurityScanConfigNotFound is returned when a code security scan config is missing.
	ErrCodeSecurityScanConfigNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrReportNotFound is returned when a findings report is missing.
	ErrReportNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrSbomExportNotFound is returned when an SBOM export is missing.
	ErrSbomExportNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrCisSessionNotFound is returned when a CIS session is missing.
	ErrCisSessionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
)
