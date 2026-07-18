package workspaces

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errResourceNotFound       = "ResourceNotFoundException"
	errInvalidParameterValues = "InvalidParameterValuesException"
)

var (
	// ErrWorkspaceNotFound is returned when a workspace does not exist.
	ErrWorkspaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInvalidParameter is returned on invalid input.
	ErrInvalidParameter = awserr.New(errInvalidParameterValues, awserr.ErrInvalidParameter)
)

var (
	errIpGroupNotFound = awserr.New( //nolint:revive,staticcheck // existing issue.
		errResourceNotFound,
		awserr.ErrNotFound,
	)

	errConnAliasNotFound   = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errBundleNotFound      = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errImageNotFound       = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errPoolNotFound        = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errPoolSessionNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errAddInNotFound       = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	errAccountLinkNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
)
