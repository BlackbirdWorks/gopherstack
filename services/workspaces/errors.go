package workspaces

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	errResourceNotFound       = "ResourceNotFoundException"
	errInvalidParameterValues = "InvalidParameterValuesException"
	errResourceAlreadyExists  = "ResourceAlreadyExistsException"
	errInvalidResourceState   = "InvalidResourceStateException"
)

var (
	// ErrWorkspaceNotFound is returned when a workspace does not exist.
	ErrWorkspaceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInvalidParameter is returned on invalid input.
	ErrInvalidParameter = awserr.New(errInvalidParameterValues, awserr.ErrInvalidParameter)

	// errDirectoryAlreadyRegistered is returned by RegisterWorkspaceDirectory
	// when the directory is already registered.
	errDirectoryAlreadyRegistered = awserr.New(errResourceAlreadyExists, awserr.ErrAlreadyExists)
	// errDirectoryHasWorkspaces is returned by DeregisterWorkspaceDirectory
	// when WorkSpaces are still registered to the directory. Real AWS: any
	// WorkSpaces registered to the directory must be removed first, before
	// the directory itself can be deregistered.
	errDirectoryHasWorkspaces = awserr.New(errInvalidResourceState, awserr.ErrConflict)
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
