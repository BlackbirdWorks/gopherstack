package efs

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Package-local sentinels used as the inner error for wrapped error types.
// They are not exported; callers should match via the exported Err* vars.
var (
	errTokenIdentical = errors.New("creation token exists with identical parameters")
	errThrottled      = errors.New("too many requests")
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("FileSystemNotFound", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same token already exists but args differ.
	ErrAlreadyExists = awserr.New("FileSystemAlreadyExists", awserr.ErrConflict)
	// ErrCreationTokenExists is returned when the same creation token with identical args is reused.
	ErrCreationTokenExists = awserr.New("FileSystemAlreadyExists", errTokenIdentical)
	// ErrMountTargetNotFound is returned when a requested mount target does not exist.
	ErrMountTargetNotFound = awserr.New("MountTargetNotFound", awserr.ErrNotFound)
	// ErrAccessPointNotFound is returned when a requested access point does not exist.
	ErrAccessPointNotFound = awserr.New("AccessPointNotFound", awserr.ErrNotFound)
	// ErrPolicyNotFound is returned when no resource policy is configured for a file system.
	ErrPolicyNotFound = awserr.New("PolicyNotFound", awserr.ErrNotFound)
	// ErrInvalidPolicy is returned when a file system policy document is malformed or too large.
	ErrInvalidPolicy = awserr.New("InvalidPolicyException", awserr.ErrInvalidParameter)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrFileSystemInUse is returned when attempting to delete a file system that has mount targets.
	ErrFileSystemInUse = awserr.New("FileSystemInUse", awserr.ErrConflict)
	// ErrMountTargetConflict is returned when a duplicate mount target is created in the same subnet.
	ErrMountTargetConflict = awserr.New("MountTargetConflict", awserr.ErrConflict)
	// ErrSecurityGroupLimitExceeded is returned when too many security groups are specified.
	ErrSecurityGroupLimitExceeded = awserr.New("SecurityGroupLimitExceeded", awserr.ErrConflict)
	// ErrTooManyRequests is returned when a throughput change cooldown is violated.
	ErrTooManyRequests = awserr.New("TooManyRequests", errThrottled)
)
