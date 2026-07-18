package resourcegroups

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a resource group is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource group already exists.
	ErrAlreadyExists = awserr.New("BadRequestException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when request validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrTagSyncTaskNotFound is returned when a tag-sync task is not found.
	ErrTagSyncTaskNotFound = awserr.New(
		"NotFoundException: tag-sync task not found",
		awserr.ErrNotFound,
	)
)
