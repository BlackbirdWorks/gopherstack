package support

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a support case is not found.
	ErrNotFound = awserr.New("CaseIdNotFound", awserr.ErrNotFound)
	// ErrAttachmentNotFound is returned when an attachment is not found.
	ErrAttachmentNotFound = awserr.New("AttachmentIdNotFound", awserr.ErrNotFound)
	// ErrValidation is returned when required input fields are missing or invalid.
	ErrValidation = awserr.New("ValidationError", awserr.ErrInvalidParameter)
	// ErrAttachmentSetNotFound indicates an unknown staged attachment set.
	ErrAttachmentSetNotFound = awserr.New("AttachmentSetIdNotFound", awserr.ErrNotFound)
	// ErrAttachmentSetExpired indicates a staged attachment set passed its one-hour lifetime.
	ErrAttachmentSetExpired = awserr.New("AttachmentSetExpired", awserr.ErrInvalidParameter)
)
