package kinesisvideo

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrStreamNotFound is returned when a stream does not exist.
	ErrStreamNotFound = awserr.New("stream not found", awserr.ErrNotFound)
	// ErrStreamAlreadyExists is returned when a stream name is already in use.
	ErrStreamAlreadyExists = awserr.New("stream already exists", awserr.ErrAlreadyExists)
	// ErrChannelNotFound is returned when a signaling channel does not exist.
	ErrChannelNotFound = awserr.New("signaling channel not found", awserr.ErrNotFound)
	// ErrChannelAlreadyExists is returned when a channel name is already in use.
	ErrChannelAlreadyExists = awserr.New("signaling channel already exists", awserr.ErrAlreadyExists)
	// ErrVersionMismatch is returned when CurrentVersion does not match the resource's version.
	ErrVersionMismatch = awserr.New("version mismatch", awserr.ErrConflict)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("invalid argument", awserr.ErrInvalidParameter)
)
