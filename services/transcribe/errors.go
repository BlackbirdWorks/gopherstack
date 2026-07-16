package transcribe

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotFound is returned when a transcription job is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the given name already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid or missing input parameters.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
	// ErrVocabularyNotFound is returned when a vocabulary is not found.
	// Real AWS returns BadRequestException (400) for missing vocabularies, not NotFoundException (404).
	ErrVocabularyNotFound = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)
