package ecrpublic

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// Sentinel errors, wrapped so callers can match with [errors.Is] while the
// message carries the real AWS exception name for classifyError.
var (
	ErrRepositoryNotFound       = awserr.New("RepositoryNotFoundException", awserr.ErrNotFound)
	ErrRepositoryAlreadyExists  = awserr.New("RepositoryAlreadyExistsException", awserr.ErrAlreadyExists)
	ErrRepositoryNotEmpty       = awserr.New("RepositoryNotEmptyException", awserr.ErrConflict)
	ErrRepositoryPolicyNotFound = awserr.New("RepositoryPolicyNotFoundException", awserr.ErrNotFound)
	ErrRegistryNotFound         = awserr.New("RegistryNotFoundException", awserr.ErrNotFound)
	ErrInvalidParameter         = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	ErrTooManyTags              = awserr.New("TooManyTagsException", awserr.ErrInvalidParameter)
	ErrInvalidTagParameter      = awserr.New("InvalidTagParameterException", awserr.ErrInvalidParameter)

	ErrUploadNotFound     = awserr.New("UploadNotFoundException", awserr.ErrNotFound)
	ErrEmptyUpload        = awserr.New("EmptyUploadException", awserr.ErrInvalidParameter)
	ErrLayerPartTooSmall  = awserr.New("LayerPartTooSmallException", awserr.ErrInvalidParameter)
	ErrInvalidLayerPart   = awserr.New("InvalidLayerPartException", awserr.ErrInvalidParameter)
	ErrLayerAlreadyExists = awserr.New("LayerAlreadyExistsException", awserr.ErrAlreadyExists)
	ErrLayersNotFound     = awserr.New("LayersNotFoundException", awserr.ErrInvalidParameter)
	ErrInvalidLayer       = awserr.New("InvalidLayerException", awserr.ErrInvalidParameter)

	ErrImageNotFound           = awserr.New("ImageNotFoundException", awserr.ErrNotFound)
	ErrImageAlreadyExists      = awserr.New("ImageAlreadyExistsException", awserr.ErrAlreadyExists)
	ErrImageDigestDoesNotMatch = awserr.New("ImageDigestDoesNotMatchException", awserr.ErrInvalidParameter)
	ErrImageTagAlreadyExists   = awserr.New("ImageTagAlreadyExistsException", awserr.ErrConflict)
)
