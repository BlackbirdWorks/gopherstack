package s3

import "errors"

var (
	ErrBucketAlreadyExists = errors.New("BucketAlreadyExists")
	ErrNoSuchBucket        = errors.New("NoSuchBucket")
	ErrNoSuchKey           = errors.New("NoSuchKey")
	ErrInvalidBucketName   = errors.New("InvalidBucketName")
	ErrBucketNotEmpty      = errors.New("BucketNotEmpty: The bucket you tried to delete is not empty")
	ErrNotImplemented      = errors.New("NotImplemented")
	ErrMethodNotAllowed    = errors.New("MethodNotAllowed")
	ErrInvalidArgument     = errors.New("InvalidArgument")
	ErrNoSuchUpload        = errors.New("NoSuchUpload")
	ErrInvalidPart         = errors.New("InvalidPart")
	ErrNoCompressor        = errors.New("data is compressed but no compressor available")
)
