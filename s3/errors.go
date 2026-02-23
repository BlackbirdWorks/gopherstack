package s3

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
)

var (
	ErrBucketAlreadyExists     = errors.New("BucketAlreadyExists")
	ErrBucketAlreadyOwnedByYou = errors.New("BucketAlreadyOwnedByYou")
	ErrNoSuchBucket            = errors.New("NoSuchBucket")
	ErrNoSuchKey               = errors.New("NoSuchKey")
	ErrInvalidBucketName       = errors.New("InvalidBucketName")
	ErrBucketNotEmpty          = errors.New(
		"BucketNotEmpty: The bucket you tried to delete is not empty",
	)
	ErrNotImplemented   = errors.New("NotImplemented")
	ErrMethodNotAllowed = errors.New("MethodNotAllowed")
	ErrInvalidArgument  = errors.New("InvalidArgument")
	ErrNoSuchUpload     = errors.New("NoSuchUpload")
	ErrInvalidPart      = errors.New("InvalidPart")
	ErrNoCompressor     = errors.New("data is compressed but no compressor available")
)

// WriteError translates a typed Go error to an S3 ErrorResponse XML payload.
func WriteError(log *slog.Logger, w http.ResponseWriter, r *http.Request, err error) {
	var code string
	var message string
	status := http.StatusInternalServerError

	switch {
	case errors.Is(err, ErrNoSuchBucket):
		code = "NoSuchBucket"
		message = "The specified bucket does not exist."
		status = http.StatusNotFound
	case errors.Is(err, ErrNoSuchKey):
		code = "NoSuchKey"
		message = "The specified key does not exist."
		status = http.StatusNotFound
	case errors.Is(err, ErrBucketAlreadyOwnedByYou):
		code = "BucketAlreadyOwnedByYou"
		message = "Your previous request to create the named bucket succeeded and you already own it."
		status = http.StatusConflict
	case errors.Is(err, ErrBucketAlreadyExists):
		code = "BucketAlreadyExists"
		message = "The requested bucket name is not available."
		status = http.StatusConflict
	case errors.Is(err, ErrInvalidBucketName):
		code = "InvalidBucketName"
		message = "The specified bucket is not valid."
		status = http.StatusBadRequest
	case errors.Is(err, ErrBucketNotEmpty):
		code = "BucketNotEmpty"
		message = "The bucket you tried to delete is not empty."
		status = http.StatusConflict
	case errors.Is(err, ErrNoSuchUpload):
		code = "NoSuchUpload"
		message = "The specified multipart upload does not exist."
		status = http.StatusNotFound
	case errors.Is(err, ErrInvalidPart):
		code = "InvalidPart"
		message = "One or more of the specified parts could not be found."
		status = http.StatusBadRequest
	case errors.Is(err, ErrInvalidArgument):
		code = "InvalidArgument"
		message = "Invalid Argument."
		status = http.StatusBadRequest
	case errors.Is(err, ErrMethodNotAllowed):
		code = "MethodNotAllowed"
		message = "The specified method is not allowed against this resource."
		status = http.StatusMethodNotAllowed
	case errors.Is(err, ErrNotImplemented):
		code = "NotImplemented"
		message = "A header you provided implies functionality that is not implemented."
		status = http.StatusNotImplemented
	default:
		code = "InternalError"
		message = "We encountered an internal error. Please try again."
	}

	resp := ErrorResponse{
		Code:    code,
		Message: message,
	}

	httputil.WriteS3ErrorResponse(log, w, r, resp, status)
}
