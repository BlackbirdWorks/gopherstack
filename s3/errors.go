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
	ErrNotImplemented    = errors.New("NotImplemented")
	ErrMethodNotAllowed  = errors.New("MethodNotAllowed")
	ErrInvalidArgument   = errors.New("InvalidArgument")
	ErrNoSuchUpload      = errors.New("NoSuchUpload")
	ErrInvalidPart       = errors.New("InvalidPart")
	ErrNoCompressor      = errors.New("data is compressed but no compressor available")
	ErrNoBucketPolicy    = errors.New("NoSuchBucketPolicy")
	ErrNoCORSConfig      = errors.New("NoSuchCORSConfiguration")
	ErrNoLifecycleConfig = errors.New("NoSuchLifecycleConfiguration")
)

type s3ErrorInfo struct {
	code    string
	message string
	status  int
}

// WriteError translates a typed Go error to an S3 ErrorResponse XML payload.
func WriteError(log *slog.Logger, w http.ResponseWriter, r *http.Request, err error) {
	type s3ErrorEntry struct {
		err  error
		info s3ErrorInfo
	}

	table := []s3ErrorEntry{
		{ErrNoSuchBucket, s3ErrorInfo{"NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound}},
		{ErrNoSuchKey, s3ErrorInfo{"NoSuchKey", "The specified key does not exist.", http.StatusNotFound}},
		{ErrBucketAlreadyOwnedByYou, s3ErrorInfo{
			"BucketAlreadyOwnedByYou",
			"Your previous request to create the named bucket succeeded and you already own it.",
			http.StatusConflict,
		}},
		{ErrBucketAlreadyExists, s3ErrorInfo{
			"BucketAlreadyExists",
			"The requested bucket name is not available.",
			http.StatusConflict,
		}},
		{ErrInvalidBucketName, s3ErrorInfo{
			"InvalidBucketName",
			"The specified bucket is not valid.",
			http.StatusBadRequest,
		}},
		{ErrBucketNotEmpty, s3ErrorInfo{
			"BucketNotEmpty",
			"The bucket you tried to delete is not empty.",
			http.StatusConflict,
		}},
		{ErrNoSuchUpload, s3ErrorInfo{
			"NoSuchUpload",
			"The specified multipart upload does not exist.",
			http.StatusNotFound,
		}},
		{ErrInvalidPart, s3ErrorInfo{
			"InvalidPart",
			"One or more of the specified parts could not be found.",
			http.StatusBadRequest,
		}},
		{ErrInvalidArgument, s3ErrorInfo{"InvalidArgument", "Invalid Argument.", http.StatusBadRequest}},
		{ErrMethodNotAllowed, s3ErrorInfo{
			"MethodNotAllowed",
			"The specified method is not allowed against this resource.",
			http.StatusMethodNotAllowed,
		}},
		{ErrNotImplemented, s3ErrorInfo{
			"NotImplemented",
			"A header you provided implies functionality that is not implemented.",
			http.StatusNotImplemented,
		}},
		{ErrNoBucketPolicy, s3ErrorInfo{
			"NoSuchBucketPolicy",
			"The bucket policy does not exist",
			http.StatusNotFound,
		}},
		{ErrNoCORSConfig, s3ErrorInfo{
			"NoSuchCORSConfiguration",
			"The CORS configuration does not exist",
			http.StatusNotFound,
		}},
		{ErrNoLifecycleConfig, s3ErrorInfo{
			"NoSuchLifecycleConfiguration",
			"The lifecycle configuration does not exist",
			http.StatusNotFound,
		}},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			httputil.WriteS3ErrorResponse(
				log, w, r,
				ErrorResponse{Code: e.info.code, Message: e.info.message},
				e.info.status,
			)

			return
		}
	}

	httputil.WriteS3ErrorResponse(log, w, r, ErrorResponse{
		Code:    "InternalError",
		Message: "We encountered an internal error. Please try again.",
	}, http.StatusInternalServerError)
}
