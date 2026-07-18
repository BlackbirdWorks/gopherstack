package s3

import (
	"context"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

var (
	ErrBucketAlreadyExists     = awserr.New("BucketAlreadyExists", awserr.ErrAlreadyExists)
	ErrBucketAlreadyOwnedByYou = awserr.New("BucketAlreadyOwnedByYou", awserr.ErrAlreadyExists)
	ErrNoSuchBucket            = awserr.New("NoSuchBucket", awserr.ErrNotFound)
	ErrNoSuchKey               = awserr.New("NoSuchKey", awserr.ErrNotFound)
	ErrInvalidBucketName       = errors.New("InvalidBucketName")
	ErrBucketNotEmpty          = errors.New(
		"BucketNotEmpty: The bucket you tried to delete is not empty",
	)
	ErrPermanentRedirect = errors.New("PermanentRedirect")
	// ErrPreconditionFailed is returned when an If-Match / If-None-Match
	// condition on a write op (PutObject / DeleteObject) fails.
	ErrPreconditionFailed         = errors.New("PreconditionFailed")
	ErrNotImplemented             = errors.New("NotImplemented")
	ErrMethodNotAllowed           = errors.New("MethodNotAllowed")
	ErrInvalidArgument            = errors.New(errInvalidArgument)
	ErrNoSuchUpload               = awserr.New("NoSuchUpload", awserr.ErrNotFound)
	ErrInvalidPart                = errors.New("InvalidPart")
	ErrInvalidPartOrder           = errors.New("InvalidPartOrder")
	ErrEmptyParts                 = errors.New("InvalidRequest")
	ErrNoCompressor               = errors.New("data is compressed but no compressor available")
	ErrNoBucketPolicy             = errors.New("NoSuchBucketPolicy")
	ErrNoCORSConfig               = errors.New("NoSuchCORSConfiguration")
	ErrNoLifecycleConfig          = errors.New("NoSuchLifecycleConfiguration")
	ErrNoObjectLockConfig         = errors.New("ObjectLockConfigurationNotFoundError")
	ErrNoWebsiteConfig            = errors.New("NoSuchWebsiteConfiguration")
	ErrNoEncryptionConfig         = errors.New("ServerSideEncryptionConfigurationNotFoundError")
	ErrObjectLocked               = errors.New(errAccessDenied)
	ErrInvalidObjectState         = errors.New("InvalidObjectState")
	ErrNoSuchObjectLockConfig     = awserr.New("NoSuchObjectLockConfiguration", awserr.ErrNotFound)
	ErrNoPublicAccessBlock        = errors.New("NoSuchPublicAccessBlockConfiguration")
	ErrNoOwnershipControls        = errors.New("OwnershipControlsNotFoundError")
	ErrNoReplicationConfig        = errors.New("ReplicationConfigurationNotFoundError")
	ErrNoAnalyticsConfig          = errors.New(errNoSuchConfig)
	ErrNoInventoryConfig          = errors.New(errNoSuchConfig)
	ErrNoMetricsConfig            = errors.New(errNoSuchConfig)
	ErrNoIntelligentTieringConfig = errors.New(errNoSuchConfig)
	ErrNoMetadataConfig           = errors.New(errNoSuchConfig)
	ErrNoMetadataTableConfig      = errors.New(errNoSuchConfig)
	ErrNoSuchTagSet               = errors.New("NoSuchTagSet")
	ErrBadChecksum                = errors.New("BadDigest")
	ErrDeleteMarker               = errors.New("DeleteMarker")
	ErrLatestDeleteMarker         = errors.New("LatestDeleteMarker")
	ErrTooManyTags                = errors.New("BadRequest")
	ErrInvalidTag                 = errors.New("InvalidTag")
	ErrCopySelfNoChange           = errors.New("CopySelfNoChange")
	ErrEntityTooSmall             = errors.New("EntityTooSmall")
	ErrAccessDenied               = errors.New(errAccessDenied)
	ErrKeyTooLongError            = errors.New("KeyTooLongError")
	ErrSSECRequired               = errors.New("InvalidRequest")
	// ErrMalformedXML is returned when an XML request body cannot be decoded.
	// The error table maps it to HTTP 400 with code "MalformedXML".
	ErrMalformedXML = errors.New(errMalformedXML)
)

type s3ErrorInfo struct {
	code    string
	message string
	status  int
}

type s3ErrorEntry struct {
	err  error
	info s3ErrorInfo
}

// errorTable returns the mapping of typed Go errors to S3 error codes and HTTP statuses.
func errorTable() []s3ErrorEntry {
	return append(coreErrorTable(), configErrorTable()...)
}

func coreErrorTable() []s3ErrorEntry {
	return append(coreErrorTableBucket(), coreErrorTableObject()...)
}

func coreErrorTableBucket() []s3ErrorEntry {
	return []s3ErrorEntry{
		{
			ErrNoSuchBucket,
			s3ErrorInfo{
				"NoSuchBucket",
				"The specified bucket does not exist.",
				http.StatusNotFound,
			},
		},
		{
			ErrNoSuchKey,
			s3ErrorInfo{errNoSuchKey, "The specified key does not exist.", http.StatusNotFound},
		},
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
		{ErrInvalidPartOrder, s3ErrorInfo{
			"InvalidPartOrder",
			"The list of parts was not in ascending order. Parts must be ordered by part number.",
			http.StatusBadRequest,
		}},
		{ErrEmptyParts, s3ErrorInfo{
			errInvalidRequest,
			"You must specify at least one part",
			http.StatusBadRequest,
		}},
		{
			ErrInvalidArgument,
			s3ErrorInfo{errInvalidArgument, "Invalid Argument.", http.StatusBadRequest},
		},
	}
}

func coreErrorTableObject() []s3ErrorEntry {
	return []s3ErrorEntry{
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
		{ErrObjectLocked, s3ErrorInfo{errAccessDenied, "Access Denied", http.StatusForbidden}},
		{ErrInvalidObjectState, s3ErrorInfo{
			"InvalidObjectState",
			"The operation is not valid for the object's storage class or lock state.",
			http.StatusConflict,
		}},
		{ErrBadChecksum, s3ErrorInfo{
			"BadDigest",
			"The Content-MD5 or checksum you specified did not match what we received.",
			http.StatusBadRequest,
		}},
		{ErrDeleteMarker, s3ErrorInfo{
			"MethodNotAllowed",
			"The specified method is not allowed against this resource.",
			http.StatusMethodNotAllowed,
		}},
		{ErrLatestDeleteMarker, s3ErrorInfo{
			errNoSuchKey,
			"The specified key does not exist.",
			http.StatusNotFound,
		}},
		{ErrTooManyTags, s3ErrorInfo{
			"BadRequest",
			"Object tags cannot be greater than 10",
			http.StatusBadRequest,
		}},
		{ErrInvalidTag, s3ErrorInfo{
			"InvalidTag",
			"The TagKey or TagValue you have provided is invalid or too long.",
			http.StatusBadRequest,
		}},
		{ErrCopySelfNoChange, s3ErrorInfo{
			errInvalidRequest,
			"This copy request is illegal because it is trying to copy an object to " +
				"itself without changing the object's metadata, storage class, website " +
				"redirect location or encryption attributes.",
			http.StatusBadRequest,
		}},
		{ErrEntityTooSmall, s3ErrorInfo{
			"EntityTooSmall",
			"Your proposed upload is smaller than the minimum allowed size.",
			http.StatusBadRequest,
		}},
		{ErrAccessDenied, s3ErrorInfo{
			errAccessDenied,
			"Access Denied",
			http.StatusForbidden,
		}},
		{ErrPreconditionFailed, s3ErrorInfo{
			"PreconditionFailed",
			"At least one of the pre-conditions you specified did not hold.",
			http.StatusPreconditionFailed,
		}},
		{ErrKeyTooLongError, s3ErrorInfo{
			"KeyTooLongError",
			"Your key is too long.",
			http.StatusBadRequest,
		}},
		{ErrSSECRequired, s3ErrorInfo{
			errInvalidRequest,
			"The object was stored using a form of Server Side Encryption. " +
				"The correct parameters must be provided to retrieve the object.",
			http.StatusBadRequest,
		}},
	}
}

func configErrorTable() []s3ErrorEntry {
	return []s3ErrorEntry{
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
		{ErrNoObjectLockConfig, s3ErrorInfo{
			"ObjectLockConfigurationNotFoundError",
			"Object Lock configuration does not exist for this bucket",
			http.StatusNotFound,
		}},
		{ErrNoWebsiteConfig, s3ErrorInfo{
			"NoSuchWebsiteConfiguration",
			"The specified bucket does not have a website configuration",
			http.StatusNotFound,
		}},
		{ErrNoEncryptionConfig, s3ErrorInfo{
			"ServerSideEncryptionConfigurationNotFoundError",
			"The server side encryption configuration was not found",
			http.StatusNotFound,
		}},
		{ErrNoPublicAccessBlock, s3ErrorInfo{
			"NoSuchPublicAccessBlockConfiguration",
			"The public access block configuration was not found",
			http.StatusNotFound,
		}},
		{ErrNoOwnershipControls, s3ErrorInfo{
			"OwnershipControlsNotFoundError",
			"The bucket ownership controls were not found",
			http.StatusNotFound,
		}},
		{ErrNoReplicationConfig, s3ErrorInfo{
			"ReplicationConfigurationNotFoundError",
			"The replication configuration was not found",
			http.StatusNotFound,
		}},
		{ErrNoAnalyticsConfig, s3ErrorInfo{
			errNoSuchConfig,
			"The analytics configuration does not exist",
			http.StatusNotFound,
		}},
		{ErrNoInventoryConfig, s3ErrorInfo{
			errNoSuchConfig,
			"The inventory configuration does not exist",
			http.StatusNotFound,
		}},
		{ErrNoMetricsConfig, s3ErrorInfo{
			errNoSuchConfig,
			"The metrics configuration does not exist",
			http.StatusNotFound,
		}},
		{ErrNoIntelligentTieringConfig, s3ErrorInfo{
			errNoSuchConfig,
			"The intelligent-tiering configuration does not exist",
			http.StatusNotFound,
		}},
		{ErrNoMetadataConfig, s3ErrorInfo{
			errNoSuchConfig,
			"The metadata configuration does not exist",
			http.StatusNotFound,
		}},
		{ErrNoMetadataTableConfig, s3ErrorInfo{
			errNoSuchConfig,
			"The metadata table configuration does not exist",
			http.StatusNotFound,
		}},
		{ErrNoSuchTagSet, s3ErrorInfo{
			"NoSuchTagSet",
			"The TagSet does not exist",
			http.StatusNotFound,
		}},
	}
}

// WriteError translates a typed Go error to an S3 ErrorResponse XML payload.
func WriteError(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) {
	for _, e := range errorTable() {
		if errors.Is(err, e.err) {
			httputils.WriteS3ErrorResponse(
				ctx, w, r,
				ErrorResponse{Code: e.info.code, Message: e.info.message},
				e.info.status,
			)

			return
		}
	}

	httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
		Code:    errCodeInternalError,
		Message: "We encountered an internal error. Please try again.",
	}, http.StatusInternalServerError)
}
