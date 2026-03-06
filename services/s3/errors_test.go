package s3_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/assert"
)

func TestWriteError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err          error
		expectedXML  string
		expectedCode int
	}{
		{
			err:          s3.ErrNoSuchBucket,
			expectedCode: http.StatusNotFound,
			expectedXML: "<Error><Code>NoSuchBucket</Code>" +
				"<Message>The specified bucket does not exist.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrNoSuchKey,
			expectedCode: http.StatusNotFound,
			expectedXML: "<Error><Code>NoSuchKey</Code>" +
				"<Message>The specified key does not exist.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrBucketAlreadyOwnedByYou,
			expectedCode: http.StatusConflict,
			expectedXML: "<Error><Code>BucketAlreadyOwnedByYou</Code>" +
				"<Message>Your previous request to create the named bucket succeeded and you already own it.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrBucketAlreadyExists,
			expectedCode: http.StatusConflict,
			expectedXML: "<Error><Code>BucketAlreadyExists</Code>" +
				"<Message>The requested bucket name is not available.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrInvalidBucketName,
			expectedCode: http.StatusBadRequest,
			expectedXML: "<Error><Code>InvalidBucketName</Code>" +
				"<Message>The specified bucket is not valid.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrBucketNotEmpty,
			expectedCode: http.StatusConflict,
			expectedXML: "<Error><Code>BucketNotEmpty</Code>" +
				"<Message>The bucket you tried to delete is not empty.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrNoSuchUpload,
			expectedCode: http.StatusNotFound,
			expectedXML: "<Error><Code>NoSuchUpload</Code>" +
				"<Message>The specified multipart upload does not exist.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrInvalidPart,
			expectedCode: http.StatusBadRequest,
			expectedXML: "<Error><Code>InvalidPart</Code>" +
				"<Message>One or more of the specified parts could not be found.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrInvalidArgument,
			expectedCode: http.StatusBadRequest,
			expectedXML: "<Error><Code>InvalidArgument</Code>" +
				"<Message>Invalid Argument.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrMethodNotAllowed,
			expectedCode: http.StatusMethodNotAllowed,
			expectedXML: "<Error><Code>MethodNotAllowed</Code>" +
				"<Message>The specified method is not allowed against this resource.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          s3.ErrNotImplemented,
			expectedCode: http.StatusNotImplemented,
			expectedXML: "<Error><Code>NotImplemented</Code>" +
				"<Message>A header you provided implies functionality that is not implemented.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
		{
			err:          nil,
			expectedCode: http.StatusInternalServerError,
			expectedXML: "<Error><Code>InternalError</Code>" +
				"<Message>We encountered an internal error. Please try again.</Message>" +
				"<Resource></Resource><RequestId></RequestId></Error>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.expectedXML, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/test", nil)

			s3.WriteError(r.Context(), w, r, tt.err)

			assert.Equal(t, tt.expectedCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.expectedXML)
			assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))
		})
	}
}
