package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_BucketEncryption verifies the PutBucketEncryption /
// GetBucketEncryption / DeleteBucketEncryption HTTP handlers, including error
// paths for missing configuration and malformed XML.
func TestHandler_BucketEncryption(t *testing.T) {
	t.Parallel()

	const encryptionXML = `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Rule><ApplyServerSideEncryptionByDefault>` +
		`<SSEAlgorithm>AES256</SSEAlgorithm>` +
		`</ApplyServerSideEncryptionByDefault></Rule>` +
		`</ServerSideEncryptionConfiguration>`

	tests := []struct {
		name     string
		method   string
		url      string
		body     string
		setup    bool
		wantCode int
	}{
		{
			name:     "put_encryption_config",
			method:   http.MethodPut,
			url:      "/enc-test-bucket?encryption",
			body:     encryptionXML,
			wantCode: http.StatusOK,
		},
		{
			name:     "get_encryption_config",
			method:   http.MethodGet,
			url:      "/enc-test-bucket?encryption",
			setup:    true,
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_encryption_config",
			method:   http.MethodDelete,
			url:      "/enc-test-bucket?encryption",
			setup:    true,
			wantCode: http.StatusNoContent,
		},
		{
			name:     "get_encryption_config_not_found",
			method:   http.MethodGet,
			url:      "/enc-notfound-bucket?encryption",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "put_encryption_invalid_xml",
			method:   http.MethodPut,
			url:      "/enc-test-bucket?encryption",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "enc-test-bucket")

			if tt.setup {
				putReq := httptest.NewRequest(
					http.MethodPut,
					"/enc-test-bucket?encryption",
					strings.NewReader(encryptionXML),
				)
				putRec := httptest.NewRecorder()
				serveS3Handler(handler, putRec, putReq)
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			var reqBody *strings.Reader
			if tt.body != "" {
				reqBody = strings.NewReader(tt.body)
			} else {
				reqBody = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.url, reqBody)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBucketEncryptionConfiguration(t *testing.T) {
	t.Parallel()

	const encryptionXML = `<ServerSideEncryptionConfiguration>` +
		`<Rule><ApplyServerSideEncryptionByDefault>` +
		`<SSEAlgorithm>AES256</SSEAlgorithm>` +
		`</ApplyServerSideEncryptionByDefault></Rule>` +
		`</ServerSideEncryptionConfiguration>`

	const kmsEncryptionXML = `<ServerSideEncryptionConfiguration>` +
		`<Rule><ApplyServerSideEncryptionByDefault>` +
		`<SSEAlgorithm>aws:kms</SSEAlgorithm>` +
		`<KMSMasterKeyID>arn:aws:kms:us-east-1:000000000000:key/test-key</KMSMasterKeyID>` +
		`</ApplyServerSideEncryptionByDefault></Rule>` +
		`</ServerSideEncryptionConfiguration>`

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, backend *s3.InMemoryBackend, bucket string)
		name    string
		bucket  string
		want    string
	}{
		{
			name:   "get returns ServerSideEncryptionConfigurationNotFoundError when not set",
			bucket: "encryption-test-bucket",
			setup: func(_ *testing.T, _ *s3.InMemoryBackend, _ string) {
				// No encryption config stored.
			},
			wantErr: s3.ErrNoEncryptionConfig,
		},
		{
			name:   "put then get returns stored AES256 config",
			bucket: "encryption-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketEncryption(t.Context(), bucket, encryptionXML)
				require.NoError(t, err)
			},
			want: encryptionXML,
		},
		{
			name:   "put then get returns stored aws:kms config",
			bucket: "encryption-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketEncryption(t.Context(), bucket, kmsEncryptionXML)
				require.NoError(t, err)
			},
			want: kmsEncryptionXML,
		},
		{
			name:   "delete clears the config",
			bucket: "encryption-test-bucket",
			setup: func(t *testing.T, backend *s3.InMemoryBackend, bucket string) {
				t.Helper()
				err := backend.PutBucketEncryption(t.Context(), bucket, encryptionXML)
				require.NoError(t, err)
				err = backend.DeleteBucketEncryption(t.Context(), bucket)
				require.NoError(t, err)
			},
			wantErr: s3.ErrNoEncryptionConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
				Bucket: aws.String(tt.bucket),
			})
			require.NoError(t, err)

			tt.setup(t, backend, tt.bucket)

			got, err := backend.GetBucketEncryption(t.Context(), tt.bucket)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPutBucketEncryption_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.PutBucketEncryption(
		t.Context(),
		"nonexistent-bucket",
		"<ServerSideEncryptionConfiguration/>",
	)
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestGetBucketEncryption_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	_, err := backend.GetBucketEncryption(t.Context(), "nonexistent-bucket")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}

func TestDeleteBucketEncryption_NotFound(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	err := backend.DeleteBucketEncryption(t.Context(), "nonexistent-bucket")
	assert.ErrorIs(t, err, s3.ErrNoSuchBucket)
}
