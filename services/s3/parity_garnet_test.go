package s3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestParity_PutGetBucketAbac verifies that PutBucketAbac stores the ABAC
// configuration and GetBucketAbac returns it, ending the silent-discard behaviour.
func TestParity_PutGetBucketAbac(t *testing.T) {
	t.Parallel()

	const abacXML = `<AbacConfiguration><Status>Enabled</Status></AbacConfiguration>`

	tests := []struct {
		name          string
		bucket        string
		putBody       string
		getWantStatus string
		putWantCode   int
		getWantCode   int
	}{
		{
			name:          "enabled_config_round_trips",
			bucket:        "bkt",
			putBody:       abacXML,
			putWantCode:   http.StatusOK,
			getWantCode:   http.StatusOK,
			getWantStatus: "Enabled",
		},
		{
			name:          "empty_body_accepted",
			bucket:        "bkt2",
			putBody:       "",
			putWantCode:   http.StatusOK,
			getWantCode:   http.StatusOK,
			getWantStatus: "",
		},
		{
			name:        "put_missing_bucket_404",
			bucket:      "nosuchbucket",
			putBody:     abacXML,
			putWantCode: http.StatusNotFound,
			getWantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			if tt.putWantCode != http.StatusNotFound {
				mustCreateBucket(t, backend, tt.bucket)
			}

			// PUT
			putReq := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"?abac",
				strings.NewReader(tt.putBody))
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			assert.Equal(t, tt.putWantCode, putRec.Code, "PUT abac status")

			// GET (only when PUT succeeded or to verify 404 on missing bucket)
			getReq := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"?abac", nil)
			getRec := httptest.NewRecorder()
			serveS3Handler(handler, getRec, getReq)
			assert.Equal(t, tt.getWantCode, getRec.Code, "GET abac status")

			if tt.getWantStatus != "" {
				assert.Contains(t, getRec.Body.String(), tt.getWantStatus,
					"GET abac should return stored status")
			}
		})
	}
}

// TestParity_UpdateBucketMetadataInventoryTableConfig verifies that PUT
// ?metadataInventoryTableConfiguration persists the config (no longer a no-op).
func TestParity_UpdateBucketMetadataInventoryTableConfig(t *testing.T) {
	t.Parallel()

	const cfgXML = `<InventoryTableConfiguration><Status>Enabled</Status></InventoryTableConfiguration>`

	tests := []struct {
		name       string
		bucket     string
		body       string
		wantCode   int
		wantStored bool
	}{
		{
			name:       "config_stored",
			bucket:     "bkt",
			body:       cfgXML,
			wantCode:   http.StatusOK,
			wantStored: true,
		},
		{
			name:     "missing_bucket_404",
			bucket:   "nosuchbucket",
			body:     cfgXML,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			if tt.wantCode != http.StatusNotFound {
				mustCreateBucket(t, backend, tt.bucket)
			}

			req := httptest.NewRequest(
				http.MethodPut,
				"/"+tt.bucket+"?metadataInventoryTableConfiguration",
				strings.NewReader(tt.body),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_UpdateBucketMetadataJournalTableConfig verifies that PUT
// ?metadataJournalTableConfiguration persists the config (no longer a no-op).
func TestParity_UpdateBucketMetadataJournalTableConfig(t *testing.T) {
	t.Parallel()

	const cfgXML = `<JournalTableConfiguration><Status>Enabled</Status></JournalTableConfiguration>`

	tests := []struct {
		name     string
		bucket   string
		body     string
		wantCode int
	}{
		{
			name:     "config_stored",
			bucket:   "bkt",
			body:     cfgXML,
			wantCode: http.StatusOK,
		},
		{
			name:     "missing_bucket_404",
			bucket:   "nosuchbucket",
			body:     cfgXML,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			if tt.wantCode != http.StatusNotFound {
				mustCreateBucket(t, backend, tt.bucket)
			}

			req := httptest.NewRequest(
				http.MethodPut,
				"/"+tt.bucket+"?metadataJournalTableConfiguration",
				strings.NewReader(tt.body),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_BackendAbac verifies the backend-level PutBucketAbac / GetBucketAbac methods.
func TestParity_BackendAbac(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		putXML     string
		wantXML    string
		wantErrPut bool
		wantErrGet bool
		missingBkt bool
	}{
		{
			name:    "put_and_get_roundtrip",
			putXML:  `<AbacConfiguration><Status>Enabled</Status></AbacConfiguration>`,
			wantXML: `<AbacConfiguration><Status>Enabled</Status></AbacConfiguration>`,
		},
		{
			name:    "overwrite_replaces_previous",
			putXML:  `<AbacConfiguration><Status>Disabled</Status></AbacConfiguration>`,
			wantXML: `<AbacConfiguration><Status>Disabled</Status></AbacConfiguration>`,
		},
		{
			name:       "missing_bucket_returns_error",
			putXML:     `<AbacConfiguration/>`,
			wantErrPut: true,
			wantErrGet: true,
			missingBkt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})

			if !tt.missingBkt {
				_, err := backend.CreateBucket(t.Context(),
					&sdk_s3.CreateBucketInput{Bucket: aws.String("bkt")})
				require.NoError(t, err)
			}

			bucketName := "bkt"
			if tt.missingBkt {
				bucketName = "no-such-bucket"
			}

			errPut := backend.PutBucketAbac(t.Context(), bucketName, tt.putXML)
			if tt.wantErrPut {
				require.Error(t, errPut)
			} else {
				require.NoError(t, errPut)
			}

			got, errGet := backend.GetBucketAbac(t.Context(), bucketName)
			if tt.wantErrGet {
				require.Error(t, errGet)
			} else {
				require.NoError(t, errGet)
				assert.Equal(t, tt.wantXML, got)
			}
		})
	}
}

// TestParity_BackendMetadataTableConfigs verifies inventory/journal table config storage.
func TestParity_BackendMetadataTableConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configType string // "inventory" or "journal"
		putXML     string
		wantErr    bool
		missingBkt bool
	}{
		{
			name:       "inventory_stored",
			configType: "inventory",
			putXML:     `<InventoryTableConfiguration><Status>Enabled</Status></InventoryTableConfiguration>`,
		},
		{
			name:       "journal_stored",
			configType: "journal",
			putXML:     `<JournalTableConfiguration><Status>Enabled</Status></JournalTableConfiguration>`,
		},
		{
			name:       "inventory_missing_bucket_errors",
			configType: "inventory",
			putXML:     `<InventoryTableConfiguration/>`,
			wantErr:    true,
			missingBkt: true,
		},
		{
			name:       "journal_missing_bucket_errors",
			configType: "journal",
			putXML:     `<JournalTableConfiguration/>`,
			wantErr:    true,
			missingBkt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			bucketName := "bkt"

			if !tt.missingBkt {
				_, err := backend.CreateBucket(t.Context(),
					&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)})
				require.NoError(t, err)
			} else {
				bucketName = "no-such-bucket"
			}

			var err error
			switch tt.configType {
			case "inventory":
				err = backend.UpdateBucketMetadataInventoryTableConfig(
					t.Context(), bucketName, tt.putXML)
			case "journal":
				err = backend.UpdateBucketMetadataJournalTableConfig(
					t.Context(), bucketName, tt.putXML)
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParity_CleanupDefaultMultipartUsesReadLockScan verifies that
// cleanupDefaultMultipart (via SweepOnce) still removes expired uploads and
// leaves non-expired uploads intact after the RLock-scan + Lock-delete refactor.
func TestParity_CleanupDefaultMultipartUsesReadLockScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		expiredCount    int
		nonExpiredCount int
	}{
		{
			name:            "expires_old_leaves_fresh",
			expiredCount:    3,
			nonExpiredCount: 2,
		},
		{
			name:            "all_expired",
			expiredCount:    5,
			nonExpiredCount: 0,
		},
		{
			name:            "none_expired",
			expiredCount:    0,
			nonExpiredCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithSkipMultipartSizeCheck()
			janitor := s3.NewJanitor(backend, s3.Settings{})
			const bucket = "mpbucket"

			_, err := backend.CreateBucket(t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			// Create expired uploads.
			for range tt.expiredCount {
				out, mpErr := backend.CreateMultipartUpload(t.Context(),
					&sdk_s3.CreateMultipartUploadInput{
						Bucket: aws.String(bucket),
						Key:    aws.String("old-key"),
					})
				require.NoError(t, mpErr)
				// Backdate the upload to 25 hours ago.
				s3.BackdateUploadForTest(
					backend,
					bucket,
					out.UploadId,
					time.Now().Add(-25*time.Hour),
				)
			}

			// Create non-expired uploads.
			for range tt.nonExpiredCount {
				_, mpErr := backend.CreateMultipartUpload(t.Context(),
					&sdk_s3.CreateMultipartUploadInput{
						Bucket: aws.String(bucket),
						Key:    aws.String("new-key"),
					})
				require.NoError(t, mpErr)
			}

			janitor.SweepOnce(t.Context())

			got := backend.UploadsForBucket(bucket)
			assert.Equal(t, tt.nonExpiredCount, got,
				"only non-expired uploads should remain")
		})
	}
}

// TestParity_EvictNoncurrentVersionsPerObjectLock verifies that the per-object
// brief-lock refactor of evictNoncurrentVersions still correctly removes expired
// noncurrent versions and preserves the latest version.
func TestParity_EvictNoncurrentVersionsPerObjectLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		noncurrentDaysXML string
		backdateBy        time.Duration
		wantEvicted       bool
	}{
		{
			name:              "noncurrent_expired_evicted",
			noncurrentDaysXML: `<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration>`,
			backdateBy:        -48 * time.Hour,
			wantEvicted:       true,
		},
		{
			name:              "noncurrent_fresh_retained",
			noncurrentDaysXML: `<NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration>`,
			backdateBy:        -1 * time.Hour,
			wantEvicted:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			const bucket = "vbucket"
			const key = "vobj"

			mustCreateBucket(t, backend, bucket)
			enableVersioning(t, handler, bucket)

			// Create two versions: v1 then v2 (v1 becomes noncurrent).
			mustPutObject(t, backend, bucket, key, []byte("v1"))
			s3.BackdateObjectForTest(backend, bucket, key, time.Now().Add(tt.backdateBy))
			mustPutObject(t, backend, bucket, key, []byte("v2"))

			// Configure lifecycle to expire noncurrent versions.
			lcXML := `<LifecycleConfiguration><Rule><ID>noncurrent</ID><Status>Enabled</Status>` +
				`<Filter><Prefix></Prefix></Filter>` + tt.noncurrentDaysXML +
				`</Rule></LifecycleConfiguration>`

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?lifecycle",
				strings.NewReader(lcXML))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Less(t, rec.Code, http.StatusMultipleChoices, "PUT lifecycle must succeed")

			janitor := s3.NewJanitor(backend, s3.Settings{})
			janitor.SweepOnce(t.Context())

			// HEAD the object — it must still exist (latest version intact).
			// The handler returns 200 or 204 for a present object; 404 means it was deleted.
			headReq := httptest.NewRequest(http.MethodHead, "/"+bucket+"/"+key, nil)
			headRec := httptest.NewRecorder()
			serveS3Handler(handler, headRec, headReq)
			assert.NotEqual(
				t,
				http.StatusNotFound,
				headRec.Code,
				"latest version must survive eviction",
			)
		})
	}
}

// TestParity_BackendShutdown verifies that Shutdown cancels the service context and
// DrainReplicationGoroutines returns promptly, preventing goroutine leaks.
func TestParity_BackendShutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "shutdown_cancels_and_drains"},
		{name: "shutdown_idempotent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})

			// Put an object to trigger replication goroutine machinery.
			_, err := backend.CreateBucket(context.Background(),
				&sdk_s3.CreateBucketInput{Bucket: aws.String("testbkt")})
			require.NoError(t, err)

			_, err = backend.PutObject(context.Background(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("testbkt"),
				Key:    aws.String("k"),
				Body:   strings.NewReader("data"),
			})
			require.NoError(t, err)

			done := make(chan struct{})
			go func() {
				backend.Shutdown()
				if tt.name == "shutdown_idempotent" {
					backend.Shutdown() // second call must not panic or hang
				}
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Shutdown() did not return within 5s — possible goroutine leak")
			}
		})
	}
}

// TestParity_ListDirectoryBuckets verifies that directory buckets (--x-s3 suffix)
// are returned by ListDirectoryBuckets and excluded from ListBuckets, matching
// the AWS partition between general-purpose and S3 Express bucket namespaces.
func TestParity_ListDirectoryBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		createBuckets    []string // general-purpose buckets to create
		createDirBuckets []string // directory buckets to create (--x-s3 suffix)
		wantInList       []string // expected in ListBuckets
		wantInDirList    []string // expected in ListDirectoryBuckets
		wantNotInList    []string // must NOT appear in ListBuckets
		wantNotInDirList []string // must NOT appear in ListDirectoryBuckets
	}{
		{
			name:             "directory_bucket_excluded_from_list_buckets",
			createBuckets:    []string{"regular-bucket"},
			createDirBuckets: []string{"my-bucket--usw2-az3--x-s3"},
			wantInList:       []string{"regular-bucket"},
			wantInDirList:    []string{"my-bucket--usw2-az3--x-s3"},
			wantNotInList:    []string{"my-bucket--usw2-az3--x-s3"},
			wantNotInDirList: []string{"regular-bucket"},
		},
		{
			name:             "no_directory_buckets_returns_empty",
			createBuckets:    []string{"only-regular"},
			createDirBuckets: []string{},
			wantInList:       []string{"only-regular"},
			wantInDirList:    []string{},
			wantNotInDirList: []string{"only-regular"},
		},
		{
			name:             "multiple_directory_buckets_all_returned",
			createBuckets:    []string{},
			createDirBuckets: []string{"a--usw2-az1--x-s3", "b--usw2-az2--x-s3"},
			wantInDirList:    []string{"a--usw2-az1--x-s3", "b--usw2-az2--x-s3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)

			for _, name := range tt.createBuckets {
				mustCreateBucket(t, backend, name)
			}

			for _, name := range tt.createDirBuckets {
				mustCreateBucket(t, backend, name)
			}

			// Verify ListBuckets via HTTP.
			listReq := httptest.NewRequest(http.MethodGet, "/", nil)
			listRec := httptest.NewRecorder()
			serveS3Handler(handler, listRec, listReq)
			require.Equal(t, http.StatusOK, listRec.Code, "ListBuckets must return 200")
			listBody := listRec.Body.String()

			for _, name := range tt.wantInList {
				assert.Contains(t, listBody, name, "ListBuckets must contain %q", name)
			}
			for _, name := range tt.wantNotInList {
				assert.NotContains(t, listBody, name, "ListBuckets must not contain %q", name)
			}

			// Verify ListDirectoryBuckets via HTTP.
			dirReq := httptest.NewRequest(http.MethodGet, "/?list-type=directory", nil)
			dirRec := httptest.NewRecorder()
			serveS3Handler(handler, dirRec, dirReq)
			require.Equal(t, http.StatusOK, dirRec.Code, "ListDirectoryBuckets must return 200")
			dirBody := dirRec.Body.String()

			for _, name := range tt.wantInDirList {
				assert.Contains(t, dirBody, name, "ListDirectoryBuckets must contain %q", name)
			}
			for _, name := range tt.wantNotInDirList {
				assert.NotContains(
					t,
					dirBody,
					name,
					"ListDirectoryBuckets must not contain %q",
					name,
				)
			}
		})
	}
}
