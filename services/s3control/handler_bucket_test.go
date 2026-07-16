package s3control_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- Outposts Bucket ----

func TestOutpostsBucket(t *testing.T) {
	t.Parallel()

	t.Run("get bucket", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "my-bucket")
		bucket, err := b.GetBucket("000000000000", "my-bucket")
		require.NoError(t, err)
		assert.Equal(t, "my-bucket", bucket.Name)
	})

	t.Run("delete bucket", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "to-delete")
		require.NoError(t, b.DeleteBucket("000000000000", "to-delete"))
		_, err := b.GetBucket("000000000000", "to-delete")
		require.Error(t, err)
	})

	t.Run("bucket policy CRUD", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "policy-bucket")
		require.NoError(
			t,
			b.PutBucketPolicy("000000000000", "policy-bucket", `{"Version":"2012-10-17"}`),
		)
		policy, err := b.GetBucketPolicy("000000000000", "policy-bucket")
		require.NoError(t, err)
		assert.Contains(t, policy, "Version")
		require.NoError(t, b.DeleteBucketPolicy("000000000000", "policy-bucket"))
		policy2, _ := b.GetBucketPolicy("000000000000", "policy-bucket")
		assert.Empty(t, policy2)
	})

	t.Run("bucket tagging CRUD", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "tag-bucket")
		require.NoError(
			t,
			b.PutBucketTagging("000000000000", "tag-bucket", s3control.TagSet{"env": "prod"}),
		)
		tags, err := b.GetBucketTagging("000000000000", "tag-bucket")
		require.NoError(t, err)
		assert.Equal(t, "prod", tags["env"])
		require.NoError(t, b.DeleteBucketTagging("000000000000", "tag-bucket"))
	})

	t.Run("bucket versioning", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "ver-bucket")
		status, _ := b.GetBucketVersioning("000000000000", "ver-bucket")
		assert.Equal(t, "Suspended", status)
		require.NoError(t, b.PutBucketVersioning("000000000000", "ver-bucket", "Enabled"))
		status2, _ := b.GetBucketVersioning("000000000000", "ver-bucket")
		assert.Equal(t, "Enabled", status2)
	})

	t.Run("bucket lifecycle CRUD", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "lc-bucket")
		require.NoError(
			t,
			b.PutBucketLifecycleConfiguration(
				"000000000000",
				"lc-bucket",
				"<LifecycleConfiguration/>",
			),
		)
		config, err := b.GetBucketLifecycleConfiguration("000000000000", "lc-bucket")
		require.NoError(t, err)
		assert.Contains(t, config, "Lifecycle")
		require.NoError(t, b.DeleteBucketLifecycleConfiguration("000000000000", "lc-bucket"))
	})

	t.Run("list regional buckets", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "b1")
		b.CreateBucket("000000000000", "b2")
		buckets := b.ListRegionalBuckets("000000000000")
		require.Len(t, buckets, 2)
	})
}

func TestHTTP_GetBucket(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateBucket("000000000000", "test-bucket")

	resp := doS3ControlNewOpRequest(
		t,
		h,
		http.MethodGet,
		"/v20180820/bucket/test-bucket",
		"000000000000",
		"",
	)
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestHTTP_ListRegionalBuckets(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateBucket("000000000000", "b1")

	resp := doS3ControlNewOpRequest(t, h, http.MethodGet, "/v20180820/bucket", "000000000000", "")
	assert.Equal(t, http.StatusOK, resp.Code)
}

// ---- Bucket Replication ----

func TestBucketReplication_PutGetDelete(t *testing.T) {
	t.Parallel()

	const accountID = "acct1"
	const bucketName = "mybucket"
	const replicationPath = "/v20180820/bucket/" + bucketName + "/replication"

	t.Run("put and get replication", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		h.Backend.CreateBucket(accountID, bucketName)

		putRec := doS3Request(t, h, http.MethodPut, replicationPath,
			`<ReplicationConfiguration><Rules>my-rule</Rules></ReplicationConfiguration>`)
		require.Equal(t, http.StatusOK, putRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, replicationPath, "")
		require.Equal(t, http.StatusOK, getRec.Code)
		assert.Contains(t, getRec.Body.String(), "my-rule")
	})

	t.Run("get missing returns 404", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		h.Backend.CreateBucket(accountID, bucketName)

		rec := doS3Request(t, h, http.MethodGet, replicationPath, "")
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("delete replication", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		h.Backend.CreateBucket(accountID, bucketName)
		_ = doS3Request(t, h, http.MethodPut, replicationPath,
			`<ReplicationConfiguration><Rules>r</Rules></ReplicationConfiguration>`)

		delRec := doS3Request(t, h, http.MethodDelete, replicationPath, "")
		require.Equal(t, http.StatusNoContent, delRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, replicationPath, "")
		assert.Equal(t, http.StatusNotFound, getRec.Code)
	})
}

func TestBackendBucketReplication(t *testing.T) {
	t.Parallel()

	t.Run("get missing returns error", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		b.CreateBucket("acct1", "bkt")

		_, err := b.GetBucketReplication("acct1", "bkt")
		require.Error(t, err)
	})

	t.Run("delete missing is idempotent", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		err := b.DeleteBucketReplication("acct1", "bkt")
		require.NoError(t, err)
	})
}

func TestBucketReplication_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		rules    string
		wantGet  string
		wantCode int
	}{
		{
			name:     "put_and_get_rules",
			bucket:   "my-bucket",
			rules:    "<Rule><ID>rule1</ID><Status>Enabled</Status></Rule>",
			wantGet:  "rule1",
			wantCode: http.StatusOK,
		},
		{
			name:     "overwrite_rules",
			bucket:   "bucket2",
			rules:    "<Rule><ID>rule2</ID></Rule>",
			wantGet:  "rule2",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			body := `<ReplicationConfiguration>` + tt.rules + `</ReplicationConfiguration>`
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/bucket/"+tt.bucket+"/replication", "000000000000", body)
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/bucket/"+tt.bucket+"/replication", "000000000000", "")
			require.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantGet)
		})
	}
}

func TestBucketReplication_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		preload  bool
		wantCode int
	}{
		{
			name:     "delete_existing",
			bucket:   "my-bucket",
			preload:  true,
			wantCode: http.StatusNoContent,
		},
		{
			name:     "delete_nonexistent_still_204",
			bucket:   "missing-bucket",
			preload:  false,
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			if tt.preload {
				b.PutBucketReplication("000000000000", tt.bucket, "<Rule/>")
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
				"/v20180820/bucket/"+tt.bucket+"/replication", "000000000000", "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBucketReplication_GetMissing(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/bucket/no-such-bucket/replication", "000000000000", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		bucketName       string
		wantBodyContains string
		wantStatus       int
		wantLocationHdr  bool
	}{
		{
			name:             "creates_outposts_bucket",
			accountID:        "123456789012",
			bucketName:       "my-outposts-bucket",
			wantStatus:       http.StatusOK,
			wantBodyContains: "BucketArn",
			wantLocationHdr:  true,
		},
		{
			name:             "creates_outposts_bucket_default_account",
			accountID:        "",
			bucketName:       "test-bucket",
			wantStatus:       http.StatusOK,
			wantBodyContains: "BucketArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			path := "/v20180820/bucket/" + tt.bucketName
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut, path, tt.accountID, "")

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}

			if tt.wantLocationHdr {
				assert.NotEmpty(t, rec.Header().Get("Location"))
			}
		})
	}
}

func TestListRegionalBuckets_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for i := range 4 {
		b.CreateBucket("acct1", fmt.Sprintf("bucket-%d", i))
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/bucket",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/bucket?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName   xml.Name `xml:"ListRegionalBucketsResult"`
				NextToken string   `xml:"NextToken"`
				Buckets   []struct {
					Bucket string `xml:"Bucket"`
				} `xml:"RegionalBucketList>RegionalBucket"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Buckets, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
