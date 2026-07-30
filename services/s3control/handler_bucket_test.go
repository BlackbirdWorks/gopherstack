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

	// "delete bucket cascade cleans state" locks in the ghost-map-row fix:
	// DeleteBucket previously only removed the bucket row itself, leaving
	// its lifecycle, policy, tagging, versioning, replication, and generic
	// resource tags behind forever -- resurfacing on a delete/recreate
	// cycle under the same name.
	t.Run("delete bucket cascade cleans state", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		bkt := b.CreateBucket("000000000000", "cascade-bucket")
		require.NoError(t, b.PutBucketPolicy("000000000000", "cascade-bucket", `{"p":1}`))
		require.NoError(t, b.PutBucketTagging("000000000000", "cascade-bucket", s3control.TagSet{"env": "prod"}))
		require.NoError(t, b.PutBucketLifecycleConfiguration("000000000000", "cascade-bucket", "<Lifecycle/>"))
		require.NoError(t, b.PutBucketVersioning("000000000000", "cascade-bucket", "Enabled"))
		require.NoError(t, b.PutBucketReplication("000000000000", "cascade-bucket", "<Replication/>"))
		b.TagResource(bkt.BucketArn, map[string]string{"team": "infra"})

		require.NoError(t, b.DeleteBucket("000000000000", "cascade-bucket"))

		b.CreateBucket("000000000000", "cascade-bucket")

		policy, err := b.GetBucketPolicy("000000000000", "cascade-bucket")
		require.NoError(t, err)
		assert.Empty(t, policy, "policy must not survive delete")

		tags, err := b.GetBucketTagging("000000000000", "cascade-bucket")
		require.NoError(t, err)
		assert.Empty(t, tags, "tagging must not survive delete")

		lc, err := b.GetBucketLifecycleConfiguration("000000000000", "cascade-bucket")
		require.NoError(t, err)
		assert.Empty(t, lc, "lifecycle must not survive delete")

		v, err := b.GetBucketVersioning("000000000000", "cascade-bucket")
		require.NoError(t, err)
		assert.Equal(t, "Suspended", v, "versioning must reset, not survive delete")

		_, err = b.GetBucketReplication("000000000000", "cascade-bucket")
		require.Error(t, err, "replication must not survive delete")

		assert.Empty(t, b.ListTagsForResource(bkt.BucketArn), "generic tags must not survive delete")
	})
}

// TestHTTP_GetBucket locks in a gopherstack-tir4 finding: GetBucketOutput
// has no BucketArn or OutpostId field in the real SDK (confirmed against
// aws-sdk-go-v2/service/s3control's GetBucketOutput, whose only members are
// Bucket/CreationDate/PublicAccessBlockEnabled). A previous version of this
// handler fabricated a BucketArn element and mislabeled an internal HTTP
// Location-header path fragment as OutpostId.
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
	assert.NotContains(t, resp.Body.String(), "BucketArn")
	assert.NotContains(t, resp.Body.String(), "OutpostId")

	var out struct {
		XMLName xml.Name `xml:"GetBucketResult"`
		Bucket  string   `xml:"Bucket"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &out))
	assert.Equal(t, "test-bucket", out.Bucket)
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

// TestBucketTagging_WireShape locks in two gopherstack-tir4 findings for
// PutBucketTagging/GetBucketTagging:
//
//  1. PutBucketTaggingInput's Tagging field is "payload"-bound in the real
//     SDK: the ENTIRE request body root is "<Tagging>", with no
//     "<PutBucketTaggingRequest>" wrapper (confirmed via
//     awsRestxml_serializeOpPutBucketTaggingRequest). A previous version of
//     this handler expected an extra wrapper level, which would reject
//     every real aws-sdk-go-v2 client's request outright (root-element
//     mismatch).
//  2. TagSet (the shared S3TagSet type) serializes entries as "<member>",
//     not "<Tag>" -- confirmed via awsRestxml_serializeDocumentS3TagSet,
//     the same type job tagging uses (see handler_jobs.go).
func TestBucketTagging_WireShape(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateBucket("acct1", "tag-bucket")
	path := "/v20180820/bucket/tag-bucket/tagging"

	putBody := `<Tagging><TagSet><member><Key>env</Key><Value>prod</Value></member></TagSet></Tagging>`
	putRec := doS3Request(t, h, http.MethodPut, path, putBody)
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doS3Request(t, h, http.MethodGet, path, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	body := getRec.Body.String()
	assert.Contains(t, body, "<TagSet><member>")
	assert.NotContains(t, body, "<Tag>")

	var out struct {
		XMLName xml.Name `xml:"GetBucketTaggingResult"`
		Tags    []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"TagSet>member"`
	}
	require.NoError(t, xml.Unmarshal(getRec.Body.Bytes(), &out))
	require.Len(t, out.Tags, 1)
	assert.Equal(t, "env", out.Tags[0].Key)
	assert.Equal(t, "prod", out.Tags[0].Value)
}

// TestBucketVersioning_WireShape locks in a gopherstack-tir4 finding:
// PutBucketVersioningInput's VersioningConfiguration field is
// "payload"-bound in the real SDK: the ENTIRE request body root is
// "<VersioningConfiguration>" with Status as its direct child, with no
// "<PutBucketVersioningRequest>" wrapper and no extra
// "<VersioningConfiguration>" nesting level (confirmed via
// awsRestxml_serializeOpPutBucketVersioningRequest). A previous version of
// this handler expected
// "<PutBucketVersioningRequest><VersioningConfiguration><Status>", which a
// real aws-sdk-go-v2 client's request would never match (root-element
// mismatch), rejecting every real PutBucketVersioning call outright.
func TestBucketVersioning_WireShape(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateBucket("acct1", "ver-bucket")
	path := "/v20180820/bucket/ver-bucket/versioning"

	putBody := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	putRec := doS3Request(t, h, http.MethodPut, path, putBody)
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doS3Request(t, h, http.MethodGet, path, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		XMLName xml.Name `xml:"GetBucketVersioningResult"`
		Status  string   `xml:"Status"`
	}
	require.NoError(t, xml.Unmarshal(getRec.Body.Bytes(), &out))
	assert.Equal(t, "Enabled", out.Status)
}
