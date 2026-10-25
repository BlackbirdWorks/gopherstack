package ociobjectstorage_test

import (
	"crypto/md5" //nolint:gosec // matches the OCI md5 header
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := common.String(e.ns)
	e.mkBucket(t, "objs", nil)

	body := "hello oci"
	sum := md5.Sum([]byte(body)) //nolint:gosec // see import
	wantMD5 := base64.StdEncoding.EncodeToString(sum[:])

	put, err := e.client.PutObject(ctx, objectstorage.PutObjectRequest{
		NamespaceName: ns, BucketName: common.String("objs"), ObjectName: common.String("dir/a b.txt"),
		ContentLength: common.Int64(int64(len(body))), PutObjectBody: io.NopCloser(strings.NewReader(body)),
		ContentType: common.String("text/plain"), ContentMD5: common.String(wantMD5),
		OpcMeta: map[string]string{"color": "blue"},
	})
	require.NoError(t, err)
	assert.Equal(t, wantMD5, *put.OpcContentMd5)
	require.NotNil(t, put.ETag)
	require.NotNil(t, put.VersionId)

	head, err := e.client.HeadObject(ctx, objectstorage.HeadObjectRequest{
		NamespaceName: ns, BucketName: common.String("objs"), ObjectName: common.String("dir/a b.txt"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, len(body), *head.ContentLength)
	assert.Equal(t, "text/plain", *head.ContentType)
	assert.Equal(t, wantMD5, *head.ContentMd5)
	assert.Equal(t, "blue", head.OpcMeta["color"])
	assert.Equal(t, *put.ETag, *head.ETag)

	assert.Equal(t, body, e.get(t, "objs", "dir/a b.txt"))

	tests := []struct {
		name   string
		rng    string
		want   string
		status int
	}{
		{name: "prefix", rng: "bytes=0-4", want: "hello"},
		{name: "open end", rng: "bytes=6-", want: "oci"},
		{name: "suffix", rng: "bytes=-3", want: "oci"},
		{name: "unsatisfiable", rng: "bytes=100-200", status: http.StatusRequestedRangeNotSatisfiable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, gerr := e.client.GetObject(ctx, objectstorage.GetObjectRequest{
				NamespaceName: ns, BucketName: common.String("objs"), ObjectName: common.String("dir/a b.txt"),
				Range: common.String(tt.rng),
			})
			if tt.status != 0 {
				assert.Equal(t, tt.status, svcErr(t, gerr).GetHTTPStatusCode())

				return
			}

			require.NoError(t, gerr)

			data, _ := io.ReadAll(resp.Content)
			assert.Equal(t, tt.want, string(data))
			assert.NotEmpty(t, *resp.ContentRange)
		})
	}
}

func TestObjectConditionalsAndErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := common.String(e.ns)
	e.mkBucket(t, "cond", nil)
	put := e.put(t, "cond", "k", "v1")

	tests := []struct {
		call   func() error
		name   string
		code   string
		status int
	}{
		{name: "get missing", status: 404, code: "ObjectNotFound", call: func() error {
			_, err := e.client.GetObject(ctx, objectstorage.GetObjectRequest{NamespaceName: ns, BucketName: common.String("cond"), ObjectName: common.String("zz")})

			return err
		}},
		{name: "missing bucket", status: 404, code: "BucketNotFound", call: func() error {
			_, err := e.client.GetObject(ctx, objectstorage.GetObjectRequest{NamespaceName: ns, BucketName: common.String("nb"), ObjectName: common.String("zz")})

			return err
		}},
		{name: "if-match fail", status: 412, code: "NoEtagMatch", call: func() error {
			_, err := e.client.GetObject(ctx, objectstorage.GetObjectRequest{NamespaceName: ns, BucketName: common.String("cond"), ObjectName: common.String("k"), IfMatch: common.String("nope")})

			return err
		}},
		{name: "if-none-match star on existing", status: 412, code: "IfNoneMatchFailed", call: func() error {
			_, err := e.client.PutObject(ctx, objectstorage.PutObjectRequest{
				NamespaceName: ns, BucketName: common.String("cond"), ObjectName: common.String("k"),
				ContentLength: common.Int64(1), PutObjectBody: io.NopCloser(strings.NewReader("x")), IfNoneMatch: common.String("*"),
			})

			return err
		}},
		{name: "bad content md5", status: 400, code: "InvalidDigest", call: func() error {
			_, err := e.client.PutObject(ctx, objectstorage.PutObjectRequest{
				NamespaceName: ns, BucketName: common.String("cond"), ObjectName: common.String("m"),
				ContentLength: common.Int64(1), PutObjectBody: io.NopCloser(strings.NewReader("x")), ContentMD5: common.String("AAAAAAAAAAAAAAAAAAAAAA=="),
			})

			return err
		}},
		{name: "delete missing", status: 404, code: "ObjectNotFound", call: func() error {
			_, err := e.client.DeleteObject(ctx, objectstorage.DeleteObjectRequest{NamespaceName: ns, BucketName: common.String("cond"), ObjectName: common.String("zz")})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := svcErr(t, tt.call())
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}

	resp, err := e.client.GetObject(ctx, objectstorage.GetObjectRequest{
		NamespaceName: ns, BucketName: common.String("cond"), ObjectName: common.String("k"), IfNoneMatch: put.ETag,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotModified, resp.RawResponse.StatusCode)
}

func TestListObjects(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := common.String(e.ns)
	e.mkBucket(t, "lst", nil)

	for _, n := range []string{"a/1", "a/2", "a/sub/3", "b/1", "c", "d"} {
		e.put(t, "lst", n, "data-"+n)
	}

	tests := []struct {
		name       string
		prefix     string
		delimiter  string
		start      string
		end        string
		limit      int
		wantNames  []string
		wantPrefix []string
		wantNext   string
	}{
		{name: "all", wantNames: []string{"a/1", "a/2", "a/sub/3", "b/1", "c", "d"}},
		{name: "prefix", prefix: "a/", wantNames: []string{"a/1", "a/2", "a/sub/3"}},
		{name: "delimiter", delimiter: "/", wantNames: []string{"c", "d"}, wantPrefix: []string{"a/", "b/"}},
		{name: "prefix delimiter", prefix: "a/", delimiter: "/", wantNames: []string{"a/1", "a/2"}, wantPrefix: []string{"a/sub/"}},
		{name: "start end", start: "a/2", end: "c", wantNames: []string{"a/2", "a/sub/3", "b/1"}},
		{name: "limit", limit: 2, wantNames: []string{"a/1", "a/2"}, wantNext: "a/sub/3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := objectstorage.ListObjectsRequest{NamespaceName: ns, BucketName: common.String("lst"), Fields: common.String("name,size,md5,etag,timeCreated,storageTier")}
			if tt.prefix != "" {
				req.Prefix = common.String(tt.prefix)
			}

			if tt.delimiter != "" {
				req.Delimiter = common.String(tt.delimiter)
			}

			if tt.start != "" {
				req.Start = common.String(tt.start)
			}

			if tt.end != "" {
				req.End = common.String(tt.end)
			}

			if tt.limit > 0 {
				req.Limit = common.Int(tt.limit)
			}

			resp, err := e.client.ListObjects(ctx, req)
			require.NoError(t, err)

			var names []string

			for _, o := range resp.Objects {
				names = append(names, *o.Name)
				assert.NotNil(t, o.Size)
				assert.NotNil(t, o.Md5)
				assert.Equal(t, objectstorage.StorageTierStandard, o.StorageTier)
			}

			assert.Equal(t, tt.wantNames, names)
			assert.Equal(t, tt.wantPrefix, resp.Prefixes)

			if tt.wantNext == "" {
				assert.Nil(t, resp.NextStartWith)
			} else {
				assert.Equal(t, tt.wantNext, *resp.NextStartWith)
			}
		})
	}
}

func TestMultipart(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := common.String(e.ns)
	e.mkBucket(t, "mp", nil)

	up, err := e.client.CreateMultipartUpload(ctx, objectstorage.CreateMultipartUploadRequest{
		NamespaceName: ns, BucketName: common.String("mp"),
		CreateMultipartUploadDetails: objectstorage.CreateMultipartUploadDetails{
			Object: common.String("big"), ContentType: common.String("text/x-big"), Metadata: map[string]string{"k": "v"},
		},
	})
	require.NoError(t, err)

	parts := []string{"AAA", "BBBB", "C"}
	etags := make([]string, len(parts))

	for i := len(parts) - 1; i >= 0; i-- {
		r, perr := e.client.UploadPart(ctx, objectstorage.UploadPartRequest{
			NamespaceName: ns, BucketName: common.String("mp"), ObjectName: common.String("big"),
			UploadId: up.UploadId, UploadPartNum: common.Int(i + 1), ContentLength: common.Int64(int64(len(parts[i]))),
			UploadPartBody: io.NopCloser(strings.NewReader(parts[i])),
		})
		require.NoError(t, perr)

		etags[i] = *r.ETag
	}

	uploads, err := e.client.ListMultipartUploads(ctx, objectstorage.ListMultipartUploadsRequest{NamespaceName: ns, BucketName: common.String("mp")})
	require.NoError(t, err)
	require.Len(t, uploads.Items, 1)

	listed, err := e.client.ListMultipartUploadParts(ctx, objectstorage.ListMultipartUploadPartsRequest{
		NamespaceName: ns, BucketName: common.String("mp"), ObjectName: common.String("big"), UploadId: up.UploadId,
	})
	require.NoError(t, err)
	require.Len(t, listed.Items, 3)
	assert.Equal(t, 1, *listed.Items[0].PartNumber)
	assert.EqualValues(t, 4, *listed.Items[1].Size)

	var commit []objectstorage.CommitMultipartUploadPartDetails
	for i := range parts {
		commit = append(commit, objectstorage.CommitMultipartUploadPartDetails{PartNum: common.Int(i + 1), Etag: common.String(etags[i])})
	}

	bad, err := e.client.CommitMultipartUpload(ctx, objectstorage.CommitMultipartUploadRequest{
		NamespaceName: ns, BucketName: common.String("mp"), ObjectName: common.String("big"), UploadId: up.UploadId,
		CommitMultipartUploadDetails: objectstorage.CommitMultipartUploadDetails{PartsToCommit: []objectstorage.CommitMultipartUploadPartDetails{
			{PartNum: common.Int(1), Etag: common.String("wrong")},
		}},
	})
	require.Error(t, err)
	assert.Equal(t, "InvalidUploadPart", svcErr(t, err).GetCode())
	_ = bad

	done, err := e.client.CommitMultipartUpload(ctx, objectstorage.CommitMultipartUploadRequest{
		NamespaceName: ns, BucketName: common.String("mp"), ObjectName: common.String("big"), UploadId: up.UploadId,
		CommitMultipartUploadDetails: objectstorage.CommitMultipartUploadDetails{PartsToCommit: commit},
	})
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(*done.OpcMultipartMd5, "-3"))

	assert.Equal(t, "AAABBBBC", e.get(t, "mp", "big"))

	head, err := e.client.HeadObject(ctx, objectstorage.HeadObjectRequest{NamespaceName: ns, BucketName: common.String("mp"), ObjectName: common.String("big")})
	require.NoError(t, err)
	assert.Equal(t, *done.OpcMultipartMd5, *head.OpcMultipartMd5)
	assert.Equal(t, "v", head.OpcMeta["k"])
	assert.Equal(t, "text/x-big", *head.ContentType)

	_, err = e.client.CommitMultipartUpload(ctx, objectstorage.CommitMultipartUploadRequest{
		NamespaceName: ns, BucketName: common.String("mp"), ObjectName: common.String("big"), UploadId: up.UploadId,
		CommitMultipartUploadDetails: objectstorage.CommitMultipartUploadDetails{PartsToCommit: commit},
	})
	assert.Equal(t, "NoSuchUpload", svcErr(t, err).GetCode())

	up2, err := e.client.CreateMultipartUpload(ctx, objectstorage.CreateMultipartUploadRequest{
		NamespaceName: ns, BucketName: common.String("mp"),
		CreateMultipartUploadDetails: objectstorage.CreateMultipartUploadDetails{Object: common.String("gone")},
	})
	require.NoError(t, err)

	_, err = e.client.AbortMultipartUpload(ctx, objectstorage.AbortMultipartUploadRequest{
		NamespaceName: ns, BucketName: common.String("mp"), ObjectName: common.String("gone"), UploadId: up2.UploadId,
	})
	require.NoError(t, err)

	uploads, err = e.client.ListMultipartUploads(ctx, objectstorage.ListMultipartUploadsRequest{NamespaceName: ns, BucketName: common.String("mp")})
	require.NoError(t, err)
	assert.Empty(t, uploads.Items)
}

func TestCopyRenameBatchDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := common.String(e.ns)
	e.mkBucket(t, "src", nil)
	e.mkBucket(t, "dst", nil)
	e.put(t, "src", "orig", "payload")

	cp, err := e.client.CopyObject(ctx, objectstorage.CopyObjectRequest{
		NamespaceName: ns, BucketName: common.String("src"),
		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName: common.String("orig"), DestinationRegion: common.String("us-ashburn-1"),
			DestinationNamespace: ns, DestinationBucket: common.String("dst"), DestinationObjectName: common.String("copy"),
			DestinationObjectMetadata: map[string]string{"copied": "yes"},
		},
	})
	require.NoError(t, err)

	wr, err := e.client.GetWorkRequest(ctx, objectstorage.GetWorkRequestRequest{WorkRequestId: cp.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, objectstorage.WorkRequestStatusCompleted, wr.Status)
	assert.Equal(t, objectstorage.WorkRequestOperationTypeCopyObject, wr.OperationType)

	wrs, err := e.client.ListWorkRequests(ctx, objectstorage.ListWorkRequestsRequest{CompartmentId: common.String(testCompartment)})
	require.NoError(t, err)
	assert.Len(t, wrs.Items, 1)

	assert.Equal(t, "payload", e.get(t, "dst", "copy"))

	head, err := e.client.HeadObject(ctx, objectstorage.HeadObjectRequest{NamespaceName: ns, BucketName: common.String("dst"), ObjectName: common.String("copy")})
	require.NoError(t, err)
	assert.Equal(t, "yes", head.OpcMeta["copied"])

	_, err = e.client.RenameObject(ctx, objectstorage.RenameObjectRequest{
		NamespaceName: ns, BucketName: common.String("src"),
		RenameObjectDetails: objectstorage.RenameObjectDetails{SourceName: common.String("orig"), NewName: common.String("renamed")},
	})
	require.NoError(t, err)
	assert.Equal(t, "payload", e.get(t, "src", "renamed"))

	_, err = e.client.HeadObject(ctx, objectstorage.HeadObjectRequest{NamespaceName: ns, BucketName: common.String("src"), ObjectName: common.String("orig")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	e.put(t, "src", "x", "1")

	del, err := e.client.BatchDeleteObjects(ctx, objectstorage.BatchDeleteObjectsRequest{
		NamespaceName: ns, BucketName: common.String("src"),
		BatchDeleteObjectsDetails: objectstorage.BatchDeleteObjectsDetails{Objects: []objectstorage.BatchDeleteObjectIdentifier{
			{ObjectName: common.String("x")}, {ObjectName: common.String("renamed")}, {ObjectName: common.String("absent")},
		}},
	})
	require.NoError(t, err)
	assert.Len(t, del.Deleted, 2)
	require.Len(t, del.Failed, 1)
	assert.Equal(t, 404, *del.Failed[0].StatusCode)
}

func TestVersioning(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := common.String(e.ns)
	e.mkBucket(t, "ver", func(d *objectstorage.CreateBucketDetails) {
		d.Versioning = objectstorage.CreateBucketDetailsVersioningEnabled
	})

	v1 := e.put(t, "ver", "k", "one")
	v2 := e.put(t, "ver", "k", "two")
	assert.NotEqual(t, *v1.VersionId, *v2.VersionId)

	del, err := e.client.DeleteObject(ctx, objectstorage.DeleteObjectRequest{NamespaceName: ns, BucketName: common.String("ver"), ObjectName: common.String("k")})
	require.NoError(t, err)
	assert.True(t, *del.IsDeleteMarker)

	_, err = e.client.GetObject(ctx, objectstorage.GetObjectRequest{NamespaceName: ns, BucketName: common.String("ver"), ObjectName: common.String("k")})
	assert.Equal(t, "ObjectNotFound", svcErr(t, err).GetCode())

	old, err := e.client.GetObject(ctx, objectstorage.GetObjectRequest{
		NamespaceName: ns, BucketName: common.String("ver"), ObjectName: common.String("k"), VersionId: v1.VersionId,
	})
	require.NoError(t, err)

	data, _ := io.ReadAll(old.Content)
	assert.Equal(t, "one", string(data))

	versions, err := e.client.ListObjectVersions(ctx, objectstorage.ListObjectVersionsRequest{NamespaceName: ns, BucketName: common.String("ver")})
	require.NoError(t, err)
	require.Len(t, versions.Items, 3)
	assert.True(t, *versions.Items[0].IsDeleteMarker)

	list, err := e.client.ListObjects(ctx, objectstorage.ListObjectsRequest{NamespaceName: ns, BucketName: common.String("ver")})
	require.NoError(t, err)
	assert.Empty(t, list.Objects)

	for _, v := range versions.Items {
		_, err = e.client.DeleteObject(ctx, objectstorage.DeleteObjectRequest{
			NamespaceName: ns, BucketName: common.String("ver"), ObjectName: common.String("k"), VersionId: v.VersionId,
		})
		require.NoError(t, err)
	}

	_, err = e.client.DeleteBucket(ctx, objectstorage.DeleteBucketRequest{NamespaceName: ns, BucketName: common.String("ver")})
	require.NoError(t, err)
}
