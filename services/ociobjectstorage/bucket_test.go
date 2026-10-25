package ociobjectstorage_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespace(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	resp, err := e.client.GetNamespace(t.Context(), objectstorage.GetNamespaceRequest{})
	require.NoError(t, err)
	assert.Equal(t, e.ns, *resp.Value)

	meta, err := e.client.UpdateNamespaceMetadata(t.Context(), objectstorage.UpdateNamespaceMetadataRequest{
		NamespaceName: common.String(e.ns),
		UpdateNamespaceMetadataDetails: objectstorage.UpdateNamespaceMetadataDetails{
			DefaultS3CompartmentId: common.String(testCompartment),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, testCompartment, *meta.DefaultS3CompartmentId)

	_, err = e.client.GetNamespaceMetadata(t.Context(), objectstorage.GetNamespaceMetadataRequest{NamespaceName: common.String("other")})
	assert.Equal(t, "NamespaceNotFound", svcErr(t, err).GetCode())
}

func TestBucketCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := common.String(e.ns)

	e.mkBucket(t, "alpha", func(d *objectstorage.CreateBucketDetails) {
		d.PublicAccessType = objectstorage.CreateBucketDetailsPublicAccessTypeObjectread
		d.FreeformTags = map[string]string{"env": "test"}
		d.ObjectEventsEnabled = common.Bool(true)
		d.Versioning = objectstorage.CreateBucketDetailsVersioningEnabled
	})
	e.mkBucket(t, "beta", nil)

	got, err := e.client.GetBucket(ctx, objectstorage.GetBucketRequest{
		NamespaceName: ns, BucketName: common.String("alpha"),
		Fields: []objectstorage.GetBucketFieldsEnum{objectstorage.GetBucketFieldsApproximatecount},
	})
	require.NoError(t, err)
	assert.Equal(t, objectstorage.BucketPublicAccessTypeObjectread, got.PublicAccessType)
	assert.Equal(t, objectstorage.BucketStorageTierStandard, got.StorageTier)
	assert.Equal(t, objectstorage.BucketVersioningEnabled, got.Versioning)
	assert.Equal(t, "test", got.FreeformTags["env"])
	assert.True(t, *got.ObjectEventsEnabled)
	assert.EqualValues(t, 0, *got.ApproximateCount)
	assert.Contains(t, *got.Id, "ocid1.bucket.oc1.")
	require.NotNil(t, got.Etag)

	list, err := e.client.ListBuckets(ctx, objectstorage.ListBucketsRequest{
		NamespaceName: ns, CompartmentId: common.String(testCompartment), Limit: common.Int(1),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "alpha", *list.Items[0].Name)
	require.NotNil(t, list.OpcNextPage)

	page2, err := e.client.ListBuckets(ctx, objectstorage.ListBucketsRequest{
		NamespaceName: ns, CompartmentId: common.String(testCompartment), Page: list.OpcNextPage,
	})
	require.NoError(t, err)
	require.Len(t, page2.Items, 1)
	assert.Equal(t, "beta", *page2.Items[0].Name)

	upd, err := e.client.UpdateBucket(ctx, objectstorage.UpdateBucketRequest{
		NamespaceName: ns, BucketName: common.String("alpha"), IfMatch: got.Etag,
		UpdateBucketDetails: objectstorage.UpdateBucketDetails{
			PublicAccessType: objectstorage.UpdateBucketDetailsPublicAccessTypeNopublicaccess,
			Versioning:       objectstorage.UpdateBucketDetailsVersioningSuspended,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, objectstorage.BucketPublicAccessTypeNopublicaccess, upd.PublicAccessType)
	assert.Equal(t, objectstorage.BucketVersioningSuspended, upd.Versioning)
	assert.NotEqual(t, *got.Etag, *upd.Etag)

	_, err = e.client.HeadBucket(ctx, objectstorage.HeadBucketRequest{NamespaceName: ns, BucketName: common.String("alpha")})
	require.NoError(t, err)
}

func TestBucketErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := common.String(e.ns)
	e.mkBucket(t, "exists", nil)
	e.put(t, "exists", "obj", "x")

	tests := []struct {
		call   func() error
		name   string
		code   string
		status int
	}{
		{name: "duplicate", status: http.StatusConflict, code: "BucketAlreadyExists", call: func() error {
			_, err := e.client.CreateBucket(ctx, objectstorage.CreateBucketRequest{NamespaceName: ns,
				CreateBucketDetails: objectstorage.CreateBucketDetails{Name: common.String("exists"), CompartmentId: common.String(testCompartment)}})

			return err
		}},
		{name: "missing compartment", status: http.StatusBadRequest, code: "MissingParameter", call: func() error {
			_, err := e.client.CreateBucket(ctx, objectstorage.CreateBucketRequest{NamespaceName: ns,
				CreateBucketDetails: objectstorage.CreateBucketDetails{Name: common.String("nocomp")}})

			return err
		}},
		{name: "bad name", status: http.StatusBadRequest, code: "InvalidParameter", call: func() error {
			_, err := e.client.CreateBucket(ctx, objectstorage.CreateBucketRequest{NamespaceName: ns,
				CreateBucketDetails: objectstorage.CreateBucketDetails{Name: common.String("a b"), CompartmentId: common.String(testCompartment)}})

			return err
		}},
		{name: "get missing", status: http.StatusNotFound, code: "BucketNotFound", call: func() error {
			_, err := e.client.GetBucket(ctx, objectstorage.GetBucketRequest{NamespaceName: ns, BucketName: common.String("nope")})

			return err
		}},
		{name: "wrong etag", status: http.StatusPreconditionFailed, code: "NoEtagMatch", call: func() error {
			_, err := e.client.DeleteBucket(ctx, objectstorage.DeleteBucketRequest{
				NamespaceName: ns, BucketName: common.String("exists"), IfMatch: common.String("bogus")})

			return err
		}},
		{name: "not empty", status: http.StatusConflict, code: "BucketNotEmpty", call: func() error {
			_, err := e.client.DeleteBucket(ctx, objectstorage.DeleteBucketRequest{NamespaceName: ns, BucketName: common.String("exists")})

			return err
		}},
		{name: "wrong namespace", status: http.StatusNotFound, code: "NamespaceNotFound", call: func() error {
			_, err := e.client.GetBucket(ctx, objectstorage.GetBucketRequest{NamespaceName: common.String("zzz"), BucketName: common.String("exists")})

			return err
		}},
	}

	t.Run("cases", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				se := svcErr(t, tt.call())
				assert.Equal(t, tt.status, se.GetHTTPStatusCode())
				assert.Equal(t, tt.code, se.GetCode())
				assert.NotEmpty(t, se.GetOpcRequestID())
			})
		}
	})

	_, err := e.client.DeleteObject(ctx, objectstorage.DeleteObjectRequest{NamespaceName: ns, BucketName: common.String("exists"), ObjectName: common.String("obj")})
	require.NoError(t, err)
	_, err = e.client.DeleteBucket(ctx, objectstorage.DeleteBucketRequest{NamespaceName: ns, BucketName: common.String("exists")})
	require.NoError(t, err)
}
