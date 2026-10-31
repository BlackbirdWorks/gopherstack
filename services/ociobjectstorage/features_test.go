package ociobjectstorage_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

func rawDo(t *testing.T, method, url, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, string(data)
}

func TestRestoreArchive(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := new(e.ns)
	e.mkBucket(t, "arch", nil)

	_, err := e.client.PutObject(ctx, objectstorage.PutObjectRequest{
		NamespaceName: ns, BucketName: new("arch"), ObjectName: new("cold"),
		ContentLength: common.Int64(4), PutObjectBody: io.NopCloser(strings.NewReader("cold")),
		StorageTier: objectstorage.PutObjectStorageTierArchive,
	})
	require.NoError(t, err)

	state := func() objectstorage.HeadObjectArchivalStateEnum {
		h, herr := e.client.HeadObject(
			ctx,
			objectstorage.HeadObjectRequest{
				NamespaceName: ns,
				BucketName:    new("arch"),
				ObjectName:    new("cold"),
			},
		)
		require.NoError(t, herr)

		return h.ArchivalState
	}
	read := func() error {
		_, gerr := e.client.GetObject(
			ctx,
			objectstorage.GetObjectRequest{
				NamespaceName: ns,
				BucketName:    new("arch"),
				ObjectName:    new("cold"),
			},
		)

		return gerr
	}
	restore := func(hours int) error {
		_, rerr := e.client.RestoreObjects(ctx, objectstorage.RestoreObjectsRequest{
			NamespaceName: ns,
			BucketName:    new("arch"),
			ObjectName:    new("cold"),
			Hours:         new(hours),
		})

		return rerr
	}

	assert.Equal(t, objectstorage.HeadObjectArchivalStateArchived, state())
	assert.Equal(t, "NotRestored", svcErr(t, read()).GetCode())
	assert.Equal(t, "InvalidParameter", svcErr(t, restore(500)).GetCode())

	require.NoError(t, restore(2))
	assert.Equal(t, objectstorage.HeadObjectArchivalStateRestoring, state())
	assert.Equal(t, "NotRestored", svcErr(t, read()).GetCode())
	assert.Equal(t, "Conflict", svcErr(t, restore(2)).GetCode())

	e.clock.Advance(time.Hour)
	assert.Equal(t, objectstorage.HeadObjectArchivalStateRestored, state())
	require.NoError(t, read())

	e.clock.Advance(2 * time.Hour)
	assert.Equal(t, objectstorage.HeadObjectArchivalStateArchived, state())

	e.put(t, "arch", "warm", "w")
	assert.Equal(t, "Conflict", svcErr(t, func() error {
		_, rerr := e.client.RestoreObjects(ctx, objectstorage.RestoreObjectsRequest{
			NamespaceName: ns, BucketName: new("arch"),
			ObjectName: new("warm"),
		})

		return rerr
	}()).GetCode())
}

type parFixture struct {
	e       *env
	read    objectstorage.PreauthenticatedRequest
	write   objectstorage.PreauthenticatedRequest
	anyRead objectstorage.PreauthenticatedRequest
}

func newPARFixture(t *testing.T) parFixture {
	t.Helper()

	e := newEnv(t, envOpts{enforce: true})
	e.client = e.clientFor(t, e.keys)
	_, err := e.backend.Auth.Register(testTenancy, testUser, e.keys.pubPEM)
	require.NoError(t, err)

	ctx := t.Context()
	ns := new(e.ns)
	e.mkBucket(t, "par", nil)
	e.put(t, "par", "doc.txt", "secret")
	e.put(t, "par", "other.txt", "other")

	expires := common.SDKTime{Time: e.clock.Now().Add(time.Hour)}
	type (
		accessType = objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeEnum
		listing    = objectstorage.PreauthenticatedRequestBucketListingActionEnum
	)

	mk := func(name string, at accessType, obj string, list listing) objectstorage.PreauthenticatedRequest {
		d := objectstorage.CreatePreauthenticatedRequestDetails{
			Name:                new(name),
			AccessType:          at,
			TimeExpires:         &expires,
			BucketListingAction: list,
		}
		if obj != "" {
			d.ObjectName = new(obj)
		}

		r, cerr := e.client.CreatePreauthenticatedRequest(ctx, objectstorage.CreatePreauthenticatedRequestRequest{
			NamespaceName: ns, BucketName: new("par"), CreatePreauthenticatedRequestDetails: d,
		})
		require.NoError(t, cerr)

		return r.PreauthenticatedRequest
	}

	read := mk("r", objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeObjectread, "doc.txt", "")
	write := mk("w", objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeObjectwrite, "upload.bin", "")
	anyRead := mk(
		"ar",
		objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeAnyobjectread,
		"",
		objectstorage.PreauthenticatedRequestBucketListingActionListobjects,
	)
	assert.True(t, strings.HasPrefix(*read.AccessUri, "/p/"))
	assert.Contains(t, *read.FullPath, e.baseURL)

	return parFixture{e: e, read: read, write: write, anyRead: anyRead}
}

func TestPreauthenticatedRequests(t *testing.T) {
	t.Parallel()

	f := newPARFixture(t)
	e, read, write, anyRead := f.e, f.read, f.write, f.anyRead

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		wantIn string
		want   int
	}{
		{name: "read ok", method: http.MethodGet, path: *read.AccessUri, want: 200, wantIn: "secret"},
		{
			name:   "read wrong object",
			method: http.MethodGet,
			path:   strings.Replace(*read.AccessUri, "doc.txt", "other.txt", 1),
			want:   404,
		},
		{name: "read token cannot write", method: http.MethodPut, path: *read.AccessUri, body: "x", want: 404},
		{name: "write ok", method: http.MethodPut, path: *write.AccessUri, body: "uploaded", want: 200},
		{name: "write token cannot read", method: http.MethodGet, path: *write.AccessUri, want: 404},
		{
			name:   "any read other object",
			method: http.MethodGet,
			path:   *anyRead.AccessUri + "other.txt",
			want:   200,
			wantIn: "other",
		},
		{
			name:   "any read listing",
			method: http.MethodGet,
			path:   strings.TrimSuffix(*anyRead.AccessUri, "/"),
			want:   200,
			wantIn: "doc.txt",
		},
		{name: "garbage token", method: http.MethodGet, path: "/p/nonsense/n/" + e.ns + "/b/par/o/doc.txt", want: 404},
		{name: "unsigned without par", method: http.MethodGet, path: "/n/" + e.ns + "/b/par/o/doc.txt", want: 401},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := rawDo(t, tt.method, e.baseURL+tt.path, tt.body)
			assert.Equal(t, tt.want, code)
			assert.Contains(t, body, tt.wantIn)
		})
	}
}

func TestPARLifecycle(t *testing.T) {
	t.Parallel()

	f := newPARFixture(t)
	e, read, write, anyRead := f.e, f.read, f.write, f.anyRead
	ctx := t.Context()
	ns := new(e.ns)

	code, _ := rawDo(t, http.MethodPut, e.baseURL+*write.AccessUri, "uploaded")
	assert.Equal(t, 200, code)
	code, _ = rawDo(
		t,
		http.MethodGet,
		e.baseURL+strings.TrimSuffix(*write.AccessUri, "upload.bin")+"upload.bin",
		"",
	)
	assert.Equal(t, 404, code)
	assert.Equal(t, "uploaded", e.get(t, "par", "upload.bin"))

	list, lerr := e.client.ListPreauthenticatedRequests(
		ctx,
		objectstorage.ListPreauthenticatedRequestsRequest{NamespaceName: ns, BucketName: new("par")},
	)
	require.NoError(t, lerr)
	assert.Len(t, list.Items, 3)

	_, derr := e.client.DeletePreauthenticatedRequest(
		ctx,
		objectstorage.DeletePreauthenticatedRequestRequest{
			NamespaceName: ns,
			BucketName:    new("par"),
			ParId:         read.Id,
		},
	)
	require.NoError(t, derr)

	code, _ = rawDo(t, http.MethodGet, e.baseURL+*read.AccessUri, "")
	assert.Equal(t, 404, code)

	code, _ = rawDo(t, http.MethodGet, e.baseURL+*anyRead.AccessUri+"other.txt", "")
	assert.Equal(t, 200, code)

	e.clock.Advance(2 * time.Hour)

	code, _ = rawDo(t, http.MethodGet, e.baseURL+*anyRead.AccessUri+"other.txt", "")
	assert.Equal(t, 404, code, "expired PAR")
}

func TestPARValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "pv", nil)

	future := common.SDKTime{Time: e.clock.Now().Add(time.Hour)}
	past := common.SDKTime{Time: e.clock.Now().Add(-time.Hour)}

	tests := []struct {
		name string
		d    objectstorage.CreatePreauthenticatedRequestDetails
		code string
	}{
		{name: "past expiry", code: "InvalidParameter", d: objectstorage.CreatePreauthenticatedRequestDetails{
			Name: new(
				"n",
			),
			AccessType:  objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeAnyobjectread,
			TimeExpires: &past,
		}},
		{
			name: "object type without name",
			code: "MissingParameter",
			d: objectstorage.CreatePreauthenticatedRequestDetails{
				Name: new(
					"n",
				),
				AccessType:  objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeObjectread,
				TimeExpires: &future,
			},
		},
		{
			name: "listing needs any read",
			code: "InvalidParameter",
			d: objectstorage.CreatePreauthenticatedRequestDetails{
				Name: new(
					"n",
				),
				AccessType:          objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeObjectread,
				ObjectName:          new("o"),
				TimeExpires:         &future,
				BucketListingAction: objectstorage.PreauthenticatedRequestBucketListingActionListobjects,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreatePreauthenticatedRequest(
				t.Context(),
				objectstorage.CreatePreauthenticatedRequestRequest{
					NamespaceName: new(
						e.ns,
					),
					BucketName:                           new("pv"),
					CreatePreauthenticatedRequestDetails: tt.d,
				},
			)
			assert.Equal(t, tt.code, svcErr(t, err).GetCode())
		})
	}
}

func TestLifecycleEnforcement(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := new(e.ns)
	e.mkBucket(t, "lc", func(d *objectstorage.CreateBucketDetails) {
		d.Versioning = objectstorage.CreateBucketDetailsVersioningEnabled
	})

	put, err := e.client.PutObjectLifecyclePolicy(ctx, objectstorage.PutObjectLifecyclePolicyRequest{
		NamespaceName: ns,
		BucketName:    new("lc"),
		Items: []objectstorage.ObjectLifecycleRule{
			{
				Name:       new("expire-tmp"),
				Action:     new("DELETE"),
				TimeAmount: common.Int64(1),
				TimeUnit:   objectstorage.ObjectLifecycleRuleTimeUnitDays,
				IsEnabled:  new(true),
				Target: new(
					"objects",
				),
				ObjectNameFilter: &objectstorage.ObjectNameFilter{InclusionPrefixes: []string{"tmp/"}},
			},
			{
				Name:             new("archive-rest"),
				Action:           new("ARCHIVE"),
				TimeAmount:       common.Int64(3),
				TimeUnit:         objectstorage.ObjectLifecycleRuleTimeUnitDays,
				IsEnabled:        new(true),
				ObjectNameFilter: &objectstorage.ObjectNameFilter{ExclusionPatterns: []string{"tmp/*"}},
			},
			{
				Name:       new("old-versions"),
				Action:     new("DELETE"),
				TimeAmount: common.Int64(2),
				TimeUnit:   objectstorage.ObjectLifecycleRuleTimeUnitDays,
				IsEnabled:  new(true),
				Target:     new("previous-object-versions"),
			},
			{
				Name:       new("abort-mp"),
				Action:     new("ABORT"),
				TimeAmount: common.Int64(1),
				TimeUnit:   objectstorage.ObjectLifecycleRuleTimeUnitDays,
				IsEnabled:  new(true),
				Target:     new("multipart-uploads"),
			},
			{
				Name:       new("disabled"),
				Action:     new("DELETE"),
				TimeAmount: common.Int64(1),
				TimeUnit:   objectstorage.ObjectLifecycleRuleTimeUnitDays,
				IsEnabled:  new(false),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, put.ETag)

	got, err := e.client.GetObjectLifecyclePolicy(
		ctx,
		objectstorage.GetObjectLifecyclePolicyRequest{NamespaceName: ns, BucketName: new("lc")},
	)
	require.NoError(t, err)
	assert.Len(t, got.Items, 5)

	e.put(t, "lc", "tmp/scratch", "s")
	e.put(t, "lc", "keep/doc", "v1")
	e.put(t, "lc", "keep/doc", "v2")

	_, err = e.client.CreateMultipartUpload(ctx, objectstorage.CreateMultipartUploadRequest{
		NamespaceName: ns, BucketName: new("lc"),
		Object: new("stale"),
	})
	require.NoError(t, err)

	assert.Equal(t, 0, e.backend.SweepLifecycle())

	e.clock.Advance(25 * time.Hour)
	assert.Equal(t, 2, e.backend.SweepLifecycle(), "tmp object + stale upload")

	uploads, err := e.client.ListMultipartUploads(
		ctx,
		objectstorage.ListMultipartUploadsRequest{NamespaceName: ns, BucketName: new("lc")},
	)
	require.NoError(t, err)
	assert.Empty(t, uploads.Items)

	_, err = e.client.GetObject(
		ctx,
		objectstorage.GetObjectRequest{
			NamespaceName: ns,
			BucketName:    new("lc"),
			ObjectName:    new("tmp/scratch"),
		},
	)
	assert.Equal(t, "ObjectNotFound", svcErr(t, err).GetCode())

	e.clock.Advance(25 * time.Hour)
	e.backend.SweepLifecycle()

	versions, err := e.client.ListObjectVersions(
		ctx,
		objectstorage.ListObjectVersionsRequest{
			NamespaceName: ns,
			BucketName:    new("lc"),
			Prefix:        new("keep/"),
		},
	)
	require.NoError(t, err)
	assert.Len(t, versions.Items, 1, "previous version aged out after 2 days")

	e.clock.Advance(48 * time.Hour)
	e.backend.SweepLifecycle()

	head, err := e.client.HeadObject(
		ctx,
		objectstorage.HeadObjectRequest{
			NamespaceName: ns,
			BucketName:    new("lc"),
			ObjectName:    new("keep/doc"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, objectstorage.HeadObjectArchivalStateArchived, head.ArchivalState)
	assert.Equal(t, objectstorage.HeadObjectStorageTierArchive, head.StorageTier)

	_, err = e.client.DeleteObjectLifecyclePolicy(
		ctx,
		objectstorage.DeleteObjectLifecyclePolicyRequest{NamespaceName: ns, BucketName: new("lc")},
	)
	require.NoError(t, err)

	_, err = e.client.GetObjectLifecyclePolicy(
		ctx,
		objectstorage.GetObjectLifecyclePolicyRequest{NamespaceName: ns, BucketName: new("lc")},
	)
	assert.Equal(t, "LifecyclePolicyNotFound", svcErr(t, err).GetCode())

	_, err = e.client.PutObjectLifecyclePolicy(ctx, objectstorage.PutObjectLifecyclePolicyRequest{
		NamespaceName: ns,
		BucketName:    new("lc"),
		Items: []objectstorage.ObjectLifecycleRule{
			{
				Name:       new("bad"),
				Action:     new("ABORT"),
				TimeAmount: common.Int64(1),
				TimeUnit:   objectstorage.ObjectLifecycleRuleTimeUnitDays,
				IsEnabled:  new(true),
			},
		},
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}

func TestRetentionRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	ns := new(e.ns)
	e.mkBucket(t, "ret", nil)

	rule, err := e.client.CreateRetentionRule(ctx, objectstorage.CreateRetentionRuleRequest{
		NamespaceName: ns, BucketName: new("ret"),
		DisplayName: new("one-day"),
		Duration: &objectstorage.Duration{
			TimeAmount: common.Int64(1),
			TimeUnit:   objectstorage.DurationTimeUnitDays,
		},
	})
	require.NoError(t, err)

	e.put(t, "ret", "k", "v")
	del := func() error {
		_, derr := e.client.DeleteObject(
			ctx,
			objectstorage.DeleteObjectRequest{
				NamespaceName: ns,
				BucketName:    new("ret"),
				ObjectName:    new("k"),
			},
		)

		return derr
	}

	assert.Equal(t, "ObjectUnderRetention", svcErr(t, del()).GetCode())

	_, err = e.client.PutObject(ctx, objectstorage.PutObjectRequest{
		NamespaceName: ns, BucketName: new("ret"), ObjectName: new("k"),
		ContentLength: common.Int64(1), PutObjectBody: io.NopCloser(strings.NewReader("z")),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "overwrite blocked too")

	e.clock.Advance(25 * time.Hour)
	require.NoError(t, del())

	lockSoon := common.SDKTime{Time: e.clock.Now().Add(time.Hour)}
	_, err = e.client.CreateRetentionRule(ctx, objectstorage.CreateRetentionRuleRequest{
		NamespaceName: ns, BucketName: new("ret"),
		TimeRuleLocked: &lockSoon,
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "lock must be 14+ days out")

	lock := common.SDKTime{Time: e.clock.Now().Add(15 * 24 * time.Hour)}
	locked, err := e.client.CreateRetentionRule(ctx, objectstorage.CreateRetentionRuleRequest{
		NamespaceName: ns, BucketName: new("ret"),
		Duration: &objectstorage.Duration{
			TimeAmount: common.Int64(2),
			TimeUnit:   objectstorage.DurationTimeUnitDays,
		},
		TimeRuleLocked: &lock,
	})
	require.NoError(t, err)

	upd, err := e.client.UpdateRetentionRule(ctx, objectstorage.UpdateRetentionRuleRequest{
		NamespaceName: ns, BucketName: new("ret"), RetentionRuleId: rule.Id, IfMatch: rule.Etag,
		DisplayName: new("renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.DisplayName)

	list, err := e.client.ListRetentionRules(
		ctx,
		objectstorage.ListRetentionRulesRequest{NamespaceName: ns, BucketName: new("ret")},
	)
	require.NoError(t, err)
	assert.Len(t, list.Items, 2)

	e.clock.Advance(16 * 24 * time.Hour)

	_, err = e.client.DeleteRetentionRule(
		ctx,
		objectstorage.DeleteRetentionRuleRequest{
			NamespaceName:   ns,
			BucketName:      new("ret"),
			RetentionRuleId: locked.Id,
		},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteRetentionRule(
		ctx,
		objectstorage.DeleteRetentionRuleRequest{
			NamespaceName:   ns,
			BucketName:      new("ret"),
			RetentionRuleId: rule.Id,
		},
	)
	require.NoError(t, err)
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true})
	e.mkBucket(t, "snap", func(d *objectstorage.CreateBucketDetails) {
		d.Versioning = objectstorage.CreateBucketDetailsVersioningEnabled
	})
	e.put(t, "snap", "k", "persisted")

	up, err := e.client.CreateMultipartUpload(t.Context(), objectstorage.CreateMultipartUploadRequest{
		NamespaceName: new(e.ns), BucketName: new("snap"),
		Object: new("pending"),
	})
	require.NoError(t, err)

	data := e.backend.Snapshot(t.Context())

	fresh := oci.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), data))

	o, err := fresh.GetObject(e.ns, "snap", "k", "", oci.Preconds{}, true)
	require.NoError(t, err)
	assert.Equal(t, "persisted", string(o.Data))

	ups, err := fresh.ListUploads(e.ns, "snap", "", 0)
	require.NoError(t, err)
	require.Len(t, ups.Data, 1)
	assert.Equal(t, *up.UploadId, ups.Data[0].UploadID)
	assert.Len(t, fresh.Auth.Records(), 1)

	require.Error(t, fresh.Restore(t.Context(), []byte(`{"buckets":{"x":null},"version":1}`)))
}
