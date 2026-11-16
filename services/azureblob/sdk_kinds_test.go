package azureblob_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestSDK_AppendBlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opts    *appendblob.AppendBlockOptions
		name    string
		wantErr bloberror.Code
		status  int
	}{
		{name: "plain", opts: nil},
		{name: "append position ok", opts: &appendblob.AppendBlockOptions{
			AppendPositionAccessConditions: &appendblob.AppendPositionAccessConditions{AppendPosition: new(int64(5))},
		}},
		{
			name: "append position mismatch",
			opts: &appendblob.AppendBlockOptions{
				AppendPositionAccessConditions: &appendblob.AppendPositionAccessConditions{
					AppendPosition: new(int64(1)),
				},
			},
			wantErr: bloberror.AppendPositionConditionNotMet, status: http.StatusPreconditionFailed,
		},
		{
			name: "max size exceeded",
			opts: &appendblob.AppendBlockOptions{
				AppendPositionAccessConditions: &appendblob.AppendPositionAccessConditions{MaxSize: new(int64(7))},
			},
			wantErr: bloberror.MaxBlobSizeConditionNotMet, status: http.StatusPreconditionFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			env := newSDKEnv(t)
			ab := env.cc.NewAppendBlobClient("log.txt")

			_, err := ab.Create(ctx, nil)
			require.NoError(t, err)

			first, err := ab.AppendBlock(ctx, body("hello"), nil)
			require.NoError(t, err)
			assert.Equal(t, "0", *first.BlobAppendOffset)
			assert.EqualValues(t, 1, *first.BlobCommittedBlockCount)

			res, err := ab.AppendBlock(ctx, body("-tail"), tt.opts)
			if tt.wantErr != "" {
				requireStorageErr(t, err, tt.wantErr, tt.status)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "5", *res.BlobAppendOffset)
			assert.EqualValues(t, 2, *res.BlobCommittedBlockCount)

			dl, err := ab.DownloadStream(ctx, nil)
			require.NoError(t, err)

			got, err := io.ReadAll(dl.Body)
			require.NoError(t, err)
			assert.Equal(t, "hello-tail", string(got))

			props, err := ab.GetProperties(ctx, nil)
			require.NoError(t, err)
			assert.Equal(t, "AppendBlob", string(*props.BlobType))
			assert.EqualValues(t, 2, *props.BlobCommittedBlockCount)
		})
	}
}

func TestSDK_AppendBlob_WrongType(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	ctx := t.Context()

	_, err := env.cc.NewBlockBlobClient("blk").UploadBuffer(ctx, []byte("x"), nil)
	require.NoError(t, err)

	_, err = env.cc.NewAppendBlobClient("blk").AppendBlock(ctx, body("y"), nil)
	requireStorageErr(t, err, bloberror.InvalidBlobType, http.StatusConflict)
}

func TestSDK_PageBlob(t *testing.T) {
	t.Parallel()

	page := bytes.Repeat([]byte{'a'}, 512)

	tests := []struct {
		name    string
		wantErr bloberror.Code
		offset  int64
		count   int64
	}{
		{name: "aligned", offset: 512, count: 512},
		{name: "misaligned", offset: 100, count: 512, wantErr: bloberror.InvalidPageRange},
		{name: "past end", offset: 1536, count: 1024, wantErr: bloberror.InvalidPageRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			env := newSDKEnv(t)
			pb := env.cc.NewPageBlobClient("disk.vhd")

			_, err := pb.Create(ctx, 2048, nil)
			require.NoError(t, err)

			_, err = pb.UploadPages(ctx, body(string(bytes.Repeat([]byte{'a'}, int(tt.count)))),
				blob.HTTPRange{Offset: tt.offset, Count: tt.count}, nil)
			if tt.wantErr != "" {
				requireStorageErr(t, err, tt.wantErr, http.StatusRequestedRangeNotSatisfiable)

				return
			}

			require.NoError(t, err)

			props, err := pb.GetProperties(ctx, nil)
			require.NoError(t, err)
			assert.Equal(t, "PageBlob", string(*props.BlobType))
			assert.EqualValues(t, 2048, *props.ContentLength)

			pager := pb.NewGetPageRangesPager(nil)
			pg, err := pager.NextPage(ctx)
			require.NoError(t, err)
			require.Len(t, pg.PageList.PageRange, 1)
			assert.EqualValues(t, 512, *pg.PageList.PageRange[0].Start)
			assert.EqualValues(t, 1023, *pg.PageList.PageRange[0].End)

			dl, err := pb.DownloadStream(ctx, nil)
			require.NoError(t, err)

			got, err := io.ReadAll(dl.Body)
			require.NoError(t, err)
			require.Len(t, got, 2048)
			assert.Equal(t, make([]byte, 512), got[:512])
			assert.Equal(t, page, got[512:1024])

			_, err = pb.ClearPages(ctx, blob.HTTPRange{Offset: 512, Count: 512}, nil)
			require.NoError(t, err)

			pg, err = pb.NewGetPageRangesPager(nil).NextPage(ctx)
			require.NoError(t, err)
			assert.Empty(t, pg.PageList.PageRange)

			_, err = pb.Resize(ctx, 4096, nil)
			require.NoError(t, err)

			props, err = pb.GetProperties(ctx, nil)
			require.NoError(t, err)
			assert.EqualValues(t, 4096, *props.ContentLength)
		})
	}
}

func TestSDK_BlobServiceProperties(t *testing.T) {
	t.Parallel()

	days := func(n int32) *int32 { return &n }

	tests := []struct {
		days    *int32
		name    string
		wantErr bool
	}{
		{name: "valid", days: days(9)},
		{name: "zero days", days: days(0), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			env := newSDKEnv(t)

			_, err := env.svc.ServiceClient().SetProperties(ctx, &service.SetPropertiesOptions{
				DeleteRetentionPolicy: &service.RetentionPolicy{Enabled: new(true), Days: tt.days},
			})
			if tt.wantErr {
				requireStatus(t, err, http.StatusBadRequest)

				return
			}

			require.NoError(t, err)

			got, err := env.svc.ServiceClient().GetProperties(ctx, nil)
			require.NoError(t, err)
			require.NotNil(t, got.DeleteRetentionPolicy)
			assert.True(t, *got.DeleteRetentionPolicy.Enabled)
			assert.EqualValues(t, 9, *got.DeleteRetentionPolicy.Days)
			require.NotNil(t, got.HourMetrics, "omitted elements keep their defaults")
			assert.Equal(t, "1.0", *got.HourMetrics.Version)
		})
	}
}

func TestSDK_RehydrationPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		priority blob.RehydratePriority
		early    bool
	}{
		{name: "high done in an hour", priority: blob.RehydratePriorityHigh, early: true},
		{name: "standard pending after an hour", priority: blob.RehydratePriorityStandard},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			env := newSDKEnv(t)
			bb := env.cc.NewBlockBlobClient("cold")

			_, err := bb.UploadBuffer(ctx, []byte("x"), nil)
			require.NoError(t, err)

			bc := env.cc.NewBlobClient("cold")

			_, err = bc.SetTier(ctx, blob.AccessTierArchive, nil)
			require.NoError(t, err)

			_, err = bc.SetTier(ctx, blob.AccessTierCool, &blob.SetTierOptions{RehydratePriority: &tt.priority})
			require.NoError(t, err)

			env.clock.Advance(azureblob.RehydrateHighAfter)

			props, err := bc.GetProperties(ctx, nil)
			require.NoError(t, err)

			if tt.early {
				assert.Equal(t, "Cool", *props.AccessTier)
				assert.Nil(t, props.ArchiveStatus)

				return
			}

			assert.Equal(t, "Archive", *props.AccessTier)
			assert.Equal(t, "rehydrate-pending-to-cool", *props.ArchiveStatus)

			env.clock.Advance(azureblob.RehydrateStandardAfter)

			props, err = bc.GetProperties(ctx, nil)
			require.NoError(t, err)
			assert.Equal(t, "Cool", *props.AccessTier)
		})
	}
}

func TestSDK_CopyProperties(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	env := newSDKEnv(t)

	_, err := env.cc.NewBlockBlobClient("src").UploadBuffer(ctx, []byte("payload"), nil)
	require.NoError(t, err)

	dst := env.cc.NewBlobClient("dst")
	cp, err := dst.StartCopyFromURL(ctx, env.url+"/sdkenv/src?sig=secret", nil)
	require.NoError(t, err)
	assert.Equal(t, "success", string(*cp.CopyStatus))

	props, err := dst.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, *cp.CopyID, *props.CopyID)
	assert.Equal(t, "success", string(*props.CopyStatus))
	assert.Equal(t, env.url+"/sdkenv/src", *props.CopySource, "the SAS is not recorded")
	assert.Equal(t, "7/7", *props.CopyProgress)
	assert.NotNil(t, props.CopyCompletionTime)

	_, err = dst.AbortCopyFromURL(ctx, *cp.CopyID, nil)
	requireStorageErr(t, err, bloberror.NoPendingCopyOperation, http.StatusConflict)

	_, err = dst.SetHTTPHeaders(ctx, blob.HTTPHeaders{BlobContentType: new("text/plain")}, nil)
	require.NoError(t, err)

	props, err = dst.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, props.CopyID, "copy properties are removed once the blob is modified")
}

func TestSDK_CopyFromOtherAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		handler http.HandlerFunc
		url     func(srv *httptest.Server) string
		name    string
		status  int
		wantErr bool
	}{
		{
			name: "fetched",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/csv")
				w.Header().Set("X-Ms-Meta-Origin", "other")
				_, _ = w.Write([]byte("a,b"))
			},
			url: func(srv *httptest.Server) string { return srv.URL + "/otheracct/c/b?sv=1&sig=zz" },
		},
		{
			name:    "source missing",
			handler: func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) },
			url:     func(srv *httptest.Server) string { return srv.URL + "/otheracct/c/b" },
			wantErr: true, status: http.StatusNotFound,
		},
		{
			name: "redirect not followed",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "/elsewhere", http.StatusFound)
			},
			url:     func(srv *httptest.Server) string { return srv.URL + "/otheracct/c/b" },
			wantErr: true, status: http.StatusNotFound,
		},
		{
			name:    "non loopback refused",
			handler: func(http.ResponseWriter, *http.Request) {},
			url:     func(*httptest.Server) string { return "http://192.0.2.1:9/otheracct/c/b" },
			wantErr: true, status: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			env := newSDKEnv(t)
			srv := httptest.NewServer(tt.handler)
			t.Cleanup(srv.Close)

			dst := env.cc.NewBlobClient("copied")

			_, err := dst.StartCopyFromURL(ctx, tt.url(srv), nil)
			if tt.wantErr {
				requireStorageErr(t, err, bloberror.CannotVerifyCopySource, tt.status)

				return
			}

			require.NoError(t, err)

			dl, err := dst.DownloadStream(ctx, nil)
			require.NoError(t, err)

			got, err := io.ReadAll(dl.Body)
			require.NoError(t, err)
			assert.Equal(t, "a,b", string(got))
			assert.Equal(t, "text/csv", *dl.ContentType)
			assert.Equal(t, "other", *dl.Metadata["Origin"])
			assert.Equal(t, srv.URL+"/otheracct/c/b", *dl.CopySource, "query is not recorded")
		})
	}
}

func TestKindsPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	env := newSDKEnv(t)

	_, err := env.cc.NewPageBlobClient("p").Create(ctx, 1024, nil)
	require.NoError(t, err)

	_, err = env.cc.NewPageBlobClient("p").UploadPages(ctx, body(string(bytes.Repeat([]byte{'z'}, 512))),
		blob.HTTPRange{Offset: 512, Count: 512}, nil)
	require.NoError(t, err)

	_, err = env.cc.NewAppendBlobClient("a").Create(ctx, nil)
	require.NoError(t, err)

	_, err = env.cc.NewAppendBlobClient("a").AppendBlock(ctx, body("abc"), nil)
	require.NoError(t, err)

	_, err = env.cc.NewBlockBlobClient("b").UploadBuffer(ctx, []byte("x"), nil)
	require.NoError(t, err)

	_, err = env.cc.NewBlobClient("b").SetTier(ctx, blob.AccessTierArchive, nil)
	require.NoError(t, err)

	_, err = env.cc.NewBlobClient("b").SetTier(ctx, blob.AccessTierHot, nil)
	require.NoError(t, err)

	_, err = env.cc.NewBlobClient("c").StartCopyFromURL(ctx, env.url+"/sdkenv/a", nil)
	require.NoError(t, err)

	require.NoError(
		t,
		env.backend.SetServiceProperties(azureblob.ServiceProperties{DefaultServiceVersion: "2020-10-02"}),
	)

	restored := newRestored(t, env, env.backend.Snapshot(ctx))

	_, data, err := restored.GetBlobAt("sdkenv", "p", "")
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{'z'}, 512), data[512:])

	info, err := restored.HeadBlobAt("sdkenv", "a", "")
	require.NoError(t, err)
	assert.Equal(t, 1, info.CommittedBlocks)

	info, err = restored.HeadBlobAt("sdkenv", "b", "")
	require.NoError(t, err)
	assert.Equal(t, "rehydrate-pending-to-hot", info.ArchiveStatus)

	info, err = restored.HeadBlobAt("sdkenv", "c", "")
	require.NoError(t, err)
	assert.Equal(t, "success", info.CopyStatus)
	assert.Equal(t, "2020-10-02", restored.GetServiceProperties().DefaultServiceVersion)
}

func TestSDK_SoftDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		advance   time.Duration
		retention int32
		restored  bool
		listed    bool
	}{
		{name: "undelete within retention", retention: 7, advance: 24 * time.Hour, restored: true, listed: true},
		{name: "expired after retention", retention: 7, advance: 8 * 24 * time.Hour},
		{name: "policy disabled", retention: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			env := newSDKEnv(t)

			if tt.retention > 0 {
				_, err := env.svc.ServiceClient().SetProperties(ctx, &service.SetPropertiesOptions{
					DeleteRetentionPolicy: &service.RetentionPolicy{Enabled: new(true), Days: new(tt.retention)},
				})
				require.NoError(t, err)
			}

			bc := env.cc.NewBlobClient("gone")

			_, err := env.cc.NewBlockBlobClient("gone").UploadBuffer(ctx, []byte("data"), nil)
			require.NoError(t, err)

			_, err = bc.Delete(ctx, nil)
			require.NoError(t, err)

			_, err = bc.GetProperties(ctx, nil)
			requireStorageErr(t, err, bloberror.BlobNotFound, http.StatusNotFound)

			env.clock.Advance(tt.advance)

			pager := env.cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
				Include: container.ListBlobsInclude{Deleted: true},
			})
			page, err := pager.NextPage(ctx)
			require.NoError(t, err)

			if tt.listed {
				require.Len(t, page.Segment.BlobItems, 1)
				assert.True(t, *page.Segment.BlobItems[0].Deleted)
				assert.EqualValues(t, 6, *page.Segment.BlobItems[0].Properties.RemainingRetentionDays)
			} else {
				assert.Empty(t, page.Segment.BlobItems)
			}

			_, err = bc.Undelete(ctx, nil)
			if !tt.restored {
				requireStorageErr(t, err, bloberror.BlobNotFound, http.StatusNotFound)

				return
			}

			require.NoError(t, err)

			props, err := bc.GetProperties(ctx, nil)
			require.NoError(t, err)
			assert.EqualValues(t, 4, *props.ContentLength)
		})
	}
}
