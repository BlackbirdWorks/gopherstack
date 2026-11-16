package azureblob_test

import (
	"bytes"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestAppendBlock_BlockCountLimit(t *testing.T) {
	t.Parallel()

	b := azureblob.NewInMemoryBackend()
	_, err := b.CreateContainerWith("c", azureblob.ContainerOptions{})
	require.NoError(t, err)

	_, err = b.PutBlobWith("c", "log", nil, azureblob.BlobAttrs{BlobType: azureblob.BlobTypeAppend})
	require.NoError(t, err)

	for range azureblob.MaxAppendBlocks {
		_, err = b.AppendBlock("c", "log", []byte("x"), azureblob.AppendOptions{})
		require.NoError(t, err)
	}

	_, err = b.AppendBlock("c", "log", []byte("x"), azureblob.AppendOptions{})
	require.ErrorContains(t, err, "50,000")
}

func TestKindWrites_BodyLimits(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	env := newSDKEnv(t)

	ab := env.cc.NewAppendBlobClient("ap")
	_, err := ab.Create(ctx, nil)
	require.NoError(t, err)

	big := bytes.Repeat([]byte("a"), azureblob.MaxAppendBlockSize+1)
	_, err = ab.AppendBlock(ctx, body(string(big)), nil)
	requireStatus(t, err, http.StatusRequestEntityTooLarge)

	pb := env.cc.NewPageBlobClient("pg")
	_, err = pb.Create(ctx, 16<<20, nil)
	require.NoError(t, err)

	bigPage := bytes.Repeat([]byte("p"), azureblob.MaxPageWrite+azureblob.PageSize)
	_, err = pb.UploadPages(ctx, body(string(bigPage)), blob.HTTPRange{Offset: 0, Count: int64(len(bigPage))}, nil)
	requireStatus(t, err, http.StatusRequestEntityTooLarge)
}

func TestPageBlob_SparseAllocation(t *testing.T) {
	t.Parallel()

	b := azureblob.NewInMemoryBackend()
	_, err := b.CreateContainerWith("c", azureblob.ContainerOptions{})
	require.NoError(t, err)

	var before, after runtime.MemStats

	runtime.ReadMemStats(&before)

	_, err = b.PutBlobWith("c", "pg", nil, azureblob.BlobAttrs{
		BlobType: azureblob.BlobTypePage, PageLength: azureblob.MaxPageBlobLength,
	})
	require.NoError(t, err)

	_, err = b.PutPage("c", "pg", 0, azureblob.PageSize-1, bytes.Repeat([]byte("z"), azureblob.PageSize), false)
	require.NoError(t, err)

	runtime.ReadMemStats(&after)

	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(azureblob.MaxPageBlobLength/8),
		"a sparse page blob must not allocate its full length")
}

func TestPageBlob_RangesMergeAndResize(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	env := newSDKEnv(t)
	pb := env.cc.NewPageBlobClient("disk")

	_, err := pb.Create(ctx, 4096, nil)
	require.NoError(t, err)

	for _, off := range []int64{0, 512, 2048} {
		_, err = pb.UploadPages(ctx, body(string(bytes.Repeat([]byte("a"), azureblob.PageSize))),
			blob.HTTPRange{Offset: off, Count: azureblob.PageSize}, nil)
		require.NoError(t, err)
	}

	ranges := func() [][2]int64 {
		pg, perr := pb.NewGetPageRangesPager(nil).NextPage(ctx)
		require.NoError(t, perr)

		out := make([][2]int64, 0, len(pg.PageList.PageRange))
		for _, r := range pg.PageList.PageRange {
			out = append(out, [2]int64{*r.Start, *r.End})
		}

		return out
	}

	assert.Equal(t, [][2]int64{{0, 1023}, {2048, 2559}}, ranges(), "adjacent pages merge")

	_, err = pb.Resize(ctx, 2048, nil)
	require.NoError(t, err)
	_, err = pb.Resize(ctx, 4096, nil)
	require.NoError(t, err)

	assert.Equal(t, [][2]int64{{0, 1023}}, ranges(), "pages beyond a shrunk size are gone")
}

func TestPageBlob_CreateNeedsAlignedLength(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)

	_, err := env.cc.NewPageBlobClient("odd").Create(t.Context(), 1000, nil)
	requireStorageErr(t, err, bloberror.InvalidHeaderValue, http.StatusBadRequest)
}

func TestSoftDelete_RemainingDaysRoundUp(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	env := newSDKEnv(t)

	_, err := env.svc.ServiceClient().SetProperties(ctx, &service.SetPropertiesOptions{
		DeleteRetentionPolicy: &service.RetentionPolicy{Enabled: new(true), Days: new(int32(7))},
	})
	require.NoError(t, err)

	_, err = env.cc.NewBlockBlobClient("gone").UploadBuffer(ctx, []byte("d"), nil)
	require.NoError(t, err)
	_, err = env.cc.NewBlobClient("gone").Delete(ctx, nil)
	require.NoError(t, err)

	env.clock.Advance(25 * time.Hour)

	page, err := env.cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Deleted: true},
	}).NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, page.Segment.BlobItems, 1)
	assert.EqualValues(t, 6, *page.Segment.BlobItems[0].Properties.RemainingRetentionDays)
}

func TestSoftDelete_RestoredBlobIsNotLeased(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	env := newSDKEnv(t)

	_, err := env.svc.ServiceClient().SetProperties(ctx, &service.SetPropertiesOptions{
		DeleteRetentionPolicy: &service.RetentionPolicy{Enabled: new(true), Days: new(int32(7))},
	})
	require.NoError(t, err)

	bb := env.cc.NewBlockBlobClient("leased")
	_, err = bb.UploadBuffer(ctx, []byte("d"), nil)
	require.NoError(t, err)

	lc, err := lease.NewBlobClient(bb, nil)
	require.NoError(t, err)

	acq, err := lc.AcquireLease(ctx, -1, nil)
	require.NoError(t, err)

	_, err = bb.Delete(ctx, &blob.DeleteOptions{
		AccessConditions: &blob.AccessConditions{
			LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: acq.LeaseID},
		},
	})
	require.NoError(t, err)
	_, err = bb.Undelete(ctx, nil)
	require.NoError(t, err)

	_, err = bb.UploadBuffer(ctx, []byte("again"), nil)
	require.NoError(t, err, "a restored blob carries no lease")
}
