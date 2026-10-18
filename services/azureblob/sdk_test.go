package azureblob_test

import (
	"bytes"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

const sdkAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func newSDKContainer(t *testing.T, name string) *container.Client {
	t.Helper()

	e := echo.New()
	e.Any("/*", azureblob.NewHandler(azureblob.NewInMemoryBackend()).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cred, err := azblob.NewSharedKeyCredential(testAccount, sdkAccountKey)
	require.NoError(t, err)

	cc, err := container.NewClientWithSharedKeyCredential(srv.URL+"/"+testAccount+"/"+name, cred, nil)
	require.NoError(t, err)

	return cc
}

func TestSDK_ContainerMetadataAndAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		access container.PublicAccessType
	}{
		{name: "blob", access: container.PublicAccessTypeBlob},
		{name: "container", access: container.PublicAccessTypeContainer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			cc := newSDKContainer(t, "sdkc")

			_, err := cc.Create(ctx, &container.CreateOptions{
				Access:   &tt.access,
				Metadata: map[string]*string{"env": new("dev")},
			})
			require.NoError(t, err)

			props, err := cc.GetProperties(ctx, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.access, *props.BlobPublicAccess)
			assert.Equal(t, "dev", *props.Metadata["Env"])

			_, err = cc.SetMetadata(ctx, &container.SetMetadataOptions{
				Metadata: map[string]*string{"team": new("x")},
			})
			require.NoError(t, err)

			props, err = cc.GetProperties(ctx, nil)
			require.NoError(t, err)
			assert.Len(t, props.Metadata, 1)
			assert.Equal(t, "x", *props.Metadata["Team"])

			_, err = cc.SetAccessPolicy(ctx, &container.SetAccessPolicyOptions{
				ContainerACL: []*container.SignedIdentifier{{
					ID: new("reader"),
					AccessPolicy: &container.AccessPolicy{
						Permission: new("r"),
						Expiry:     new(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
					},
				}},
			})
			require.NoError(t, err)

			acl, err := cc.GetAccessPolicy(ctx, nil)
			require.NoError(t, err)
			assert.Nil(t, acl.BlobPublicAccess, "set ACL without access header resets to private")
			require.Len(t, acl.SignedIdentifiers, 1)
			assert.Equal(t, "reader", *acl.SignedIdentifiers[0].ID)
			assert.Equal(t, "r", *acl.SignedIdentifiers[0].AccessPolicy.Permission)
		})
	}
}

func TestSDK_BlobMetadataPropertiesTierSnapshots(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cc := newSDKContainer(t, "sdkb")
	_, err := cc.Create(ctx, nil)
	require.NoError(t, err)

	bb := cc.NewBlockBlobClient("dir/file.txt")
	_, err = bb.UploadBuffer(ctx, []byte("v1"), &blockblob.UploadBufferOptions{
		Metadata: map[string]*string{"k": new("v")},
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: new("text/plain"), BlobCacheControl: new("no-cache"), BlobContentLanguage: new("en"),
		},
		AccessTier: new(blob.AccessTierCool),
	})
	require.NoError(t, err)

	bc := cc.NewBlobClient("dir/file.txt")

	props, err := bc.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "v", *props.Metadata["K"])
	assert.Equal(t, "text/plain", *props.ContentType)
	assert.Equal(t, "no-cache", *props.CacheControl)
	assert.Equal(t, "en", *props.ContentLanguage)
	assert.Equal(t, "Cool", *props.AccessTier)

	_, err = bc.SetMetadata(ctx, map[string]*string{"a": new("1"), "b": new("2")}, nil)
	require.NoError(t, err)

	_, err = bc.SetHTTPHeaders(ctx, blob.HTTPHeaders{BlobContentType: new("application/json")}, nil)
	require.NoError(t, err)

	props, err = bc.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, props.Metadata, 2)
	assert.Equal(t, "application/json", *props.ContentType)
	assert.Nil(t, props.CacheControl, "unspecified properties are cleared")

	_, err = bc.SetTier(ctx, blob.AccessTierArchive, nil)
	require.NoError(t, err)

	_, err = bc.DownloadStream(ctx, nil)
	require.Error(t, err, "archived blob cannot be read")

	_, err = bc.SetTier(ctx, blob.AccessTierHot, nil)
	require.NoError(t, err)

	snap, err := bc.CreateSnapshot(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, snap.Snapshot)

	_, err = bb.UploadBuffer(ctx, []byte("version2"), nil)
	require.NoError(t, err)

	snapClient, err := bc.WithSnapshot(*snap.Snapshot)
	require.NoError(t, err)

	dl, err := snapClient.DownloadStream(ctx, nil)
	require.NoError(t, err)

	body, err := io.ReadAll(dl.Body)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), body)

	var seen []string

	pager := cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Snapshots: true, Metadata: true},
	})
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, item := range page.Segment.BlobItems {
			seen = append(seen, *item.Name+"|"+ptrconv.String(item.Snapshot))
		}
	}

	assert.Equal(t, []string{"dir/file.txt|" + *snap.Snapshot, "dir/file.txt|"}, seen)

	_, err = bc.Delete(ctx, nil)
	require.Error(t, err, "snapshots present")

	_, err = snapClient.Delete(ctx, nil)
	require.NoError(t, err)

	_, err = bc.Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: new(blob.DeleteSnapshotsOptionTypeInclude)})
	require.NoError(t, err)
}

func TestSDK_ListHierarchyPaging(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cc := newSDKContainer(t, "sdkh")
	_, err := cc.Create(ctx, nil)
	require.NoError(t, err)

	for _, name := range []string{"a.txt", "logs/2026/a", "logs/2026/b", "logs/2027/c", "logs/root", "z.txt"} {
		_, err = cc.NewBlockBlobClient(name).UploadBuffer(ctx, bytes.Repeat([]byte("x"), 3), nil)
		require.NoError(t, err)
	}

	tests := []struct {
		name         string
		prefix       string
		wantBlobs    []string
		wantPrefixes []string
		maxResults   int32
	}{
		{
			name: "root", prefix: "", maxResults: 2, wantBlobs: []string{"a.txt", "z.txt"},
			wantPrefixes: []string{"logs/"},
		},
		{
			name: "logs", prefix: "logs/", maxResults: 1, wantBlobs: []string{"logs/root"},
			wantPrefixes: []string{"logs/2026/", "logs/2027/"},
		},
		{name: "deep", prefix: "logs/2026/", maxResults: 5, wantBlobs: []string{"logs/2026/a", "logs/2026/b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var blobs, prefixes []string

			pages := 0
			pager := cc.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
				Prefix: &tt.prefix, MaxResults: &tt.maxResults,
			})

			for pager.More() {
				page, pageErr := pager.NextPage(ctx)
				require.NoError(t, pageErr)

				pages++

				for _, b := range page.Segment.BlobItems {
					blobs = append(blobs, *b.Name)
				}

				for _, p := range page.Segment.BlobPrefixes {
					prefixes = append(prefixes, *p.Name)
				}
			}

			assert.Equal(t, tt.wantBlobs, blobs)
			assert.Equal(t, tt.wantPrefixes, prefixes)

			if tt.name != "deep" {
				assert.Greater(t, pages, 1, "results span several pages")
			}
		})
	}
}
