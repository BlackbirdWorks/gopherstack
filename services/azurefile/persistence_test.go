package azurefile_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/lease"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.setShareRetention(t, 4)

	sc := src.svc.NewShareClient("persisted")
	_, err := sc.Create(ctx(t), nil)
	require.NoError(t, err)

	dir := sc.NewDirectoryClient("d")
	_, err = dir.Create(ctx(t), nil)
	require.NoError(t, err)

	fc := dir.NewFileClient("f.bin")
	_, err = fc.Create(ctx(t), 1<<30, &file.CreateOptions{Metadata: map[string]*string{"K": new("v")}})
	require.NoError(t, err)
	upload(t, fc, 100, "persist-me")

	snap, err := sc.CreateSnapshot(ctx(t), nil)
	require.NoError(t, err)

	lc, err := lease.NewFileClient(fc, nil)
	require.NoError(t, err)
	_, err = lc.Acquire(ctx(t), nil)
	require.NoError(t, err)

	gone := src.svc.NewShareClient("gone")
	_, err = gone.Create(ctx(t), nil)
	require.NoError(t, err)
	_, err = gone.Delete(ctx(t), nil)
	require.NoError(t, err)

	data := src.h.Snapshot(ctx(t))
	require.NotEmpty(t, data)

	dst := newEnv(t)
	require.NoError(t, dst.h.Restore(ctx(t), data))

	rfc := dst.svc.NewShareClient("persisted").NewDirectoryClient("d").NewFileClient("f.bin")

	props, err := rfc.GetProperties(ctx(t), nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1<<30), *props.ContentLength)
	assert.Equal(t, "v", md(props.Metadata, "k"))
	assert.Equal(t, lease.StateTypeLeased, *props.LeaseState)

	got, err := rfc.DownloadStream(ctx(t), &file.DownloadStreamOptions{Range: file.HTTPRange{Offset: 100, Count: 10}})
	require.NoError(t, err)

	defer got.Body.Close()

	buf := make([]byte, 10)
	n, _ := got.Body.Read(buf)
	assert.Equal(t, "persist-me", string(buf[:n]))

	snapShare, err := dst.svc.NewShareClient("persisted").WithSnapshot(*snap.Snapshot)
	require.NoError(t, err)
	_, err = snapShare.GetProperties(ctx(t), nil)
	require.NoError(t, err)

	assert.Equal(
		t,
		[]string{"gone@deleted", "persisted", "persisted@snap"},
		listNames(
			t,
			dst,
			&service.ListSharesOptions{Include: service.ListSharesInclude{Deleted: true, Snapshots: true}},
		),
	)
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
		wantLen int
	}{
		{name: "garbage", data: `not json`, wantErr: true},
		{name: "null share", data: `{"version":1,"accounts":{"a":{"shares":{"s":null}}}}`, wantErr: true},
		{name: "null account", data: `{"version":1,"accounts":{"a":null}}`, wantErr: true},
		{name: "other version starts empty", data: `{"version":99,"accounts":{"a":{"shares":{}}}}`},
		{name: "empty accounts", data: `{"version":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.svc.NewShareClient("before").Create(ctx(t), nil)
			require.NoError(t, err)

			err = e.h.Restore(ctx(t), []byte(tt.data))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, listNames(t, e, nil))
		})
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.svc.NewShareClient("wipeme").Create(ctx(t), nil)
	require.NoError(t, err)

	e.h.Reset()
	assert.Empty(t, listNames(t, e, nil))
}
