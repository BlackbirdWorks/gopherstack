package azurefile_test

import (
	"maps"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_ReservedNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		ok   bool
	}{
		{name: "nul", path: "NUL"},
		{name: "com1 lower", path: "com1"},
		{name: "lpt9", path: "LPT9"},
		{name: "clock", path: "CLOCK$"},
		{name: "reserved dir", path: "con/f"},
		{name: "com10 allowed", path: "com10", ok: true},
		{name: "com0 allowed", path: "com0", ok: true},
		{name: "normal", path: "readme", ok: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			hdr := map[string]string{"x-ms-type": "file", "x-ms-content-length": "1"}
			resp := rawDo(t, http.MethodPut, sc.URL()+"/"+tt.path, "", hdr)

			if tt.ok {
				assert.Equal(t, http.StatusCreated, resp.StatusCode)

				return
			}

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "InvalidFileOrDirectoryPathName", resp.Header.Get("X-Ms-Error-Code"))
		})
	}
}

func TestSDK_TrailingDot(t *testing.T) {
	t.Parallel()

	dots := map[string]string{"x-ms-allow-trailing-dot": "true"}
	tests := []struct {
		createHdr map[string]string
		name      string
		stored    string
		missing   string
		nfs       bool
	}{
		{name: "smb trims", stored: "dots", missing: "dots..."},
		{name: "smb keeps with header", createHdr: dots, stored: "dots...", missing: "dots"},
		{name: "nfs keeps", nfs: true, stored: "dots...", missing: "dots"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newShare(t)
			if tt.nfs {
				sc = newNFSShare(t)
			}

			hdr := map[string]string{"x-ms-type": "file", "x-ms-content-length": "1"}
			maps.Copy(hdr, tt.createHdr)

			resp := rawDo(t, http.MethodPut, sc.URL()+"/dots...", "", hdr)
			require.Equal(t, http.StatusCreated, resp.StatusCode)

			// the stored name is probed with the header that makes the dots literal
			probe := map[string]string{"x-ms-allow-trailing-dot": "true"}
			assert.Equal(t, http.StatusOK, rawDo(t, http.MethodHead, sc.URL()+"/"+tt.stored, "", probe).StatusCode)
			assert.Equal(t, http.StatusNotFound,
				rawDo(t, http.MethodHead, sc.URL()+"/"+tt.missing, "", probe).StatusCode)
		})
	}
}

func TestSDK_LinksRefuseShareSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hdr   map[string]string
		name  string
		query string
	}{
		{name: "hard link", query: "restype=hardlink", hdr: map[string]string{"x-ms-file-target-file": "src"}},
		{name: "symbolic link", query: "restype=symboliclink", hdr: map[string]string{"x-ms-link-text": "src"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc := newNFSShare(t)
			newFile(t, sc, "src", 1)

			snap, err := sc.CreateSnapshot(ctx(t), nil)
			require.NoError(t, err)

			resp := rawDo(t, http.MethodPut, sc.URL()+"/l?"+tt.query+"&sharesnapshot="+*snap.Snapshot, "", tt.hdr)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "InvalidQueryParameterValue", resp.Header.Get("X-Ms-Error-Code"))
		})
	}
}

func TestSDK_PaidBurstingLimitsNeedBursting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, sc *share.Client)
		name  string
		fail  bool
	}{
		{name: "create max iops without enable", fail: true},
		{name: "create max iops with enable", setup: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.Create(ctx(t), &share.CreateOptions{
				PaidBurstingEnabled: new(true), PaidBurstingMaxIops: new(int64(5000)),
			})
			require.NoError(t, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sc := e.svc.NewShareClient("burst")

			if tt.setup != nil {
				tt.setup(t, sc)

				return
			}

			_, err := sc.Create(ctx(t), &share.CreateOptions{PaidBurstingMaxIops: new(int64(5000))})
			if tt.fail {
				requireCode(t, err, fileerror.InvalidHeaderValue)
			}

			_, err = sc.GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.ShareNotFound)
		})
	}

	t.Run("set on bursting share", func(t *testing.T) {
		t.Parallel()

		sc := newEnv(t).svc.NewShareClient("burst")
		_, err := sc.Create(ctx(t), &share.CreateOptions{PaidBurstingEnabled: new(true)})
		require.NoError(t, err)

		_, err = sc.SetProperties(ctx(t), &share.SetPropertiesOptions{PaidBurstingMaxIops: new(int64(7000))})
		require.NoError(t, err)
	})

	t.Run("set on plain share", func(t *testing.T) {
		t.Parallel()

		sc := newEnv(t).svc.NewShareClient("burst")
		_, err := sc.Create(ctx(t), nil)
		require.NoError(t, err)

		_, err = sc.SetProperties(ctx(t), &share.SetPropertiesOptions{PaidBurstingMaxIops: new(int64(7000))})
		requireCode(t, err, fileerror.InvalidHeaderValue)
	})
}
