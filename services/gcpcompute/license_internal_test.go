package gcpcompute

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	computeapi "google.golang.org/api/compute/v1"
)

func TestLicenseDeleteBlockedByUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hold func(b *InMemoryBackend, lic string)
		name string
	}{
		{func(b *InMemoryBackend, lic string) {
			b.disks["p/z/d"] = &computeapi.Disk{
				SelfLink: "https://x/compute/v1/projects/p/zones/z/disks/d",
				Licenses: []string{lic},
			}
		}, "disk"},
		{func(b *InMemoryBackend, lic string) {
			b.images["p/i"] = &computeapi.Image{
				SelfLink: "https://x/compute/v1/projects/p/global/images/i",
				Licenses: []string{lic},
			}
		}, "image"},
		{func(b *InMemoryBackend, lic string) {
			b.snapshots["p/s"] = &computeapi.Snapshot{
				SelfLink: "https://x/compute/v1/projects/p/global/snapshots/s",
				Licenses: []string{lic},
			}
		}, "snapshot"},
		{func(b *InMemoryBackend, lic string) {
			b.instances["p/z/i"] = &instRec{Inst: &computeapi.Instance{
				SelfLink: "https://x/compute/v1/projects/p/zones/z/instances/i",
				Disks:    []*computeapi.AttachedDisk{{Licenses: []string{lic}}},
			}}
		}, "instance disk"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			_, err := lbInsert(b, licenseSpec(), lbCtx{project: "p", scope: scopeGlobal},
				&computeapi.License{Name: "lic"}, map[string]any{})
			require.NoError(t, err)

			self := globalLink("p", collLicenses, "lic")
			tt.hold(b, "projects/p/global/licenses/lic")

			_, err = lbDelete(b, licenseSpec(), lbCtx{project: "p", scope: scopeGlobal}, "lic", nil)

			var apiErr *APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.Equal(t, "resourceInUseByAnotherResource", apiErr.Reason)
			assert.Contains(t, apiErr.Error(), self[len(apiBase):])
		})
	}
}
