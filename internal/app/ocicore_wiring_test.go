package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func TestWireOCICoreVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantErr bool
		wire    bool
	}{
		{"unwired accepts any key", "ocid1.key.oc1.iad.ghost", false, false},
		{"wired rejects unknown key", "ocid1.key.oc1.iad.ghost", true, true},
		{"wired accepts no key", "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			coreH := ocicorebackend.NewHandler(ocicorebackend.NewInMemoryBackend())
			vaultH := ocivaultbackend.NewHandler(ocivaultbackend.NewInMemoryBackend())

			if tt.wire {
				wireOCICoreVault(map[string]service.Registerable{"OCICore": coreH, "OCIVault": vaultH})
			}

			_, _, aerr := coreH.Backend.CreateVolume(ocicorebackend.CreateVolumeIn{
				CompartmentID: "ocid1.compartment.oc1..x", AvailabilityDomain: "Abcd:US-ASHBURN-AD-1", KmsKeyID: tt.key,
			})

			if tt.wantErr {
				require.NotNil(t, aerr)
				assert.Equal(t, "NotAuthorizedOrNotFound", aerr.Code)

				return
			}

			assert.Nil(t, aerr)
		})
	}
}
