package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestWireOCICore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compartment func(idH *ociidentitybackend.Handler) string
		wantOK      bool
	}{
		{"tenancy is accepted", func(*ociidentitybackend.Handler) string { return ociidentitybackend.TenancyID }, true},
		{"unknown compartment is rejected", func(*ociidentitybackend.Handler) string { return "ocid1.compartment.oc1..ghost" }, false},
		{"identity compartment is accepted", func(h *ociidentitybackend.Handler) string {
			c, _, err := h.Backend.CreateCompartment(ociidentitybackend.CreateCompartmentIn{
				CompartmentID: ociidentitybackend.TenancyID, Name: "net", Description: "d",
			})
			require.NoError(t, err)

			return c.ID
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idH := ociidentitybackend.NewHandler(ociidentitybackend.NewInMemoryBackend())
			coreH := ocicorebackend.NewHandler(ocicorebackend.NewInMemoryBackend())

			wireOCIIdentityKeys(map[string]service.Registerable{"OCIIdentity": idH, "OCICore": coreH})

			_, _, aerr := coreH.Backend.CreateVcn(ocicorebackend.CreateVcnIn{
				CompartmentID: tt.compartment(idH), CidrBlocks: []string{"10.0.0.0/16"},
			})
			assert.Equal(t, tt.wantOK, aerr == nil)
		})
	}
}
