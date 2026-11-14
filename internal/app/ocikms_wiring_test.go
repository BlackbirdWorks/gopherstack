package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocifilestoragebackend "github.com/blackbirdworks/gopherstack/services/ocifilestorage"
	ocifunctionsbackend "github.com/blackbirdworks/gopherstack/services/ocifunctions"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func ociPost(t *testing.T, h echo.HandlerFunc, path, body string) int {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	if err := h(e.NewContext(req, rec)); err != nil {
		e.HTTPErrorHandler(e.NewContext(req, rec), err)
	}

	return rec.Code
}

func TestWireOCIKeyLookups(t *testing.T) {
	t.Parallel()

	const ghost = "ocid1.key.oc1.iad.ghost"

	fileBody := `{"availabilityDomain":"Abcd:US-ASHBURN-AD-1","compartmentId":"ocid1.compartment.oc1..x",` +
		`"kmsKeyId":"` + ghost + `"}`
	appBody := `{"compartmentId":"ocid1.compartment.oc1..x","displayName":"app","subnetIds":["ocid1.subnet.oc1..s"],` +
		`"imagePolicyConfig":{"isPolicyEnabled":true,"keyDetails":[{"kmsKeyId":"` + ghost + `"}]}}`

	tests := []struct {
		name     string
		wire     bool
		wantFile int
		wantApp  int
	}{
		{name: "unwired accepts any key", wantFile: http.StatusOK, wantApp: http.StatusOK},
		{name: "wired rejects unknown key", wire: true, wantFile: http.StatusNotFound, wantApp: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fsH := ocifilestoragebackend.NewHandler(ocifilestoragebackend.NewInMemoryBackend())
			fnH := ocifunctionsbackend.NewHandler(ocifunctionsbackend.NewInMemoryBackend())
			vaultH := ocivaultbackend.NewHandler(ocivaultbackend.NewInMemoryBackend())

			if tt.wire {
				wireOCIKeyLookups(map[string]service.Registerable{
					"OCIFileStorage": fsH, "OCIFunctions": fnH, "OCIVault": vaultH,
				})
			}

			assert.Equal(t, tt.wantFile, ociPost(t, fsH.Trusted(), "/20171215/fileSystems", fileBody))
			assert.Equal(t, tt.wantApp, ociPost(t, fnH.Handler(), "/20181201/applications", appBody))
		})
	}
}
