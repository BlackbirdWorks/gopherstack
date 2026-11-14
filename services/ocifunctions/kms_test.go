package ocifunctions_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/require"
)

func vaultKeys(id string) (string, bool) {
	switch id {
	case "ocid1.key.oc1..enabled":
		return "ENABLED", true
	case "ocid1.key.oc1..disabled":
		return "DISABLED", true
	}

	return "", false
}

func TestApplicationImagePolicyKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		key        string
		wantCode   string
		wantStatus int
		unwired    bool
	}{
		{name: "enabled", key: "ocid1.key.oc1..enabled"},
		{
			name:       "missing",
			key:        "ocid1.key.oc1..none",
			wantStatus: http.StatusNotFound,
			wantCode:   "NotAuthorizedOrNotFound",
		},
		{name: "disabled", key: "ocid1.key.oc1..disabled", wantStatus: http.StatusConflict, wantCode: "IncorrectState"},
		{name: "unwired", key: "ocid1.key.oc1..none", unwired: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			if !tt.unwired {
				e.backend.SetKeyChecker(vaultKeys)
			}

			policy := &functions.ImagePolicyConfig{
				IsPolicyEnabled: new(true), KeyDetails: []functions.KeyDetails{{KmsKeyId: new(tt.key)}},
			}

			_, err := e.mgmt.CreateApplication(t.Context(), functions.CreateApplicationRequest{
				CompartmentId: new(testCompartment), DisplayName: new("app"), SubnetIds: []string{testSubnet},
				ImagePolicyConfig: policy,
			})
			if tt.wantStatus != 0 {
				se := svcErr(t, err)
				require.Equal(t, tt.wantStatus, se.GetHTTPStatusCode())
				require.Equal(t, tt.wantCode, se.GetCode())

				return
			}

			require.NoError(t, err)
		})
	}
}
