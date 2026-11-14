package ocifunctions_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func TestTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		free   map[string]string
		defd   map[string]map[string]any
		name   string
		code   string
		status int
	}{
		{
			name: "valid", free: map[string]string{"a": "b"},
			defd: map[string]map[string]any{"Ops": {"Cost": "1"}}, status: http.StatusOK,
		},
		{
			name: "long value", free: map[string]string{"a": strings.Repeat("x", 257)},
			status: http.StatusBadRequest, code: "InvalidParameter",
		},
		{
			name: "unknown namespace", defd: map[string]map[string]any{"Nope": {"k": "v"}},
			status: http.StatusNotFound, code: "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetTagLookup(func(ns, key string) (ocitags.Def, bool) {
				return ocitags.Def{}, strings.EqualFold(ns, "ops") && strings.EqualFold(key, "cost")
			})

			app := e.mkApp(t, "base", nil)
			fn := e.mkFn(t, app, "basefn", nil)

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.mgmt.CreateApplication(t.Context(), functions.CreateApplicationRequest{
				CompartmentId: new(testCompartment), DisplayName: new("tagged"), SubnetIds: []string{testSubnet},
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{
				DisplayName: new("taggedfn"), ApplicationId: app.Id, MemoryInMBs: new(int64(256)),
				SourceDetails: functions.CreateContainerImageFunctionSourceDetails{Image: new("example/fn:1")},
				FreeformTags:  tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.mgmt.UpdateApplication(t.Context(), functions.UpdateApplicationRequest{
				ApplicationId: app.Id,
				FreeformTags:  tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.mgmt.UpdateFunction(t.Context(), functions.UpdateFunctionRequest{
				FunctionId:   fn.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
