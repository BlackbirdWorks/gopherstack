package ocistreaming_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/streaming"
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

			pool := e.mkPool(t, "tp", nil)

			creates := map[string]func() error{
				"stream": func() error {
					_, err := e.admin.CreateStream(t.Context(), streaming.CreateStreamRequest{
						Name: new("ts"), Partitions: new(1), StreamPoolId: pool.Id,
						FreeformTags: tt.free, DefinedTags: tt.defd,
					})

					return err
				},
				"pool": func() error {
					_, err := e.admin.CreateStreamPool(t.Context(), streaming.CreateStreamPoolRequest{
						CompartmentId: new(testCompartment), Name: new("tp2"),
						FreeformTags: tt.free, DefinedTags: tt.defd,
					})

					return err
				},
				"harness": func() error {
					_, err := e.admin.CreateConnectHarness(t.Context(), streaming.CreateConnectHarnessRequest{
						CompartmentId: new(testCompartment), Name: new("th"),
						FreeformTags: tt.free, DefinedTags: tt.defd,
					})

					return err
				},
				"update pool": func() error {
					_, err := e.admin.UpdateStreamPool(t.Context(), streaming.UpdateStreamPoolRequest{
						StreamPoolId: pool.Id,
						FreeformTags: tt.free, DefinedTags: tt.defd,
					})

					return err
				},
			}

			for name, fn := range creates {
				err := fn()
				if tt.status == http.StatusOK {
					require.NoError(t, err, name)

					continue
				}

				se := svcErr(t, err)
				assert.Equal(t, tt.status, se.GetHTTPStatusCode(), name)
				assert.Equal(t, tt.code, se.GetCode(), name)
			}
		})
	}
}
