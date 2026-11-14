package ocidns_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	ocidnssdk "github.com/oracle/oci-go-sdk/v65/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, e *env, name string) (string, error)
		remove func(t *testing.T, e *env, id string)
		name   string
	}{
		{
			name: "zone",
			create: func(t *testing.T, e *env, name string) (string, error) {
				t.Helper()

				r, err := e.client.CreateZone(t.Context(), ocidnssdk.CreateZoneRequest{
					OpcRetryToken: new("tok"),
					CreateZoneDetails: ocidnssdk.CreateZoneDetails{
						Name: new(name), CompartmentId: new(testCompartment),
					},
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteZone(t.Context(), ocidnssdk.DeleteZoneRequest{ZoneNameOrId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "view",
			create: func(t *testing.T, e *env, name string) (string, error) {
				t.Helper()

				r, err := e.client.CreateView(t.Context(), ocidnssdk.CreateViewRequest{
					OpcRetryToken: new("tok"),
					DisplayName:   new(name), CompartmentId: new(testCompartment),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteView(t.Context(), ocidnssdk.DeleteViewRequest{ViewId: &id})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt.example")
			require.NoError(t, err)

			id2, err := tt.create(t, e, "rt.example")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt.example")
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			id2, err := tt.create(t, fresh, "rt.example")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt.example")
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			id2, err := tt.create(t, e, "rt2.example")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id, err := tt.create(t, e, "rt.example")
			require.NoError(t, err)

			tt.remove(t, e, id)

			_, err = tt.create(t, e, "rt.example")
			require.Error(t, err)
			assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

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

			zone := e.createZone(t, "base.example", nil)
			view := e.createView(t, "base")

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.client.CreateZone(t.Context(), ocidnssdk.CreateZoneRequest{
				CreateZoneDetails: ocidnssdk.CreateZoneDetails{
					Name: new("tagged.example"), CompartmentId: new(testCompartment),
					FreeformTags: tt.free, DefinedTags: tt.defd,
				},
			})
			check(err)

			_, err = e.client.CreateView(t.Context(), ocidnssdk.CreateViewRequest{
				DisplayName: new("tagged"), CompartmentId: new(testCompartment),
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.client.UpdateZone(t.Context(), ocidnssdk.UpdateZoneRequest{
				ZoneNameOrId: zone.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.client.UpdateView(t.Context(), ocidnssdk.UpdateViewRequest{
				ViewId:       view.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
