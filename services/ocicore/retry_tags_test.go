package ocicore_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, e *env) (string, error)
		remove func(t *testing.T, e *env, id string)
		name   string
	}{
		{
			name: "vcn",
			create: func(t *testing.T, e *env) (string, error) {
				t.Helper()

				r, err := e.client.CreateVcn(t.Context(), core.CreateVcnRequest{
					OpcRetryToken: new("tok"),
					CompartmentId: new(testComp), CidrBlocks: []string{"10.0.0.0/16"}, DisplayName: new("rv"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.DeleteVcn(t.Context(), core.DeleteVcnRequest{VcnId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "volume",
			create: func(t *testing.T, e *env) (string, error) {
				t.Helper()

				r, err := e.block.CreateVolume(t.Context(), core.CreateVolumeRequest{
					OpcRetryToken: new("tok"),
					CompartmentId: new(testComp), AvailabilityDomain: new(testAD), SizeInGBs: new(int64(50)),
					DisplayName: new("rvol"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.block.DeleteVolume(t.Context(), core.DeleteVolumeRequest{VolumeId: &id})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			id1, err := tt.create(t, e)
			require.NoError(t, err)

			id2, err := tt.create(t, e)
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			id1, err := tt.create(t, e)
			require.NoError(t, err)

			fresh := newEnv(t, false)
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			id2, err := tt.create(t, fresh)
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			id1, err := tt.create(t, e)
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			id2, err := tt.create(t, e)
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			id, err := tt.create(t, e)
			require.NoError(t, err)

			e.clock.Advance(3 * time.Second)
			tt.remove(t, e, id)
			e.clock.Advance(3 * time.Second)

			_, err = tt.create(t, e)
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

			e := newEnv(t, false)
			e.backend.SetTagLookup(func(ns, key string) (ocitags.Def, bool) {
				return ocitags.Def{}, strings.EqualFold(ns, "ops") && strings.EqualFold(key, "cost")
			})

			base := e.mkVcn(t, "10.1.0.0/16", "base")

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.client.CreateVcn(t.Context(), core.CreateVcnRequest{
				CompartmentId: new(testComp), CidrBlocks: []string{"10.2.0.0/16"},
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.client.UpdateVcn(t.Context(), core.UpdateVcnRequest{
				VcnId:        base.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.block.CreateVolume(t.Context(), core.CreateVolumeRequest{
				CompartmentId: new(testComp), AvailabilityDomain: new(testAD), SizeInGBs: new(int64(50)),
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
