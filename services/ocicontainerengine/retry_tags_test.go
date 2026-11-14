package ocicontainerengine_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, e *env, clusterID *string) (string, error)
		remove func(t *testing.T, e *env, clusterID *string)
		name   string
	}{
		{
			name: "cluster",
			create: func(t *testing.T, e *env, _ *string) (string, error) {
				t.Helper()

				r, err := e.ce.CreateCluster(t.Context(), ke.CreateClusterRequest{
					OpcRetryToken: new("tok"), CreateClusterDetails: clusterDetails("rc"),
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, _ *string) {
				t.Helper()

				list, err := e.ce.ListClusters(t.Context(), ke.ListClustersRequest{
					CompartmentId: new(testCompartment), Name: new("rc"),
				})
				require.NoError(t, err)
				require.NotEmpty(t, list.Items)

				_, err = e.ce.DeleteCluster(t.Context(), ke.DeleteClusterRequest{ClusterId: list.Items[0].Id})
				require.NoError(t, err)
			},
		},
		{
			name: "node pool",
			create: func(t *testing.T, e *env, clusterID *string) (string, error) {
				t.Helper()

				r, err := e.ce.CreateNodePool(t.Context(), ke.CreateNodePoolRequest{
					OpcRetryToken: new("tok"), CreateNodePoolDetails: poolDetails(clusterID, "rp"),
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, clusterID *string) {
				t.Helper()

				list, err := e.ce.ListNodePools(t.Context(), ke.ListNodePoolsRequest{
					CompartmentId: new(testCompartment), ClusterId: clusterID, Name: new("rp"),
				})
				require.NoError(t, err)
				require.NotEmpty(t, list.Items)

				_, err = e.ce.DeleteNodePool(t.Context(), ke.DeleteNodePoolRequest{NodePoolId: list.Items[0].Id})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		setup := func(t *testing.T) (*env, *string) {
			t.Helper()

			e := newEnv(t, envOpts{})
			c := e.mkCluster(t, "base", nil)

			return e, c.Id
		}

		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e, cid := setup(t)
			wr1, err := tt.create(t, e, cid)
			require.NoError(t, err)

			wr2, err := tt.create(t, e, cid)
			require.NoError(t, err)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e, cid := setup(t)
			wr1, err := tt.create(t, e, cid)
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			wr2, err := tt.create(t, fresh, cid)
			require.NoError(t, err)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			e, cid := setup(t)
			wr1, err := tt.create(t, e, cid)
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			wr2, err := tt.create(t, e, cid)
			require.NoError(t, err)
			assert.NotEqual(t, wr1, wr2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e, cid := setup(t)
			_, err := tt.create(t, e, cid)
			require.NoError(t, err)

			e.settle()
			tt.remove(t, e, cid)
			e.settle()

			_, err = tt.create(t, e, cid)
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

			c := e.mkCluster(t, "base", nil)
			p := e.mkPool(t, c.Id, "basepool", nil)

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			cd := clusterDetails("tagged")
			cd.FreeformTags, cd.DefinedTags = tt.free, tt.defd

			_, err := e.ce.CreateCluster(t.Context(), ke.CreateClusterRequest{CreateClusterDetails: cd})
			check(err)

			pd := poolDetails(c.Id, "taggedpool")
			pd.FreeformTags, pd.DefinedTags = tt.free, tt.defd

			_, err = e.ce.CreateNodePool(t.Context(), ke.CreateNodePoolRequest{CreateNodePoolDetails: pd})
			check(err)

			_, err = e.ce.UpdateCluster(t.Context(), ke.UpdateClusterRequest{
				ClusterId:    c.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.ce.UpdateNodePool(t.Context(), ke.UpdateNodePoolRequest{
				NodePoolId:   p.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
