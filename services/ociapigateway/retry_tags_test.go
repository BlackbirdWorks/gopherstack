package ociapigateway_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
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
			name: "gateway",
			create: func(t *testing.T, e *env, name string) (string, error) {
				t.Helper()

				r, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
					OpcRetryToken: new("tok"),
					CreateGatewayDetails: apigateway.CreateGatewayDetails{
						CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
						SubnetId: new(testSubnet), DisplayName: new(name),
					},
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				e.settle()

				_, err := e.gw.DeleteGateway(t.Context(), apigateway.DeleteGatewayRequest{GatewayId: &id})
				require.NoError(t, err)
				e.settle()
			},
		},
		{
			name: "api",
			create: func(t *testing.T, e *env, name string) (string, error) {
				t.Helper()

				r, err := e.api.CreateApi(t.Context(), apigateway.CreateApiRequest{
					OpcRetryToken: new("tok"),
					CreateApiDetails: apigateway.CreateApiDetails{
						CompartmentId: new(testCompartment), DisplayName: new(name), Content: new(petsYAML),
					},
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				e.settle()

				_, err := e.api.DeleteApi(t.Context(), apigateway.DeleteApiRequest{ApiId: &id})
				require.NoError(t, err)
				e.settle()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt")
			require.NoError(t, err)

			id2, err := tt.create(t, e, "rt")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt")
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			id2, err := tt.create(t, fresh, "rt")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, err := tt.create(t, e, "rt")
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			id2, err := tt.create(t, e, "rt2")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id, err := tt.create(t, e, "rt")
			require.NoError(t, err)

			tt.remove(t, e, id)

			_, err = tt.create(t, e, "rt")
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

			gw := e.mkGateway(t, "base")
			base := e.mkAPI(t, "base", petsYAML)

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
				CreateGatewayDetails: apigateway.CreateGatewayDetails{
					CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
					SubnetId: new(testSubnet), DisplayName: new("tagged"),
					FreeformTags: tt.free, DefinedTags: tt.defd,
				},
			})
			check(err)

			_, err = e.api.CreateApi(t.Context(), apigateway.CreateApiRequest{
				CreateApiDetails: apigateway.CreateApiDetails{
					CompartmentId: new(testCompartment), DisplayName: new("taggedapi"), Content: new(petsYAML),
					FreeformTags: tt.free, DefinedTags: tt.defd,
				},
			})
			check(err)

			_, err = e.gw.UpdateGateway(t.Context(), apigateway.UpdateGatewayRequest{
				GatewayId:            gw.Id,
				UpdateGatewayDetails: apigateway.UpdateGatewayDetails{FreeformTags: tt.free, DefinedTags: tt.defd},
			})
			check(err)

			_, err = e.api.UpdateApi(t.Context(), apigateway.UpdateApiRequest{
				ApiId:            base.Id,
				UpdateApiDetails: apigateway.UpdateApiDetails{FreeformTags: tt.free, DefinedTags: tt.defd},
			})
			check(err)
		})
	}
}
