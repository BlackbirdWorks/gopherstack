package ociloadbalancer_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

type retryCase struct {
	create func(t *testing.T, e *env, lbID string) (string, error)
	remove func(t *testing.T, e *env, lbID string)
	name   string
}

func retryCases() []retryCase {
	return []retryCase{
		{
			name: "backend set",
			create: func(t *testing.T, e *env, lbID string) (string, error) {
				t.Helper()

				r, err := e.lb.CreateBackendSet(t.Context(), loadbalancer.CreateBackendSetRequest{
					OpcRetryToken: new("tok"), LoadBalancerId: &lbID,
					Name: new("web"), Policy: new("ROUND_ROBIN"), HealthChecker: hc(),
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, lbID string) {
				t.Helper()

				_, err := e.lb.DeleteBackendSet(t.Context(), loadbalancer.DeleteBackendSetRequest{
					LoadBalancerId: &lbID, BackendSetName: new("web"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "hostname",
			create: func(t *testing.T, e *env, lbID string) (string, error) {
				t.Helper()

				r, err := e.lb.CreateHostname(t.Context(), loadbalancer.CreateHostnameRequest{
					OpcRetryToken: new("tok"), LoadBalancerId: &lbID,
					Name: new("h1"), Hostname: new("app.example.com"),
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, lbID string) {
				t.Helper()

				_, err := e.lb.DeleteHostname(t.Context(), loadbalancer.DeleteHostnameRequest{
					LoadBalancerId: &lbID, Name: new("h1"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "update",
			create: func(t *testing.T, e *env, lbID string) (string, error) {
				t.Helper()

				r, err := e.lb.UpdateLoadBalancer(t.Context(), loadbalancer.UpdateLoadBalancerRequest{
					OpcRetryToken: new("tok"), LoadBalancerId: &lbID,
					DisplayName: new("renamed"),
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, lbID string) {
				t.Helper()

				_, err := e.lb.DeleteLoadBalancer(t.Context(), loadbalancer.DeleteLoadBalancerRequest{
					LoadBalancerId: &lbID,
				})
				require.NoError(t, err)
			},
		},
	}
}

func TestSubResourceRetryToken(t *testing.T) {
	t.Parallel()

	for _, tt := range retryCases() {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id := e.mkLB(t, baseDetails("rt"))

			wr1, err := tt.create(t, e, id)
			require.NoError(t, err)

			wr2, err := tt.create(t, e, id)
			require.NoError(t, err)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id := e.mkLB(t, baseDetails("rt"))

			wr1, err := tt.create(t, e, id)
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			wr2, err := tt.create(t, fresh, id)
			require.NoError(t, err)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id := e.mkLB(t, baseDetails("rt"))

			_, err := tt.create(t, e, id)
			require.NoError(t, err)

			e.settle()
			tt.remove(t, e, id)
			e.settle()

			_, err = tt.create(t, e, id)
			require.Error(t, err)
			assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestCreateLoadBalancerRetryToken(t *testing.T) {
	t.Parallel()

	create := func(t *testing.T, e *env) (string, error) {
		t.Helper()

		r, err := e.lb.CreateLoadBalancer(t.Context(), loadbalancer.CreateLoadBalancerRequest{
			OpcRetryToken: new("tok"), CreateLoadBalancerDetails: baseDetails("rt"),
		})
		if err != nil {
			return "", err
		}

		return *r.OpcWorkRequestId, nil
	}

	tests := []struct {
		act  func(t *testing.T, e *env, first string)
		name string
	}{
		{
			name: "replay",
			act: func(t *testing.T, e *env, first string) {
				t.Helper()

				again, err := create(t, e)
				require.NoError(t, err)
				assert.Equal(t, first, again)

				list, err := e.lb.ListLoadBalancers(t.Context(), loadbalancer.ListLoadBalancersRequest{
					CompartmentId: new(testCompartment),
				})
				require.NoError(t, err)
				assert.Len(t, list.Items, 1)
			},
		},
		{
			name: "expired",
			act: func(t *testing.T, e *env, first string) {
				t.Helper()

				e.clock.Advance(25 * time.Hour)

				again, err := create(t, e)
				require.NoError(t, err)
				assert.NotEqual(t, first, again)
			},
		},
		{
			name: "invalidated",
			act: func(t *testing.T, e *env, first string) {
				t.Helper()

				wr, err := e.lb.GetWorkRequest(t.Context(), loadbalancer.GetWorkRequestRequest{WorkRequestId: &first})
				require.NoError(t, err)

				e.settle()

				_, err = e.lb.DeleteLoadBalancer(t.Context(), loadbalancer.DeleteLoadBalancerRequest{
					LoadBalancerId: wr.LoadBalancerId,
				})
				require.NoError(t, err)

				_, err = create(t, e)
				assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
				assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			first, err := create(t, e)
			require.NoError(t, err)

			tt.act(t, e, first)
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

			id := e.mkLB(t, baseDetails("base"))

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			d := baseDetails("tagged")
			d.FreeformTags, d.DefinedTags = tt.free, tt.defd

			_, err := e.lb.CreateLoadBalancer(t.Context(), loadbalancer.CreateLoadBalancerRequest{
				CreateLoadBalancerDetails: d,
			})
			check(err)

			_, err = e.lb.UpdateLoadBalancer(t.Context(), loadbalancer.UpdateLoadBalancerRequest{
				LoadBalancerId: &id,
				FreeformTags:   tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
