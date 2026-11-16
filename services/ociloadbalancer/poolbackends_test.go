package ociloadbalancer_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ociloadbalancer "github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
)

func TestApplyBackends(t *testing.T) {
	t.Parallel()

	one := ociloadbalancer.BackendAddr{IP: "10.0.1.5", Port: 8080}
	two := ociloadbalancer.BackendAddr{IP: "10.0.1.6", Port: 8080}

	tests := []struct {
		name    string
		changes [][]ociloadbalancer.BackendChange
		want    []string
	}{
		{
			name:    "add_two",
			changes: [][]ociloadbalancer.BackendChange{{{Set: "web", Add: []ociloadbalancer.BackendAddr{one, two}}}},
			want:    []string{"10.0.1.5:8080", "10.0.1.6:8080"},
		},
		{name: "add_is_idempotent", changes: [][]ociloadbalancer.BackendChange{
			{
				{Set: "web", Add: []ociloadbalancer.BackendAddr{one}},
			}, {{Set: "web", Add: []ociloadbalancer.BackendAddr{one}}},
		}, want: []string{"10.0.1.5:8080"}},
		{name: "remove_one", changes: [][]ociloadbalancer.BackendChange{
			{{Set: "web", Add: []ociloadbalancer.BackendAddr{one, two}}},
			{{Set: "web", Remove: []ociloadbalancer.BackendAddr{one}}},
		}, want: []string{"10.0.1.6:8080"}},
		{name: "remove_missing", changes: [][]ociloadbalancer.BackendChange{
			{{Set: "web", Remove: []ociloadbalancer.BackendAddr{one}}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id := e.mkLB(t, baseDetails("pool"))

			_, err := e.lb.CreateBackendSet(t.Context(), loadbalancer.CreateBackendSetRequest{
				LoadBalancerId: &id,
				Name:           new("web"), Policy: new("ROUND_ROBIN"), HealthChecker: hc(),
			})
			require.NoError(t, err)
			e.settle()

			assert.True(t, e.backend.HasBackendSet(id, "web"))
			assert.False(t, e.backend.HasBackendSet(id, "nope"))
			assert.False(t, e.backend.HasBackendSet("ocid1.loadbalancer.oc1..nope", "web"))

			for _, c := range tt.changes {
				require.NoError(t, e.backend.ApplyBackends(id, c))
				e.settle()
			}

			list, err := e.lb.ListBackends(t.Context(), loadbalancer.ListBackendsRequest{
				LoadBalancerId: &id, BackendSetName: new("web"),
			})
			require.NoError(t, err)

			got := make([]string, 0, len(list.Items))
			for _, b := range list.Items {
				got = append(got, *b.Name)
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestApplyBackendsMissingTarget(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	id := e.mkLB(t, baseDetails("pool"))

	err := e.backend.ApplyBackends(id, []ociloadbalancer.BackendChange{{Set: "nope"}})
	require.Error(t, err)

	var apiErr *ociloadbalancer.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.Status)

	require.Error(t, e.backend.ApplyBackends("ocid1.loadbalancer.oc1..nope", nil))
}
