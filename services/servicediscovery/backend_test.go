package servicediscovery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// TestInMemoryBackend_DiscoverInstances_HealthyOrElseAll verifies the
// HEALTHY_OR_ELSE_ALL HealthStatus filter: it returns only healthy instances
// unless none are healthy, in which case it "fails open" and returns every
// registered instance -- matching real Cloud Map DiscoverInstances semantics.
func TestInMemoryBackend_DiscoverInstances_HealthyOrElseAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		unhealthyIDs  []string
		wantInstances []string
	}{
		{
			name:          "some healthy returns only healthy",
			unhealthyIDs:  []string{"i-2"},
			wantInstances: []string{"i-1"},
		},
		{
			name:          "none healthy fails open to all",
			unhealthyIDs:  []string{"i-1", "i-2"},
			wantInstances: []string{"i-1", "i-2"},
		},
		{
			name:          "all healthy returns all",
			unhealthyIDs:  nil,
			wantInstances: []string{"i-1", "i-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

			_, err := b.CreateHTTPNamespace("ns-fail-open", "", nil)
			require.NoError(t, err)
			nsID := b.ListNamespaces(servicediscovery.ListNamespacesFilter{})[0].ID

			svc, err := b.CreateService(
				"svc-fail-open", nsID, "", "", nil, nil,
				&servicediscovery.HealthCheckCustomConfig{FailureThreshold: 1}, nil,
			)
			require.NoError(t, err)

			_, err = b.RegisterInstance(svc.ID, "i-1", nil)
			require.NoError(t, err)
			_, err = b.RegisterInstance(svc.ID, "i-2", nil)
			require.NoError(t, err)

			for _, id := range tt.unhealthyIDs {
				require.NoError(t, b.UpdateInstanceCustomHealthStatus(svc.ID, id, "UNHEALTHY"))
			}

			discovered, _, err := b.DiscoverInstances("ns-fail-open", "svc-fail-open", "HEALTHY_OR_ELSE_ALL", nil)
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(discovered))
			for _, d := range discovered {
				gotIDs = append(gotIDs, d.InstanceID)
			}

			assert.ElementsMatch(t, tt.wantInstances, gotIDs)
		})
	}
}

// TestInMemoryBackend_UpdateInstanceCustomHealthStatus_RequiresCustomHealthCheck
// verifies that UpdateInstanceCustomHealthStatus rejects services that were not
// configured with HealthCheckCustomConfig, matching real Cloud Map: "You can use
// UpdateInstanceCustomHealthStatus to change the status only for custom health checks.".
func TestInMemoryBackend_UpdateInstanceCustomHealthStatus_RequiresCustomHealthCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		newService func(id, name, namespaceID string) *servicediscovery.Service
		name       string
	}{
		{
			name:       "no custom health check configured",
			newService: servicediscovery.NewServiceForTest,
			wantErr:    servicediscovery.ErrCustomHealthNotFound,
		},
		{
			name:       "custom health check configured",
			newService: servicediscovery.NewServiceWithCustomHealthForTest,
			wantErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")

			svc := tt.newService("svc-0000001", "svc-one", "")
			servicediscovery.AddServiceInternal(b, svc)

			inst := servicediscovery.NewInstanceForTest("i-1", svc.ID, nil)
			servicediscovery.AddInstanceInternal(b, inst)

			err := b.UpdateInstanceCustomHealthStatus(svc.ID, "i-1", "UNHEALTHY")

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
