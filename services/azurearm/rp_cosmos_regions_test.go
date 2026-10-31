package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func (f *fakeCosmos) regionState() ([]azurearm.CosmosRegion, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.regions, f.multiWrite
}

func twoRegionProps(multi bool) map[string]any {
	return map[string]any{
		"enableMultipleWriteLocations": multi,
		"locations": []any{
			map[string]any{"locationName": "East US", "failoverPriority": 0},
			map[string]any{"locationName": "West US", "failoverPriority": 1},
		},
	}
}

func failoverBody(east, west int) map[string]any {
	return map[string]any{"failoverPolicies": []any{
		map[string]any{"locationName": "East US", "failoverPriority": east},
		map[string]any{"locationName": "West US", "failoverPriority": west},
	}}
}

func TestCosmos_RegionsReachDataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		multi     bool
		wantWrite int
	}{
		{name: "single_writer", wantWrite: 1},
		{name: "multi_writer", multi: true, wantWrite: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, fake := newCosmosHandler()
			out := putCosmosAccount(t, h, twoRegionProps(tt.multi))

			props := asMap(t, out["properties"])
			assert.Len(t, props["readLocations"], 2)
			assert.Len(t, props["writeLocations"], tt.wantWrite)

			regions, multi := fake.regionState()
			assert.Equal(
				t,
				[]azurearm.CosmosRegion{{Name: "East US", Priority: 0}, {Name: "West US", Priority: 1}},
				regions,
			)
			assert.Equal(t, tt.multi, multi)
		})
	}
}

func TestCosmos_FailoverPriorityChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantWriter string
		wantStatus int
	}{
		{name: "swap", body: failoverBody(1, 0), wantStatus: http.StatusAccepted, wantWriter: "West US"},
		{name: "unchanged", body: failoverBody(0, 1), wantStatus: http.StatusAccepted, wantWriter: "East US"},
		{name: "no_zero", body: failoverBody(1, 2), wantStatus: http.StatusBadRequest, wantWriter: "East US"},
		{
			name:       "duplicate_priority",
			body:       failoverBody(0, 0),
			wantStatus: http.StatusBadRequest,
			wantWriter: "East US",
		},
		{name: "missing_location", body: map[string]any{"failoverPolicies": []any{
			map[string]any{"locationName": "East US", "failoverPriority": 0},
		}}, wantStatus: http.StatusBadRequest, wantWriter: "East US"},
		{name: "unknown_location", body: map[string]any{"failoverPolicies": []any{
			map[string]any{"locationName": "East US", "failoverPriority": 0},
			map[string]any{"locationName": "Mars", "failoverPriority": 1},
		}}, wantStatus: http.StatusBadRequest, wantWriter: "East US"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, fake := newCosmosHandler()
			putCosmosAccount(t, h, twoRegionProps(false))

			st, _ := cosmosDo(t, h, http.MethodPost, cosmosAcct+"/failoverPriorityChange", tt.body)
			require.Equal(t, tt.wantStatus, st)

			_, got := cosmosDo(t, h, http.MethodGet, cosmosAcct, nil)
			writes, _ := asMap(t, got["properties"])["writeLocations"].([]any)
			require.Len(t, writes, 1)
			assert.Equal(t, tt.wantWriter, asMap(t, writes[0])["locationName"])

			regions, _ := fake.regionState()
			require.Len(t, regions, 2)
			assert.Equal(t, tt.wantWriter, regions[0].Name)
		})
	}
}

func TestCosmos_FailoverUnknownAccount(t *testing.T) {
	t.Parallel()

	h, _ := newCosmosHandler()
	st, _ := cosmosDo(t, h, http.MethodPost, cosmosAcct+"/failoverPriorityChange", failoverBody(1, 0))
	assert.Equal(t, http.StatusNotFound, st)
}

func TestCosmos_DeleteAccountResetsRegions(t *testing.T) {
	t.Parallel()

	h, fake := newCosmosHandler()
	putCosmosAccount(t, h, twoRegionProps(true))

	st, _ := cosmosDo(t, h, http.MethodDelete, cosmosAcct, nil)
	require.Equal(t, http.StatusAccepted, st)

	regions, multi := fake.regionState()
	assert.Empty(t, regions)
	assert.False(t, multi)
}

func (f *fakeCosmos) SetAccountPolicies(p azurearm.CosmosAccountPolicies) {
	f.mu.Lock()
	f.policies = p
	f.mu.Unlock()
}

func (f *fakeCosmos) policyState() azurearm.CosmosAccountPolicies {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.policies
}

func TestCosmos_AccountPoliciesReachDataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props map[string]any
		name  string
		want  azurearm.CosmosAccountPolicies
	}{
		{
			name:  "defaults",
			props: map[string]any{},
			want: azurearm.CosmosAccountPolicies{
				Consistency:        "Session",
				Backup:             "Periodic",
				MaxStalenessPrefix: 100,
				MaxIntervalSeconds: 5,
			},
		},
		{
			name: "bounded staleness, failover and continuous backup",
			props: map[string]any{
				"enableAutomaticFailover": true,
				"consistencyPolicy": map[string]any{
					"defaultConsistencyLevel": "BoundedStaleness",
					"maxStalenessPrefix":      200000,
					"maxIntervalInSeconds":    600,
				},
				"backupPolicy": map[string]any{
					"type": "Continuous", "continuousModeProperties": map[string]any{"tier": "Continuous7Days"},
				},
			},
			want: azurearm.CosmosAccountPolicies{
				Consistency: "BoundedStaleness", Backup: "Continuous7Days", MaxStalenessPrefix: 200000,
				MaxIntervalSeconds: 600, AutomaticFailover: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, fake := newCosmosHandler()
			putCosmosAccount(t, h, tt.props)

			assert.Equal(t, tt.want, fake.policyState())
		})
	}
}
