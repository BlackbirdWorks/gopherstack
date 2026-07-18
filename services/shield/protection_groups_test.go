package shield_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestParity_ListProtectionGroups_SortOutsideLock verifies concurrent reads don't block each other.
func TestInMemoryBackend_ListProtectionGroupsSortOutsideLock(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddSubscriptionInternal()

	for i := range 10 {
		_, err := b.CreateProtectionGroup("grp-"+string(rune('a'+i)), "SUM", "ALL", "", nil)
		require.NoError(t, err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		for range 100 {
			_ = b.ListProtectionGroups()
		}
	}()

	for range 100 {
		_ = b.ListProtectionGroups()
	}

	<-done
}

// TestBackend_CreateDeleteProtectionGroup tests backend protection group operations.
func TestBackend_CreateDeleteProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		aggregation  string
		pattern      string
		resourceType string
		groupID      string
		members      []string
		wantErr      bool
	}{
		{
			name:        "success all required",
			groupID:     "grp-1",
			aggregation: "MAX",
			pattern:     "ALL",
		},
		{
			name:         "success with members",
			groupID:      "grp-2",
			aggregation:  "SUM",
			pattern:      "ARBITRARY",
			resourceType: "ELASTIC_IP_ALLOCATION",
			members:      []string{"arn:aws:ec2:us-east-1:123:eip/eipalloc-1"},
		},
		{
			name:        "missing id",
			groupID:     "",
			aggregation: "MAX",
			pattern:     "ALL",
			wantErr:     true,
		},
		{
			name:        "missing aggregation",
			groupID:     "grp-3",
			aggregation: "",
			pattern:     "ALL",
			wantErr:     true,
		},
		{
			name:        "missing pattern",
			groupID:     "grp-4",
			aggregation: "MAX",
			pattern:     "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.CreateSubscription())

			pg, err := b.CreateProtectionGroup(tt.groupID, tt.aggregation, tt.pattern, tt.resourceType, tt.members)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, pg)
			assert.Equal(t, tt.groupID, pg.ID)
			assert.Equal(t, 1, shield.ProtectionGroupCount(b))

			// Test delete.
			require.NoError(t, b.DeleteProtectionGroup(tt.groupID))
			assert.Equal(t, 0, shield.ProtectionGroupCount(b))
		})
	}
}

// TestAudit_Gap24_ListResourcesByResourceType verifies BY_RESOURCE_TYPE pattern.
func TestInMemoryBackend_ListResourcesByResourceType(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	// Add an EIP and an ALB protection.
	_, err := b.CreateProtection("eip-prot", eipARN("1"), nil)
	require.NoError(t, err)
	_, err = b.CreateProtection("alb-prot", albARN("myapp"), nil)
	require.NoError(t, err)

	// Create group with BY_RESOURCE_TYPE for EIP.
	_, err = b.CreateProtectionGroup(
		"eip-group",
		shield.AggregationMax,
		shield.PatternByResourceType,
		"ELASTIC_IP_ALLOCATION",
		nil,
	)
	require.NoError(t, err)

	arns, err := b.ListResourcesInProtectionGroup("eip-group")
	require.NoError(t, err)
	assert.Len(t, arns, 1)
	assert.Contains(t, arns[0], "eip")
}

// --- Gap 25: SimulateAttack endpoint ---

// TestRefinement1_DescribeProtectionGroup tests DescribeProtectionGroup.
func TestInMemoryBackend_DescribeProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*shield.InMemoryBackend)
		groupID string
		name    string
		wantErr bool
	}{
		{
			name: "found",
			setup: func(b *shield.InMemoryBackend) {
				b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
			},
			groupID: "grp-1",
		},
		{
			name:    "not_found",
			setup:   func(*shield.InMemoryBackend) {},
			groupID: "no-such-group",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			pg, err := b.DescribeProtectionGroup(tt.groupID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, pg.ID)
			assert.NotEmpty(t, pg.ProtectionGroupArn)
		})
	}
}

// TestRefinement1_ListProtectionGroups tests ListProtectionGroups.
func TestInMemoryBackend_ListProtectionGroups(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddProtectionGroupInternal("grp-b", shield.AggregationSum, shield.PatternAll)
	b.AddProtectionGroupInternal("grp-a", shield.AggregationMax, shield.PatternAll)

	groups := b.ListProtectionGroups()
	require.Len(t, groups, 2)
	assert.Equal(t, "grp-a", groups[0].ID)
	assert.Equal(t, "grp-b", groups[1].ID)
}

// TestRefinement1_UpdateProtectionGroup tests UpdateProtectionGroup.
func TestInMemoryBackend_UpdateProtectionGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*shield.InMemoryBackend)
		groupID      string
		aggregation  string
		pattern      string
		resourceType string
		name         string
		members      []string
		wantErr      bool
	}{
		{
			name: "success",
			setup: func(b *shield.InMemoryBackend) {
				b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)
			},
			groupID:     "grp-1",
			aggregation: shield.AggregationSum,
			pattern:     shield.PatternAll,
		},
		{
			name:        "not_found",
			setup:       func(*shield.InMemoryBackend) {},
			groupID:     "no-such-group",
			aggregation: shield.AggregationMax,
			pattern:     shield.PatternAll,
			wantErr:     true,
		},
		{
			name: "invalid_aggregation",
			setup: func(b *shield.InMemoryBackend) {
				b.AddProtectionGroupInternal("grp-2", shield.AggregationMax, shield.PatternAll)
			},
			groupID:     "grp-2",
			aggregation: "INVALID",
			pattern:     shield.PatternAll,
			wantErr:     true,
		},
		{
			name: "arbitrary_requires_members",
			setup: func(b *shield.InMemoryBackend) {
				b.AddProtectionGroupInternal("grp-3", shield.AggregationMax, shield.PatternAll)
			},
			groupID:     "grp-3",
			aggregation: shield.AggregationMax,
			pattern:     shield.PatternArbitrary,
			members:     nil,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.UpdateProtectionGroup(tt.groupID, tt.aggregation, tt.pattern, tt.resourceType, tt.members)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_ProtectionGroupARN verifies protection group ARN format.
func TestInMemoryBackend_ProtectionGroupARN(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	pg := b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)

	require.NotNil(t, pg)
	assert.Contains(t, pg.ProtectionGroupArn, "arn:aws:shield::")
	assert.Contains(t, pg.ProtectionGroupArn, "protection-group/grp-1")
}

// TestRefinement1_ValidAggregationConstants tests that constants are valid aggregation values.
func TestInMemoryBackend_ValidAggregationConstants(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	for _, agg := range []string{shield.AggregationSum, shield.AggregationMean, shield.AggregationMax} {
		_, err := b.CreateProtectionGroup(agg+"-grp", agg, shield.PatternAll, "", nil)
		require.NoError(t, err, "aggregation %q should be valid", agg)
	}
}

// TestRefinement1_InvalidAggregation tests invalid aggregation is rejected.
func TestInMemoryBackend_InvalidAggregation(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp", "INVALID", shield.PatternAll, "", nil)
	require.Error(t, err)
}

// TestRefinement1_PatternArbitraryRequiresMembers tests ARBITRARY requires members.
func TestInMemoryBackend_PatternArbitraryRequiresMembers(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp", shield.AggregationMax, shield.PatternArbitrary, "", nil)
	require.Error(t, err)
}

// TestRefinement1_PatternByResourceTypeRequiresResourceType tests requirement.
func TestInMemoryBackend_PatternByResourceTypeRequiresResourceType(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp", shield.AggregationMax, shield.PatternByResourceType, "", nil)
	require.Error(t, err)
}

// TestRefinement1_AddProtectionGroupInternal verifies the seed helper.
func TestInMemoryBackend_AddProtectionGroupInternal(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	pg := b.AddProtectionGroupInternal("grp-1", shield.AggregationMax, shield.PatternAll)

	require.NotNil(t, pg)
	assert.Equal(t, "grp-1", pg.ID)
	assert.Equal(t, shield.AggregationMax, pg.Aggregation)
	assert.Equal(t, shield.PatternAll, pg.Pattern)
	assert.Equal(t, 1, shield.ProtectionGroupCount(b))
}
