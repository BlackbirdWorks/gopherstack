package applicationautoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

// TestDescribeScalingActivities_TracksRegistrations verifies that registering and
// updating a scalable target records scaling activities and that
// DescribeScalingActivities returns and filters them correctly.
func TestDescribeScalingActivities_TracksRegistrations(t *testing.T) {
	t.Parallel()

	const (
		ns        = "ecs"
		resA      = "service/default/svc-a"
		resB      = "service/default/svc-b"
		dimension = "ecs:service:DesiredCount"
	)

	tests := []struct {
		name          string
		filterRes     string
		filterDim     string
		wantNamespace string
		wantCount     int
	}{
		{
			name:          "all_in_namespace",
			wantCount:     3, // svc-a registered+updated (2) + svc-b registered (1)
			wantNamespace: ns,
		},
		{
			name:      "filter_by_resource_a",
			filterRes: resA,
			wantCount: 2,
		},
		{
			name:      "filter_by_resource_b",
			filterRes: resB,
			wantCount: 1,
		},
		{
			name:      "filter_by_resource_and_dimension",
			filterRes: resA,
			filterDim: dimension,
			wantCount: 2,
		},
		{
			name:      "filter_unknown_resource",
			filterRes: "service/default/missing",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")

			// Register svc-a, then update it (RegisterScalableTarget upserts).
			_, err := b.RegisterScalableTarget(ns, resA, dimension, 1, 5, nil, "", nil)
			require.NoError(t, err)
			_, err = b.RegisterScalableTarget(ns, resA, dimension, 2, 10, nil, "", nil)
			require.NoError(t, err)

			// Register svc-b once.
			_, err = b.RegisterScalableTarget(ns, resB, dimension, 1, 3, nil, "", nil)
			require.NoError(t, err)

			activities, _ := b.DescribeScalingActivities(applicationautoscaling.DescribeScalingActivitiesFilter{
				ServiceNamespace:  ns,
				ResourceID:        tt.filterRes,
				ScalableDimension: tt.filterDim,
			})
			assert.Len(t, activities, tt.wantCount)

			for _, a := range activities {
				assert.Equal(t, ns, a.ServiceNamespace)
				assert.Equal(t, "Successful", a.StatusCode)
				assert.NotEmpty(t, a.ActivityID)
				assert.NotEmpty(t, a.Description)

				if tt.filterRes != "" {
					assert.Equal(t, tt.filterRes, a.ResourceID)
				}
			}
		})
	}
}

// TestDescribeScalingActivities_ResetClears verifies Reset clears recorded activities.
func TestDescribeScalingActivities_ResetClears(t *testing.T) {
	t.Parallel()

	b := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.RegisterScalableTarget(
		"ecs", "service/default/svc", "ecs:service:DesiredCount", 1, 5, nil, "", nil,
	)
	require.NoError(t, err)

	got, _ := b.DescribeScalingActivities(
		applicationautoscaling.DescribeScalingActivitiesFilter{ServiceNamespace: "ecs"},
	)
	require.Len(t, got, 1)

	b.Reset()
	after, _ := b.DescribeScalingActivities(
		applicationautoscaling.DescribeScalingActivitiesFilter{ServiceNamespace: "ecs"},
	)
	assert.Empty(t, after)
}
