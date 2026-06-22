package applicationautoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

func TestApplicationAutoScaling_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *applicationautoscaling.InMemoryBackend)
		verify func(t *testing.T, b *applicationautoscaling.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty",
			setup: func(_ *applicationautoscaling.InMemoryBackend) {},
			verify: func(t *testing.T, b *applicationautoscaling.InMemoryBackend) {
				t.Helper()

				targets, _ := b.DescribeScalableTargets(applicationautoscaling.DescribeScalableTargetsFilter{})
				assert.Empty(t, targets)
			},
		},
		{
			name: "scalable_target_preserved_with_arn_index",
			setup: func(b *applicationautoscaling.InMemoryBackend) {
				_, _ = b.RegisterScalableTarget(
					"ecs",
					"service/my-cluster/my-svc",
					"ecs:service:DesiredCount",
					1,
					10,
					nil,
					"",
					nil,
				)
			},
			verify: func(t *testing.T, b *applicationautoscaling.InMemoryBackend) {
				t.Helper()

				targets, _ := b.DescribeScalableTargets(
					applicationautoscaling.DescribeScalableTargetsFilter{ServiceNamespace: "ecs"},
				)
				require.Len(t, targets, 1)
				// Verify ARN index is rebuilt — tag ops should work
				err := b.TagResource(targets[0].ARN, map[string]string{"team": "platform"})
				require.NoError(t, err)
				gotTags, err := b.ListTagsForResource(targets[0].ARN)
				require.NoError(t, err)
				assert.Equal(t, "platform", gotTags["team"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}
