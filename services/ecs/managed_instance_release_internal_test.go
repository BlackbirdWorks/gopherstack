package ecs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPutClusterCapacityProviders_RetiresOrphanedManagedInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		remaining     []string
		wantDeleteErr bool
	}{
		{name: "dissociated", remaining: nil},
		{name: "still_associated", remaining: []string{"cp"}, wantDeleteErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, asg, _ := scalingBackend(t, &ManagedScaling{Status: "ENABLED"}, "")
			asg.ids = []string{"i-1"}

			targets := b.scalingTargets()
			require.Len(t, targets, 1)

			info, ok := asg.DescribeGroup("")
			require.True(t, ok)
			b.syncManagedInstances(targets[0], info)

			_, err := b.PutClusterCapacityProviders("default", tt.remaining, nil)
			require.NoError(t, err)

			_, err = b.DeleteCluster("default")
			if tt.wantDeleteErr {
				require.ErrorIs(t, err, ErrClusterContainsContainerInstances)

				return
			}

			require.NoError(t, err)
		})
	}
}
