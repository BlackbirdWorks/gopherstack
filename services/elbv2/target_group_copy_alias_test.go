package elbv2_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testsnap"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// Target groups handed out must not alias the Targets slice or attributes map
// that health reconciliation and ModifyTargetGroupAttributes mutate in place.
func TestTargetGroupCopies_NotAliasedToBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		transition func(t *testing.T, b *elbv2.InMemoryBackend, tgArn string)
	}{
		{
			name: "health_state",
			transition: func(t *testing.T, b *elbv2.InMemoryBackend, tgArn string) {
				t.Helper()
				require.NoError(t, b.SetTargetHealthState(tgArn, "i-1", 80, "healthy", ""))
			},
		},
		{
			name: "attributes",
			transition: func(t *testing.T, b *elbv2.InMemoryBackend, tgArn string) {
				t.Helper()
				_, err := b.ModifyTargetGroupAttributes(tgArn, map[string]string{"stickiness.enabled": "true"})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditBackend(t)

			created, err := b.CreateTargetGroup(elbv2.CreateTargetGroupInput{
				Name: "alias-tg", Protocol: "HTTP", Port: 80, VpcID: "vpc-1", TargetType: "instance",
			})
			require.NoError(t, err)

			tgArn := created.TargetGroupArn
			require.NoError(t, b.RegisterTargets(tgArn, []elbv2.Target{{ID: "i-1", Port: 80}}))

			described, err := b.DescribeTargetGroups([]string{tgArn}, nil, "")
			require.NoError(t, err)
			require.Len(t, described, 1)
			require.Len(t, described[0].Targets, 1)

			testsnap.Unchanged(t, &described[0], func() { tt.transition(t, b, tgArn) })

			again, err := b.DescribeTargetGroups([]string{tgArn}, nil, "")
			require.NoError(t, err)
			testsnap.Unchanged(t, &again[0], func() { tt.transition(t, b, tgArn) })
		})
	}
}
