package ecs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeASGResolver map[string]bool

func (f fakeASGResolver) AutoScalingGroupExists(arnOrName string) bool { return f[arnOrName] }

func TestCreateCapacityProvider_ValidatesAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asg     string
		wantErr bool
	}{
		{name: "existing", asg: "asg-ok"},
		{name: "missing", asg: "asg-missing", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			b.SetAutoScalingGroupResolver(fakeASGResolver{"asg-ok": true})

			_, err := b.CreateCapacityProvider(CreateCapacityProviderInput{
				Name:                     "cp",
				AutoScalingGroupProvider: &AutoScalingGroupProvider{AutoScalingGroupArn: tt.asg},
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
		})
	}
}
