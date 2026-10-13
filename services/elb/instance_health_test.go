package elb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

type stateResolver struct{ states map[string]string }

func (stateResolver) SecurityGroupExists(string) bool { return true }
func (stateResolver) SubnetExists(string) bool        { return true }
func (r stateResolver) InstanceExists(id string) bool {
	_, ok := r.states[id]

	return ok
}
func (r stateResolver) InstanceStateName(id string) (string, bool) {
	s, ok := r.states[id]

	return s, ok
}

func TestDescribeInstanceHealth_EC2State(t *testing.T) {
	t.Parallel()

	const id = "i-0123456789abcdef0"

	tests := []struct {
		name        string
		ec2State    string
		state       string
		reasonCode  string
		description string
		remove      bool
	}{
		{name: "running", ec2State: "running", state: "InService", reasonCode: "N/A", description: "N/A"},
		{
			name: "pending", ec2State: "pending", state: "OutOfService", reasonCode: "Instance",
			description: "Instance is in pending state.",
		},
		{
			name: "stopped", ec2State: "stopped", state: "OutOfService", reasonCode: "Instance",
			description: "Instance is in stopped state.",
		},
		{
			name: "terminated", ec2State: "terminated", state: "OutOfService", reasonCode: "Instance",
			description: "Instance is in terminated state.",
		},
		{name: "purged", ec2State: "running", remove: true, state: "Unknown", reasonCode: "N/A", description: "N/A"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := stateResolver{states: map[string]string{id: tt.ec2State}}
			backend := newBackend()
			backend.SetEC2Resolver(res)

			h := elb.NewHandler(backend)
			mustCreateLB(t, h, "health-lb")

			_, err := backend.RegisterInstancesWithLoadBalancer(
				t.Context(),
				"health-lb",
				[]elb.Instance{{InstanceID: id}},
			)
			require.NoError(t, err)

			if tt.remove {
				delete(res.states, id)
			}

			got, err := backend.DescribeInstanceHealth(t.Context(), "health-lb", nil)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, tt.state, got[0].State)
			assert.Equal(t, tt.reasonCode, got[0].ReasonCode)
			assert.Equal(t, tt.description, got[0].Description)
		})
	}
}
