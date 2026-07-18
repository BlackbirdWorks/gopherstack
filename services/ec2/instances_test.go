package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendDiagnosticInterrupt(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	require.NoError(t, b.SendDiagnosticInterrupt(instances[0].ID))
	require.ErrorIs(t, b.SendDiagnosticInterrupt("i-missing"), ec2.ErrInstanceNotFound)
	require.ErrorIs(t, b.SendDiagnosticInterrupt(""), ec2.ErrInvalidParameter)
}

func TestDescribeElasticGpus(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	assert.Empty(t, b.DescribeElasticGpus(nil))
}

// TestDescribeInstancesByVPC verifies secondary-index instance lookup.
func TestDescribeInstancesByVPC(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.RunInstances("ami-0c55b159cbfafe1f0", "t3.micro", "", 3)
	require.NoError(t, err)

	insts := b.DescribeInstancesByVPC("vpc-default")
	assert.Len(t, insts, 3)
}

// TestDescribeInstancesByVPC_EmptyOnNoInstances verifies no panics on empty VPC.

// TestDescribeInstancesByVPC_EmptyOnNoInstances verifies no panics on empty VPC.
func TestDescribeInstancesByVPC_EmptyOnNoInstances(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	insts := b.DescribeInstancesByVPC("vpc-nonexistent")
	assert.Nil(t, insts)
}

// TestDescribeInstanceTypeOfferings verifies the instance type list.
func TestDescribeInstanceTypeOfferings(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	offerings := b.DescribeInstanceTypeOfferings()

	assert.NotEmpty(t, offerings)

	var foundT3Micro bool

	for _, o := range offerings {
		if o.InstanceType == "t3.micro" {
			foundT3Micro = true

			assert.Equal(t, "availability-zone", o.LocationType)

			break
		}
	}

	assert.True(t, foundT3Micro, "t3.micro should be in offerings")
}

// TestCreateVpcPeeringConnection tests peering creation.

func TestStartStopInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*ec2.InMemoryBackend) string
		name       string
		op         string
		instanceID string
		wantState  string
		wantErr    bool
	}{
		{
			name: "stop_running_instance",
			setup: func(b *ec2.InMemoryBackend) string {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				if err != nil {
					return ""
				}
				b.TickLifecycleForTest() // pending → running

				return instances[0].ID
			},
			op:        "stop",
			wantErr:   false,
			wantState: "stopping",
		},
		{
			name: "start_stopped_instance",
			setup: func(b *ec2.InMemoryBackend) string {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				if err != nil {
					return ""
				}
				b.TickLifecycleForTest() // pending → running

				id := instances[0].ID
				_, _ = b.StopInstances([]string{id})
				b.TickLifecycleForTest() // stopping → stopped

				return id
			},
			op:        "start",
			wantErr:   false,
			wantState: "pending",
		},
		{
			name:       "stop_nonexistent",
			op:         "stop",
			instanceID: "i-doesnotexist",
			wantErr:    true,
		},
		{
			name:       "start_nonexistent",
			op:         "start",
			instanceID: "i-doesnotexist",
			wantErr:    true,
		},
		{
			// start a running instance must fail (pending is not stopped)
			name: "start_running_instance",
			setup: func(b *ec2.InMemoryBackend) string {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				if err != nil {
					return ""
				}
				b.TickLifecycleForTest() // pending → running

				return instances[0].ID
			},
			op:      "start",
			wantErr: true,
		},
		{
			// stop an already-stopped instance must fail
			name: "stop_stopped_instance",
			setup: func(b *ec2.InMemoryBackend) string {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				if err != nil {
					return ""
				}
				b.TickLifecycleForTest() // pending → running

				id := instances[0].ID
				_, _ = b.StopInstances([]string{id})
				b.TickLifecycleForTest() // stopping → stopped

				return id
			},
			op:      "stop",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.instanceID

			if tt.setup != nil {
				id = tt.setup(b)
			}

			if tt.op == "stop" {
				changes, err := b.StopInstances([]string{id})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.Len(t, changes, 1)
				assert.Equal(t, tt.wantState, changes[0].CurrentState.Name)
			} else {
				changes, err := b.StartInstances([]string{id})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.Len(t, changes, 1)
				assert.Equal(t, tt.wantState, changes[0].CurrentState.Name)
			}
		})
	}
}

func TestRebootInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		instanceID string
		wantErr    bool
	}{
		{
			name:    "reboot_existing",
			wantErr: false,
		},
		{
			name:       "reboot_nonexistent",
			instanceID: "i-doesnotexist",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.instanceID

			if id == "" {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				id = instances[0].ID
			}

			err := b.RebootInstances([]string{id})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDescribeInstanceStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runCount  int
		filterIDs bool
		wantCount int
	}{
		{name: "all_instances", runCount: 2, filterIDs: false, wantCount: 2},
		{name: "filtered_by_id", runCount: 2, filterIDs: true, wantCount: 1},
		{name: "empty", runCount: 0, filterIDs: false, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var firstID string

			for range tt.runCount {
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				if firstID == "" {
					firstID = instances[0].ID
				}
			}

			var ids []string
			if tt.filterIDs && firstID != "" {
				ids = []string{firstID}
			}

			statuses := b.DescribeInstanceStatus(ids)
			assert.Len(t, statuses, tt.wantCount)
		})
	}
}

// ---- Handler tests ----

func TestRunInstancesPrivateIP(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.NotEmpty(t, instances[0].PrivateIP, "instance should have a private IP assigned")

	enis := b.DescribeNetworkInterfaces(nil)
	assert.NotEmpty(t, enis, "ENI should be created with the instance")
	assert.Equal(t, instances[0].PrivateIP, enis[0].PrivateIP)
	assert.Equal(t, instances[0].ID, enis[0].InstanceID)
}
