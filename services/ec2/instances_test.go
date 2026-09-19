package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRunInstances_CreditSpecification_RealClient covers gopherstack-xhu2t:
// CreditSpecification.CpuCredits was never read, so
// DescribeInstanceCreditSpecifications always reported the "standard" default
// regardless of the request.
func TestRunInstances_CreditSpecification_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	run, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-test"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		CreditSpecification: &types.CreditSpecificationRequest{
			CpuCredits: aws.String("unlimited"),
		},
	})
	require.NoError(t, err)
	require.Len(t, run.Instances, 1)
	instanceID := aws.ToString(run.Instances[0].InstanceId)

	out, err := client.DescribeInstanceCreditSpecifications(
		t.Context(), &ec2sdk.DescribeInstanceCreditSpecificationsInput{InstanceIds: []string{instanceID}},
	)
	require.NoError(t, err)
	require.Len(t, out.InstanceCreditSpecifications, 1)
	assert.Equal(t, "unlimited", aws.ToString(out.InstanceCreditSpecifications[0].CpuCredits),
		"CreditSpecification.CpuCredits dropped - RunInstances never read it")
}

// TestRunInstances_PrivateDnsNameOptions_RealClient covers gopherstack-xhu2t:
// PrivateDnsNameOptions was never read on RunInstances, and Instance never
// echoed it on DescribeInstances even though ModifyPrivateDnsNameOptions
// already stored it (write-only field).
func TestRunInstances_PrivateDnsNameOptions_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	run, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-test"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		PrivateDnsNameOptions: &types.PrivateDnsNameOptionsRequest{
			HostnameType:                 types.HostnameTypeResourceName,
			EnableResourceNameDnsARecord: aws.Bool(true),
		},
	})
	require.NoError(t, err)
	require.Len(t, run.Instances, 1)
	instanceID := aws.ToString(run.Instances[0].InstanceId)

	out, err := client.DescribeInstances(
		t.Context(), &ec2sdk.DescribeInstancesInput{InstanceIds: []string{instanceID}},
	)
	require.NoError(t, err)
	require.Len(t, out.Reservations, 1)
	require.Len(t, out.Reservations[0].Instances, 1)
	inst := out.Reservations[0].Instances[0]
	require.NotNil(t, inst.PrivateDnsNameOptions, "PrivateDnsNameOptions dropped - RunInstances never read it")
	assert.Equal(t, types.HostnameTypeResourceName, inst.PrivateDnsNameOptions.HostnameType)
	assert.True(t, aws.ToBool(inst.PrivateDnsNameOptions.EnableResourceNameDnsARecord))
}

func TestSendDiagnosticInterrupt(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	require.NoError(t, b.SendDiagnosticInterrupt(instances[0].ID))
	require.ErrorIs(t, b.SendDiagnosticInterrupt("i-missing"), ec2.ErrInstanceNotFound)
	require.ErrorIs(t, b.SendDiagnosticInterrupt(""), ec2.ErrInvalidParameter)
}

func TestRunInstancesCountBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		count     int
		wantCount int
	}{
		{nil, "count below one clamps to one", 0, 1},
		{nil, "count at bound succeeds", 1000, 1000},
		{ec2.ErrResourceCountExceeded, "count above bound errors", 1001, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			instances, err := b.RunInstances("ami-test", "t3.micro", "", tt.count)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Empty(t, instances)

				return
			}

			require.NoError(t, err)
			assert.Len(t, instances, tt.wantCount)
		})
	}
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

func TestSetInstanceLaunchConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(t *testing.T, b *ec2.InMemoryBackend) string
		name           string
		keyName        string
		securityGroups []string
		wantErr        bool
	}{
		{
			name: "sets_key_name_and_security_groups",
			setup: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()
				insts, err := b.RunInstances("ami-test", "t3.micro", "", 1)
				require.NoError(t, err)

				return insts[0].ID
			},
			keyName:        "my-key",
			securityGroups: []string{"sg-123456", "sg-789012"},
			wantErr:        false,
		},
		{
			name: "instance_not_found",
			setup: func(t *testing.T, _ *ec2.InMemoryBackend) string {
				t.Helper()

				return "i-nonexistent"
			},
			keyName: "my-key",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.setup(t, b)

			err := b.SetInstanceLaunchConfig(id, tt.keyName, tt.securityGroups)
			if tt.wantErr {
				require.ErrorIs(t, err, ec2.ErrInstanceNotFound)

				return
			}

			require.NoError(t, err)
			insts := b.DescribeInstances([]string{id}, "")
			require.Len(t, insts, 1)
			assert.Equal(t, tt.keyName, insts[0].KeyName)
			assert.Equal(t, tt.securityGroups, insts[0].SecurityGroups)
		})
	}
}
