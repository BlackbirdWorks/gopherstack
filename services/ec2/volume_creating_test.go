package ec2_test

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

func TestRealClient_CreateVolumeCreatingState(t *testing.T) {
	t.Parallel()

	const dwell = time.Minute

	tests := []struct {
		name      string
		wantFirst types.VolumeState
		wantThen  types.VolumeState
		delay     time.Duration
		advance   time.Duration
	}{
		{name: "no delay", wantFirst: types.VolumeStateAvailable, wantThen: types.VolumeStateAvailable},
		{
			name: "before deadline", delay: dwell, advance: dwell - time.Second,
			wantFirst: types.VolumeStateCreating, wantThen: types.VolumeStateCreating,
		},
		{
			name: "after deadline", delay: dwell, advance: dwell,
			wantFirst: types.VolumeStateCreating, wantThen: types.VolumeStateAvailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clk := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			backend.SetClock(clk.Now)
			backend.SetVolumeCreateDelay(tt.delay)
			client := newTestEC2Client(t, ec2.NewHandler(backend))

			created, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
				AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(10),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantFirst, created.State)

			state := func() types.VolumeState {
				out, derr := client.DescribeVolumes(t.Context(), &ec2sdk.DescribeVolumesInput{
					VolumeIds: []string{aws.ToString(created.VolumeId)},
				})
				require.NoError(t, derr)
				require.Len(t, out.Volumes, 1)

				return out.Volumes[0].State
			}

			assert.Equal(t, tt.wantFirst, state())

			if tt.wantFirst == types.VolumeStateCreating {
				_, aerr := client.AttachVolume(t.Context(), &ec2sdk.AttachVolumeInput{
					VolumeId:   created.VolumeId,
					InstanceId: aws.String("i-0123456789abcdef0"),
					Device:     aws.String("/dev/sdf"),
				})
				require.ErrorContains(t, aerr, "IncorrectState")
			}

			clk.Advance(tt.advance)
			backend.TickLifecycleForTest()
			assert.Equal(t, tt.wantThen, state())

			if tt.wantThen == types.VolumeStateAvailable {
				w := ec2sdk.NewVolumeAvailableWaiter(client)
				_, werr := w.WaitForOutput(t.Context(), &ec2sdk.DescribeVolumesInput{
					VolumeIds: []string{aws.ToString(created.VolumeId)},
				}, time.Minute)
				require.NoError(t, werr)
			}
		})
	}
}
