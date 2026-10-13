package appstream_test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	appstreamtypes "github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

func newClockedBackend(delay time.Duration) (*appstream.InMemoryBackend, *atomic.Int64) {
	var offset atomic.Int64

	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	b := appstream.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetClock(func() time.Time { return start.Add(time.Duration(offset.Load())) })
	b.SetStateTransitionDelay(delay)

	return b, &offset
}

func TestFleetLifecycle_InterimStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStarting appstreamtypes.FleetState
		wantStopping appstreamtypes.FleetState
		delay        time.Duration
	}{
		{
			name: "no delay", wantStarting: appstreamtypes.FleetStateRunning,
			wantStopping: appstreamtypes.FleetStateStopped,
		},
		{
			name: "delayed", delay: time.Minute,
			wantStarting: appstreamtypes.FleetStateStarting, wantStopping: appstreamtypes.FleetStateStopping,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, offset := newClockedBackend(tt.delay)
			client := newTestAppStreamClient(t, appstream.NewHandler(b))
			ctx := t.Context()

			_, err := client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
				Name: aws.String("f"), InstanceType: aws.String("stream.standard.medium"),
				ComputeCapacity: &appstreamtypes.ComputeCapacity{DesiredInstances: aws.Int32(2)},
			})
			require.NoError(t, err)

			state := func() *appstreamtypes.Fleet {
				out, descErr := client.DescribeFleets(ctx, &appstreamsdk.DescribeFleetsInput{Names: []string{"f"}})
				require.NoError(t, descErr)

				return &out.Fleets[0]
			}

			_, err = client.StartFleet(ctx, &appstreamsdk.StartFleetInput{Name: aws.String("f")})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStarting, state().State)

			_, err = client.DeleteFleet(ctx, &appstreamsdk.DeleteFleetInput{Name: aws.String("f")})
			var inUse *appstreamtypes.ResourceInUseException
			require.ErrorAs(t, err, &inUse)

			offset.Store(int64(2 * time.Minute))
			f := state()
			assert.Equal(t, appstreamtypes.FleetStateRunning, f.State)
			assert.Equal(t, int32(2), aws.ToInt32(f.ComputeCapacityStatus.Running))
			assert.Equal(t, int32(2), aws.ToInt32(f.ComputeCapacityStatus.Available))

			_, err = client.StopFleet(ctx, &appstreamsdk.StopFleetInput{Name: aws.String("f")})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStopping, state().State)

			offset.Store(int64(4 * time.Minute))
			f = state()
			assert.Equal(t, appstreamtypes.FleetStateStopped, f.State)
			assert.Equal(t, int32(0), aws.ToInt32(f.ComputeCapacityStatus.Running))
		})
	}
}

func TestImageBuilderLifecycle_PendingUntilDelayElapses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  appstreamtypes.ImageBuilderState
		delay time.Duration
	}{
		{name: "no delay", want: appstreamtypes.ImageBuilderStateRunning},
		{name: "delayed", delay: time.Minute, want: appstreamtypes.ImageBuilderStatePending},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, offset := newClockedBackend(tt.delay)
			client := newTestAppStreamClient(t, appstream.NewHandler(b))
			ctx := t.Context()

			out, err := client.CreateImageBuilder(ctx, &appstreamsdk.CreateImageBuilderInput{
				Name: aws.String("ib"), InstanceType: aws.String("stream.standard.medium"),
				ImageName: aws.String("base"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.ImageBuilder.State)

			offset.Store(int64(2 * time.Minute))

			got, err := client.DescribeImageBuilders(ctx,
				&appstreamsdk.DescribeImageBuildersInput{Names: []string{"ib"}})
			require.NoError(t, err)
			assert.Equal(t, appstreamtypes.ImageBuilderStateRunning, got.ImageBuilders[0].State)
		})
	}
}

func TestCreateFleet_RangeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxUser      *int32
		disconnect   *int32
		idle         *int32
		name         string
		instanceType string
		desired      int32
		wantErr      bool
	}{
		{name: "valid", instanceType: "stream.standard.medium", desired: 1, maxUser: aws.Int32(600)},
		{name: "bad instance type", instanceType: "m5.large", desired: 1, wantErr: true},
		{name: "negative capacity", instanceType: "stream.standard.medium", desired: -1, wantErr: true},
		{name: "max duration low", instanceType: "stream.standard.medium", desired: 1,
			maxUser: aws.Int32(10), wantErr: true},
		{name: "max duration high", instanceType: "stream.standard.medium", desired: 1,
			maxUser: aws.Int32(432001), wantErr: true},
		{name: "disconnect low", instanceType: "stream.standard.medium", desired: 1,
			disconnect: aws.Int32(30), wantErr: true},
		{name: "idle ok zero", instanceType: "stream.standard.medium", desired: 1, idle: aws.Int32(0)},
		{name: "idle low", instanceType: "stream.standard.medium", desired: 1, idle: aws.Int32(30), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestAppStreamClient(t, appstream.NewHandler(backend))

			_, err := client.CreateFleet(t.Context(), &appstreamsdk.CreateFleetInput{
				Name: aws.String("rv"), InstanceType: aws.String(tt.instanceType),
				ComputeCapacity: &appstreamtypes.ComputeCapacity{
					DesiredInstances: aws.Int32(tt.desired),
				},
				MaxUserDurationInSeconds:       tt.maxUser,
				DisconnectTimeoutInSeconds:     tt.disconnect,
				IdleDisconnectTimeoutInSeconds: tt.idle,
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var ip *appstreamtypes.InvalidParameterCombinationException
			require.ErrorAs(t, err, &ip)
			assert.False(
				t,
				strings.HasPrefix(aws.ToString(ip.Message), "InvalidParameterCombinationException"),
				ip.Message,
			)
		})
	}
}
