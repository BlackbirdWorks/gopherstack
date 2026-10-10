package medialive_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

type fakeClock struct{ ns atomic.Int64 }

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func newDelayedClient(t *testing.T, delay time.Duration) (*medialivesdk.Client, *fakeClock) {
	t.Helper()

	h := newTestHandler(t)
	b := h.Backend.(*medialive.InMemoryBackend)
	clk := &fakeClock{}
	clk.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	medialive.SetNow(b, clk.now)
	b.SetLifecycleDelay(delay)

	return newTestMediaLiveClient(t, h), clk
}

func TestChannelLifecycleDelay(t *testing.T) {
	t.Parallel()

	const delay = 30 * time.Second

	client, clk := newDelayedClient(t, delay)
	ctx := t.Context()

	state := func(id *string) string {
		out, err := client.DescribeChannel(ctx, &medialivesdk.DescribeChannelInput{ChannelId: id})
		require.NoError(t, err)

		return string(out.State)
	}

	created, err := client.CreateChannel(ctx, &medialivesdk.CreateChannelInput{
		Name: aws.String("lc"), EncoderSettings: minimalValidEncoderSettings(),
	})
	require.NoError(t, err)
	id := created.Channel.Id
	assert.Equal(t, types.ChannelStateCreating, created.Channel.State)
	assert.Equal(t, "CREATING", state(id))

	_, err = client.StartChannel(ctx, &medialivesdk.StartChannelInput{ChannelId: id})
	require.Error(t, err, "start while creating")

	clk.advance(delay)
	assert.Equal(t, "IDLE", state(id))

	started, err := client.StartChannel(ctx, &medialivesdk.StartChannelInput{ChannelId: id})
	require.NoError(t, err)
	assert.Equal(t, types.ChannelStateStarting, started.State)
	assert.Equal(t, "STARTING", state(id))

	_, err = client.DeleteChannel(ctx, &medialivesdk.DeleteChannelInput{ChannelId: id})
	require.Error(t, err, "delete while starting")

	clk.advance(delay)
	assert.Equal(t, "RUNNING", state(id))

	stopped, err := client.StopChannel(ctx, &medialivesdk.StopChannelInput{ChannelId: id})
	require.NoError(t, err)
	assert.Equal(t, types.ChannelStateStopping, stopped.State)
	assert.Equal(t, "STOPPING", state(id))

	clk.advance(delay)
	assert.Equal(t, "IDLE", state(id))

	deleted, err := client.DeleteChannel(ctx, &medialivesdk.DeleteChannelInput{ChannelId: id})
	require.NoError(t, err)
	assert.Equal(t, types.ChannelStateDeleting, deleted.State)
	assert.Equal(t, "DELETING", state(id))

	clk.advance(delay)
	assert.Equal(t, "DELETED", state(id))

	_, err = client.StartChannel(ctx, &medialivesdk.StartChannelInput{ChannelId: id})
	require.Error(t, err, "start on deleted channel")
}

func TestChannelWaitersWithoutDelay(t *testing.T) {
	t.Parallel()

	client := newTestMediaLiveClient(t, newTestHandler(t))
	ctx := t.Context()

	created, err := client.CreateChannel(ctx, &medialivesdk.CreateChannelInput{
		Name: aws.String("w"), EncoderSettings: minimalValidEncoderSettings(),
	})
	require.NoError(t, err)

	in := &medialivesdk.DescribeChannelInput{ChannelId: created.Channel.Id}
	require.NoError(t, medialivesdk.NewChannelCreatedWaiter(client).Wait(ctx, in, time.Minute))

	_, err = client.StartChannel(ctx, &medialivesdk.StartChannelInput{ChannelId: created.Channel.Id})
	require.NoError(t, err)
	require.NoError(t, medialivesdk.NewChannelRunningWaiter(client).Wait(ctx, in, time.Minute))

	_, err = client.StopChannel(ctx, &medialivesdk.StopChannelInput{ChannelId: created.Channel.Id})
	require.NoError(t, err)
	require.NoError(t, medialivesdk.NewChannelStoppedWaiter(client).Wait(ctx, in, time.Minute))

	_, err = client.DeleteChannel(ctx, &medialivesdk.DeleteChannelInput{ChannelId: created.Channel.Id})
	require.NoError(t, err)
	require.NoError(t, medialivesdk.NewChannelDeletedWaiter(client).Wait(ctx, in, time.Minute))
}

func TestMultiplexLifecycleDelay(t *testing.T) {
	t.Parallel()

	const delay = time.Minute

	client, clk := newDelayedClient(t, delay)
	ctx := t.Context()

	created, err := client.CreateMultiplex(ctx, &medialivesdk.CreateMultiplexInput{
		Name:              aws.String("mx"),
		RequestId:         aws.String("r1"),
		AvailabilityZones: []string{"us-east-1a", "us-east-1b"},
		MultiplexSettings: &types.MultiplexSettings{
			TransportStreamBitrate: aws.Int32(1000000), TransportStreamId: aws.Int32(1),
			TransportStreamReservedBitrate: aws.Int32(1000),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.MultiplexStateCreating, created.Multiplex.State)

	in := &medialivesdk.DescribeMultiplexInput{MultiplexId: created.Multiplex.Id}
	d, err := client.DescribeMultiplex(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, types.MultiplexStateCreating, d.State)

	clk.advance(delay)
	d, err = client.DescribeMultiplex(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, types.MultiplexStateIdle, d.State)

	_, err = client.StartMultiplex(ctx, &medialivesdk.StartMultiplexInput{MultiplexId: created.Multiplex.Id})
	require.NoError(t, err)

	d, err = client.DescribeMultiplex(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, types.MultiplexStateStarting, d.State)

	clk.advance(delay)
	d, err = client.DescribeMultiplex(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, types.MultiplexStateRunning, d.State)
}

func TestCreateValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(c *medialivesdk.Client) error
		name string
	}{
		{func(c *medialivesdk.Client) error {
			_, err := c.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
				Name: aws.String("x"), ChannelClass: types.ChannelClass("BOGUS"),
			})

			return err
		}, "channel_class"},
		{func(c *medialivesdk.Client) error {
			_, err := c.CreateInput(t.Context(), &medialivesdk.CreateInputInput{
				Name: aws.String("x"), Type: types.InputType("BOGUS"),
			})

			return err
		}, "input_type"},
		{func(c *medialivesdk.Client) error {
			_, err := c.CreateInputSecurityGroup(t.Context(), &medialivesdk.CreateInputSecurityGroupInput{
				WhitelistRules: []types.InputWhitelistRuleCidr{{Cidr: aws.String("not-a-cidr")}},
			})

			return err
		}, "whitelist_cidr"},
		{func(c *medialivesdk.Client) error {
			_, err := c.CreateMultiplex(t.Context(), &medialivesdk.CreateMultiplexInput{
				Name: aws.String("x"), RequestId: aws.String("r"), AvailabilityZones: []string{"us-east-1a"},
				MultiplexSettings: &types.MultiplexSettings{
					TransportStreamBitrate: aws.Int32(1), TransportStreamId: aws.Int32(1),
				},
			})

			return err
		}, "multiplex_azs"},
		{func(c *medialivesdk.Client) error {
			_, err := c.ListChannels(t.Context(), &medialivesdk.ListChannelsInput{MaxResults: aws.Int32(1001)})

			return err
		}, "max_results"},
		{func(c *medialivesdk.Client) error {
			_, err := c.ListInputs(t.Context(), &medialivesdk.ListInputsInput{NextToken: aws.String("%%bad")})

			return err
		}, "bad_token"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.run(newTestMediaLiveClient(t, newTestHandler(t)))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "BadRequestException")
		})
	}
}
