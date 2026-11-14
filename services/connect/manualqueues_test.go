package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListRoutingProfileManualAssignmentQueues(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "manual")
	queue, _, _ := f.defaultQueue(t, inst)

	rp, err := f.client.CreateRoutingProfile(t.Context(), &sdk.CreateRoutingProfileInput{
		InstanceId: aws.String(inst), Name: aws.String("Manual"), Description: aws.String("d"),
		DefaultOutboundQueueId: aws.String(queue),
		MediaConcurrencies:     []types.MediaConcurrency{{Channel: types.ChannelTask, Concurrency: aws.Int32(1)}},
		QueueConfigs: []types.RoutingProfileQueueConfig{{
			Delay: aws.Int32(0), Priority: aws.Int32(1),
			QueueReference: &types.RoutingProfileQueueReference{QueueId: aws.String(queue), Channel: types.ChannelTask},
		}},
		ManualAssignmentQueueConfigs: []types.RoutingProfileManualAssignmentQueueConfig{{
			QueueReference: &types.RoutingProfileQueueReference{QueueId: aws.String(queue), Channel: types.ChannelTask},
		}},
	})
	require.NoError(t, err)

	out, err := f.client.ListRoutingProfileManualAssignmentQueues(t.Context(),
		&sdk.ListRoutingProfileManualAssignmentQueuesInput{
			InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId,
		})
	require.NoError(t, err)
	require.Len(t, out.RoutingProfileManualAssignmentQueueConfigSummaryList, 1)

	s := out.RoutingProfileManualAssignmentQueueConfigSummaryList[0]
	assert.Equal(t, queue, aws.ToString(s.QueueId))
	assert.Equal(t, types.ChannelTask, s.Channel)
	assert.NotEmpty(t, aws.ToString(s.QueueName))
	assert.Contains(t, aws.ToString(s.QueueArn), queue)
	assert.NotNil(t, out.LastModifiedTime)
	assert.Equal(t, "us-east-1", aws.ToString(out.LastModifiedRegion))

	_, err = f.client.ListRoutingProfileManualAssignmentQueues(t.Context(),
		&sdk.ListRoutingProfileManualAssignmentQueuesInput{
			InstanceId: aws.String(inst), RoutingProfileId: aws.String("nope"),
		})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
