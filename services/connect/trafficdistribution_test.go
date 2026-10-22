package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrafficDistributionGroupLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "tdg"))
	_, _, profile := f.defaultQueue(t, aws.ToString(inst))
	user := f.newUser(t, aws.ToString(inst), "ada", profile)

	created, err := f.client.CreateTrafficDistributionGroup(t.Context(), &sdk.CreateTrafficDistributionGroupInput{
		InstanceId: inst, Name: aws.String("failover"), Description: aws.String("d"), Tags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	_, err = f.client.CreateTrafficDistributionGroup(t.Context(), &sdk.CreateTrafficDistributionGroupInput{
		InstanceId: inst, Name: aws.String("FAILOVER"),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.CreateTrafficDistributionGroup(t.Context(), &sdk.CreateTrafficDistributionGroupInput{
		InstanceId: aws.String("missing"), Name: aws.String("other"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	got, err := f.client.DescribeTrafficDistributionGroup(t.Context(), &sdk.DescribeTrafficDistributionGroupInput{
		TrafficDistributionGroupId: created.Arn,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TrafficDistributionGroupStatusCreationInProgress, got.TrafficDistributionGroup.Status)
	assert.Equal(t, "v", got.TrafficDistributionGroup.Tags["k"])

	update := &sdk.UpdateTrafficDistributionInput{
		Id: created.Id,
		TelephonyConfig: &types.TelephonyConfig{Distributions: []types.Distribution{
			{Region: aws.String("us-east-1"), Percentage: 60}, {Region: aws.String("eu-west-1"), Percentage: 40},
		}},
	}
	_, err = f.client.UpdateTrafficDistribution(t.Context(), update)
	assert.Equal(t, "ResourceNotReadyException", apiCode(err))

	f.clock.advance(2 * time.Second)

	update.TelephonyConfig.Distributions[1].Percentage = 30
	_, err = f.client.UpdateTrafficDistribution(t.Context(), update)
	assert.Equal(t, "InvalidParameterException", apiCode(err), "sum is not 100")

	update.TelephonyConfig.Distributions[1].Percentage = 40
	_, err = f.client.UpdateTrafficDistribution(t.Context(), update)
	require.NoError(t, err)

	dist, err := f.client.GetTrafficDistribution(t.Context(), &sdk.GetTrafficDistributionInput{Id: created.Id})
	require.NoError(t, err)
	assert.Len(t, dist.TelephonyConfig.Distributions, 2)
	assert.Len(t, dist.AgentConfig.Distributions, 1)
	assert.True(t, dist.SignInConfig.Distributions[0].Enabled)

	_, err = f.client.AssociateTrafficDistributionGroupUser(
		t.Context(),
		&sdk.AssociateTrafficDistributionGroupUserInput{
			TrafficDistributionGroupId: created.Id, InstanceId: inst, UserId: aws.String("ghost"),
		},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	for range 2 {
		_, err = f.client.AssociateTrafficDistributionGroupUser(
			t.Context(),
			&sdk.AssociateTrafficDistributionGroupUserInput{
				TrafficDistributionGroupId: created.Id, InstanceId: inst, UserId: user.UserId,
			},
		)
		require.NoError(t, err)
	}

	users, err := f.client.ListTrafficDistributionGroupUsers(t.Context(), &sdk.ListTrafficDistributionGroupUsersInput{
		TrafficDistributionGroupId: created.Id,
	})
	require.NoError(t, err)
	require.Len(t, users.TrafficDistributionGroupUserSummaryList, 1)

	_, err = f.client.DeleteTrafficDistributionGroup(
		t.Context(),
		&sdk.DeleteTrafficDistributionGroupInput{TrafficDistributionGroupId: created.Id},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	dst := f.restored(t)
	users, err = dst.client.ListTrafficDistributionGroupUsers(t.Context(), &sdk.ListTrafficDistributionGroupUsersInput{
		TrafficDistributionGroupId: created.Id,
	})
	require.NoError(t, err)
	assert.Len(t, users.TrafficDistributionGroupUserSummaryList, 1)

	list, err := f.client.ListTrafficDistributionGroups(
		t.Context(),
		&sdk.ListTrafficDistributionGroupsInput{InstanceId: inst},
	)
	require.NoError(t, err)
	require.Len(t, list.TrafficDistributionGroupSummaryList, 1)
	assert.Equal(t, types.TrafficDistributionGroupStatusActive, list.TrafficDistributionGroupSummaryList[0].Status)

	_, err = f.client.DisassociateTrafficDistributionGroupUser(
		t.Context(),
		&sdk.DisassociateTrafficDistributionGroupUserInput{
			TrafficDistributionGroupId: created.Id, InstanceId: inst, UserId: user.UserId,
		},
	)
	require.NoError(t, err)
	_, err = f.client.DisassociateTrafficDistributionGroupUser(
		t.Context(),
		&sdk.DisassociateTrafficDistributionGroupUserInput{
			TrafficDistributionGroupId: created.Id, InstanceId: inst, UserId: user.UserId,
		},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteTrafficDistributionGroup(
		t.Context(),
		&sdk.DeleteTrafficDistributionGroupInput{TrafficDistributionGroupId: created.Id},
	)
	require.NoError(t, err)
	_, err = f.client.DescribeTrafficDistributionGroup(
		t.Context(),
		&sdk.DescribeTrafficDistributionGroupInput{TrafficDistributionGroupId: created.Id},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
