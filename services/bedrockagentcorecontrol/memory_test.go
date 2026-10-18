package bedrockagentcorecontrol_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStrategies(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	created, err := f.client.CreateMemory(ctx, &sdk.CreateMemoryInput{
		Name: aws.String("chat_memory"), EventExpiryDuration: aws.Int32(30),
		MemoryStrategies: []types.MemoryStrategyInput{
			&types.MemoryStrategyInputMemberSemanticMemoryStrategy{Value: types.SemanticMemoryStrategyInput{
				Name: aws.String("facts"), NamespaceTemplates: []string{"/facts/{actorId}"},
			}},
			&types.MemoryStrategyInputMemberSummaryMemoryStrategy{Value: types.SummaryMemoryStrategyInput{
				Name: aws.String("summaries"),
			}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Memory)
	assert.Equal(t, types.MemoryStatusCreating, created.Memory.Status)
	assert.Contains(t, aws.ToString(created.Memory.Id), "chat_memory-")
	require.Len(t, created.Memory.Strategies, 2)
	assert.Equal(t, types.MemoryStrategyTypeSemantic, created.Memory.Strategies[0].Type)
	assert.Equal(t, types.MemoryStrategyTypeSummarization, created.Memory.Strategies[1].Type)

	id := created.Memory.Id

	_, err = f.client.CreateMemory(
		ctx,
		&sdk.CreateMemoryInput{Name: aws.String("chat_memory"), EventExpiryDuration: aws.Int32(30)},
	)
	assert.Equal(t, "ConflictException", apiCode(err))

	got, err := f.client.GetMemory(ctx, &sdk.GetMemoryInput{MemoryId: id})
	require.NoError(t, err)
	assert.Equal(t, types.MemoryStatusActive, got.Memory.Status)
	assert.Equal(t, int32(30), aws.ToInt32(got.Memory.EventExpiryDuration))

	first := got.Memory.Strategies[0].StrategyId

	upd, err := f.client.UpdateMemory(ctx, &sdk.UpdateMemoryInput{
		MemoryId: id, Description: aws.String("d"),
		MemoryStrategies: &types.ModifyMemoryStrategies{
			DeleteMemoryStrategies: []types.DeleteMemoryStrategyInput{
				{MemoryStrategyId: got.Memory.Strategies[1].StrategyId},
			},
			ModifyMemoryStrategies: []types.ModifyMemoryStrategyInput{
				{MemoryStrategyId: first, Description: aws.String("modified")},
			},
			AddMemoryStrategies: []types.MemoryStrategyInput{
				&types.MemoryStrategyInputMemberUserPreferenceMemoryStrategy{
					Value: types.UserPreferenceMemoryStrategyInput{
						Name: aws.String("prefs"),
					},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.MemoryStatusUpdating, upd.Memory.Status)

	got, err = f.client.GetMemory(ctx, &sdk.GetMemoryInput{MemoryId: id})
	require.NoError(t, err)
	require.Len(t, got.Memory.Strategies, 2)
	assert.Equal(t, "modified", aws.ToString(got.Memory.Strategies[0].Description))
	assert.Equal(t, types.MemoryStrategyTypeUserPreference, got.Memory.Strategies[1].Type)

	_, err = f.client.UpdateMemory(ctx, &sdk.UpdateMemoryInput{
		MemoryId: id,
		MemoryStrategies: &types.ModifyMemoryStrategies{
			DeleteMemoryStrategies: []types.DeleteMemoryStrategyInput{{MemoryStrategyId: aws.String("nope")}},
		},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err := f.client.ListMemories(ctx, &sdk.ListMemoriesInput{})
	require.NoError(t, err)
	require.Len(t, list.Memories, 1)

	del, err := f.client.DeleteMemory(ctx, &sdk.DeleteMemoryInput{MemoryId: id})
	require.NoError(t, err)
	assert.Equal(t, types.MemoryStatusDeleting, del.Status)
}

func TestMemoryValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	tests := []struct {
		name     string
		in       *sdk.CreateMemoryInput
		wantCode string
	}{
		{
			"expiry too small",
			&sdk.CreateMemoryInput{Name: aws.String("m1"), EventExpiryDuration: aws.Int32(3)},
			"ValidationException",
		},
		{
			"expiry too large",
			&sdk.CreateMemoryInput{Name: aws.String("m2"), EventExpiryDuration: aws.Int32(400)},
			"ValidationException",
		},
		{
			"bad name",
			&sdk.CreateMemoryInput{Name: aws.String("has space"), EventExpiryDuration: aws.Int32(30)},
			"ValidationException",
		},
		{"ok", &sdk.CreateMemoryInput{Name: aws.String("m3"), EventExpiryDuration: aws.Int32(7)}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.CreateMemory(t.Context(), tt.in)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}
