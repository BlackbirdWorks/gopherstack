package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListEvaluationFormAIVersions(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "aiversions")

	tests := []struct {
		name     string
		inst     string
		kind     types.ContactInteractionType
		code     string
		max      *int32
		versions []string
	}{
		{
			name: "agent", inst: inst, kind: types.ContactInteractionTypeAgent,
			versions: []string{"AGENT_EVALUATION_2025-11-12", "AGENT_EVALUATION_2025-09-01"},
		},
		{name: "automated", inst: inst, kind: types.ContactInteractionTypeAutomated},
		{
			name:     "paged",
			inst:     inst,
			kind:     types.ContactInteractionTypeAgent,
			max:      aws.Int32(1),
			versions: []string{"AGENT_EVALUATION_2025-11-12"},
		},
		{name: "badtype", inst: inst, kind: "BOGUS", code: "InvalidParameterException"},
		{
			name: "toomany",
			inst: inst,
			kind: types.ContactInteractionTypeAgent,
			max:  aws.Int32(101),
			code: "InvalidParameterException",
		},
		{
			name: "noinstance",
			inst: "missing",
			kind: types.ContactInteractionTypeAgent,
			code: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := f.client.ListEvaluationFormAIVersions(t.Context(), &sdk.ListEvaluationFormAIVersionsInput{
				InstanceId: aws.String(tt.inst), ContactInteractionType: tt.kind, MaxResults: tt.max,
			})
			if tt.code != "" {
				assert.Equal(t, tt.code, apiCode(err))

				return
			}

			require.NoError(t, err)

			got := []string{}
			for _, s := range out.AIVersionSummaries {
				got = append(got, aws.ToString(s.AIVersionName))
			}

			assert.Equal(t, append([]string{}, tt.versions...), got)

			if tt.max != nil {
				assert.NotNil(t, out.NextToken)
			}
		})
	}

	t.Run("lifecycle", func(t *testing.T) {
		t.Parallel()

		out, err := f.client.ListEvaluationFormAIVersions(t.Context(), &sdk.ListEvaluationFormAIVersionsInput{
			InstanceId: aws.String(inst), ContactInteractionType: types.ContactInteractionTypeAgent,
		})
		require.NoError(t, err)
		require.Len(t, out.AIVersionSummaries, 2)

		active, old := out.AIVersionSummaries[0].AIVersionLifecycle, out.AIVersionSummaries[1].AIVersionLifecycle
		assert.Equal(t, types.EvaluationFormAIVersionStatusActive, active.Status)
		assert.Nil(t, active.EndOfLifeTime)
		assert.Equal(t, 2025, active.StartOfLifeTime.UTC().Year())
		assert.Equal(t, types.EvaluationFormAIVersionStatusDeprecated, old.Status)
		require.NotNil(t, old.EndOfLifeTime)
		assert.Equal(t, 12, int(old.EndOfLifeTime.UTC().Month()))
	})
}
