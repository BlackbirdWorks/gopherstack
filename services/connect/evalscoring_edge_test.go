package connect_test

import (
	"math"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluationScoringStaysFinite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode types.EvaluationFormScoringMode
		secs []scoreSec
	}{
		{
			name: "question weights that overflow when summed",
			mode: types.EvaluationFormScoringModeQuestionOnly,
			secs: []scoreSec{{ref: "s", order: []string{"q1", "q2"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{0, 5}, weight: math.MaxFloat64, pick: 1},
				"q2": {scores: []int32{0, 5}, weight: math.MaxFloat64, pick: 1},
			}}},
		},
		{
			name: "section weights that overflow when summed",
			mode: types.EvaluationFormScoringModeSectionOnly,
			secs: []scoreSec{
				{ref: "a", weight: math.MaxFloat64, order: []string{"q1"},
					qs: map[string]scoreQ{"q1": {scores: []int32{0, 5}, pick: 1}}},
				{ref: "b", weight: math.MaxFloat64, order: []string{"q2"},
					qs: map[string]scoreQ{"q2": {scores: []int32{0, 5}, pick: 1}}},
			},
		},
		{
			name: "a negative option score",
			mode: types.EvaluationFormScoringModePointsBased,
			secs: []scoreSec{{ref: "s", order: []string{"q1"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{-5, 5}, pick: 0},
			}}},
		},
		{
			name: "zero weights",
			mode: types.EvaluationFormScoringModeQuestionOnly,
			secs: []scoreSec{{ref: "s", order: []string{"q1"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{0, 5}, pick: 1},
			}}},
		},
		{
			name: "only zero-point options",
			mode: types.EvaluationFormScoringModePointsBased,
			secs: []scoreSec{{ref: "s", order: []string{"q1"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{0, 0}, pick: 1},
			}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			items, answers := scoringItems(tt.secs)

			form, err := e.f.client.CreateEvaluationForm(t.Context(), &sdk.CreateEvaluationFormInput{
				InstanceId: aws.String(e.inst), Title: aws.String("QA"), Items: items,
				ScoringStrategy: &types.EvaluationFormScoringStrategy{
					Mode: tt.mode, Status: types.EvaluationFormScoringStatusEnabled,
				},
			})
			if err != nil {
				assert.Equal(t, "InvalidParameterException", apiCode(err))

				return
			}

			contact := e.outbound(t, "+442071230001", true)
			started, err := e.f.client.StartContactEvaluation(t.Context(), &sdk.StartContactEvaluationInput{
				InstanceId: aws.String(e.inst), ContactId: aws.String(contact), EvaluationFormId: form.EvaluationFormId,
			})
			require.NoError(t, err)

			_, err = e.f.client.SubmitContactEvaluation(t.Context(), &sdk.SubmitContactEvaluationInput{
				InstanceId: aws.String(e.inst), EvaluationId: started.EvaluationId, Answers: answers,
			})
			require.NoError(t, err)

			got, err := e.f.client.DescribeContactEvaluation(t.Context(),
				&sdk.DescribeContactEvaluationInput{InstanceId: aws.String(e.inst), EvaluationId: started.EvaluationId})
			require.NoError(t, err)

			all := make([]types.EvaluationScore, 0, len(got.Evaluation.Scores)+1)
			all = append(all, *got.Evaluation.Metadata.Score)

			for _, s := range got.Evaluation.Scores {
				all = append(all, s)
			}

			for _, s := range all {
				assert.False(t, math.IsNaN(s.Percentage) || math.IsInf(s.Percentage, 0))
				assert.GreaterOrEqual(t, s.Percentage, 0.0)
				assert.LessOrEqual(t, s.Percentage, 100.0)
			}
		})
	}
}
