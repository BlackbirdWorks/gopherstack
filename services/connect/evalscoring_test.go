package connect_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scoreQ struct {
	failTarget string
	scores     []int32
	weight     float64
	pick       int
	failOption int
	excluded   bool
	na         bool
}

type scoreSec struct {
	ref    string
	qs     map[string]scoreQ
	order  []string
	weight float64
}

type wantScore struct {
	category   types.PerformanceCategoryName
	pct        float64
	weight     float64
	autoFail   bool
	notApplic  bool
	hasWeight  bool
	checkExtra bool
}

func (q scoreQ) item(ref string) types.EvaluationFormItem {
	opts := make([]types.EvaluationFormSingleSelectQuestionOption, 0, len(q.scores))

	for i, s := range q.scores {
		o := types.EvaluationFormSingleSelectQuestionOption{
			RefId: aws.String("o" + strconv.Itoa(i)), Text: aws.String("opt"), Score: s,
		}
		if q.failOption > 0 && i == q.failOption {
			o.AutomaticFail = true
			if q.failTarget != "" {
				o.AutomaticFailConfiguration = &types.AutomaticFailConfiguration{
					TargetSection: aws.String(q.failTarget),
				}
			}
		}

		opts = append(opts, o)
	}

	return &types.EvaluationFormItemMemberQuestion{Value: types.EvaluationFormQuestion{
		RefId: aws.String(ref), Title: aws.String(ref), QuestionType: types.EvaluationFormQuestionTypeSingleselect,
		Weight: q.weight, NotApplicableEnabled: q.na,
		ScoringConfiguration: &types.EvaluationFormQuestionScoringConfiguration{IsExcludedFromScoring: q.excluded},
		QuestionTypeProperties: &types.EvaluationFormQuestionTypePropertiesMemberSingleSelect{
			Value: types.EvaluationFormSingleSelectQuestionProperties{Options: opts},
		},
	}}
}

func scoringItems(secs []scoreSec) ([]types.EvaluationFormItem, map[string]types.EvaluationAnswerInput) {
	items := make([]types.EvaluationFormItem, 0, len(secs))
	answers := map[string]types.EvaluationAnswerInput{}

	for _, s := range secs {
		inner := make([]types.EvaluationFormItem, 0, len(s.order))

		for _, ref := range s.order {
			q := s.qs[ref]
			inner = append(inner, q.item(ref))

			if q.na {
				answers[ref] = types.EvaluationAnswerInput{
					Value: &types.EvaluationAnswerDataMemberNotApplicable{Value: true},
				}
			} else {
				answers[ref] = str("o" + strconv.Itoa(q.pick))
			}
		}

		items = append(items, &types.EvaluationFormItemMemberSection{Value: types.EvaluationFormSection{
			RefId: aws.String(s.ref), Title: aws.String(s.ref), Items: inner, Weight: s.weight,
		}})
	}

	return items, answers
}

func TestEvaluationScoringStrategies(t *testing.T) {
	t.Parallel()

	thresholds := []types.EvaluationFormScoreThreshold{
		{
			PerformanceCategory: types.PerformanceCategoryNameNeedsImprovement,
			MinScorePercentage:  0, MaxScorePercentage: 69.99,
		},
		{
			PerformanceCategory: types.PerformanceCategoryNameExceedsExpectations,
			MinScorePercentage:  70, MaxScorePercentage: 100,
		},
	}

	tests := []struct {
		want    map[string]wantScore
		name    string
		mode    types.EvaluationFormScoringMode
		secs    []scoreSec
		overall wantScore
		useThr  bool
	}{
		{
			name: "question weights weigh each question by its share",
			mode: types.EvaluationFormScoringModeQuestionOnly, useThr: true,
			secs: []scoreSec{{ref: "s", order: []string{"q1", "q2"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{0, 5}, weight: 60, pick: 1},
				"q2": {scores: []int32{0, 10}, weight: 40, pick: 0},
			}}},
			overall: wantScore{pct: 60, category: types.PerformanceCategoryNameNeedsImprovement},
			want: map[string]wantScore{
				"q1": {pct: 100, hasWeight: true, weight: 60, checkExtra: true},
				"q2": {pct: 0, hasWeight: true, weight: 40, checkExtra: true},
				"s":  {pct: 60},
			},
		},
		{
			name: "a not applicable question hands its weight to the others",
			mode: types.EvaluationFormScoringModeQuestionOnly, useThr: true,
			secs: []scoreSec{{ref: "s", order: []string{"q1", "q2"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{0, 5}, weight: 60, pick: 1},
				"q2": {scores: []int32{0, 10}, weight: 40, na: true},
			}}},
			overall: wantScore{pct: 100, category: types.PerformanceCategoryNameExceedsExpectations},
			want: map[string]wantScore{
				"q1": {pct: 100, hasWeight: true, weight: 100, checkExtra: true},
				"q2": {notApplic: true, checkExtra: true},
			},
		},
		{
			name: "section weights weigh each section",
			mode: types.EvaluationFormScoringModeSectionOnly,
			secs: []scoreSec{
				{
					ref:    "a",
					weight: 70,
					order:  []string{"q1"},
					qs:     map[string]scoreQ{"q1": {scores: []int32{0, 5}, pick: 1}},
				},
				{
					ref:    "b",
					weight: 30,
					order:  []string{"q2"},
					qs:     map[string]scoreQ{"q2": {scores: []int32{0, 10}, pick: 0}},
				},
			},
			overall: wantScore{pct: 70},
			want: map[string]wantScore{
				"a": {pct: 100, hasWeight: true, weight: 70, checkExtra: true},
				"b": {pct: 0, hasWeight: true, weight: 30, checkExtra: true},
			},
		},
		{
			name: "points mode sums points",
			mode: types.EvaluationFormScoringModePointsBased,
			secs: []scoreSec{{ref: "s", order: []string{"q1", "q2"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{0, 5}, weight: 60, pick: 1},
				"q2": {scores: []int32{0, 10}, weight: 40, pick: 0},
			}}},
			overall: wantScore{pct: 100.0 / 3.0},
			want:    map[string]wantScore{"s": {pct: 100.0 / 3.0}},
		},
		{
			name: "an excluded question does not score",
			mode: types.EvaluationFormScoringModePointsBased,
			secs: []scoreSec{{ref: "s", order: []string{"q1", "q2"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{0, 5}, pick: 1},
				"q2": {scores: []int32{0, 10}, pick: 0, excluded: true},
			}}},
			overall: wantScore{pct: 100},
			want:    map[string]wantScore{"s": {pct: 100}},
		},
		{
			name: "an automatic fail with a target section zeroes only that section",
			mode: types.EvaluationFormScoringModePointsBased,
			secs: []scoreSec{
				{ref: "a", order: []string{"q1"}, qs: map[string]scoreQ{
					"q1": {scores: []int32{0, 5, 0}, pick: 2, failOption: 2, failTarget: "a"},
				}},
				{ref: "b", order: []string{"q2"}, qs: map[string]scoreQ{"q2": {scores: []int32{0, 10}, pick: 1}}},
			},
			overall: wantScore{pct: 100.0 * 10 / 15},
			want: map[string]wantScore{
				"a": {pct: 0, autoFail: true, checkExtra: true},
				"b": {pct: 100},
			},
		},
		{
			name: "an automatic fail without a target fails the evaluation",
			mode: types.EvaluationFormScoringModePointsBased,
			secs: []scoreSec{{ref: "a", order: []string{"q1", "q2"}, qs: map[string]scoreQ{
				"q1": {scores: []int32{0, 5, 0}, pick: 2, failOption: 2},
				"q2": {scores: []int32{0, 10}, pick: 1},
			}}},
			overall: wantScore{pct: 0, autoFail: true},
			want:    map[string]wantScore{"a": {pct: 0, autoFail: true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			items, answers := scoringItems(tt.secs)
			strategy := &types.EvaluationFormScoringStrategy{
				Mode:   tt.mode,
				Status: types.EvaluationFormScoringStatusEnabled,
			}

			if tt.useThr {
				strategy.ScoreThresholds = thresholds
			}

			form, err := e.f.client.CreateEvaluationForm(t.Context(), &sdk.CreateEvaluationFormInput{
				InstanceId: aws.String(e.inst), Title: aws.String("QA"), Items: items, ScoringStrategy: strategy,
			})
			require.NoError(t, err)

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

			require.NotNil(t, got.Evaluation.Metadata.Score)
			checkScore(t, "overall", *got.Evaluation.Metadata.Score, tt.overall, true)

			for ref, w := range tt.want {
				s, ok := got.Evaluation.Scores[ref]
				require.True(t, ok, ref)
				checkScore(t, ref, s, w, w.checkExtra)
			}
		})
	}
}

func checkScore(t *testing.T, name string, got types.EvaluationScore, want wantScore, extra bool) {
	t.Helper()

	assert.InDelta(t, want.pct, got.Percentage, 0.001, name+" percentage")
	assert.Equal(t, want.autoFail, got.AutomaticFail, name+" automatic fail")
	assert.Equal(t, want.notApplic, got.NotApplicable, name+" not applicable")

	if !extra {
		return
	}

	assert.Equal(t, want.category, got.PerformanceCategory, name+" category")

	if want.hasWeight {
		require.NotNil(t, got.AppliedWeight, name+" weight")
		assert.InDelta(t, want.weight, *got.AppliedWeight, 0.001, name+" weight")
	} else {
		assert.Nil(t, got.AppliedWeight, name+" weight")
	}
}
