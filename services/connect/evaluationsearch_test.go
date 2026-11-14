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

func TestSearchContactEvaluations(t *testing.T) {
	t.Parallel()

	e := newEvalEnv(t, true)
	inst := aws.String(e.inst)
	done := e.start(t)

	_, setupErr := e.f.client.SubmitContactEvaluation(t.Context(), &sdk.SubmitContactEvaluationInput{
		InstanceId: inst, EvaluationId: done.EvaluationId,
		Answers: map[string]types.EvaluationAnswerInput{"greet": str("yes"), "wait": num(30), "comment": str("ok")},
	})
	require.NoError(t, setupErr)

	draft := e.start(t)
	past := e.f.clock.now().Add(-time.Hour)
	future := e.f.clock.now().Add(time.Hour)

	search := func(in *sdk.SearchContactEvaluationsInput) (*sdk.SearchContactEvaluationsOutput, error) {
		in.InstanceId = inst

		return e.f.client.SearchContactEvaluations(t.Context(), in)
	}

	str1 := func(field, value string) *types.EvaluationSearchCriteria {
		return &types.EvaluationSearchCriteria{StringCondition: &types.StringCondition{
			FieldName: aws.String(field), Value: aws.String(value), ComparisonType: types.StringComparisonTypeExact,
		}}
	}

	tests := []struct {
		crit   *types.EvaluationSearchCriteria
		filter *types.EvaluationSearchFilter
		name   string
		code   string
		want   []string
	}{
		{name: "all", want: []string{aws.ToString(draft.EvaluationId), aws.ToString(done.EvaluationId)}},
		{
			name: "byid",
			crit: str1("EvaluationId", aws.ToString(done.EvaluationId)),
			want: []string{aws.ToString(done.EvaluationId)},
		},
		{name: "bystatus", crit: str1("Status", "DRAFT"), want: []string{aws.ToString(draft.EvaluationId)}},
		{
			name: "number", want: []string{aws.ToString(done.EvaluationId)},
			crit: &types.EvaluationSearchCriteria{NumberCondition: &types.NumberCondition{
				FieldName: aws.String(
					"EarnedPoints",
				),
				MinValue:       aws.Int32(10),
				ComparisonType: types.NumberComparisonTypeGreaterOrEqual,
			}},
		},
		{
			name: "decimal", want: []string{aws.ToString(done.EvaluationId)},
			crit: &types.EvaluationSearchCriteria{DecimalCondition: &types.DecimalCondition{
				FieldName: aws.String(
					"ScorePercentage",
				),
				MinValue:       aws.Float64(99.5),
				ComparisonType: types.DecimalComparisonTypeGreater,
			}},
		},
		{
			name: "bool", want: []string{aws.ToString(draft.EvaluationId), aws.ToString(done.EvaluationId)},
			crit: &types.EvaluationSearchCriteria{BooleanCondition: &types.BooleanCondition{
				FieldName: aws.String("AutoEvaluationEnabled"), ComparisonType: types.BooleanComparisonTypeIsFalse,
			}},
		},
		{
			name: "datefuture",
			crit: &types.EvaluationSearchCriteria{DateTimeCondition: &types.DateTimeCondition{
				FieldName: aws.String("LastModifiedTime"), MinValue: aws.String(future.Format(time.RFC3339)),
				ComparisonType: types.DateTimeComparisonTypeGreaterThan,
			}},
		},
		{
			name: "datepast", want: []string{aws.ToString(draft.EvaluationId), aws.ToString(done.EvaluationId)},
			crit: &types.EvaluationSearchCriteria{DateTimeCondition: &types.DateTimeCondition{
				FieldName: aws.String("CreatedTime"), MinValue: aws.String(past.Format(time.RFC3339)),
				ComparisonType: types.DateTimeComparisonTypeGreaterThan,
			}},
		},
		{
			name: "or", want: []string{aws.ToString(draft.EvaluationId), aws.ToString(done.EvaluationId)},
			crit: &types.EvaluationSearchCriteria{OrConditions: []types.EvaluationSearchCriteria{
				*str1("Status", "DRAFT"), *str1("Status", "SUBMITTED"),
			}},
		},
		{
			name: "and", want: []string{aws.ToString(done.EvaluationId)},
			crit: &types.EvaluationSearchCriteria{AndConditions: []types.EvaluationSearchCriteria{
				*str1("Status", "SUBMITTED"), *str1("EvaluationFormTitle", "QA"),
			}},
		},
		{
			name: "tagfilter", want: []string{aws.ToString(draft.EvaluationId), aws.ToString(done.EvaluationId)},
			filter: &types.EvaluationSearchFilter{AttributeFilter: &types.ControlPlaneAttributeFilter{
				TagCondition: &types.TagCondition{TagKey: aws.String("batch"), TagValue: aws.String("1")},
			}},
		},
		{
			name: "tagmiss",
			filter: &types.EvaluationSearchFilter{AttributeFilter: &types.ControlPlaneAttributeFilter{
				TagCondition: &types.TagCondition{TagKey: aws.String("batch"), TagValue: aws.String("2")},
			}},
		},
		{
			name: "attrmiss",
			filter: &types.EvaluationSearchFilter{
				ContactEvaluationAttributeFilter: &types.ContactEvaluationAttributeFilter{
					ContactEvaluationAttributeCondition: &types.ContactEvaluationAttributeCondition{
						AttributeKey:   types.ContactEvaluationAttributeKeyContactAgentId,
						AttributeValue: &types.ContactEvaluationAttributeValue{StringValue: aws.String("nobody")},
						ComparisonType: types.ContactEvaluationAttributeComparisonTypeExact,
					},
				},
			},
		},
		{name: "badfield", crit: str1("Nope", "x"), code: "InvalidParameterException"},
		{
			name: "nobound", code: "InvalidParameterException",
			crit: &types.EvaluationSearchCriteria{NumberCondition: &types.NumberCondition{
				FieldName: aws.String("EarnedPoints"), ComparisonType: types.NumberComparisonTypeEqual,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, serr := search(&sdk.SearchContactEvaluationsInput{SearchCriteria: tt.crit, SearchFilter: tt.filter})
			if tt.code != "" {
				assert.Equal(t, tt.code, apiCode(serr))

				return
			}

			require.NoError(t, serr)

			got := []string{}
			for _, s := range out.EvaluationSearchSummaryList {
				got = append(got, aws.ToString(s.EvaluationId))
			}

			assert.ElementsMatch(t, tt.want, got)
			assert.EqualValues(t, len(tt.want), aws.ToInt64(out.ApproximateTotalCount))
		})
	}

	t.Run("summary", func(t *testing.T) {
		t.Parallel()

		out, err := search(
			&sdk.SearchContactEvaluationsInput{SearchCriteria: str1("EvaluationId", aws.ToString(done.EvaluationId))},
		)
		require.NoError(t, err)
		require.Len(t, out.EvaluationSearchSummaryList, 1)

		s := out.EvaluationSearchSummaryList[0]
		assert.Equal(t, types.EvaluationStatusSubmitted, s.Status)
		assert.Equal(t, "QA", aws.ToString(s.EvaluationFormTitle))
		assert.Equal(t, e.contact, aws.ToString(s.Metadata.ContactId))
		assert.InDelta(t, 100, s.Metadata.ScorePercentage, 0.001)
		assert.EqualValues(t, 10, aws.ToInt32(s.Metadata.EarnedPoints))
		assert.Equal(t, map[string]string{"batch": "1"}, s.Tags)
	})

	t.Run("paging", func(t *testing.T) {
		t.Parallel()

		first, err := search(&sdk.SearchContactEvaluationsInput{MaxResults: aws.Int32(1)})
		require.NoError(t, err)
		require.Len(t, first.EvaluationSearchSummaryList, 1)
		require.NotNil(t, first.NextToken)

		second, err := search(&sdk.SearchContactEvaluationsInput{MaxResults: aws.Int32(1), NextToken: first.NextToken})
		require.NoError(t, err)
		require.Len(t, second.EvaluationSearchSummaryList, 1)
		assert.NotEqual(
			t,
			first.EvaluationSearchSummaryList[0].EvaluationId,
			second.EvaluationSearchSummaryList[0].EvaluationId,
		)
	})

	t.Run("maxresults", func(t *testing.T) {
		t.Parallel()

		_, err := search(&sdk.SearchContactEvaluationsInput{MaxResults: aws.Int32(101)})
		assert.Equal(t, "InvalidParameterException", apiCode(err))
	})

	t.Run("persisted", func(t *testing.T) {
		t.Parallel()

		r := e.f.restored(t)
		out, err := r.client.SearchContactEvaluations(t.Context(), &sdk.SearchContactEvaluationsInput{InstanceId: inst})
		require.NoError(t, err)
		assert.Len(t, out.EvaluationSearchSummaryList, 2)
	})
}
