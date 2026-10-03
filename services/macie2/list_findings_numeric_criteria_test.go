package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestListFindings_NumericOperators(t *testing.T) {
	t.Parallel()

	const score = 2

	tests := []struct {
		name string
		prop string
		cond types.CriterionAdditionalProperties
		want int
	}{
		{
			name: "score_gt_equal_excluded",
			prop: "severity.score",
			cond: types.CriterionAdditionalProperties{Gt: aws.Int64(score)},
			want: 0,
		},
		{
			name: "score_gte_equal_included",
			prop: "severity.score",
			cond: types.CriterionAdditionalProperties{Gte: aws.Int64(score)},
			want: 1,
		},
		{
			name: "score_lt_equal_excluded",
			prop: "severity.score",
			cond: types.CriterionAdditionalProperties{Lt: aws.Int64(score)},
			want: 0,
		},
		{
			name: "score_lte_equal_included",
			prop: "severity.score",
			cond: types.CriterionAdditionalProperties{Lte: aws.Int64(score)},
			want: 1,
		},
		{
			name: "score_range",
			prop: "severity.score",
			cond: types.CriterionAdditionalProperties{Gt: aws.Int64(score - 1), Lt: aws.Int64(score + 1)},
			want: 1,
		},
		{
			name: "created_far_future_gt",
			prop: "createdAt",
			cond: types.CriterionAdditionalProperties{Gt: aws.Int64(4102444800000)},
			want: 0,
		},
		{
			name: "created_epoch_gte",
			prop: "createdAt",
			cond: types.CriterionAdditionalProperties{Gte: aws.Int64(0)},
			want: 1,
		},
		{name: "count_gt_one", prop: "count", cond: types.CriterionAdditionalProperties{Gt: aws.Int64(1)}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMacie2SDKClient(
				t,
				macie2.NewHandler(macie2.NewInMemoryBackend("000000000000", "us-east-1")),
			)

			_, err := client.CreateSampleFindings(t.Context(), &macie2sdk.CreateSampleFindingsInput{
				FindingTypes: []types.FindingType{types.FindingTypeSensitiveDataS3ObjectPersonal},
			})
			require.NoError(t, err)

			out, err := client.ListFindings(t.Context(), &macie2sdk.ListFindingsInput{
				FindingCriteria: &types.FindingCriteria{
					Criterion: map[string]types.CriterionAdditionalProperties{tt.prop: tt.cond},
				},
			})
			require.NoError(t, err)
			assert.Len(t, out.FindingIds, tt.want)
		})
	}
}
