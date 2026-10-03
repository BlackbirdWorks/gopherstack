package bedrock_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ListFoundationModelsFilters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		input   bedrocksdk.ListFoundationModelsInput
		errCode string
		want    []string
	}{
		{
			name:  "provider",
			input: bedrocksdk.ListFoundationModelsInput{ByProvider: aws.String("Anthropic")},
			want:  []string{"anthropic.claude-3-sonnet-20240229-v1:0", "anthropic.claude-v2"},
		},
		{
			name:  "output_modality",
			input: bedrocksdk.ListFoundationModelsInput{ByOutputModality: types.ModelModalityEmbedding},
			want:  []string{"amazon.titan-embed-text-v1"},
		},
		{
			name: "customization_and_provider",
			input: bedrocksdk.ListFoundationModelsInput{
				ByCustomizationType: types.ModelCustomizationFineTuning,
				ByProvider:          aws.String("Meta"),
			},
			want: []string{"meta.llama3-8b-instruct-v1:0"},
		},
		{
			name:  "no_match",
			input: bedrocksdk.ListFoundationModelsInput{ByProvider: aws.String("Nobody")},
			want:  []string{},
		},
		{
			name:    "invalid_enum",
			input:   bedrocksdk.ListFoundationModelsInput{ByInferenceType: "BOGUS"},
			errCode: "ValidationException",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			out, err := client.ListFoundationModels(t.Context(), &tt.input)

			if tt.errCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.errCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)

			got := make([]string, 0, len(out.ModelSummaries))
			for _, m := range out.ModelSummaries {
				got = append(got, aws.ToString(m.ModelId))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestRealClient_ListSortParamValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		sortBy types.SortModelsBy
		order  types.SortOrder
		valid  bool
	}{
		{name: "valid", sortBy: "CreationTime", order: types.SortOrderAscending, valid: true},
		{name: "bad_sort_by", sortBy: "Name", order: types.SortOrderAscending},
		{name: "bad_order", sortBy: "CreationTime", order: "Sideways"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.ListCustomModels(t.Context(), &bedrocksdk.ListCustomModelsInput{
				SortBy: tt.sortBy, SortOrder: tt.order,
			})

			if tt.valid {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}

func TestRealClient_ListEvaluationJobsSortOrder(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		order types.SortOrder
		want  []string
	}{
		{name: "ascending", order: types.SortOrderAscending, want: []string{"job-a", "job-b", "job-c"}},
		{name: "descending", order: types.SortOrderDescending, want: []string{"job-c", "job-b", "job-a"}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestBedrockClient(t, h)

			for _, n := range []string{"job-a", "job-b", "job-c"} {
				rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": n})
				require.Less(t, rec.Code, 300)
			}

			out, err := client.ListEvaluationJobs(t.Context(), &bedrocksdk.ListEvaluationJobsInput{
				SortBy: types.SortJobsByCreationTime, SortOrder: tt.order,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.JobSummaries))
			for _, j := range out.JobSummaries {
				got = append(got, aws.ToString(j.JobName))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRealClient_ListAutomatedReasoningPoliciesVersionsAndPaging(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)

	arns := make([]string, 0, 3)

	for _, n := range []string{"arp-a", "arp-b", "arp-c"} {
		p, err := client.CreateAutomatedReasoningPolicy(
			t.Context(), &bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String(n)},
		)
		require.NoError(t, err)

		arns = append(arns, aws.ToString(p.PolicyArn))
	}

	versionArns := make([]string, 0, 2)

	for range 2 {
		v, err := client.CreateAutomatedReasoningPolicyVersion(
			t.Context(),
			&bedrocksdk.CreateAutomatedReasoningPolicyVersionInput{
				PolicyArn:                 aws.String(arns[0]),
				LastUpdatedDefinitionHash: aws.String(""),
			},
		)
		require.NoError(t, err)

		versionArns = append(versionArns, aws.ToString(v.PolicyArn))
	}

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		var names []string

		var token *string

		for {
			out, err := client.ListAutomatedReasoningPolicies(
				t.Context(),
				&bedrocksdk.ListAutomatedReasoningPoliciesInput{MaxResults: aws.Int32(2), NextToken: token},
			)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(out.AutomatedReasoningPolicySummaries), 2)

			for _, s := range out.AutomatedReasoningPolicySummaries {
				names = append(names, aws.ToString(s.Name))
			}

			if token = out.NextToken; token == nil {
				break
			}
		}

		assert.Equal(t, []string{"arp-a", "arp-b", "arp-c"}, names)
	})

	t.Run("policy_arn_lists_versions", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListAutomatedReasoningPolicies(
			t.Context(),
			&bedrocksdk.ListAutomatedReasoningPoliciesInput{PolicyArn: aws.String(arns[0])},
		)
		require.NoError(t, err)

		got := make([]string, 0, len(out.AutomatedReasoningPolicySummaries))
		for _, s := range out.AutomatedReasoningPolicySummaries {
			got = append(got, aws.ToString(s.PolicyArn))
		}

		assert.Equal(t, versionArns, got)
	})

	t.Run("unknown_policy_arn", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListAutomatedReasoningPolicies(
			t.Context(),
			&bedrocksdk.ListAutomatedReasoningPoliciesInput{PolicyArn: aws.String(arns[0] + "-missing")},
		)

		var nf *types.ResourceNotFoundException
		require.ErrorAs(t, err, &nf)
	})
}
