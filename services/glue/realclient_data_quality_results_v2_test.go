package glue_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func publishDQV2(
	t *testing.T,
	client *gluesdk.Client,
	score float64,
	evalCtx string,
	dims map[string]string,
	token string,
) string {
	t.Helper()

	in := &gluesdk.PublishDataQualityResultV2Input{
		Score:             aws.Float64(score),
		EvaluationContext: aws.String(evalCtx),
		Dimensions:        dims,
		RuleResults: []types.DataQualityRuleResult{{
			Name:             aws.String("r1"),
			EvaluatedRule:    aws.String("IsComplete \"id\""),
			Result:           types.DataQualityRuleResultStatusPass,
			EvaluatedMetrics: map[string]float64{"Completeness.id": 1},
		}},
		AggregatedMetrics:  &types.DataQualityAggregatedMetrics{TotalRulesPassed: aws.Float64(1)},
		RulesetName:        aws.String("rs1"),
		Tags:               map[string]string{"k": "v"},
		Descriptors:        map[string]string{"d": "e"},
		ExecutionStartedOn: aws.Time(time.Unix(1700000000, 0)),
	}
	if token != "" {
		in.ClientToken = aws.String(token)
	}

	out, err := client.PublishDataQualityResultV2(t.Context(), in)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.ResultId))

	return aws.ToString(out.ResultId)
}

func TestRealClient_DataQualityResultsV2(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, client *gluesdk.Client, backend *glue.InMemoryBackend)
		name string
	}{
		{name: "publish_get_roundtrip", fn: func(t *testing.T, client *gluesdk.Client, _ *glue.InMemoryBackend) {
			t.Helper()
			id := publishDQV2(t, client, 0.75, "ctx1", map[string]string{"team": "a"}, "")

			got, err := client.GetDataQualityResultV2(t.Context(), &gluesdk.GetDataQualityResultV2Input{
				ResultId: aws.String(id),
			})
			require.NoError(t, err)
			assert.Equal(t, id, aws.ToString(got.ResultId))
			assert.InDelta(t, 0.75, aws.ToFloat64(got.Score), 1e-9)
			assert.Equal(t, "ctx1", aws.ToString(got.EvaluationContext))
			assert.Equal(t, "rs1", aws.ToString(got.RulesetName))
			assert.Equal(t, map[string]string{"team": "a"}, got.Dimensions)
			assert.Equal(t, map[string]string{"k": "v"}, got.Tags)
			assert.Equal(t, map[string]string{"d": "e"}, got.Descriptors)
			require.Len(t, got.RuleResults, 1)
			assert.Equal(t, types.DataQualityRuleResultStatusPass, got.RuleResults[0].Result)
			assert.InDelta(t, 1.0, got.RuleResults[0].EvaluatedMetrics["Completeness.id"], 1e-9)
			assert.InDelta(t, 1.0, aws.ToFloat64(got.AggregatedMetrics.TotalRulesPassed), 1e-9)
			require.NotNil(t, got.ResultPublishedOn)
			assert.WithinDuration(t, time.Now(), *got.ResultPublishedOn, time.Minute)
			require.NotNil(t, got.ExecutionStartedOn)
			assert.Equal(t, int64(1700000000), got.ExecutionStartedOn.Unix())
			assert.Nil(t, got.ExecutionCompletedOn)
		}},
		{name: "get_not_found", fn: func(t *testing.T, client *gluesdk.Client, _ *glue.InMemoryBackend) {
			t.Helper()
			_, err := client.GetDataQualityResultV2(t.Context(), &gluesdk.GetDataQualityResultV2Input{
				ResultId: aws.String("dqresult-missing"),
			})
			var ae smithy.APIError
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, "EntityNotFoundException", ae.ErrorCode())
		}},
		{name: "publish_invalid_status", fn: func(t *testing.T, client *gluesdk.Client, _ *glue.InMemoryBackend) {
			t.Helper()
			_, err := client.PublishDataQualityResultV2(t.Context(), &gluesdk.PublishDataQualityResultV2Input{
				Score:             aws.Float64(1),
				EvaluationContext: aws.String("c"),
				Dimensions:        map[string]string{},
				RuleResults:       []types.DataQualityRuleResult{{Result: "BOGUS"}},
			})
			var ae smithy.APIError
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, "InvalidInputException", ae.ErrorCode())
		}},
		{name: "batch_get_found_and_missing", fn: func(t *testing.T, client *gluesdk.Client, _ *glue.InMemoryBackend) {
			t.Helper()
			id := publishDQV2(t, client, 0.5, "c", map[string]string{}, "")

			out, err := client.BatchGetDataQualityResultV2(t.Context(), &gluesdk.BatchGetDataQualityResultV2Input{
				ResultIds: []string{id, "nope"},
			})
			require.NoError(t, err)
			require.Len(t, out.Results, 1)
			assert.Equal(t, id, aws.ToString(out.Results[0].ResultId))
			assert.Equal(t, []string{"nope"}, out.ResultsNotFound)
		}},
		{name: "client_token_idempotent", fn: func(t *testing.T, client *gluesdk.Client, _ *glue.InMemoryBackend) {
			t.Helper()
			a := publishDQV2(t, client, 0.5, "c", map[string]string{}, "tok")
			b := publishDQV2(t, client, 0.9, "c", map[string]string{}, "tok")
			assert.Equal(t, a, b)

			out, err := client.ListDataQualityResultsV2(t.Context(), &gluesdk.ListDataQualityResultsV2Input{})
			require.NoError(t, err)
			assert.Len(t, out.Results, 1)
		}},
		{name: "list_pagination_and_filter", fn: func(t *testing.T, client *gluesdk.Client, _ *glue.InMemoryBackend) {
			t.Helper()
			ids := map[string]bool{}
			for range 3 {
				ids[publishDQV2(t, client, 1, "ctx-a", map[string]string{"team": "a", "env": "p"}, "")] = true
			}
			other := publishDQV2(t, client, 1, "ctx-b", map[string]string{"team": "b"}, "")

			var seen []string
			var next *string
			pages := 0
			for {
				out, err := client.ListDataQualityResultsV2(t.Context(), &gluesdk.ListDataQualityResultsV2Input{
					MaxResults: aws.Int32(3),
					NextToken:  next,
				})
				require.NoError(t, err)
				pages++
				for _, r := range out.Results {
					seen = append(seen, aws.ToString(r.ResultId))
					assert.NotNil(t, r.ResultPublishedOn)
				}
				if out.NextToken == nil {
					break
				}
				next = out.NextToken
			}
			assert.Equal(t, 2, pages)
			assert.Len(t, seen, 4)
			assert.Contains(t, seen, other)

			byCtx, err := client.ListDataQualityResultsV2(t.Context(), &gluesdk.ListDataQualityResultsV2Input{
				Filter: &types.DataQualityResultsV2FilterCriteria{EvaluationContext: aws.String("ctx-a")},
			})
			require.NoError(t, err)
			assert.Len(t, byCtx.Results, 3)

			byDim, err := client.ListDataQualityResultsV2(t.Context(), &gluesdk.ListDataQualityResultsV2Input{
				Filter: &types.DataQualityResultsV2FilterCriteria{Dimensions: map[string]string{"team": "b"}},
			})
			require.NoError(t, err)
			require.Len(t, byDim.Results, 1)
			assert.Equal(t, other, aws.ToString(byDim.Results[0].ResultId))

			future, err := client.ListDataQualityResultsV2(t.Context(), &gluesdk.ListDataQualityResultsV2Input{
				Filter: &types.DataQualityResultsV2FilterCriteria{PublishedAfter: aws.Time(time.Now().Add(time.Hour))},
			})
			require.NoError(t, err)
			assert.Empty(t, future.Results)

			past, err := client.ListDataQualityResultsV2(t.Context(), &gluesdk.ListDataQualityResultsV2Input{
				Filter: &types.DataQualityResultsV2FilterCriteria{PublishedAfter: aws.Time(time.Now().Add(-time.Hour))},
			})
			require.NoError(t, err)
			assert.Len(t, past.Results, 4)

			ds, err := client.ListDataQualityResultsV2(t.Context(), &gluesdk.ListDataQualityResultsV2Input{
				Filter: &types.DataQualityResultsV2FilterCriteria{
					DataSource: &types.DataSource{GlueTable: &types.GlueTable{
						DatabaseName: aws.String("d"), TableName: aws.String("t"),
					}},
				},
			})
			require.NoError(t, err)
			assert.Empty(t, ds.Results)
		}},
		{name: "v1_v2_consistency", fn: func(t *testing.T, client *gluesdk.Client, backend *glue.InMemoryBackend) {
			t.Helper()
			id := publishDQV2(t, client, 0.6, "c", map[string]string{}, "")
			backend.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "v1-only", Score: 0.4})

			v1, err := client.GetDataQualityResult(t.Context(), &gluesdk.GetDataQualityResultInput{ResultId: aws.String(id)})
			require.NoError(t, err)
			assert.InDelta(t, 0.6, aws.ToFloat64(v1.Score), 1e-9)

			v1list, err := client.ListDataQualityResults(t.Context(), &gluesdk.ListDataQualityResultsInput{})
			require.NoError(t, err)
			assert.Len(t, v1list.Results, 2)

			v2, err := client.GetDataQualityResultV2(t.Context(), &gluesdk.GetDataQualityResultV2Input{
				ResultId: aws.String("v1-only"),
			})
			require.NoError(t, err)
			assert.InDelta(t, 0.4, aws.ToFloat64(v2.Score), 1e-9)

			v2list, err := client.ListDataQualityResultsV2(t.Context(), &gluesdk.ListDataQualityResultsV2Input{})
			require.NoError(t, err)
			assert.Len(t, v2list.Results, 2)
		}},
		{name: "persistence_roundtrip", fn: func(t *testing.T, client *gluesdk.Client, backend *glue.InMemoryBackend) {
			t.Helper()
			id := publishDQV2(t, client, 0.8, "c", map[string]string{"x": "y"}, "tok")

			snap := backend.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			restored := glue.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, restored.Restore(t.Context(), snap))

			c2 := newTestGlueClient(t, glue.NewHandler(restored))
			got, err := c2.GetDataQualityResultV2(t.Context(), &gluesdk.GetDataQualityResultV2Input{ResultId: aws.String(id)})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"x": "y"}, got.Dimensions)
			require.NotNil(t, got.ResultPublishedOn)

			again := publishDQV2(t, c2, 0.1, "c", map[string]string{}, "tok")
			assert.Equal(t, id, again)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			tc.fn(t, newTestGlueClient(t, glue.NewHandler(backend)), backend)
		})
	}
}
