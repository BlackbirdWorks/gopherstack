package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTokenTestQueue(t *testing.T, client *batchsdk.Client) string {
	t.Helper()

	_, err := client.CreateComputeEnvironment(t.Context(), &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String("ce"), Type: types.CETypeManaged,
	})
	require.NoError(t, err)

	_, err = client.CreateJobQueue(t.Context(), &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String("q"), Priority: aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String("ce")},
		},
	})
	require.NoError(t, err)

	return "q"
}

func TestSubmitServiceJob_ClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		secondToken string
		secondBody  string
		wantSame    bool
		wantErr     bool
	}{
		{name: "same_token_same_payload", secondToken: "tok", secondBody: `{"a":1}`, wantSame: true},
		{name: "same_token_other_payload", secondToken: "tok", secondBody: `{"a":2}`, wantErr: true},
		{name: "other_token", secondToken: "tok-2", secondBody: `{"a":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			queue := newTokenTestQueue(t, client)

			submit := func(token, payload string) (*batchsdk.SubmitServiceJobOutput, error) {
				return client.SubmitServiceJob(t.Context(), &batchsdk.SubmitServiceJobInput{
					JobName: aws.String("sj"), JobQueue: aws.String(queue), ClientToken: aws.String(token),
					ServiceJobType: types.ServiceJobTypeSagemakerTraining, ServiceRequestPayload: aws.String(payload),
				})
			}

			first, err := submit("tok", `{"a":1}`)
			require.NoError(t, err)

			second, err := submit(tt.secondToken, tt.secondBody)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, aws.ToString(first.JobId) == aws.ToString(second.JobId))

			list, err := client.ListServiceJobs(t.Context(), &batchsdk.ListServiceJobsInput{
				JobQueue: aws.String(queue), JobStatus: types.ServiceJobStatusSubmitted,
			})
			require.NoError(t, err)

			want := 2
			if tt.wantSame {
				want = 1
			}

			assert.Len(t, list.JobSummaryList, want)
		})
	}
}

func TestUpdateConsumableResource_ClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		token     string
		wantTotal int64
	}{
		{name: "same_token_applies_once", token: "tok", wantTotal: 15},
		{name: "distinct_tokens_apply_twice", wantTotal: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)

			_, err := client.CreateConsumableResource(t.Context(), &batchsdk.CreateConsumableResourceInput{
				ConsumableResourceName: aws.String("cr"), ResourceType: aws.String("REPLENISHABLE"),
				TotalQuantity: aws.Int64(10),
			})
			require.NoError(t, err)

			var last *batchsdk.UpdateConsumableResourceOutput

			for i, tok := range []string{"a", "b"} {
				if tt.token != "" {
					tok = tt.token
				}

				last, err = client.UpdateConsumableResource(t.Context(), &batchsdk.UpdateConsumableResourceInput{
					ConsumableResource: aws.String("cr"), Operation: aws.String("ADD"), Quantity: aws.Int64(5),
					ClientToken: aws.String(tok),
				})
				require.NoError(t, err, "attempt %d", i)
			}

			assert.Equal(t, tt.wantTotal, aws.ToInt64(last.TotalQuantity))
		})
	}
}
