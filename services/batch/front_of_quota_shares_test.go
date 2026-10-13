package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetJobQueueSnapshot_FrontOfQuotaShares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantShare map[string]int
		name      string
		runnable  []int
	}{
		{name: "none runnable", runnable: nil, wantShare: map[string]int{}},
		{name: "one per share", runnable: []int{0, 2}, wantShare: map[string]int{"a": 0, "b": 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t)
			ctx := t.Context()

			shares := []string{"a", "a", "b"}
			ids := make([]string, len(shares))
			arns := make([]string, len(shares))

			for i, share := range shares {
				out, err := env.client.SubmitServiceJob(ctx, &batchsdk.SubmitServiceJobInput{
					JobName:               aws.String("sj"),
					JobQueue:              aws.String("q"),
					ServiceJobType:        types.ServiceJobTypeSagemakerTraining,
					ServiceRequestPayload: aws.String(`{}`),
					QuotaShareName:        aws.String(share),
				})
				require.NoError(t, err)

				ids[i] = aws.ToString(out.JobId)
				arns[i] = aws.ToString(out.JobArn)
			}

			for _, i := range tt.runnable {
				env.bk.SetServiceJobStatusForTest(ids[i], "RUNNABLE")
			}

			snap, err := env.client.GetJobQueueSnapshot(
				ctx,
				&batchsdk.GetJobQueueSnapshotInput{JobQueue: aws.String("q")},
			)
			require.NoError(t, err)
			require.NotNil(t, snap.FrontOfQuotaShares)
			assert.Len(t, snap.FrontOfQuotaShares.QuotaShares, len(tt.wantShare))

			for share, idx := range tt.wantShare {
				jobs := snap.FrontOfQuotaShares.QuotaShares[share]
				require.Len(t, jobs, 1)
				assert.Equal(t, arns[idx], aws.ToString(jobs[0].JobArn))
			}
		})
	}
}
