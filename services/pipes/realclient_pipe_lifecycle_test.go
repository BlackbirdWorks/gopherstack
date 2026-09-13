package pipes_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	pipestypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

const testAsyncWait = 2 * time.Second
const testAsyncPoll = 10 * time.Millisecond

// TestRealClient_PipeLifecycle drives pipes' typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_PipeLifecycle(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "tags",
			run: func(t *testing.T) {
				t.Helper()

				h := pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-east-1"))
				client := newTestPipesClient(t, h)
				ctx := t.Context()

				created, err := client.CreatePipe(ctx, &pipessdk.CreatePipeInput{
					Name:    aws.String("s15-tags-pipe"),
					RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
					Source:  aws.String("arn:aws:sqs:us-east-1:123456789012:s15-source"),
					Target:  aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn"),
					Tags:    map[string]string{"env": "test"},
				})
				require.NoError(t, err)
				arn := aws.ToString(created.Arn)
				require.NotEmpty(t, arn)

				_, err = client.TagResource(ctx, &pipessdk.TagResourceInput{
					ResourceArn: aws.String(arn),
					Tags:        map[string]string{"team": "platform"},
				})
				require.NoError(t, err)

				listOut, err := client.ListTagsForResource(ctx, &pipessdk.ListTagsForResourceInput{
					ResourceArn: aws.String(arn),
				})
				require.NoError(t, err)
				assert.Equal(t, map[string]string{"env": "test", "team": "platform"}, listOut.Tags)

				_, err = client.UntagResource(ctx, &pipessdk.UntagResourceInput{
					ResourceArn: aws.String(arn),
					TagKeys:     []string{"team"},
				})
				require.NoError(t, err)

				afterOut, err := client.ListTagsForResource(ctx, &pipessdk.ListTagsForResourceInput{
					ResourceArn: aws.String(arn),
				})
				require.NoError(t, err)
				assert.Equal(t, map[string]string{"env": "test"}, afterOut.Tags)
			},
		},
		{
			name: "start stop delete and list",
			run: func(t *testing.T) {
				t.Helper()

				h := pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-east-1"))
				client := newTestPipesClient(t, h)
				ctx := t.Context()

				_, err := client.CreatePipe(ctx, &pipessdk.CreatePipeInput{
					Name:         aws.String("s15-lifecycle-pipe"),
					RoleArn:      aws.String("arn:aws:iam::123456789012:role/r"),
					Source:       aws.String("arn:aws:sqs:us-east-1:123456789012:s15-source"),
					Target:       aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn"),
					DesiredState: pipestypes.RequestedPipeStateStopped,
				})
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					d, descErr := client.DescribePipe(
						ctx,
						&pipessdk.DescribePipeInput{Name: aws.String("s15-lifecycle-pipe")},
					)

					return descErr == nil && d.CurrentState == pipestypes.PipeStateStopped
				}, testAsyncWait, testAsyncPoll)

				listOut, err := client.ListPipes(ctx, &pipessdk.ListPipesInput{})
				require.NoError(t, err)
				names := map[string]bool{}
				for _, p := range listOut.Pipes {
					names[aws.ToString(p.Name)] = true
				}
				assert.True(t, names["s15-lifecycle-pipe"])

				startOut, err := client.StartPipe(ctx, &pipessdk.StartPipeInput{Name: aws.String("s15-lifecycle-pipe")})
				require.NoError(t, err)
				assert.Equal(t, pipestypes.RequestedPipeStateRunning, startOut.DesiredState)

				require.Eventually(t, func() bool {
					d, descErr := client.DescribePipe(
						ctx,
						&pipessdk.DescribePipeInput{Name: aws.String("s15-lifecycle-pipe")},
					)

					return descErr == nil && d.CurrentState == pipestypes.PipeStateRunning
				}, testAsyncWait, testAsyncPoll)

				stopOut, err := client.StopPipe(ctx, &pipessdk.StopPipeInput{Name: aws.String("s15-lifecycle-pipe")})
				require.NoError(t, err)
				assert.Equal(t, pipestypes.RequestedPipeStateStopped, stopOut.DesiredState)

				require.Eventually(t, func() bool {
					d, descErr := client.DescribePipe(
						ctx,
						&pipessdk.DescribePipeInput{Name: aws.String("s15-lifecycle-pipe")},
					)

					return descErr == nil && d.CurrentState == pipestypes.PipeStateStopped
				}, testAsyncWait, testAsyncPoll)

				_, err = client.DeletePipe(ctx, &pipessdk.DeletePipeInput{Name: aws.String("s15-lifecycle-pipe")})
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					_, descErr := client.DescribePipe(
						ctx,
						&pipessdk.DescribePipeInput{Name: aws.String("s15-lifecycle-pipe")},
					)

					return descErr != nil
				}, testAsyncWait, testAsyncPoll)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
