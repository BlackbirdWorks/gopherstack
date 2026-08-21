package pipes_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	pipestypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestCreatePipe_BatchEnvironmentRoundTrip drives CreatePipe and GetPipe
// through the real aws-sdk-go-v2 pipes client. BatchContainerOverrides.
// Environment is a list of {Name, Value} objects on the real shape
// (types.go), reused unchanged by both serializers.go (request) and
// deserializers.go (response) -- gopherstack previously modeled it as a
// bare map, which fails every real client's CreatePipe request decode
// outright the moment an environment variable override is set, and would
// equally break GetPipe's response decode once only the request side was
// fixed.
func TestCreatePipe_BatchEnvironmentRoundTrip(t *testing.T) {
	t.Parallel()

	b := pipes.NewInMemoryBackend("123456789012", "us-east-1")
	h := pipes.NewHandler(b)
	client := newTestPipesClient(t, h)

	name := "batch-env-pipe"

	_, err := client.CreatePipe(t.Context(), &pipessdk.CreatePipeInput{
		Name:    aws.String(name),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
		Source:  aws.String("arn:aws:sqs:us-east-1:123456789012:q"),
		Target:  aws.String("arn:aws:batch:us-east-1:123456789012:job-queue/q"),
		TargetParameters: &pipestypes.PipeTargetParameters{
			BatchJobParameters: &pipestypes.PipeTargetBatchJobParameters{
				JobDefinition: aws.String("arn:aws:batch:us-east-1:123456789012:job-definition/jd:1"),
				JobName:       aws.String("job"),
				ContainerOverrides: &pipestypes.BatchContainerOverrides{
					Environment: []pipestypes.BatchEnvironmentVariable{
						{Name: aws.String("ENV"), Value: aws.String("prod")},
					},
				},
			},
		},
	})
	require.NoError(t, err, "real SDK client's CreatePipe request must decode without error")

	out, err := client.DescribePipe(t.Context(), &pipessdk.DescribePipeInput{Name: aws.String(name)})
	require.NoError(t, err, "real SDK client must decode DescribePipe response without error")
	require.NotNil(t, out.TargetParameters)
	require.NotNil(t, out.TargetParameters.BatchJobParameters)
	require.NotNil(t, out.TargetParameters.BatchJobParameters.ContainerOverrides)
	env := out.TargetParameters.BatchJobParameters.ContainerOverrides.Environment
	require.Len(t, env, 1)
	assert.Equal(t, "ENV", aws.ToString(env[0].Name))
	assert.Equal(t, "prod", aws.ToString(env[0].Value))
}
