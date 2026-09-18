package pipes_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestUpdatePipe_PreservesOmittedTargetAndEnrichment proves the zeroguard
// fix (cmd/zeroguard): UpdatePipeInput.Target and .Enrichment are plain
// strings in the real SDK's request type. Before the fix both were decoded
// as string guarded by `!= ""`, so an UpdatePipe call that omitted them was
// indistinguishable from one explicitly clearing them -- though it happened
// to work by accident for the omitted case, it could never let a client
// explicitly clear Enrichment (a documented real operation: detaching an
// enrichment step from a pipe) since an explicit empty string was silently
// treated the same as omitted.
func TestUpdatePipe_PreservesOmittedTargetAndEnrichment(t *testing.T) {
	t.Parallel()

	backend := pipes.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestPipesClient(t, pipes.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreatePipe(ctx, &pipessdk.CreatePipeInput{
		Name:    aws.String("target-enrich-pipe"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
		Source:  aws.String("arn:aws:sqs:us-east-1:123456789012:q"),
		Target:  aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn1"),
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, backend, "target-enrich-pipe")

	_, err = client.UpdatePipe(ctx, &pipessdk.UpdatePipeInput{
		Name:       aws.String("target-enrich-pipe"),
		RoleArn:    aws.String("arn:aws:iam::123456789012:role/r"),
		Target:     aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn2"),
		Enrichment: aws.String("arn:aws:lambda:us-east-1:123456789012:function:enrich"),
	})
	require.NoError(t, err)

	desc, err := client.DescribePipe(ctx, &pipessdk.DescribePipeInput{Name: aws.String("target-enrich-pipe")})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:fn2", aws.ToString(desc.Target))
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:enrich", aws.ToString(desc.Enrichment))

	// Omitted Target/Enrichment must preserve the prior values.
	_, err = client.UpdatePipe(ctx, &pipessdk.UpdatePipeInput{
		Name:    aws.String("target-enrich-pipe"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
	})
	require.NoError(t, err)

	desc, err = client.DescribePipe(ctx, &pipessdk.DescribePipeInput{Name: aws.String("target-enrich-pipe")})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:fn2", aws.ToString(desc.Target),
		"omitted Target must survive")
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:enrich", aws.ToString(desc.Enrichment),
		"omitted Enrichment must survive")

	// Explicit empty string clears Enrichment (detaching it from the pipe);
	// Target is untouched.
	_, err = client.UpdatePipe(ctx, &pipessdk.UpdatePipeInput{
		Name:       aws.String("target-enrich-pipe"),
		RoleArn:    aws.String("arn:aws:iam::123456789012:role/r"),
		Enrichment: aws.String(""),
	})
	require.NoError(t, err)

	desc, err = client.DescribePipe(ctx, &pipessdk.DescribePipeInput{Name: aws.String("target-enrich-pipe")})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(desc.Enrichment))
	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:fn2", aws.ToString(desc.Target),
		"unrelated field untouched")
}
