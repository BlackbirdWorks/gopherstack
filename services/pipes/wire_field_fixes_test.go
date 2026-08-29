package pipes_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestUpdatePipe_KmsKeyIdentifierCanBeCleared drives
// CreatePipe/UpdatePipe/DescribePipe through the real SDK client.
// UpdatePipeInput.KmsKeyIdentifier was a plain string guarded by != "" (not
// *string like the real SDK's UpdatePipeInput, api_op_UpdatePipe.go), whose
// doc comment says "To update a pipe that is using a customer managed key to
// use the default Amazon Web Services owned key, specify an empty string" --
// so a real client's documented way to revert to the default key was
// silently dropped, leaving the old customer-managed key in place.
func TestUpdatePipe_KmsKeyIdentifierCanBeCleared(t *testing.T) {
	t.Parallel()

	backend := pipes.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestPipesClient(t, pipes.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreatePipe(ctx, &pipessdk.CreatePipeInput{
		Name:             aws.String("kms-clear-pipe"),
		RoleArn:          aws.String("arn:aws:iam::123456789012:role/r"),
		Source:           aws.String("arn:aws:sqs:us-east-1:123456789012:q"),
		Target:           aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn"),
		KmsKeyIdentifier: aws.String("arn:aws:kms:us-east-1:123456789012:key/custom-key"),
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, backend, "kms-clear-pipe")

	before, err := client.DescribePipe(ctx, &pipessdk.DescribePipeInput{Name: aws.String("kms-clear-pipe")})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/custom-key", aws.ToString(before.KmsKeyIdentifier))

	_, err = client.UpdatePipe(ctx, &pipessdk.UpdatePipeInput{
		Name:             aws.String("kms-clear-pipe"),
		RoleArn:          aws.String("arn:aws:iam::123456789012:role/r"),
		KmsKeyIdentifier: aws.String(""),
	})
	require.NoError(t, err)

	after, err := client.DescribePipe(ctx, &pipessdk.DescribePipeInput{Name: aws.String("kms-clear-pipe")})
	require.NoError(t, err)
	require.Empty(t, aws.ToString(after.KmsKeyIdentifier),
		"explicit empty KmsKeyIdentifier on UpdatePipe must revert to the default key, not be silently ignored")
}

// TestCreateUpdatePipe_RuntimeMetricsStreamingNotAccepted proves
// gopherstack-101r's fix for handler.go's createPipeRequest/updatePipeRequest:
// RuntimeMetricsStreaming does not exist anywhere in the real Pipes SDK
// (aws-sdk-go-v2/service/pipes@v1.26.4 -- absent from CreatePipeInput,
// UpdatePipeInput, and types.Pipe), so no typed client can express it; this
// exercises the raw body a stray caller might still send and proves the key
// is gone entirely rather than silently accepted.
func TestCreateUpdatePipe_RuntimeMetricsStreamingNotAccepted(t *testing.T) {
	t.Parallel()

	h := b2Handler(t)

	created := b2Create(t, h, "rms-gone", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/r",
		"Source":  b2SQSSource,
		"Target":  b2LambdaTarget,
		"RuntimeMetricsStreaming": map[string]any{
			"Level": "ALL",
		},
	})
	_, present := created["RuntimeMetricsStreaming"]
	require.False(t, present, "Create must not echo back a RuntimeMetricsStreaming key")

	updated := b2Update(t, h, "rms-gone", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/r",
		"RuntimeMetricsStreaming": map[string]any{
			"Level": "ERRORS",
		},
	})
	_, present = updated["RuntimeMetricsStreaming"]
	require.False(t, present, "Update must not echo back a RuntimeMetricsStreaming key")
}
