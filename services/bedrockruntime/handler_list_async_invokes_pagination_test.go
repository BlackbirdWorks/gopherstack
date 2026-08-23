package bedrockruntime_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockruntimesdk "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// TestListAsyncInvokes_SDKRoundTrip_Pagination drives the real SDK client across two
// pages of ListAsyncInvokes and asserts the pages are disjoint. Before the fix,
// handleListAsyncInvokes ignored maxResults/nextToken (both httpQuery-bound in
// ListAsyncInvokesInput -- serializers.go:1120-1126) and always returned every
// invocation in a single unbounded page with no nextToken, so a second call with the
// first page's NextToken returned the exact same 25 invocations again.
func TestListAsyncInvokes_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")
	h := bedrockruntime.NewHandler(backend)
	client := newTestBedrockRuntimeSDKClient(t, h)

	const total = 25

	for i := range total {
		_, err := backend.StartAsyncInvoke(
			"anthropic.claude-v2",
			"s3://bucket/out/",
			"token-"+string(rune('a'+i)),
			nil,
		)
		require.NoError(t, err)
	}

	page1, err := client.ListAsyncInvokes(t.Context(), &bedrockruntimesdk.ListAsyncInvokesInput{
		MaxResults: aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, page1.AsyncInvokeSummaries, 10)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListAsyncInvokes(t.Context(), &bedrockruntimesdk.ListAsyncInvokesInput{
		MaxResults: aws.Int32(10),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.AsyncInvokeSummaries, 10)

	seen := make(map[string]bool, 20)
	for _, inv := range page1.AsyncInvokeSummaries {
		seen[aws.ToString(inv.InvocationArn)] = true
	}

	for _, inv := range page2.AsyncInvokeSummaries {
		assert.False(t, seen[aws.ToString(inv.InvocationArn)],
			"page 2 repeated an invocation from page 1: %s", aws.ToString(inv.InvocationArn))
		seen[aws.ToString(inv.InvocationArn)] = true
	}

	assert.Len(t, seen, 20)
}
