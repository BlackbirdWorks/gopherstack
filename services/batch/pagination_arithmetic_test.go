package batch_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeComputeEnvironments_RealClient_BoundaryWalk confirms, through
// the real aws-sdk-go-v2 client, that describeResourcesPaginated (which
// delegates to paginateMapKeys → pkgs/page.NewHMAC, an offset token that is
// always clamped to the collection length) walks a full
// DescribeComputeEnvironments collection without dropping or duplicating
// entries, and that a stale token (naming a since-deleted compute
// environment position) terminates instead of looping.
func TestDescribeComputeEnvironments_RealClient_BoundaryWalk(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestBatchClient(t, h)

	const n = 7

	names := make([]string, n)
	for i := range n {
		name := fmt.Sprintf("ce-%03d", i)
		names[i] = name
		_, err := client.CreateComputeEnvironment(t.Context(), &batchsdk.CreateComputeEnvironmentInput{
			ComputeEnvironmentName: aws.String(name),
			Type:                   types.CETypeUnmanaged,
			State:                  types.CEStateDisabled,
		})
		require.NoError(t, err)
	}

	var got []string

	var token *string
	for range n + 1 {
		out, err := client.DescribeComputeEnvironments(t.Context(), &batchsdk.DescribeComputeEnvironmentsInput{
			MaxResults: aws.Int32(3),
			NextToken:  token,
		})
		require.NoError(t, err)

		for _, ce := range out.ComputeEnvironments {
			got = append(got, aws.ToString(ce.ComputeEnvironmentName))
		}

		token = out.NextToken
		if aws.ToString(token) == "" {
			break
		}
	}

	assert.ElementsMatch(t, names, got, "boundary walk must reproduce the collection exactly, no drops or dupes")

	// Stale cursor: an offset token from before every environment is
	// deleted must terminate cleanly, not loop or error.
	page1, err := client.DescribeComputeEnvironments(t.Context(), &batchsdk.DescribeComputeEnvironmentsInput{
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotNil(t, page1.NextToken)
	staleToken := aws.ToString(page1.NextToken)

	for _, name := range names {
		_, err = client.DeleteComputeEnvironment(t.Context(), &batchsdk.DeleteComputeEnvironmentInput{
			ComputeEnvironment: aws.String(name),
		})
		require.NoError(t, err)
	}

	page2, err := client.DescribeComputeEnvironments(t.Context(), &batchsdk.DescribeComputeEnvironmentsInput{
		MaxResults: aws.Int32(3),
		NextToken:  aws.String(staleToken),
	})
	require.NoError(t, err, "resuming with a stale offset token must not error or hang")
	assert.Empty(t, page2.ComputeEnvironments)
}
