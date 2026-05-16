package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	timestreamquerysdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_TimestreamQuery_DescribeEndpoints exercises the endpoint
// discovery operation that the AWS SDK v2 timestreamquery client invokes
// before any data-plane call.
func TestIntegration_TimestreamQuery_DescribeEndpoints(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createTimestreamQueryClient(t)
	ctx := t.Context()

	out, err := client.DescribeEndpoints(ctx, &timestreamquerysdk.DescribeEndpointsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.Endpoints)
	assert.NotEmpty(t, aws.ToString(out.Endpoints[0].Address))
}
