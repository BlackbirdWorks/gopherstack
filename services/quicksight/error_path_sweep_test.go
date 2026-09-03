package quicksight_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	qstypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestGetFlowMetadata_UnknownFlow_RealClient covers a wrong-code bug:
// GetFlowMetadata/GetFlowPermissions/UpdateFlowPermissions raised
// ResourceNotFoundException for an unresolvable FlowId, but — unlike their
// sibling CreateFlow/DescribeFlow/UpdateFlow/DeleteFlow — none of these three
// ops model ResourceNotFoundException in their own deserializer
// (quicksight@v1.123.1 deserializers.go); they model only
// InvalidParameterValueException.
func TestGetFlowMetadata_UnknownFlow_RealClient(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestQuickSightClient(t, quicksight.NewHandler(backend))
	ctx := t.Context()

	_, err := client.GetFlowMetadata(ctx, &quicksightsdk.GetFlowMetadataInput{
		AwsAccountId: aws.String("000000000000"),
		FlowId:       aws.String("no-such-flow"),
	})
	require.Error(t, err)

	var ipe *qstypes.InvalidParameterValueException
	require.ErrorAs(t, err, &ipe, "expected a real InvalidParameterValueException from the SDK deserializer")
}

func TestGetFlowPermissions_UnknownFlow_RealClient(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestQuickSightClient(t, quicksight.NewHandler(backend))
	ctx := t.Context()

	_, err := client.GetFlowPermissions(ctx, &quicksightsdk.GetFlowPermissionsInput{
		AwsAccountId: aws.String("000000000000"),
		FlowId:       aws.String("no-such-flow"),
	})
	require.Error(t, err)

	var ipe *qstypes.InvalidParameterValueException
	require.ErrorAs(t, err, &ipe, "expected a real InvalidParameterValueException from the SDK deserializer")
}

func TestUpdateFlowPermissions_UnknownFlow_RealClient(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestQuickSightClient(t, quicksight.NewHandler(backend))
	ctx := t.Context()

	_, err := client.UpdateFlowPermissions(ctx, &quicksightsdk.UpdateFlowPermissionsInput{
		AwsAccountId: aws.String("000000000000"),
		FlowId:       aws.String("no-such-flow"),
	})
	require.Error(t, err)

	var ipe *qstypes.InvalidParameterValueException
	require.ErrorAs(t, err, &ipe, "expected a real InvalidParameterValueException from the SDK deserializer")
}
