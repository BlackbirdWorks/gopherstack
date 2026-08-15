package verifiedpermissions_test

import (
	"testing"

	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	avptypes "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/stretchr/testify/require"
)

// TestGetPolicyStore_TagResource_LiveSync_RealClient proves that
// verifiedpermissions's central resourceTags side map (written by
// TagResource/UntagResource) and PolicyStore's own creation-time Tags
// snapshot are two independently-written stores: GetPolicyStore echoed the
// snapshot (and the handler layer didn't even surface a "tags" field at
// all), so a tag applied via TagResource after CreatePolicyStore was
// invisible to GetPolicyStore, even though PolicyStore.Tags is a real
// GetPolicyStoreOutput member (verifiedpermissions@v1.36.4:
// api_op_GetPolicyStore.go) and ListTagsForResource for the same ARN
// returned it correctly. Same shape fixed for ecs's
// DescribeTasks/StopTask/UpdateCapacityProvider (gopherstack-g8k9, commit
// 9a40453a2).
func TestGetPolicyStore_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreatePolicyStore(ctx, &avpsdk.CreatePolicyStoreInput{
		ValidationSettings: &avptypes.ValidationSettings{Mode: avptypes.ValidationModeOff},
		Tags:               map[string]string{"owner": "sre"},
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &avpsdk.TagResourceInput{
		ResourceArn: createOut.Arn,
		Tags:        map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	getOut, err := client.GetPolicyStore(ctx, &avpsdk.GetPolicyStoreInput{
		PolicyStoreId: createOut.PolicyStoreId,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "sre", "env": "prod"}, getOut.Tags)

	_, err = client.UntagResource(ctx, &avpsdk.UntagResourceInput{
		ResourceArn: createOut.Arn,
		TagKeys:     []string{"owner"},
	})
	require.NoError(t, err)

	getOut, err = client.GetPolicyStore(ctx, &avpsdk.GetPolicyStoreInput{
		PolicyStoreId: createOut.PolicyStoreId,
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod"}, getOut.Tags)
}
