package ram_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// Test_SDKRoundTrip_ListPermissionAssociations_ArnAndVersionShape proves
// ListPermissionAssociationsOutput.Permissions decodes as types.AssociatedPermission
// (types/types.go:11-), whose wire key for the permission ARN is "arn" -- not
// "permissionArn" -- and whose PermissionVersion is a JSON string, not a number
// (deserializers.go's awsRestjson1_deserializeDocumentAssociatedPermission type-asserts
// permissionVersion to string and returns a decode error otherwise). Before the fix,
// gopherstack emitted "permissionArn" (so the real client's Arn always decoded nil) and a
// numeric permissionVersion (so the real client's call failed outright with a decode error).
func Test_SDKRoundTrip_ListPermissionAssociations_ArnAndVersionShape(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient(t, h)

	perm, err := backend.CreatePermission("assoc-shape-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	share, err := backend.CreateResourceShare("assoc-shape-share", false, nil, nil, nil)
	require.NoError(t, err)

	err = backend.AssociateResourceSharePermission(share.ARN, perm.ARN, false, nil)
	require.NoError(t, err)

	out, err := client.ListPermissionAssociations(t.Context(), &ramsdk.ListPermissionAssociationsInput{
		PermissionArn: aws.String(perm.ARN),
	})
	require.NoError(t, err)
	require.Len(t, out.Permissions, 1)

	got := out.Permissions[0]
	require.NotNil(t, got.Arn)
	assert.Equal(t, perm.ARN, *got.Arn)
	require.NotNil(t, got.ResourceShareArn)
	assert.Equal(t, share.ARN, *got.ResourceShareArn)
	require.NotNil(t, got.PermissionVersion)
	assert.Equal(t, "1", *got.PermissionVersion)
}
