package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRegisterInstanceEventNotificationAttributes_WireShape_RealClient covers
// handleRegisterInstanceEventNotificationAttributes, which pre-fix rendered a
// bare <return>true</return> via stubResponse. The real
// RegisterInstanceEventNotificationAttributesOutput has no Return member at
// all -- only a nested InstanceTagAttribute (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentRegisterInstanceEventNotificationAttributesOutput
// matches "instanceTagAttribute", not "return") -- so a client confirming
// whether all-tags registration took effect saw a nil InstanceTagAttribute
// pre-fix, even though the registration genuinely happened.
func TestRegisterInstanceEventNotificationAttributes_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.RegisterInstanceEventNotificationAttributes(
		t.Context(),
		&ec2sdk.RegisterInstanceEventNotificationAttributesInput{
			InstanceTagAttribute: &types.RegisterInstanceTagAttributeRequest{
				IncludeAllTagsOfInstance: aws.Bool(true),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.InstanceTagAttribute, "pre-fix this field was never rendered, only a bare Return bool")
	assert.True(t, aws.ToBool(out.InstanceTagAttribute.IncludeAllTagsOfInstance))
}

// TestDeregisterInstanceEventNotificationAttributes_WireShape_RealClient
// covers handleDeregisterInstanceEventNotificationAttributes, same shape gap
// as Register (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDeregisterInstanceEventNotificationAttributesOutput).
func TestDeregisterInstanceEventNotificationAttributes_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	_, err := client.RegisterInstanceEventNotificationAttributes(
		t.Context(),
		&ec2sdk.RegisterInstanceEventNotificationAttributesInput{
			InstanceTagAttribute: &types.RegisterInstanceTagAttributeRequest{
				IncludeAllTagsOfInstance: aws.Bool(true),
			},
		},
	)
	require.NoError(t, err)

	out, err := client.DeregisterInstanceEventNotificationAttributes(
		t.Context(),
		&ec2sdk.DeregisterInstanceEventNotificationAttributesInput{
			InstanceTagAttribute: &types.DeregisterInstanceTagAttributeRequest{
				IncludeAllTagsOfInstance: aws.Bool(true),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.InstanceTagAttribute, "pre-fix this field was never rendered, only a bare Return bool")
	assert.False(
		t, aws.ToBool(out.InstanceTagAttribute.IncludeAllTagsOfInstance),
		"deregister should clear IncludeAllTagsOfInstance",
	)
}
