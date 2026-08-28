package ec2_test

import (
	"testing"

	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestGetLaunchTemplateData_InstanceFields_RealClient covers
// handleGetLaunchTemplateData, which pre-fix only ever populated ImageId and
// InstanceType on the response, discarding KeyName, SecurityGroupIds,
// DisableApiTermination, DisableApiStop, and
// InstanceInitiatedShutdownBehavior even though the source instance tracks
// all of them (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentResponseLaunchTemplateData matches
// "keyName", "securityGroupIdSet", "disableApiTermination",
// "disableApiStop", and "instanceInitiatedShutdownBehavior"), so a real
// client always saw these as empty/zero despite HTTP 200/err==nil.
func TestGetLaunchTemplateData_InstanceFields_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	insts, err := b.RunInstances("ami-sweep27", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, insts, 1)
	instanceID := insts[0].ID

	require.NoError(t, b.SetInstanceLaunchConfig(instanceID, "sweep27-key", []string{"sg-sweep27"}))

	out, err := client.GetLaunchTemplateData(t.Context(), &ec2sdk.GetLaunchTemplateDataInput{
		InstanceId: &instanceID,
	})
	require.NoError(t, err)
	require.NotNil(t, out.LaunchTemplateData)

	data := out.LaunchTemplateData
	assert.Equal(t, "sweep27-key", *data.KeyName, "KeyName empty - pre-fix never populated")
	assert.Equal(
		t, []string{"sg-sweep27"}, data.SecurityGroupIds,
		"SecurityGroupIds empty - pre-fix never populated",
	)
}
