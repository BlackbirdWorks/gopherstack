package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestResetEbsDefaultKmsKeyId_WireShape_RealClient covers
// handleResetEbsDefaultKmsKeyID, which pre-fix rendered a bare
// <return>true</return> via stubResponse. The real
// ResetEbsDefaultKmsKeyIdOutput has no Return member at all -- only KmsKeyId
// (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentResetEbsDefaultKmsKeyIdOutput has no case
// for "return") -- so a client confirming the reset default key saw an empty
// string pre-fix, even though the reset genuinely happened (the same
// alias/aws/ebs GetEbsDefaultKmsKeyId already renders correctly).
func TestResetEbsDefaultKmsKeyId_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	_, err := client.ModifyEbsDefaultKmsKeyId(t.Context(), &ec2sdk.ModifyEbsDefaultKmsKeyIdInput{
		KmsKeyId: aws.String("arn:aws:kms:us-east-1:000000000000:key/sweep21-key"),
	})
	require.NoError(t, err)

	out, err := client.ResetEbsDefaultKmsKeyId(t.Context(), &ec2sdk.ResetEbsDefaultKmsKeyIdInput{})
	require.NoError(t, err)
	assert.Equal(
		t, "alias/aws/ebs", aws.ToString(out.KmsKeyId),
		"KmsKeyId empty - real deserializer has no case for <return>, only <kmsKeyId>",
	)
}
