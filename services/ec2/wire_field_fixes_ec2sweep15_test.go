package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestResetAddressAttribute_WireShape_RealClient covers
// handleResetAddressAttribute, which pre-fix rendered a bare
// <return>true</return> via stubResponse. The real ResetAddressAttributeOutput
// has no Return member at all -- only a nested Address (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeOpDocumentResetAddressAttributeOutput
// matches "address", not "return") -- so a client confirming which address it
// just reset got a nil Address pre-fix, even though the reset genuinely
// happened.
func TestResetAddressAttribute_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	out, err := client.ResetAddressAttribute(t.Context(), &ec2sdk.ResetAddressAttributeInput{
		AllocationId: aws.String(addr.AllocationID),
		Attribute:    "domain-name",
	})
	require.NoError(t, err)
	require.NotNil(t, out.Address, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, addr.AllocationID, aws.ToString(out.Address.AllocationId))
	assert.Equal(t, addr.PublicIP, aws.ToString(out.Address.PublicIp))
}

// TestDisableAddressTransfer_WireShape_RealClient covers
// handleDisableAddressTransfer, which pre-fix rendered a bare
// <return>true</return>. The real DisableAddressTransferOutput has no Return
// member -- only a nested AddressTransfer (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDisableAddressTransferOutput matches
// "addressTransfer") -- so a client confirming which transfer it just
// cancelled saw a nil AddressTransfer pre-fix.
func TestDisableAddressTransfer_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	addr, err := b.AllocateAddress()
	require.NoError(t, err)
	_, err = b.EnableAddressTransfer(addr.AllocationID, "111111111111")
	require.NoError(t, err)

	out, err := client.DisableAddressTransfer(t.Context(), &ec2sdk.DisableAddressTransferInput{
		AllocationId: aws.String(addr.AllocationID),
	})
	require.NoError(t, err)
	require.NotNil(t, out.AddressTransfer, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, addr.AllocationID, aws.ToString(out.AddressTransfer.AllocationId))
	assert.Equal(t, "111111111111", aws.ToString(out.AddressTransfer.TransferAccountId))
}

// TestRestoreAddressToClassic_WireShape_RealClient covers
// handleRestoreAddressToClassic, which pre-fix rendered a bare
// <return>true</return>. The real RestoreAddressToClassicOutput has no Return
// member -- only publicIp and status (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentRestoreAddressToClassicOutput matches
// "publicIp" and "status") -- so a client checking the move outcome saw both
// fields empty pre-fix regardless of the restore's outcome.
func TestRestoreAddressToClassic_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.RestoreAddressToClassic(t.Context(), &ec2sdk.RestoreAddressToClassicInput{
		PublicIp: aws.String("203.0.113.99"),
	})
	require.NoError(t, err)
	assert.Equal(
		t, "203.0.113.99", aws.ToString(out.PublicIp),
		"PublicIp empty - real deserializer has no case for <return>, only <publicIp>/<status>",
	)
	assert.Equal(t, "InClassic", string(out.Status))
}
