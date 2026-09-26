package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- EIP attributes ---- //nolint:godot // existing issue.
func TestAddressAttribute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	addr, _ := b.AllocateAddress()

	t.Run("modify and describe", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyAddressAttribute(addr.AllocationID, "ec2.example.com"))
		attrs := b.DescribeAddressesAttribute([]string{addr.AllocationID})
		require.Len(t, attrs, 1)
		assert.Equal(t, "ec2.example.com", attrs[0].DomainName)
	})

	t.Run("reset clears domain name", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ResetAddressAttribute(addr.AllocationID)
		require.NoError(t, err)
		// Real AWS's DescribeAddressesAttribute(Attribute=domain-name) returns
		// no entry for an allocation with no domain name set -- matching
		// aws_eip_domain_name's delete waiter, which needs this lookup to
		// come back NotFound (see DescribeAddressesAttribute's doc comment).
		attrs := b.DescribeAddressesAttribute([]string{addr.AllocationID})
		assert.Empty(t, attrs)
	})
}

// TestResetAddressAttribute_HTTP_DomainNameLifecycle verifies the wire
// shape at each stage of aws_eip_domain_name's lifecycle. After
// ModifyAddressAttribute, DescribeAddressesAttribute includes a
// ptrRecordUpdate element with no status -- terraform-provider-aws's create
// waiter (waitEIPDomainNameAttributeUpdated) polls for exactly that empty
// status. After ResetAddressAttribute, DescribeAddressesAttribute must
// return NO item for the allocation at all: its delete waiter
// (waitEIPDomainNameAttributeDeleted, internal/service/ec2/wait.go) has an
// empty Target, which terraform-plugin-sdk's retry.StateChangeConf only
// satisfies on a NotFound refresh result -- an item with an empty
// PtrRecordUpdate.Status previously produced "unexpected state ”, wanted
// target ”" instead of completing.
func TestResetAddressAttribute_HTTP_DomainNameLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	addr, err := h.Backend.AllocateAddress()
	require.NoError(t, err)

	modifyResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":       {"ModifyAddressAttribute"},
		"AllocationId": {addr.AllocationID},
		"DomainName":   {"ec2.example.com"},
	})
	require.NoError(t, err)
	assert.Contains(t, modifyResp, "<ptrRecordUpdate>")
	assert.NotContains(t, modifyResp, "<status>")

	descAfterModify, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"DescribeAddressesAttribute"},
		"AllocationId.1": {addr.AllocationID},
	})
	require.NoError(t, err)
	assert.Contains(t, descAfterModify, "<ptrRecordUpdate>")
	assert.NotContains(t, descAfterModify, "<status>")

	resetResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":       {"ResetAddressAttribute"},
		"AllocationId": {addr.AllocationID},
	})
	require.NoError(t, err)
	assert.Contains(t, resetResp, addr.AllocationID)

	descAfterReset, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"DescribeAddressesAttribute"},
		"AllocationId.1": {addr.AllocationID},
	})
	require.NoError(t, err)
	assert.NotContains(t, descAfterReset, addr.AllocationID,
		"a reset allocation must be absent from the response, not present with an empty status")
}

// ---- Instance ---- //nolint:godot // existing issue.

// ---- Address transfers ---- //nolint:godot // existing issue.
func TestAddressTransfers(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	addr, _ := b.AllocateAddress()

	t.Run("enable transfer", func(t *testing.T) { //nolint:paralleltest // existing issue.
		transfer, err := b.EnableAddressTransfer(addr.AllocationID, "111111111111")
		require.NoError(t, err)
		assert.Equal(t, "pending", transfer.TransferOfferStatus)
	})

	t.Run("describe returns transfer", func(t *testing.T) { //nolint:paralleltest // existing issue.
		transfers := b.DescribeAddressTransfers([]string{addr.AllocationID})
		require.Len(t, transfers, 1)
		assert.Equal(t, "111111111111", transfers[0].TransferAccountID)
	})

	t.Run("disable transfer removes it", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DisableAddressTransfer(addr.AllocationID)
		require.NoError(t, err)
		transfers := b.DescribeAddressTransfers([]string{addr.AllocationID})
		assert.Empty(t, transfers)
	})

	t.Run("empty allocation ID returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.EnableAddressTransfer("", "111")
		require.Error(t, err)
	})
}

// ---- Subnet CIDR reservations ---- //nolint:godot // existing issue.

func TestMoveAddressToVpcAndDescribeMovingAddressesHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	addr, err := h.Backend.AllocateAddress()
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":   {"MoveAddressToVpc"},
		"PublicIp": {addr.PublicIP},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<MoveAddressToVpcResponse>")
	assert.Contains(t, resp, "<status>MoveInProgress</status>")

	resp, err = ec2.ExportDispatch(h, url.Values{"Action": {"DescribeMovingAddresses"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeMovingAddressesResponse>")
	assert.Contains(t, resp, "<publicIp>"+addr.PublicIP+"</publicIp>")
	assert.Contains(t, resp, "<moveStatus>movingToVpc</moveStatus>")
}

// TestAllocateAddress_TagSpecification verifies that AllocateAddress applies
// TagSpecifications (previously dropped entirely -- the handler discarded
// its url.Values parameter), so a tag:Name filter on DescribeAddresses can
// find the allocated EIP, matching aws_eip's tags argument.
func TestAllocateAddress_TagSpecification(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                          {"AllocateAddress"},
		"TagSpecification.1.ResourceType": {"elastic-ip"},
		"TagSpecification.1.Tag.1.Key":    {"Name"},
		"TagSpecification.1.Tag.1.Value":  {"my-eip"},
	})
	require.NoError(t, err)
	allocationID := extractXMLTag(resp, "allocationId")
	require.NotEmpty(t, allocationID)

	descResp, err := ec2.ExportDispatch(h, url.Values{
		"Action":           {"DescribeAddresses"},
		"Filter.1.Name":    {"tag:Name"},
		"Filter.1.Value.1": {"my-eip"},
	})
	require.NoError(t, err)
	assert.Contains(t, descResp, allocationID)
}

func TestAssociateAddress_NetworkInterfaceId_Accepted(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	// Create an instance so its ENI exists.
	insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":             {"AssociateAddress"},
		"Version":            {"2016-11-15"},
		"AllocationId":       {addr.AllocationID},
		"NetworkInterfaceId": {insts[0].ID}, // use instance ID as target (backend accepts it)
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "AssociateAddressResponse")
}

func TestAssociateAddress_NoTarget_Rejected(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	vals := url.Values{
		"Action":       {"AssociateAddress"},
		"Version":      {"2016-11-15"},
		"AllocationId": {addr.AllocationID},
		// Neither InstanceId nor NetworkInterfaceId provided.
	}

	_, err = ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required")
}

func TestAssociateAddress_NoAllocationId_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	vals := url.Values{
		"Action":     {"AssociateAddress"},
		"Version":    {"2016-11-15"},
		"InstanceId": {"i-12345678"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AllocationId")
}

func TestAssociateAddress_InstanceId_StillWorks(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":       {"AssociateAddress"},
		"Version":      {"2016-11-15"},
		"AllocationId": {addr.AllocationID},
		"InstanceId":   {insts[0].ID},
	}

	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "AssociateAddressResponse")
}

// ---- Gap F: instanceItem groupSet in DescribeInstances response ----

func TestAssociateAddress_BothInstanceAndNetworkInterface_UsesInstance(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
	require.NoError(t, err)

	vals := url.Values{
		"Action":             {"AssociateAddress"},
		"Version":            {"2016-11-15"},
		"AllocationId":       {addr.AllocationID},
		"InstanceId":         {insts[0].ID},
		"NetworkInterfaceId": {"eni-12345678"},
	}

	// When both are given, InstanceId takes precedence (InstanceId is checked first).
	resp, err := ec2.ExportDispatch(h, vals)
	require.NoError(t, err)
	assert.Contains(t, resp, "AssociateAddressResponse")
}

// ---- runTestHandlerWithBackend helper ----

func newTestHandlerWithBackend(b *ec2.InMemoryBackend) *ec2.Handler {
	h := ec2.NewHandler(b)
	h.AccountID = "123456789012"
	h.Region = "us-east-1"

	return h
}

// ---- Gap M: DescribeInstances grouped filters preserve state filter semantics ----
