package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Network interface ---- //nolint:godot // existing issue.
func TestNetworkInterfaceAttribute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "test NI")

	t.Run( //nolint:paralleltest // existing issue.
		"describe returns description and sourceDestCheck",
		func(t *testing.T) {
			result, err := b.DescribeNetworkInterfaceAttribute(ni.ID, "description")
			require.NoError(t, err)
			assert.Equal(t, ni.ID, result.NetworkInterfaceID)
		},
	)

	t.Run("reset sets sourceDestCheck to true", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ResetNetworkInterfaceAttribute(ni.ID))
	})

	t.Run("unknown NI returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DescribeNetworkInterfaceAttribute("eni-missing", "description")
		require.Error(t, err)
	})
}

func TestNetworkInterfacePermissions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "perm test NI")

	t.Run("create permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
		perm, err := b.CreateNetworkInterfacePermission(
			ni.ID,
			"123456789012",
			"",
			"INSTANCE-ATTACH",
		)
		require.NoError(t, err)
		assert.Equal(t, "granted", perm.State)
	})

	t.Run("describe returns created permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
		perms := b.DescribeNetworkInterfacePermissions([]string{ni.ID})
		require.Len(t, perms, 1)
		assert.Equal(t, "INSTANCE-ATTACH", perms[0].Permission)

		t.Run("delete permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
			permID := perms[0].PermissionID
			require.NoError(t, b.DeleteNetworkInterfacePermission(permID))
			remaining := b.DescribeNetworkInterfacePermissions([]string{ni.ID})
			assert.Empty(t, remaining)
		})
	})
}

func TestIPv6Addresses(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "ipv6 NI")

	t.Run("assign IPv6 addresses", func(t *testing.T) { //nolint:paralleltest // existing issue.
		addrs, err := b.AssignIpv6Addresses(ni.ID, 2)
		require.NoError(t, err)
		require.Len(t, addrs, 2)
	})

	t.Run("unassign IPv6 addresses", func(t *testing.T) { //nolint:paralleltest // existing issue.
		addrs, _ := b.AssignIpv6Addresses(ni.ID, 1)
		require.NoError(t, b.UnassignIpv6Addresses(ni.ID, addrs))
	})
}

// TestHTTP_NetworkInterfaceWireFields verifies the wire shapes this pass
// added: CreateNetworkInterface's TagSpecification was previously discarded
// entirely (never parsed), and OwnerId/TagSet/attachment.deleteOnTermination
// were absent from every ENI response despite real, available backing data.
func TestHTTP_NetworkInterfaceWireFields(t *testing.T) {
	t.Parallel()

	t.Run("create_with_tag_specification_renders_ownerid_and_tags", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
		h := ec2.NewHandler(b)

		resp, err := ec2.ExportDispatch(h, url.Values{
			"Action":                          []string{"CreateNetworkInterface"},
			"Version":                         []string{"2016-11-15"},
			"SubnetId":                        []string{"subnet-default"},
			"TagSpecification.1.ResourceType": []string{"network-interface"},
			"TagSpecification.1.Tag.1.Key":    []string{"Name"},
			"TagSpecification.1.Tag.1.Value":  []string{"eth1"},
		})
		require.NoError(t, err)
		assert.Contains(t, resp, "<ownerId>123456789012</ownerId>")
		assert.Contains(t, resp, "<key>Name</key>")
		assert.Contains(t, resp, "<value>eth1</value>")

		eniID := accuracyExtractXMLValue(resp, "networkInterfaceId")
		require.NotEmpty(t, eniID)

		descResp, err := ec2.ExportDispatch(h, url.Values{
			"Action":               []string{"DescribeNetworkInterfaces"},
			"Version":              []string{"2016-11-15"},
			"NetworkInterfaceId.1": []string{eniID},
		})
		require.NoError(t, err)
		assert.Contains(t, descResp, "<key>Name</key>",
			"a create-time TagSpecification tag must also be visible via DescribeNetworkInterfaces")
	})

	t.Run("modify_attachment_delete_on_termination", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
		h := ec2.NewHandler(b)

		insts, err := b.RunInstances("ami-test", "t2.micro", "", 1)
		require.NoError(t, err)

		eni, err := b.CreateNetworkInterface("subnet-default", "")
		require.NoError(t, err)

		attachID, err := b.AttachNetworkInterface(eni.ID, insts[0].ID, 1)
		require.NoError(t, err)

		_, err = ec2.ExportDispatch(h, url.Values{
			"Action":                         []string{"ModifyNetworkInterfaceAttribute"},
			"Version":                        []string{"2016-11-15"},
			"NetworkInterfaceId":             []string{eni.ID},
			"Attachment.AttachmentId":        []string{attachID},
			"Attachment.DeleteOnTermination": []string{"true"},
		})
		require.NoError(t, err)

		enis := b.DescribeNetworkInterfaces([]string{eni.ID})
		require.Len(t, enis, 1)
		assert.True(t, enis[0].DeleteOnTermination)

		// Terminating the instance must now delete this ENI (not merely detach
		// it), since DeleteOnTermination was flipped to true.
		_, err = b.TerminateInstances([]string{insts[0].ID})
		require.NoError(t, err)
		assert.Empty(t, b.DescribeNetworkInterfaces([]string{eni.ID}))
	})
}

// ---- Account/misc ---- //nolint:godot // existing issue.
