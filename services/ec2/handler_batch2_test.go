package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- VPC endpoint connection notifications ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_VpcEndpointConnectionNotification(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var notifID string

	t.Run("create notification", func(t *testing.T) {
		notif, err := b.CreateVpcEndpointConnectionNotification(
			"vpce-svc-123", "", "arn:aws:sns:us-east-1:000000000000:test",
			[]string{"Accept", "Reject"},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, notif.ConnectionNotificationID)
		assert.Equal(t, "Enabled", notif.ConnectionNotificationState)
		notifID = notif.ConnectionNotificationID
	})

	t.Run("describe returns created", func(t *testing.T) {
		notifs := b.DescribeVpcEndpointConnectionNotifications([]string{notifID})
		require.Len(t, notifs, 1)
		assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:test", notifs[0].ConnectionNotificationARN)
	})

	t.Run("modify notification ARN", func(t *testing.T) {
		updated, err := b.ModifyVpcEndpointConnectionNotification(
			notifID, "arn:aws:sns:us-east-1:000000000000:updated", nil,
		)
		require.NoError(t, err)
		assert.Contains(t, updated.ConnectionNotificationARN, "updated")
	})

	t.Run("delete notification", func(t *testing.T) {
		require.NoError(t, b.DeleteVpcEndpointConnectionNotifications([]string{notifID}))
		notifs := b.DescribeVpcEndpointConnectionNotifications(nil)
		assert.Empty(t, notifs)
	})

	t.Run("empty ARN returns error", func(t *testing.T) {
		_, err := b.CreateVpcEndpointConnectionNotification("svc-1", "", "", nil)
		require.Error(t, err)
	})
}

// ---- VPC Endpoint operations ----

func TestBatch2_VpcEndpointOperations(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("DescribeVpcEndpointConnections returns empty", func(t *testing.T) {
		t.Parallel()
		conns := b.DescribeVpcEndpointConnections(nil)
		assert.Empty(t, conns)
	})

	t.Run("DescribeVpcEndpointAssociations returns empty", func(t *testing.T) {
		t.Parallel()
		assocs := b.DescribeVpcEndpointAssociations(nil)
		assert.Empty(t, assocs)
	})
}

func TestBatch2_VpcEndpointServicePermissions(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	// Create a service config to test against
	svcCfg, setupErr := b.CreateVpcEndpointServiceConfiguration(false, []string{"nlb-123"})
	require.NoError(t, setupErr)
	svcID := svcCfg.ServiceID

	t.Run("modify adds principals", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, b.ModifyVpcEndpointServicePermissions(svcID,
			[]string{"arn:aws:iam::111111111111:root"}, nil))
		principals := b.DescribeVpcEndpointServicePermissions(svcID)
		require.Len(t, principals, 1)
		assert.Equal(t, "arn:aws:iam::111111111111:root", principals[0])
	})
}

// ---- EBS encryption ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_EbsEncryption(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("default is false", func(t *testing.T) {
		assert.False(t, b.GetEbsEncryptionByDefault())
	})

	t.Run("enable sets true", func(t *testing.T) {
		b.EnableEbsEncryptionByDefault()
		assert.True(t, b.GetEbsEncryptionByDefault())
	})

	t.Run("disable sets false", func(t *testing.T) {
		b.DisableEbsEncryptionByDefault()
		assert.False(t, b.GetEbsEncryptionByDefault())
	})

	t.Run("default KMS key is alias/aws/ebs", func(t *testing.T) {
		assert.Equal(t, "alias/aws/ebs", b.GetEbsDefaultKmsKeyID())
	})

	t.Run("modify KMS key ID", func(t *testing.T) {
		require.NoError(t, b.ModifyEbsDefaultKmsKeyID("alias/my-key"))
		assert.Equal(t, "alias/my-key", b.GetEbsDefaultKmsKeyID())
	})

	t.Run("empty KMS key returns error", func(t *testing.T) {
		require.Error(t, b.ModifyEbsDefaultKmsKeyID(""))
	})
}

func TestBatch2_EnableVolumeIO(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, setupErr := b.CreateVolume("us-east-1a", "gp2", 10)
	require.NoError(t, setupErr)

	t.Run("enables IO for known volume", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, b.EnableVolumeIO(vol.ID))
	})

	t.Run("unknown volume returns error", func(t *testing.T) {
		t.Parallel()
		require.Error(t, b.EnableVolumeIO("vol-missing"))
	})
}

// ---- Snapshot locking ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_SnapshotLocking(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 10)
	snap, _ := b.CreateSnapshot(vol.ID, "lockable")

	t.Run("lock snapshot", func(t *testing.T) {
		lock, err := b.LockSnapshot(snap.SnapshotID, "compliance", 90)
		require.NoError(t, err)
		assert.Equal(t, "locked", lock.LockState)
		assert.Equal(t, 90, lock.LockDurationDays)
	})

	t.Run("describe returns lock", func(t *testing.T) {
		locks := b.DescribeLockedSnapshots([]string{snap.SnapshotID})
		require.Len(t, locks, 1)
		assert.Equal(t, snap.SnapshotID, locks[0].SnapshotID)
	})

	t.Run("double lock returns error", func(t *testing.T) {
		_, err := b.LockSnapshot(snap.SnapshotID, "governance", 0)
		require.Error(t, err)
	})

	t.Run("unlock snapshot", func(t *testing.T) {
		require.NoError(t, b.UnlockSnapshot(snap.SnapshotID))
		locks := b.DescribeLockedSnapshots([]string{snap.SnapshotID})
		assert.Empty(t, locks)
	})

	t.Run("unlock non-locked snapshot returns error", func(t *testing.T) {
		require.Error(t, b.UnlockSnapshot(snap.SnapshotID))
	})
}

// ---- CopyVolumes ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_CopyVolumes(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	v1, _ := b.CreateVolume("us-east-1a", "gp2", 10)
	v2, _ := b.CreateVolume("us-east-1a", "gp3", 20)

	t.Run("creates copies", func(t *testing.T) {
		results, err := b.CopyVolumes([]string{v1.ID, v2.ID}, "us-west-2")
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.NotEqual(t, v1.ID, results[0].DestVolumeID)
	})

	t.Run("empty list returns error", func(t *testing.T) {
		_, err := b.CopyVolumes(nil, "us-west-2")
		require.Error(t, err)
	})
}

// ---- VPC CIDR disassociation ----

//nolint:tparallel // single subtest
func TestBatch2_DisassociateVpcCidrBlock(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vpc, _ := b.CreateVpc("10.0.0.0/16")
	assoc, setupErr := b.AssociateVpcCidrBlock(vpc.ID, "10.1.0.0/16")
	require.NoError(t, setupErr)

	t.Run("disassociates cidr block", func(t *testing.T) {
		require.NoError(t, b.DisassociateVpcCidrBlock(assoc.AssociationID))
	})
}

// ---- NAT gateway address ops ----

//nolint:tparallel,paralleltest // subtests share state
func TestBatch2_NatGatewayAddressOps(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	addr, _ := b.AllocateAddress()
	nat, setupErr := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, setupErr)

	t.Run("disassociate address", func(t *testing.T) {
		require.NoError(t, b.DisassociateNatGatewayAddress(nat.ID))
	})

	t.Run("associate address", func(t *testing.T) {
		addr2, _ := b.AllocateAddress()
		require.NoError(t, b.AssociateNatGatewayAddress(nat.ID, addr2.AllocationID))
	})

	t.Run("assign private address", func(t *testing.T) {
		require.NoError(t, b.AssignPrivateNatGatewayAddress(nat.ID))
	})

	t.Run("unknown NAT GW returns error", func(t *testing.T) {
		require.Error(t, b.DisassociateNatGatewayAddress("nat-missing"))
	})
}

// ---- Image lifecycle ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_ImageLifecycle(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	imageID := "ami-testimg"

	t.Run("disable and enable image", func(t *testing.T) {
		require.NoError(t, b.DisableImage(imageID))
		require.NoError(t, b.EnableImage(imageID))
	})

	t.Run("enable/disable image block public access", func(t *testing.T) {
		assert.Equal(t, "block-new-sharing", b.GetImageBlockPublicAccessState())
		require.NoError(t, b.EnableImageBlockPublicAccess("block-new-sharing"))
		assert.Equal(t, "block-new-sharing", b.GetImageBlockPublicAccessState())
		b.DisableImageBlockPublicAccess()
		assert.Equal(t, "unblocked", b.GetImageBlockPublicAccessState())
	})

	t.Run("enable/disable image deprecation", func(t *testing.T) {
		require.NoError(t, b.EnableImageDeprecation(imageID, "2027-01-01T00:00:00Z"))
		require.NoError(t, b.DisableImageDeprecation(imageID))
	})

	t.Run("enable/disable deregistration protection", func(t *testing.T) {
		require.NoError(t, b.EnableImageDeregistrationProtection(imageID))
		require.NoError(t, b.DisableImageDeregistrationProtection(imageID))
	})

	t.Run("modify and reset image attribute", func(t *testing.T) {
		require.NoError(t, b.ModifyImageAttribute(imageID, "description", "my new desc"))
		require.NoError(t, b.ResetImageAttribute(imageID, "description"))
	})

	t.Run("empty image ID returns error for disable", func(t *testing.T) {
		require.Error(t, b.DisableImage(""))
	})
}

//nolint:tparallel // single subtest
func TestBatch2_DescribeInstanceImageMetadata(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)

	t.Run("returns metadata for instances", func(t *testing.T) {
		items := b.DescribeInstanceImageMetadata([]string{insts[0].ID})
		require.Len(t, items, 1)
		assert.Equal(t, insts[0].ID, items[0].InstanceID)
		assert.Equal(t, "available", items[0].ImageState)
	})
}

// ---- Serial console ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_SerialConsole(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("default is false", func(t *testing.T) {
		assert.False(t, b.GetSerialConsoleAccessStatus())
	})

	t.Run("enable sets true", func(t *testing.T) {
		b.EnableSerialConsoleAccess()
		assert.True(t, b.GetSerialConsoleAccessStatus())
	})

	t.Run("disable sets false", func(t *testing.T) {
		b.DisableSerialConsoleAccess()
		assert.False(t, b.GetSerialConsoleAccessStatus())
	})
}

// ---- VGW route propagation ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_VgwRoutePropagation(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rt, _ := b.CreateRouteTable("vpc-default")

	t.Run("enable propagation", func(t *testing.T) {
		require.NoError(t, b.EnableVgwRoutePropagation(rt.ID, "vgw-12345678"))
	})

	t.Run("disable propagation", func(t *testing.T) {
		require.NoError(t, b.DisableVgwRoutePropagation(rt.ID, "vgw-12345678"))
	})

	t.Run("empty IDs return error", func(t *testing.T) {
		require.Error(t, b.EnableVgwRoutePropagation("", "vgw-123"))
	})
}

// ---- Default credit specification ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_DefaultCreditSpecification(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("default is standard", func(t *testing.T) {
		assert.Equal(t, "standard", b.GetDefaultCreditSpecification())
	})

	t.Run("modify changes setting", func(t *testing.T) {
		require.NoError(t, b.ModifyDefaultCreditSpecification("unlimited-cpu-credits"))
		assert.Equal(t, "unlimited-cpu-credits", b.GetDefaultCreditSpecification())
	})

	t.Run("empty value returns error", func(t *testing.T) {
		require.Error(t, b.ModifyDefaultCreditSpecification(""))
	})
}

// ---- Replace root volume tasks ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_ReplaceRootVolumeTasks(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	var taskID string

	t.Run("create task", func(t *testing.T) {
		task, err := b.CreateReplaceRootVolumeTask(inst.ID, "")
		require.NoError(t, err)
		assert.NotEmpty(t, task.ReplaceRootVolumeTaskID)
		assert.Equal(t, "completed", task.TaskState)
		taskID = task.ReplaceRootVolumeTaskID
	})

	t.Run("describe returns task", func(t *testing.T) {
		tasks := b.DescribeReplaceRootVolumeTasks([]string{taskID})
		require.Len(t, tasks, 1)
		assert.Equal(t, inst.ID, tasks[0].InstanceID)
	})

	t.Run("unknown instance returns error", func(t *testing.T) {
		_, err := b.CreateReplaceRootVolumeTask("i-missing", "")
		require.Error(t, err)
	})
}

// ---- Address transfers ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_AddressTransfers(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	addr, _ := b.AllocateAddress()

	t.Run("enable transfer", func(t *testing.T) {
		transfer, err := b.EnableAddressTransfer(addr.AllocationID, "111111111111")
		require.NoError(t, err)
		assert.Equal(t, "pending", transfer.TransferOfferStatus)
	})

	t.Run("describe returns transfer", func(t *testing.T) {
		transfers := b.DescribeAddressTransfers([]string{addr.AllocationID})
		require.Len(t, transfers, 1)
		assert.Equal(t, "111111111111", transfers[0].TransferAccountID)
	})

	t.Run("disable transfer removes it", func(t *testing.T) {
		require.NoError(t, b.DisableAddressTransfer(addr.AllocationID))
		transfers := b.DescribeAddressTransfers([]string{addr.AllocationID})
		assert.Empty(t, transfers)
	})

	t.Run("empty allocation ID returns error", func(t *testing.T) {
		_, err := b.EnableAddressTransfer("", "111")
		require.Error(t, err)
	})
}

// ---- Subnet CIDR reservations ----

//nolint:tparallel,paralleltest // subtests share sequential state
func TestBatch2_SubnetCidrReservations(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var reservationID string

	t.Run("create reservation", func(t *testing.T) {
		res, err := b.CreateSubnetCidrReservation("subnet-default", "172.31.0.0/28", "prefix", "test")
		require.NoError(t, err)
		assert.NotEmpty(t, res.SubnetCIDRReservationID)
		assert.Equal(t, "172.31.0.0/28", res.CIDR)
		reservationID = res.SubnetCIDRReservationID
	})

	t.Run("delete reservation", func(t *testing.T) {
		require.NoError(t, b.DeleteSubnetCidrReservation(reservationID))
	})

	t.Run("unknown subnet returns error", func(t *testing.T) {
		_, err := b.CreateSubnetCidrReservation("subnet-missing", "10.0.0.0/28", "prefix", "")
		require.Error(t, err)
	})
}

// ---- HTTP dispatch tests ----

func TestBatch2_HTTP_GetEbsEncryptionByDefault(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetEbsEncryptionByDefault"},
	})
	require.NoError(t, err)
}

func TestBatch2_HTTP_GetSerialConsoleAccessStatus(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetSerialConsoleAccessStatus"},
	})
	require.NoError(t, err)
}

func TestBatch2_HTTP_GetDefaultCreditSpecification(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetDefaultCreditSpecification"},
	})
	require.NoError(t, err)
}

func TestBatch2_HTTP_DescribeVpcEndpointConnectionNotifications(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeVpcEndpointConnectionNotifications"},
	})
	require.NoError(t, err)
}

func TestBatch2_HTTP_DescribeLockedSnapshots(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeLockedSnapshots"},
	})
	require.NoError(t, err)
}

func TestBatch2_HTTP_GetImageBlockPublicAccessState(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetImageBlockPublicAccessState"},
	})
	require.NoError(t, err)
}

func TestBatch2_HTTP_CreateSubnetCidrReservation(t *testing.T) {
	t.Parallel()
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":          []string{"CreateSubnetCidrReservation"},
		"SubnetId":        []string{"subnet-default"},
		"Cidr":            []string{"172.31.0.0/28"},
		"ReservationType": []string{"prefix"},
	})
	require.NoError(t, err)
}
