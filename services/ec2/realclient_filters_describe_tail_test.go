package ec2_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

const tailAcct = "000000000000"

type tailCase struct {
	name    string
	filters []types.Filter
	want    []string
}

func tailFilter(name string, values ...string) []types.Filter {
	return []types.Filter{{Name: aws.String(name), Values: values}}
}

func runTailCases[T any](
	t *testing.T,
	cases []tailCase,
	call func(ctx context.Context, f []types.Filter) ([]T, error),
	id func(T) string,
) {
	t.Helper()

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			items, err := call(t.Context(), tt.filters)
			require.NoError(t, err)

			got := make([]string, 0, len(items))
			for _, it := range items {
				got = append(got, id(it))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestRealClient_DescribeInstanceConnectEndpointsFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	ep1, err := b.CreateInstanceConnectEndpoint("subnet-default", nil, false)
	require.NoError(t, err)
	ep2, err := b.CreateInstanceConnectEndpoint("subnet-default", nil, false)
	require.NoError(t, err)
	require.NoError(t, b.CreateTags([]string{ep1.InstanceConnectEndpointID}, map[string]string{"Owner": "TeamA"}))

	id1, id2 := ep1.InstanceConnectEndpointID, ep2.InstanceConnectEndpointID
	both := []string{id1, id2}

	runTailCases(t, []tailCase{
		{"id", tailFilter("instance-connect-endpoint-id", id2), []string{id2}},
		{"state-hit", tailFilter("state", "create-complete"), both},
		{"state-miss", tailFilter("state", "delete-complete"), nil},
		{"subnet", tailFilter("subnet-id", ep1.SubnetID), both},
		{"vpc-miss", tailFilter("vpc-id", "vpc-nope"), nil},
		{"tag", tailFilter("tag:Owner", "TeamA"), []string{id1}},
		{"tag-value", tailFilter("tag-value", "TeamA"), []string{id1}},
	}, func(ctx context.Context, f []types.Filter) ([]types.Ec2InstanceConnectEndpoint, error) {
		out, callErr := client.DescribeInstanceConnectEndpoints(
			ctx, &ec2sdk.DescribeInstanceConnectEndpointsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.InstanceConnectEndpoints, nil
	}, func(e types.Ec2InstanceConnectEndpoint) string { return aws.ToString(e.InstanceConnectEndpointId) })
}

func TestRealClient_DescribeStoreImageTasksFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	img1, err := b.RegisterImage("a", "a", "")
	require.NoError(t, err)
	img2, err := b.RegisterImage("b", "b", "")
	require.NoError(t, err)
	_, err = b.CreateStoreImageTask(img1.ImageID, "bucket-one")
	require.NoError(t, err)
	_, err = b.CreateStoreImageTask(img2.ImageID, "bucket-two")
	require.NoError(t, err)

	call := func(ctx context.Context, f []types.Filter) ([]types.StoreImageTaskResult, error) {
		out, callErr := client.DescribeStoreImageTasks(ctx, &ec2sdk.DescribeStoreImageTasksInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.StoreImageTaskResults, nil
	}

	runTailCases(t, []tailCase{
		{"bucket", tailFilter("bucket", "bucket-two"), []string{img2.ImageID}},
		{"state-hit", tailFilter("task-state", "Completed"), []string{img1.ImageID, img2.ImageID}},
		{"state-miss", tailFilter("task-state", "InProgress"), nil},
	}, call, func(r types.StoreImageTaskResult) string {
		assert.Equal(t, "Completed", aws.ToString(r.StoreTaskState))

		return aws.ToString(r.AmiId)
	})

	t.Run("ids-ignore-filters", func(t *testing.T) {
		t.Parallel()

		out, callErr := client.DescribeStoreImageTasks(t.Context(), &ec2sdk.DescribeStoreImageTasksInput{
			ImageIds: []string{img1.ImageID}, Filters: tailFilter("bucket", "bucket-two"),
		})
		require.NoError(t, callErr)
		require.Len(t, out.StoreImageTaskResults, 1)
		assert.Equal(t, img1.ImageID, aws.ToString(out.StoreImageTaskResults[0].AmiId))
	})
}

func TestRealClient_DescribeScheduledInstancesFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	sis, err := b.PurchaseScheduledInstances([]ec2.ScheduledInstancePurchaseRequest{
		{PurchaseToken: "sit-us-east-1-c4large-weekly", InstanceCount: 1},
		{PurchaseToken: "sit-us-east-1-m4large-daily", InstanceCount: 1},
	})
	require.NoError(t, err)
	require.Len(t, sis, 2)

	id0, id1 := sis[0].ScheduledInstanceID, sis[1].ScheduledInstanceID

	runTailCases(t, []tailCase{
		{"az", tailFilter("availability-zone", "us-east-1b"), []string{id1}},
		{"type", tailFilter("instance-type", "c4.large"), []string{id0}},
		{"platform-hit", tailFilter("platform", sis[0].Platform), []string{id0, id1}},
		{"platform-miss", tailFilter("platform", "Windows"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.ScheduledInstance, error) {
		out, callErr := client.DescribeScheduledInstances(ctx, &ec2sdk.DescribeScheduledInstancesInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.ScheduledInstanceSet, nil
	}, func(s types.ScheduledInstance) string { return aws.ToString(s.ScheduledInstanceId) })
}

func TestRealClient_DescribeTrunkInterfaceAssociationsFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))
	branch, trunk := newTrunkTestENIs(t, b)

	vlan, err := b.AssociateTrunkInterface(branch, trunk, 5, 0, nil)
	require.NoError(t, err)
	branch2, trunk2 := newTrunkTestENIs(t, b)
	gre, err := b.AssociateTrunkInterface(branch2, trunk2, 0, 7, nil)
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"protocol-vlan", tailFilter("interface-protocol", "VLAN"), []string{vlan.AssociationID}},
		{"protocol-gre", tailFilter("interface-protocol", "GRE"), []string{gre.AssociationID}},
		{"gre-key", tailFilter("gre-key", "7"), []string{gre.AssociationID}},
		{"gre-key-miss", tailFilter("gre-key", "99"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.TrunkInterfaceAssociation, error) {
		out, callErr := client.DescribeTrunkInterfaceAssociations(
			ctx, &ec2sdk.DescribeTrunkInterfaceAssociationsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.InterfaceAssociations, nil
	}, func(a types.TrunkInterfaceAssociation) string { return aws.ToString(a.AssociationId) })
}

func TestRealClient_DescribeReservedInstancesListingsFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	b.SeedReservedInstancesOffering(
		"rio-tail", "t3.medium", "us-east-1a", "Linux/UNIX", "All Upfront", "standard", 94608000, 500.0, 0.0,
	)

	sched := []ec2.PriceScheduleEntry{{CurrencyCode: "USD", Price: 10, Term: 1}}
	ri1, err := b.PurchaseReservedInstancesOffering("rio-tail", 1)
	require.NoError(t, err)
	ri2, err := b.PurchaseReservedInstancesOffering("rio-tail", 1)
	require.NoError(t, err)
	l1, err := b.CreateReservedInstancesListing(ri1.ReservedInstancesID, 1, sched)
	require.NoError(t, err)
	l2, err := b.CreateReservedInstancesListing(ri2.ReservedInstancesID, 1, sched)
	require.NoError(t, err)
	_, err = b.CancelReservedInstancesListing(l2.ReservedInstancesListingID)
	require.NoError(t, err)

	id1, id2 := l1.ReservedInstancesListingID, l2.ReservedInstancesListingID

	runTailCases(t, []tailCase{
		{"ri-id", tailFilter("reserved-instances-id", ri1.ReservedInstancesID), []string{id1}},
		{"listing-id", tailFilter("reserved-instances-listing-id", id2), []string{id2}},
		{"status", tailFilter("status", "cancelled"), []string{id2}},
		{"status-miss", tailFilter("status", "closed"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.ReservedInstancesListing, error) {
		out, callErr := client.DescribeReservedInstancesListings(
			ctx, &ec2sdk.DescribeReservedInstancesListingsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.ReservedInstancesListings, nil
	}, func(l types.ReservedInstancesListing) string { return aws.ToString(l.ReservedInstancesListingId) })

	t.Run("ri-id-param", func(t *testing.T) {
		t.Parallel()

		in := &ec2sdk.DescribeReservedInstancesListingsInput{ReservedInstancesId: aws.String(ri2.ReservedInstancesID)}
		out, callErr := client.DescribeReservedInstancesListings(t.Context(), in)
		require.NoError(t, callErr)
		require.Len(t, out.ReservedInstancesListings, 1)
		assert.Equal(t, id2, aws.ToString(out.ReservedInstancesListings[0].ReservedInstancesListingId))
	})
}

func TestRealClient_DescribeFastSnapshotRestoresFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = tailAcct
	client := newTestEC2Client(t, h)

	vol, err := b.CreateVolume("us-east-1a", "gp3", 8, "")
	require.NoError(t, err)
	snap, err := b.CreateSnapshot(vol.ID, "s")
	require.NoError(t, err)
	require.NoError(t, b.EnableFastSnapshotRestores([]string{snap.SnapshotID}, []string{"us-east-1a", "us-east-1b"}))

	azs := []string{"us-east-1a", "us-east-1b"}

	runTailCases(t, []tailCase{
		{"az", tailFilter("availability-zone", "us-east-1b"), []string{"us-east-1b"}},
		{"snapshot", tailFilter("snapshot-id", snap.SnapshotID), azs},
		{"snapshot-miss", tailFilter("snapshot-id", "snap-nope"), nil},
		{"state", tailFilter("state", "enabled"), azs},
		{"owner-hit", tailFilter("owner-id", tailAcct), azs},
		{"owner-miss", tailFilter("owner-id", "111111111111"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.DescribeFastSnapshotRestoreSuccessItem, error) {
		out, callErr := client.DescribeFastSnapshotRestores(ctx, &ec2sdk.DescribeFastSnapshotRestoresInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.FastSnapshotRestores, nil
	}, func(r types.DescribeFastSnapshotRestoreSuccessItem) string { return aws.ToString(r.AvailabilityZone) })
}

func TestRealClient_DescribeMacModificationTasksFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	inst := newMacInstance(t, b)
	task, err := b.CreateMacSystemIntegrityProtectionModificationTask(inst, "enabled", nil, nil)
	require.NoError(t, err)

	want := []string{task.MacModificationTaskID}

	runTailCases(t, []tailCase{
		{"instance", tailFilter("instance-id", inst), want},
		{"instance-miss", tailFilter("instance-id", "i-nope"), nil},
		{"type", tailFilter("task-type", "sip-modification"), want},
		{"state-miss", tailFilter("task-state", "failed"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.MacModificationTask, error) {
		out, callErr := client.DescribeMacModificationTasks(ctx, &ec2sdk.DescribeMacModificationTasksInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.MacModificationTasks, nil
	}, func(m types.MacModificationTask) string { return aws.ToString(m.MacModificationTaskId) })
}

func TestRealClient_DescribeSecurityGroupVpcAssociationsFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	vpc1, err := b.CreateVpc("10.0.0.0/16", "default")
	require.NoError(t, err)
	vpc2, err := b.CreateVpc("10.1.0.0/16", "default")
	require.NoError(t, err)
	sg, err := b.CreateSecurityGroup("tail-sg", "d", vpc1.ID)
	require.NoError(t, err)
	_, err = b.AssociateSecurityGroupVpc(sg.ID, vpc2.ID)
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"vpc", tailFilter("vpc-id", vpc2.ID), []string{vpc2.ID}},
		{"vpc-miss", tailFilter("vpc-id", "vpc-nope"), nil},
		{"group", tailFilter("group-id", sg.ID), []string{vpc2.ID}},
		{"state-miss", tailFilter("state", "disassociated"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.SecurityGroupVpcAssociation, error) {
		out, callErr := client.DescribeSecurityGroupVpcAssociations(
			ctx, &ec2sdk.DescribeSecurityGroupVpcAssociationsInput{Filters: f},
		)
		if callErr != nil {
			return nil, callErr
		}

		return out.SecurityGroupVpcAssociations, nil
	}, func(a types.SecurityGroupVpcAssociation) string { return aws.ToString(a.VpcId) })
}

func TestRealClient_DescribeExportImageTasksFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))

	exp, err := b.ExportImage("ami-test", "", "", "", "", "")
	require.NoError(t, err)

	runTailCases(t, []tailCase{
		{"completed", tailFilter("task-state", "completed"), []string{exp.ExportImageTaskID}},
		{"active-miss", tailFilter("task-state", "active"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.ExportImageTask, error) {
		out, callErr := client.DescribeExportImageTasks(ctx, &ec2sdk.DescribeExportImageTasksInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.ExportImageTasks, nil
	}, func(e types.ExportImageTask) string { return aws.ToString(e.ExportImageTaskId) })
}

func TestRealClient_DescribeFastLaunchImagesFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = tailAcct
	client := newTestEC2Client(t, h)

	img, err := b.RegisterImage("fl-ami", "d", "x86_64")
	require.NoError(t, err)
	require.NoError(t, b.EnableFastLaunch(img.ImageID, ec2.FastLaunchConfig{ResourceType: "snapshot"}))

	want := []string{img.ImageID}

	runTailCases(t, []tailCase{
		{"type", tailFilter("resource-type", "snapshot"), want},
		{"type-miss", tailFilter("resource-type", "launch-template"), nil},
		{"state", tailFilter("state", "enabled"), want},
		{"owner", tailFilter("owner-id", tailAcct), want},
		{"owner-miss", tailFilter("owner-id", "999999999999"), nil},
	}, func(ctx context.Context, f []types.Filter) ([]types.DescribeFastLaunchImagesSuccessItem, error) {
		out, callErr := client.DescribeFastLaunchImages(ctx, &ec2sdk.DescribeFastLaunchImagesInput{Filters: f})
		if callErr != nil {
			return nil, callErr
		}

		return out.FastLaunchImages, nil
	}, func(i types.DescribeFastLaunchImagesSuccessItem) string { return aws.ToString(i.ImageId) })
}

func TestRealClient_DescribeInstancesSourceDestCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		modify *bool
		name   string
		want   bool
	}{
		{nil, "default-true", true},
		{aws.Bool(false), "disabled", false},
		{aws.Bool(true), "re-enabled", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestEC2Client(t, ec2.NewHandler(b))

			run, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
				ImageId: aws.String("ami-12345678"), InstanceType: types.InstanceTypeT3Micro,
				MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
			})
			require.NoError(t, err)
			require.NotNil(t, run.Instances[0].SourceDestCheck)
			assert.True(t, *run.Instances[0].SourceDestCheck)

			id := aws.ToString(run.Instances[0].InstanceId)

			if tt.modify != nil {
				_, err = client.ModifyInstanceAttribute(t.Context(), &ec2sdk.ModifyInstanceAttributeInput{
					InstanceId: aws.String(id), SourceDestCheck: &types.AttributeBooleanValue{Value: tt.modify},
				})
				require.NoError(t, err)
			}

			out, err := client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{InstanceIds: []string{id}})
			require.NoError(t, err)
			require.NotNil(t, out.Reservations[0].Instances[0].SourceDestCheck)
			assert.Equal(t, tt.want, *out.Reservations[0].Instances[0].SourceDestCheck)
		})
	}
}
