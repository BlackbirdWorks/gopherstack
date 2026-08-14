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

// TestCreateFlowLogs_TagSet_RealClient drives CreateFlowLogs with an inline
// TagSpecification through the real SDK client. Flow logs are recognized by
// the generic tag store (resourceExistsLocked in resource_types.go), so
// CreateTags/DescribeTags already work against a flow log ID -- but neither
// CreateFlowLogs nor DescribeFlowLogs ever emitted tagSet
// (ec2@v1.319.1 deserializers.go's FlowLog EqualFold list includes "tagSet"),
// and CreateFlowLogs never read TagSpecification from the request at all.
func TestCreateFlowLogs_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.40.0.0/16")})
	require.NoError(t, err)

	out, err := client.CreateFlowLogs(t.Context(), &ec2sdk.CreateFlowLogsInput{
		ResourceIds:        []string{aws.ToString(vpc.Vpc.VpcId)},
		ResourceType:       types.FlowLogsResourceTypeVpc,
		TrafficType:        types.TrafficTypeAll,
		LogDestinationType: types.LogDestinationTypeS3,
		LogDestination:     aws.String("arn:aws:s3:::wire-field-fixes-flow-logs"),
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeVpcFlowLog,
			Tags:         []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-flowlog")}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.FlowLogIds, 1)

	desc, err := client.DescribeFlowLogs(t.Context(), &ec2sdk.DescribeFlowLogsInput{
		FlowLogIds: out.FlowLogIds,
	})
	require.NoError(t, err)
	require.Len(t, desc.FlowLogs, 1)
	require.NotEmpty(t, desc.FlowLogs[0].Tags, "Tags empty - never emitted by CreateFlowLogs/DescribeFlowLogs")
	assert.Equal(t, "Name", aws.ToString(desc.FlowLogs[0].Tags[0].Key))
	assert.Equal(t, "wire-field-fixes-flowlog", aws.ToString(desc.FlowLogs[0].Tags[0].Value))
}

// TestCreateLaunchTemplate_TagSet_RealClient mirrors the flow-log case for
// launch templates: recognized by the generic tag store
// (resource_types.go's launchTemplates.Has), but CreateLaunchTemplate never
// read TagSpecifications from the request and neither Create/Describe/Modify
// nor the launchTemplateItem shape emitted tagSet (ec2@v1.319.1
// deserializers.go's LaunchTemplate EqualFold list includes "tagSet").
func TestCreateLaunchTemplate_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("wire-field-fixes-lt"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-12345678"),
			InstanceType: types.InstanceTypeT3Micro,
		},
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeLaunchTemplate,
			Tags:         []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-lt-tag")}},
		}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.LaunchTemplate.Tags, "Tags empty - CreateLaunchTemplate never read TagSpecifications")
	assert.Equal(t, "Name", aws.ToString(out.LaunchTemplate.Tags[0].Key))

	desc, err := client.DescribeLaunchTemplates(t.Context(), &ec2sdk.DescribeLaunchTemplatesInput{
		LaunchTemplateNames: []string{"wire-field-fixes-lt"},
	})
	require.NoError(t, err)
	require.Len(t, desc.LaunchTemplates, 1)
	require.NotEmpty(t, desc.LaunchTemplates[0].Tags, "Tags empty - never emitted by DescribeLaunchTemplates")
	assert.Equal(t, "wire-field-fixes-lt-tag", aws.ToString(desc.LaunchTemplates[0].Tags[0].Value))
}

// TestDeleteLaunchTemplate_ReturnsTemplate_RealClient drives
// DeleteLaunchTemplate through the real SDK client. Real
// DeleteLaunchTemplateOutput.LaunchTemplate (api_op_DeleteLaunchTemplate.go)
// carries the deleted template, wrapped under "launchTemplate"
// (deserializers.go's awsEc2query_deserializeOpDocumentDeleteLaunchTemplateOutput)
// -- the handler previously returned an entirely empty envelope, so a real
// client's out.LaunchTemplate was always nil regardless of what was deleted.
func TestDeleteLaunchTemplate_ReturnsTemplate_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("wire-field-fixes-lt-delete"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId: aws.String("ami-12345678"),
		},
	})
	require.NoError(t, err)

	out, err := client.DeleteLaunchTemplate(t.Context(), &ec2sdk.DeleteLaunchTemplateInput{
		LaunchTemplateId: created.LaunchTemplate.LaunchTemplateId,
	})
	require.NoError(t, err)
	require.NotNil(t, out.LaunchTemplate, "LaunchTemplate nil - DeleteLaunchTemplate returned an empty envelope")
	assert.Equal(
		t,
		aws.ToString(created.LaunchTemplate.LaunchTemplateId),
		aws.ToString(out.LaunchTemplate.LaunchTemplateId),
	)
	assert.Equal(t, "wire-field-fixes-lt-delete", aws.ToString(out.LaunchTemplate.LaunchTemplateName))
}

// TestDescribeLaunchTemplateVersions_RealShape_RealClient drives
// CreateLaunchTemplateVersion then DescribeLaunchTemplateVersions through the
// real SDK client. DescribeLaunchTemplateVersions previously reused the flat
// summary "LaunchTemplate" item shape (defaultVersionNumber/
// latestVersionNumber) instead of the real "LaunchTemplateVersion" shape
// (versionNumber, defaultVersion as a bool, and a nested launchTemplateData
// object -- ec2@v1.319.1 deserializers.go's
// awsEc2query_deserializeDocumentLaunchTemplateVersion). Since none of the
// emitted field names existed on the real type, a real client's
// VersionNumber, DefaultVersion and LaunchTemplateData were always zero/nil
// regardless of the tracked backend state.
func TestDescribeLaunchTemplateVersions_RealShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("wire-field-fixes-lt-versions"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-11112222"),
			InstanceType: types.InstanceTypeT3Small,
		},
	})
	require.NoError(t, err)
	ltID := created.LaunchTemplate.LaunchTemplateId

	_, err = client.CreateLaunchTemplateVersion(t.Context(), &ec2sdk.CreateLaunchTemplateVersionInput{
		LaunchTemplateId: ltID,
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-33334444"),
			InstanceType: types.InstanceTypeT3Medium,
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeLaunchTemplateVersions(t.Context(), &ec2sdk.DescribeLaunchTemplateVersionsInput{
		LaunchTemplateId: ltID,
	})
	require.NoError(t, err)
	require.Len(t, out.LaunchTemplateVersions, 1)

	v := out.LaunchTemplateVersions[0]
	assert.Equal(t, int64(2), aws.ToInt64(v.VersionNumber), "VersionNumber wrong/zero - wrong item shape used")
	assert.False(t, aws.ToBool(v.DefaultVersion), "version 2 was never set as default - wrong item shape used")
	require.NotNil(t, v.LaunchTemplateData, "LaunchTemplateData nil - never emitted by the flat summary shape")
	assert.Equal(t, "ami-33334444", aws.ToString(v.LaunchTemplateData.ImageId))
	assert.Equal(t, types.InstanceTypeT3Medium, v.LaunchTemplateData.InstanceType)
}

// TestDeleteLaunchTemplateVersions_WrapperKey_RealClient drives
// DeleteLaunchTemplateVersions through the real SDK client. The real
// wrapper key is "successfullyDeletedLaunchTemplateVersionSet"
// (ec2@v1.319.1 deserializers.go's
// awsEc2query_deserializeOpDocumentDeleteLaunchTemplateVersionsOutput) --
// the handler emitted "successfullyDeletedLaunchTemplateVersions" (missing
// the "Set" suffix), so a real client's
// SuccessfullyDeletedLaunchTemplateVersions was always empty regardless of
// what was actually deleted.
func TestDeleteLaunchTemplateVersions_WrapperKey_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	created, err := client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("wire-field-fixes-lt-delver"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId: aws.String("ami-55556666"),
		},
	})
	require.NoError(t, err)
	ltID := created.LaunchTemplate.LaunchTemplateId

	_, err = client.CreateLaunchTemplateVersion(t.Context(), &ec2sdk.CreateLaunchTemplateVersionInput{
		LaunchTemplateId:   ltID,
		LaunchTemplateData: &types.RequestLaunchTemplateData{ImageId: aws.String("ami-77778888")},
	})
	require.NoError(t, err)

	out, err := client.DeleteLaunchTemplateVersions(t.Context(), &ec2sdk.DeleteLaunchTemplateVersionsInput{
		LaunchTemplateId: ltID,
		Versions:         []string{"2"},
	})
	require.NoError(t, err)
	require.NotEmpty(
		t,
		out.SuccessfullyDeletedLaunchTemplateVersions,
		"SuccessfullyDeletedLaunchTemplateVersions empty - wrong wrapper key",
	)
	assert.Equal(t, int64(2), aws.ToInt64(out.SuccessfullyDeletedLaunchTemplateVersions[0].VersionNumber))
	assert.Equal(t, "wire-field-fixes-lt-delver",
		aws.ToString(out.SuccessfullyDeletedLaunchTemplateVersions[0].LaunchTemplateName))
}

// TestCreatePlacementGroup_ReturnsGroup_RealClient drives
// CreatePlacementGroup through the real SDK client. Real
// CreatePlacementGroupOutput.PlacementGroup (api_op_CreatePlacementGroup.go)
// is wrapped under "placementGroup" (deserializers.go's
// awsEc2query_deserializeOpDocumentCreatePlacementGroupOutput) -- the
// handler previously emitted only an invented "return" bool with no
// PlacementGroup at all, so a real client's out.PlacementGroup was always
// nil. Placement groups are also recognized by the generic tag store
// (resource_types.go's placementGroups.Has), so this also covers tagSet,
// never emitted despite CreatePlacementGroup/DescribePlacementGroups.
func TestCreatePlacementGroup_ReturnsGroup_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.CreatePlacementGroup(t.Context(), &ec2sdk.CreatePlacementGroupInput{
		GroupName: aws.String("wire-field-fixes-pg"),
		Strategy:  types.PlacementStrategyCluster,
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypePlacementGroup,
			Tags:         []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-pg-tag")}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, out.PlacementGroup, "PlacementGroup nil - CreatePlacementGroup returned an empty envelope")
	assert.Equal(t, "wire-field-fixes-pg", aws.ToString(out.PlacementGroup.GroupName))
	require.NotEmpty(t, out.PlacementGroup.Tags, "Tags empty - CreatePlacementGroup never read TagSpecifications")

	desc, err := client.DescribePlacementGroups(t.Context(), &ec2sdk.DescribePlacementGroupsInput{
		GroupNames: []string{"wire-field-fixes-pg"},
	})
	require.NoError(t, err)
	require.Len(t, desc.PlacementGroups, 1)
	require.NotEmpty(t, desc.PlacementGroups[0].Tags, "Tags empty - never emitted by DescribePlacementGroups")
	assert.Equal(t, "wire-field-fixes-pg-tag", aws.ToString(desc.PlacementGroups[0].Tags[0].Value))
}

// TestRequestSpotInstances_TagSet_RealClient drives RequestSpotInstances
// through the real SDK client. Spot requests are recognized by the generic
// tag store (resource_types.go's spotRequests.Has), but RequestSpotInstances
// never read TagSpecifications and neither Request/DescribeSpotInstanceRequests
// emitted tagSet (ec2@v1.319.1 deserializers.go's SpotInstanceRequest
// EqualFold list includes "tagSet").
func TestRequestSpotInstances_TagSet_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.RequestSpotInstances(t.Context(), &ec2sdk.RequestSpotInstancesInput{
		SpotPrice: aws.String("0.05"),
		LaunchSpecification: &types.RequestSpotLaunchSpecification{
			ImageId:      aws.String("ami-abcdef01"),
			InstanceType: types.InstanceTypeT3Micro,
		},
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeSpotInstancesRequest,
			Tags:         []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-spotreq")}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.SpotInstanceRequests, 1)
	require.NotEmpty(t, out.SpotInstanceRequests[0].Tags,
		"Tags empty - RequestSpotInstances never read TagSpecifications")

	reqID := out.SpotInstanceRequests[0].SpotInstanceRequestId
	desc, err := client.DescribeSpotInstanceRequests(t.Context(), &ec2sdk.DescribeSpotInstanceRequestsInput{
		SpotInstanceRequestIds: []string{aws.ToString(reqID)},
	})
	require.NoError(t, err)
	require.Len(t, desc.SpotInstanceRequests, 1)
	require.NotEmpty(t, desc.SpotInstanceRequests[0].Tags, "Tags empty - never emitted by DescribeSpotInstanceRequests")
	assert.Equal(t, "wire-field-fixes-spotreq", aws.ToString(desc.SpotInstanceRequests[0].Tags[0].Value))
}

// TestDescribeSpotFleetRequests_LaunchSpecifications_RealClient drives
// RequestSpotFleet then DescribeSpotFleetRequests through the real SDK
// client. The real nested field is "launchSpecifications"
// (ec2@v1.319.1 deserializers.go's SpotFleetRequestConfigData EqualFold
// list) -- the handler emitted "launchSpecificationsSet", so a real
// client's LaunchSpecifications was always nil regardless of what was
// configured. Also covers tagSet on the fleet resource itself (spot fleets
// are recognized by the generic tag store, resource_types.go's
// spotFleets.Has, but DescribeSpotFleetRequests never emitted it).
func TestDescribeSpotFleetRequests_LaunchSpecifications_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	req, err := client.RequestSpotFleet(t.Context(), &ec2sdk.RequestSpotFleetInput{
		SpotFleetRequestConfig: &types.SpotFleetRequestConfigData{
			IamFleetRole:   aws.String("arn:aws:iam::000000000000:role/fleet-role"),
			TargetCapacity: aws.Int32(1),
			LaunchSpecifications: []types.SpotFleetLaunchSpecification{{
				ImageId:      aws.String("ami-fleet0001"),
				InstanceType: types.InstanceTypeM5Large,
			}},
		},
	})
	require.NoError(t, err)
	fleetID := req.SpotFleetRequestId

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{aws.ToString(fleetID)},
		Tags:      []types.Tag{{Key: aws.String("Name"), Value: aws.String("wire-field-fixes-fleet")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeSpotFleetRequests(t.Context(), &ec2sdk.DescribeSpotFleetRequestsInput{
		SpotFleetRequestIds: []string{aws.ToString(fleetID)},
	})
	require.NoError(t, err)
	require.Len(t, out.SpotFleetRequestConfigs, 1)

	cfg := out.SpotFleetRequestConfigs[0].SpotFleetRequestConfig
	require.NotNil(t, cfg, "SpotFleetRequestConfig nil")
	require.NotEmpty(t, cfg.LaunchSpecifications, "LaunchSpecifications empty - wrong wrapper key")
	assert.Equal(t, "ami-fleet0001", aws.ToString(cfg.LaunchSpecifications[0].ImageId))

	require.NotEmpty(t, out.SpotFleetRequestConfigs[0].Tags, "Tags empty - never emitted by DescribeSpotFleetRequests")
	assert.Equal(t, "wire-field-fixes-fleet", aws.ToString(out.SpotFleetRequestConfigs[0].Tags[0].Value))
}

// TestDescribeHostReservations_OfferingID_RealClient drives
// PurchaseHostReservation then DescribeHostReservations through the real SDK
// client. HostReservation.OfferingID is set at purchase time
// (host_reservations.go's PurchaseHostReservation) and the real wire field
// "offeringId" is part of HostReservation (ec2@v1.319.1 deserializers.go's
// awsEc2query_deserializeDocumentHostReservation), but the response item
// never carried it.
func TestDescribeHostReservations_OfferingID_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	offerings, err := client.DescribeHostReservationOfferings(
		t.Context(), &ec2sdk.DescribeHostReservationOfferingsInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, offerings.OfferingSet)
	offeringID := offerings.OfferingSet[0].OfferingId

	dh, err := client.AllocateHosts(t.Context(), &ec2sdk.AllocateHostsInput{
		AvailabilityZone: aws.String("us-east-1a"),
		InstanceType:     aws.String("m5.large"),
		Quantity:         aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotEmpty(t, dh.HostIds)

	_, err = client.PurchaseHostReservation(t.Context(), &ec2sdk.PurchaseHostReservationInput{
		HostIdSet:  dh.HostIds,
		OfferingId: offeringID,
	})
	require.NoError(t, err)

	out, err := client.DescribeHostReservations(t.Context(), &ec2sdk.DescribeHostReservationsInput{})
	require.NoError(t, err)
	require.Len(t, out.HostReservationSet, 1)
	assert.Equal(t, aws.ToString(offeringID), aws.ToString(out.HostReservationSet[0].OfferingId),
		"OfferingId empty - never emitted by DescribeHostReservations")
}
