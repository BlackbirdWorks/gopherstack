package autoscaling_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestAutoscalingHandler_DescribeAutoScalingGroupsStatusEmpty verifies that DescribeAutoScalingGroups
// returns empty Status for a normal group (AWS only populates Status during deletion).
func TestAutoscalingHandler_DescribeAutoScalingGroupsStatusEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asgName string
	}{
		{"basic group", "status-test-asg"},
		{"second group", "status-test-asg-2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":       {tc.asgName},
				"MinSize":                    {"0"},
				"MaxSize":                    {"3"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
				"AutoScalingGroupNames.member.1": {tc.asgName},
			})
			require.Equal(t, 200, code)

			assert.NotContains(t, body, "<Status>Active</Status>",
				"operational group must not have Status='Active'")
		})
	}
}

// TestAutoscalingHandler_DescribeAutoScalingGroupsTagsShape verifies that DescribeAutoScalingGroups
// returns tags with ResourceId and ResourceType populated (matching real AWS).
func TestAutoscalingHandler_DescribeAutoScalingGroupsTagsShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asgName string
		tagKey  string
		tagVal  string
	}{
		{"env tag", "tagged-asg", "Environment", "prod"},
		{"name tag", "tagged-asg-2", "Name", "my-asg"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
				"AutoScalingGroupName":            {tc.asgName},
				"MinSize":                         {"0"},
				"MaxSize":                         {"3"},
				"AvailabilityZones.member.1":      {"us-east-1a"},
				"Tags.member.1.Key":               {tc.tagKey},
				"Tags.member.1.Value":             {tc.tagVal},
				"Tags.member.1.PropagateAtLaunch": {"true"},
			})
			require.Equal(t, 200, code, body)

			code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
				"AutoScalingGroupNames.member.1": {tc.asgName},
			})
			require.Equal(t, 200, code)

			assert.Contains(t, body, fmt.Sprintf("<ResourceId>%s</ResourceId>", tc.asgName),
				"tag must have ResourceId=asgName")
			assert.Contains(t, body, "<ResourceType>auto-scaling-group</ResourceType>",
				"tag must have ResourceType=auto-scaling-group")
		})
	}
}

// TestAutoscalingHandler_DescribeAutoScalingGroupsInstanceType verifies that instances in DescribeAutoScalingGroups
// include InstanceType when the launch configuration specifies one.
func TestAutoscalingHandler_DescribeAutoScalingGroupsInstanceType(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	lcName := "it-lc"
	asgName := "it-asg"

	code, body := doAS(t, h, "CreateLaunchConfiguration", url.Values{
		"LaunchConfigurationName": {lcName},
		"ImageId":                 {"ami-12345678"},
		"InstanceType":            {"m5.large"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"LaunchConfigurationName":    {lcName},
		"MinSize":                    {"1"},
		"MaxSize":                    {"3"},
		"DesiredCapacity":            {"1"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code)

	assert.Contains(t, body, "<InstanceType>m5.large</InstanceType>",
		"instance in ASG must include InstanceType; got: %s", body)
}

// TestAutoscalingHandler_DescribeAutoScalingGroupsServiceLinkedRoleARN verifies that DescribeAutoScalingGroups includes
// ServiceLinkedRoleARN in the response.
func TestAutoscalingHandler_DescribeAutoScalingGroupsServiceLinkedRoleARN(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "slr-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code)

	assert.Contains(t, body, "AWSServiceRoleForAutoScaling",
		"DescribeAutoScalingGroups must include ServiceLinkedRoleARN; got: %s", body)
	assert.Contains(t, body, "autoscaling.amazonaws.com",
		"ServiceLinkedRoleARN must reference autoscaling.amazonaws.com service principal")
}

// xmlDescribeASGStructureResponse is a minimal struct for parsing DescribeAutoScalingGroups.
type xmlDescribeASGStructureResponse struct {
	XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
	Result  struct {
		AutoScalingGroups struct {
			Members []struct {
				AutoScalingGroupName string `xml:"AutoScalingGroupName"`
				Status               string `xml:"Status"`
				ServiceLinkedRoleARN string `xml:"ServiceLinkedRoleARN"`
				Tags                 struct {
					Members []struct {
						Key          string `xml:"Key"`
						Value        string `xml:"Value"`
						ResourceID   string `xml:"ResourceId"`
						ResourceType string `xml:"ResourceType"`
					} `xml:"member"`
				} `xml:"Tags"`
				Instances struct {
					Members []struct {
						InstanceID   string `xml:"InstanceId"`
						InstanceType string `xml:"InstanceType"`
					} `xml:"member"`
				} `xml:"Instances"`
			} `xml:"member"`
		} `xml:"AutoScalingGroups"`
	} `xml:"DescribeAutoScalingGroupsResult"`
}

// TestAutoscalingHandler_DescribeAutoScalingGroupsXMLStructure parses DescribeAutoScalingGroups
// XML and verifies field values.
func TestAutoscalingHandler_DescribeAutoScalingGroupsXMLStructure(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "xml-struct-asg"
	lcName := "xml-struct-lc"

	code, body := doAS(t, h, "CreateLaunchConfiguration", url.Values{
		"LaunchConfigurationName": {lcName},
		"ImageId":                 {"ami-00000001"},
		"InstanceType":            {"t3.small"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":            {asgName},
		"LaunchConfigurationName":         {lcName},
		"MinSize":                         {"1"},
		"MaxSize":                         {"3"},
		"DesiredCapacity":                 {"1"},
		"AvailabilityZones.member.1":      {"us-east-1a"},
		"Tags.member.1.Key":               {"Project"},
		"Tags.member.1.Value":             {"gopherstack"},
		"Tags.member.1.PropagateAtLaunch": {"true"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	var parsed xmlDescribeASGStructureResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	require.Len(t, parsed.Result.AutoScalingGroups.Members, 1)

	asg := parsed.Result.AutoScalingGroups.Members[0]

	assert.Equal(t, asgName, asg.AutoScalingGroupName)
	assert.Empty(t, asg.Status, "operational ASG must have empty Status")
	assert.Contains(t, asg.ServiceLinkedRoleARN, "AWSServiceRoleForAutoScaling",
		"ServiceLinkedRoleARN must be set")

	require.Len(t, asg.Tags.Members, 1)
	tag := asg.Tags.Members[0]
	assert.Equal(t, asgName, tag.ResourceID, "tag ResourceId must equal ASG name")
	assert.Equal(t, "auto-scaling-group", tag.ResourceType)

	require.NotEmpty(t, asg.Instances.Members, "ASG with DesiredCapacity=1 must have an instance")
	inst := asg.Instances.Members[0]
	assert.Equal(t, "t3.small", inst.InstanceType, "instance must have InstanceType")
}

// TestAutoscalingHandler_MixedInstancesPolicyRoundTrip verifies that CreateAutoScalingGroup parses
// MixedInstancesPolicy (launch template, overrides, instances distribution) and that
// DescribeAutoScalingGroups returns it. Previously this field was silently dropped:
// accepted on the wire, never stored, never returned.
func TestAutoscalingHandler_MixedInstancesPolicyRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "mip-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"AvailabilityZones.member.1": {"us-east-1a"},
		"MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateId": {"lt-0123456789"},
		"MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification.Version":          {"$Latest"},
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.InstanceType":              {"t3.micro"},
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.WeightedCapacity":          {"1"},
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.2.InstanceType":              {"t3.small"},
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.2.WeightedCapacity":          {"2"},
		"MixedInstancesPolicy.InstancesDistribution.OnDemandBaseCapacity":                  {"1"},
		"MixedInstancesPolicy.InstancesDistribution.SpotAllocationStrategy":                {"capacity-optimized"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	parsed := describeASGInstances(t, body)
	mip := parsed.Result.AutoScalingGroups.Members[0].MixedInstancesPolicy

	assert.Equal(t, "lt-0123456789", mip.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateID)
	require.Len(t, mip.LaunchTemplate.Overrides.Members, 2)
	assert.Equal(t, "t3.micro", mip.LaunchTemplate.Overrides.Members[0].InstanceType)
	assert.Equal(t, "t3.small", mip.LaunchTemplate.Overrides.Members[1].InstanceType)
	assert.Equal(t, int32(1), mip.InstancesDistribution.OnDemandBaseCapacity)
	assert.Equal(t, "capacity-optimized", mip.InstancesDistribution.SpotAllocationStrategy)
}

// TestAutoscalingHandler_MixedInstancesPolicyInstanceRequirementsRoundTrip verifies
// that an override using InstanceRequirements (attribute-based instance type
// selection, previously an entirely unmodelled 25-field struct) round-trips through
// CreateAutoScalingGroup/DescribeAutoScalingGroups, and that an InstanceRequirements-
// only override (no InstanceType) no longer truncates the Overrides list: the
// loop-continuation check in parseLaunchTemplateOverrides previously only looked at
// InstanceType/WeightedCapacity/LaunchTemplateSpecification, so an override carrying
// only InstanceRequirements was indistinguishable from "no more members" and every
// override after it in the list was silently dropped too.
func TestAutoscalingHandler_MixedInstancesPolicyInstanceRequirementsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	asgName := "mip-ir-asg"

	const irPrefix = "MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.InstanceRequirements."

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName":       {asgName},
		"MinSize":                    {"0"},
		"MaxSize":                    {"5"},
		"AvailabilityZones.member.1": {"us-east-1a"},
		"MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateId": {"lt-0123456789"},
		irPrefix + "VCpuCount.Min":                                  {"2"},
		irPrefix + "VCpuCount.Max":                                  {"4"},
		irPrefix + "MemoryMiB.Min":                                  {"2048"},
		irPrefix + "BurstablePerformance":                           {"excluded"},
		irPrefix + "RequireHibernateSupport":                        {"true"},
		irPrefix + "MaxSpotPriceAsPercentageOfOptimalOnDemandPrice": {"75"},
		irPrefix + "AllowedInstanceTypes.member.1":                  {"m5.*"},
		irPrefix + "CpuManufacturers.member.1":                      {"intel"},
		// A second override with a plain InstanceType, to prove the first
		// (InstanceRequirements-only) member didn't truncate the list.
		"MixedInstancesPolicy.LaunchTemplate.Overrides.member.2.InstanceType": {"t3.large"},
	})
	require.Equal(t, 200, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {asgName},
	})
	require.Equal(t, 200, code, body)

	assert.Contains(t, body, "<InstanceType>t3.large</InstanceType>",
		"second override must survive; got: %s", body)
	assert.Contains(t, body, "<VCpuCount><Min>2</Min><Max>4</Max></VCpuCount>",
		"VCpuCount range must round-trip; got: %s", body)
	assert.Contains(t, body, "<MemoryMiB><Min>2048</Min></MemoryMiB>",
		"MemoryMiB range must round-trip; got: %s", body)
	assert.Contains(t, body, "<BurstablePerformance>excluded</BurstablePerformance>",
		"BurstablePerformance must round-trip; got: %s", body)
	assert.Contains(t, body, "<RequireHibernateSupport>true</RequireHibernateSupport>",
		"RequireHibernateSupport must round-trip; got: %s", body)
	assert.Contains(t, body,
		"<MaxSpotPriceAsPercentageOfOptimalOnDemandPrice>75</MaxSpotPriceAsPercentageOfOptimalOnDemandPrice>",
		"MaxSpotPriceAsPercentageOfOptimalOnDemandPrice must round-trip; got: %s", body)
	assert.Contains(t, body, "<AllowedInstanceTypes><member>m5.*</member></AllowedInstanceTypes>",
		"AllowedInstanceTypes must round-trip; got: %s", body)
	assert.Contains(t, body, "<CpuManufacturers><member>intel</member></CpuManufacturers>",
		"CpuManufacturers must round-trip; got: %s", body)
}

func TestAutoscalingHandler_CreateAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=test-asg&MinSize=1&MaxSize=5" +
				"&DesiredCapacity=2&AvailabilityZones.member.1=us-east-1a",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_name",
			body:       "Action=CreateAutoScalingGroup&Version=2011-01-01&MinSize=1&MaxSize=5",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate_group",
			body:       "Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dup-asg&MinSize=1&MaxSize=5",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			if tt.name == "duplicate_group" {
				// pre-create
				rec := postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dup-asg&MinSize=1&MaxSize=5",
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_DescribeAutoScalingGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
		wantGroups int
	}{
		{
			name:       "empty",
			body:       "Action=DescribeAutoScalingGroups&Version=2011-01-01",
			wantStatus: http.StatusOK,
			wantGroups: 0,
		},
		{
			name: "with_groups",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=asg-a&MinSize=1&MaxSize=3",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=asg-b&MinSize=2&MaxSize=6",
				)
			},
			body:       "Action=DescribeAutoScalingGroups&Version=2011-01-01",
			wantStatus: http.StatusOK,
			wantGroups: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantGroups > 0 {
				var resp struct {
					XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
					Result  struct {
						AutoScalingGroups struct {
							Members []struct {
								Name string `xml:"AutoScalingGroupName"`
							} `xml:"member"`
						} `xml:"AutoScalingGroups"`
					} `xml:"DescribeAutoScalingGroupsResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp.Result.AutoScalingGroups.Members, tt.wantGroups)
			}
		})
	}
}

func TestAutoscalingHandler_DeleteAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "delete_existing",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=del-asg&MinSize=0&MaxSize=0",
				)
			},
			body:       "Action=DeleteAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=del-asg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_nonexistent",
			body:       "Action=DeleteAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_UpdateAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "update_existing",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=upd-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=upd-asg&MaxSize=10",
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_nonexistent",
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=no-such&MaxSize=3",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
