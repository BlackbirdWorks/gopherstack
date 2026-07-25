package autoscaling_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// xmlDescribeASGPolicyFieldsResponse is a minimal struct for parsing the seven
// previously-unwired CreateAutoScalingGroup/UpdateAutoScalingGroup fields back
// out of DescribeAutoScalingGroups.
type xmlDescribeASGPolicyFieldsResponse struct {
	XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
	Result  struct {
		AutoScalingGroups struct {
			Members []struct {
				AvailabilityZoneDistribution struct {
					CapacityDistributionStrategy string `xml:"CapacityDistributionStrategy"`
				} `xml:"AvailabilityZoneDistribution"`
				CapacityReservationSpecification struct {
					CapacityReservationPreference string `xml:"CapacityReservationPreference"`
				} `xml:"CapacityReservationSpecification"`
				InstanceLifecyclePolicy struct {
					RetentionTriggers struct {
						TerminateHookAbandon string `xml:"TerminateHookAbandon"`
					} `xml:"RetentionTriggers"`
				} `xml:"InstanceLifecyclePolicy"`
				DeletionProtection               string `xml:"DeletionProtection"`
				AutoScalingGroupName             string `xml:"AutoScalingGroupName"`
				AvailabilityZoneImpairmentPolicy struct {
					ImpairedZoneHealthCheckBehavior string `xml:"ImpairedZoneHealthCheckBehavior"`
					ZonalShiftEnabled               bool   `xml:"ZonalShiftEnabled"`
				} `xml:"AvailabilityZoneImpairmentPolicy"`
				InstanceMaintenancePolicy struct {
					MinHealthyPercentage int32 `xml:"MinHealthyPercentage"`
					MaxHealthyPercentage int32 `xml:"MaxHealthyPercentage"`
				} `xml:"InstanceMaintenancePolicy"`
			} `xml:"member"`
		} `xml:"AutoScalingGroups"`
	} `xml:"DescribeAutoScalingGroupsResult"`
}

// TestAutoscalingHandler_NewPolicyFieldsWireRoundTrip locks the full HTTP
// wire round trip (form-encoded request -> XML response) for the seven
// CreateAutoScalingGroupInput fields that PARITY.md flagged as accepted by
// the input struct but never parsed from the request or projected onto the
// response: AvailabilityZoneDistribution, AvailabilityZoneImpairmentPolicy,
// CapacityReservationSpecification, DeletionProtection,
// InstanceLifecyclePolicy, InstanceMaintenancePolicy (SkipZonalShiftValidation
// is a write-only request flag with no response projection in real AWS, so it
// is not asserted here).
func TestAutoscalingHandler_NewPolicyFieldsWireRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	name := "policy-fields-wire-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName": {name},
		"MinSize":              {"1"},
		"MaxSize":              {"3"},
		"AvailabilityZoneDistribution.CapacityDistributionStrategy":        {"balanced-only"},
		"AvailabilityZoneImpairmentPolicy.ImpairedZoneHealthCheckBehavior": {"IgnoreUnhealthy"},
		"AvailabilityZoneImpairmentPolicy.ZonalShiftEnabled":               {"true"},
		"CapacityReservationSpecification.CapacityReservationPreference":   {"capacity-reservations-only"},
		"DeletionProtection": {"prevent-force-deletion"},
		"InstanceLifecyclePolicy.RetentionTriggers.TerminateHookAbandon": {"retain"},
		"InstanceMaintenancePolicy.MinHealthyPercentage":                 {"50"},
		"InstanceMaintenancePolicy.MaxHealthyPercentage":                 {"150"},
		"SkipZonalShiftValidation":                                       {"true"},
	})
	require.Equal(t, http.StatusOK, code, body)

	code, body = doAS(t, h, "DescribeAutoScalingGroups", url.Values{
		"AutoScalingGroupNames.member.1": {name},
	})
	require.Equal(t, http.StatusOK, code, body)

	var parsed xmlDescribeASGPolicyFieldsResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	require.Len(t, parsed.Result.AutoScalingGroups.Members, 1)

	asg := parsed.Result.AutoScalingGroups.Members[0]

	assert.Equal(t, "balanced-only", asg.AvailabilityZoneDistribution.CapacityDistributionStrategy)
	assert.Equal(t, "IgnoreUnhealthy", asg.AvailabilityZoneImpairmentPolicy.ImpairedZoneHealthCheckBehavior)
	assert.True(t, asg.AvailabilityZoneImpairmentPolicy.ZonalShiftEnabled)
	assert.Equal(t, "capacity-reservations-only", asg.CapacityReservationSpecification.CapacityReservationPreference)
	assert.Equal(t, "prevent-force-deletion", asg.DeletionProtection)
	assert.Equal(t, "retain", asg.InstanceLifecyclePolicy.RetentionTriggers.TerminateHookAbandon)
	assert.Equal(t, int32(50), asg.InstanceMaintenancePolicy.MinHealthyPercentage)
	assert.Equal(t, int32(150), asg.InstanceMaintenancePolicy.MaxHealthyPercentage)
}

// TestAutoscalingHandler_DeletionProtectionErrorCode locks that deleting a
// DeletionProtection=prevent-all-deletion group surfaces the real AWS
// ResourceInUse error code (not a generic 500 or the wrong 400 code).
func TestAutoscalingHandler_DeletionProtectionErrorCode(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	name := "delete-protected-asg"

	code, body := doAS(t, h, "CreateAutoScalingGroup", url.Values{
		"AutoScalingGroupName": {name},
		"MinSize":              {"0"},
		"MaxSize":              {"1"},
		"DeletionProtection":   {"prevent-all-deletion"},
	})
	require.Equal(t, http.StatusOK, code, body)

	code, body = doAS(t, h, "DeleteAutoScalingGroup", url.Values{
		"AutoScalingGroupName": {name},
	})
	assert.Equal(t, http.StatusBadRequest, code, body)
	assert.Contains(t, body, "ResourceInUse")
}
