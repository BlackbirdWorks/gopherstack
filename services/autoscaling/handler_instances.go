package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleAttachInstances(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")

	if err := h.Backend.AttachInstances(groupName, instanceIDs); err != nil {
		return nil, err
	}

	return &attachInstancesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-attach-instances"},
	}, nil
}

func (h *Handler) handleTerminateInstanceInAutoScalingGroup(vals url.Values) (any, error) {
	instanceID := vals.Get("InstanceId")
	decrement := vals.Get("ShouldDecrementDesiredCapacity") == formValueTrue

	activity, err := h.Backend.TerminateInstanceInAutoScalingGroup(instanceID, decrement)
	if err != nil {
		return nil, err
	}

	return &terminateInstanceInAutoScalingGroupResponse{
		Xmlns: autoscalingXMLNS,
		Result: terminateInstanceResult{
			Activity: toXMLScalingActivity(activity),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-terminate-instance"},
	}, nil
}

func (h *Handler) handleDescribeAutoScalingInstances(vals url.Values) (any, error) {
	instanceIDs := parseMembers(vals, "InstanceIds.member")

	instances, err := h.Backend.DescribeAutoScalingInstances(instanceIDs)
	if err != nil {
		return nil, err
	}

	members := make([]xmlInstanceDetails, 0, len(instances))
	for _, inst := range instances {
		members = append(members, xmlInstanceDetails{
			InstanceID:              inst.InstanceID,
			AutoScalingGroupName:    inst.AutoScalingGroupName,
			AvailabilityZone:        inst.AvailabilityZone,
			LifecycleState:          inst.LifecycleState,
			HealthStatus:            inst.HealthStatus,
			LaunchConfigurationName: inst.LaunchConfigurationName,
			InstanceType:            inst.InstanceType,
			ProtectedFromScaleIn:    inst.ProtectedFromScaleIn,
		})
	}

	return &describeAutoScalingInstancesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAutoScalingInstancesResult{
			AutoScalingInstances: xmlInstanceDetailsList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-instances"},
	}, nil
}

// --- new XML response types ---

type attachInstancesResponse struct {
	XMLName          xml.Name            `xml:"AttachInstancesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type terminateInstanceResult struct {
	Activity xmlScalingActivity `xml:"Activity"`
}

type terminateInstanceInAutoScalingGroupResponse struct {
	XMLName          xml.Name                `xml:"TerminateInstanceInAutoScalingGroupResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           terminateInstanceResult `xml:"TerminateInstanceInAutoScalingGroupResult"`
}

type xmlInstanceDetails struct {
	InstanceID              string `xml:"InstanceId"`
	AutoScalingGroupName    string `xml:"AutoScalingGroupName"`
	AvailabilityZone        string `xml:"AvailabilityZone"`
	LifecycleState          string `xml:"LifecycleState"`
	HealthStatus            string `xml:"HealthStatus"`
	LaunchConfigurationName string `xml:"LaunchConfigurationName,omitempty"`
	InstanceType            string `xml:"InstanceType,omitempty"`
	ProtectedFromScaleIn    bool   `xml:"ProtectedFromScaleIn"`
}

type xmlInstanceDetailsList struct {
	Members []xmlInstanceDetails `xml:"member"`
}

type describeAutoScalingInstancesResult struct {
	NextToken            string                 `xml:"NextToken,omitempty"`
	AutoScalingInstances xmlInstanceDetailsList `xml:"AutoScalingInstances"`
}

type describeAutoScalingInstancesResponse struct {
	XMLName          xml.Name                           `xml:"DescribeAutoScalingInstancesResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           describeAutoScalingInstancesResult `xml:"DescribeAutoScalingInstancesResult"`
}

func (h *Handler) handleDetachInstances(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")
	decrement := vals.Get("ShouldDecrementDesiredCapacity") == formValueTrue

	activities, err := h.Backend.DetachInstances(groupName, instanceIDs, decrement)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &detachInstancesResponse{
		Xmlns: autoscalingXMLNS,
		Result: detachInstancesResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-detach-instances"},
	}, nil
}

func (h *Handler) handleEnterStandby(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")
	decrement := vals.Get("ShouldDecrementDesiredCapacity") == formValueTrue

	activities, err := h.Backend.EnterStandby(groupName, instanceIDs, decrement)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &enterStandbyResponse{
		Xmlns: autoscalingXMLNS,
		Result: enterStandbyResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-enter-standby"},
	}, nil
}

func (h *Handler) handleExitStandby(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")

	activities, err := h.Backend.ExitStandby(groupName, instanceIDs)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &exitStandbyResponse{
		Xmlns: autoscalingXMLNS,
		Result: exitStandbyResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-exit-standby"},
	}, nil
}

func (h *Handler) handleSetInstanceHealth(vals url.Values) (any, error) {
	instanceID := vals.Get("InstanceId")
	healthStatus := vals.Get("HealthStatus")
	respectGracePeriod := vals.Get("ShouldRespectGracePeriod") != "false"

	if err := h.Backend.SetInstanceHealth(instanceID, healthStatus, respectGracePeriod); err != nil {
		return nil, err
	}

	return &setInstanceHealthResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-set-instance-health"},
	}, nil
}

func (h *Handler) handleSetInstanceProtection(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	instanceIDs := parseMembers(vals, "InstanceIds.member")
	protected := vals.Get("ProtectedFromScaleIn") == formValueTrue

	if err := h.Backend.SetInstanceProtection(groupName, instanceIDs, protected); err != nil {
		return nil, err
	}

	return &setInstanceProtectionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-set-instance-protection"},
	}, nil
}

func (h *Handler) handleLaunchInstances(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	clientToken := vals.Get("ClientToken")

	// The real API field is RequestedCapacity, not DesiredCapacity (LaunchInstances
	// does not affect the group's DesiredCapacity setting the way SetDesiredCapacity
	// does; it launches an explicit, additional count of instances).
	requestedCapacity, err := parseIntVal(vals.Get("RequestedCapacity"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid RequestedCapacity", ErrInvalidParameter)
	}

	count := int32(1)
	if requestedCapacity > 0 {
		count = requestedCapacity
	}

	instances, launchErr := h.Backend.LaunchInstances(groupName, count)
	if launchErr != nil {
		return nil, launchErr
	}

	return &launchInstancesResponse{
		Xmlns: autoscalingXMLNS,
		Result: launchInstancesResult{
			AutoScalingGroupName: groupName,
			ClientToken:          clientToken,
			Instances:            toXMLInstanceCollections(instances),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-launch-instances"},
	}, nil
}

// toXMLInstanceCollections groups launched instances by (AvailabilityZone,
// InstanceType) into the InstanceCollection shape LaunchInstancesOutput actually
// returns (a flat per-instance list with LifecycleState/HealthStatus belongs to
// DescribeAutoScalingGroups/DescribeAutoScalingInstances, not this operation).
func toXMLInstanceCollections(instances []Instance) xmlInstanceCollectionList {
	type collectionKey struct{ az, instanceType string }

	order := make([]collectionKey, 0, len(instances))
	grouped := make(map[collectionKey][]string, len(instances))

	for _, inst := range instances {
		key := collectionKey{az: inst.AvailabilityZone, instanceType: inst.InstanceType}
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}

		grouped[key] = append(grouped[key], inst.InstanceID)
	}

	members := make([]xmlInstanceCollection, 0, len(order))

	for _, key := range order {
		ids := make([]xmlStringValue, 0, len(grouped[key]))
		for _, id := range grouped[key] {
			ids = append(ids, xmlStringValue{Value: id})
		}

		members = append(members, xmlInstanceCollection{
			AvailabilityZone: key.az,
			InstanceType:     key.instanceType,
			InstanceIDs:      xmlStringValueList{Members: ids},
		})
	}

	return xmlInstanceCollectionList{Members: members}
}

type detachInstancesResult struct {
	Activities xmlScalingActivityList `xml:"Activities"`
}

type detachInstancesResponse struct {
	XMLName          xml.Name              `xml:"DetachInstancesResponse"`
	Xmlns            string                `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata   `xml:"ResponseMetadata"`
	Result           detachInstancesResult `xml:"DetachInstancesResult"`
}

type enterStandbyResult struct {
	Activities xmlScalingActivityList `xml:"Activities"`
}

type enterStandbyResponse struct {
	XMLName          xml.Name            `xml:"EnterStandbyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           enterStandbyResult  `xml:"EnterStandbyResult"`
}

type exitStandbyResult struct {
	Activities xmlScalingActivityList `xml:"Activities"`
}

type exitStandbyResponse struct {
	XMLName          xml.Name            `xml:"ExitStandbyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           exitStandbyResult   `xml:"ExitStandbyResult"`
}

type setInstanceHealthResponse struct {
	XMLName          xml.Name            `xml:"SetInstanceHealthResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type setInstanceProtectionResponse struct {
	XMLName          xml.Name            `xml:"SetInstanceProtectionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// xmlInstanceCollection mirrors the real LaunchInstancesOutput.Instances shape: AWS
// groups newly-launched instances by (AvailabilityZone, InstanceType, ...) and
// reports their IDs together, NOT as a flat per-instance list with lifecycle/health
// fields (that shape belongs to DescribeAutoScalingGroups/DescribeAutoScalingInstances).
type xmlInstanceCollection struct {
	AvailabilityZone string             `xml:"AvailabilityZone,omitempty"`
	InstanceType     string             `xml:"InstanceType,omitempty"`
	InstanceIDs      xmlStringValueList `xml:"InstanceIds,omitempty"`
}

type xmlInstanceCollectionList struct {
	Members []xmlInstanceCollection `xml:"member"`
}

type launchInstancesResult struct {
	AutoScalingGroupName string                    `xml:"AutoScalingGroupName,omitempty"`
	ClientToken          string                    `xml:"ClientToken,omitempty"`
	Instances            xmlInstanceCollectionList `xml:"Instances"`
}

type launchInstancesResponse struct {
	XMLName          xml.Name              `xml:"LaunchInstancesResponse"`
	Xmlns            string                `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata   `xml:"ResponseMetadata"`
	Result           launchInstancesResult `xml:"LaunchInstancesResult"`
}
