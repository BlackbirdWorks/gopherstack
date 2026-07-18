package cloudformation

import (
	"fmt"
	"strconv"

	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// maxAutoScalingCapacity is a hard upper bound on AutoScaling group sizes created via CloudFormation.
// It prevents excessive memory allocations when provisioning AutoScaling groups from untrusted templates.
const maxAutoScalingCapacity int32 = 1000

// ---- AutoScaling ----

// parseASGSizes reads MinSize, MaxSize, and DesiredCapacity from CloudFormation
// template properties, returning clamped int32 values safe for allocation.
func parseASGSizes(
	props map[string]any,
	params, physicalIDs map[string]string,
) (int32, int32, int32) {
	var minSize, maxSize, desired int32 = 1, 1, 1

	if v, ok := props["MinSize"].(float64); ok {
		minSize = int32(v)
	} else if s := strProp(props, "MinSize", params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			minSize = int32(n)
		}
	}

	if v, ok := props["MaxSize"].(float64); ok {
		maxSize = int32(v)
	} else if s := strProp(props, "MaxSize", params, physicalIDs); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			maxSize = int32(n)
		}
	}

	if v, ok := props["DesiredCapacity"].(float64); ok {
		desired = int32(v)
	}

	// Clamp to [0, maxAutoScalingCapacity] to prevent excessive allocations.
	minSize = min(max(0, minSize), maxAutoScalingCapacity)
	maxSize = min(max(0, maxSize), maxAutoScalingCapacity)
	desired = min(max(0, desired), maxAutoScalingCapacity)

	return minSize, maxSize, desired
}

func (rc *ResourceCreator) createAutoScalingGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Autoscaling == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "AutoScalingGroupName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	lcName := strProp(props, "LaunchConfigurationName", params, physicalIDs)
	minSize, maxSize, desired := parseASGSizes(props, params, physicalIDs)

	_, err := rc.backends.Autoscaling.Backend.CreateAutoScalingGroup(
		autoscalingbackend.CreateAutoScalingGroupInput{
			AutoScalingGroupName:    name,
			LaunchConfigurationName: lcName,
			MinSize:                 minSize,
			MaxSize:                 maxSize,
			DesiredCapacity:         desired,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create AutoScaling group %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteAutoScalingGroup(name string) error {
	if rc.backends.Autoscaling == nil {
		return nil
	}

	// ForceDelete=true matches CloudFormation's behaviour: it always force-deletes the group.
	return rc.backends.Autoscaling.Backend.DeleteAutoScalingGroup(name, true)
}

func (rc *ResourceCreator) createLaunchConfiguration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Autoscaling == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "LaunchConfigurationName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	imageID := strProp(props, "ImageId", params, physicalIDs)
	instanceType := strProp(props, "InstanceType", params, physicalIDs)

	_, err := rc.backends.Autoscaling.Backend.CreateLaunchConfiguration(
		autoscalingbackend.CreateLaunchConfigurationInput{
			LaunchConfigurationName: name,
			ImageID:                 imageID,
			InstanceType:            instanceType,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create LaunchConfiguration %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteLaunchConfiguration(name string) error {
	if rc.backends.Autoscaling == nil {
		return nil
	}

	return rc.backends.Autoscaling.Backend.DeleteLaunchConfiguration(name)
}
