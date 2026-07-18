package cloudformation

import "fmt"

func (rc *ResourceCreator) createExtraNetworkResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2Volume:
		id, err := rc.createEC2Volume(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::EC2::VolumeAttachment":
		id, err := rc.createEC2VolumeAttachment(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeEC2NetworkInterface:
		id, err := rc.createEC2NetworkInterface(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteExtraNetworkResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeEC2Volume:

		return true, rc.deleteEC2Volume(physicalID)
	case "AWS::EC2::VolumeAttachment":

		return true, rc.deleteEC2VolumeAttachment(physicalID)
	case resTypeEC2NetworkInterface:

		return true, rc.deleteEC2NetworkInterface(physicalID)
	default:

		return false, nil
	}
}

const defaultVolumeSizeGiB = 8

func (rc *ResourceCreator) createEC2Volume(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	az := strProp(props, "AvailabilityZone", params, physicalIDs)
	volType := strProp(props, "VolumeType", params, physicalIDs)
	if volType == "" {
		volType = "gp2"
	}

	size := intProp(props, "Size")
	if size == 0 {
		size = defaultVolumeSizeGiB
	}

	vol, err := rc.backends.EC2.Backend.CreateVolume(az, volType, size)
	if err != nil {
		return "", fmt.Errorf("create EC2 volume: %w", err)
	}

	return vol.ID, nil
}

func (rc *ResourceCreator) deleteEC2Volume(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteVolume(id)
}

func (rc *ResourceCreator) createEC2VolumeAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	volumeID := strProp(props, "VolumeId", params, physicalIDs)
	instanceID := strProp(props, "InstanceId", params, physicalIDs)
	device := strProp(props, "Device", params, physicalIDs)
	if device == "" {
		device = "/dev/sdf"
	}

	if _, err := rc.backends.EC2.Backend.AttachVolume(volumeID, instanceID, device); err != nil {
		return "", fmt.Errorf("attach EC2 volume %s to %s: %w", volumeID, instanceID, err)
	}

	return volumeID, nil
}

func (rc *ResourceCreator) deleteEC2VolumeAttachment(volumeID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	_, err := rc.backends.EC2.Backend.DetachVolume(volumeID, true)

	return err
}

func (rc *ResourceCreator) createEC2NetworkInterface(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	subnetID := strProp(props, "SubnetId", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	eni, err := rc.backends.EC2.Backend.CreateNetworkInterface(subnetID, description)
	if err != nil {
		return "", fmt.Errorf("create EC2 network interface in %s: %w", subnetID, err)
	}

	return eni.ID, nil
}

func (rc *ResourceCreator) deleteEC2NetworkInterface(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteNetworkInterface(id)
}
