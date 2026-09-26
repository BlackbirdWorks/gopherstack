package cloudformation

import "fmt"

const (
	resTypeKinesisVideoStream           = "AWS::KinesisVideo::Stream"
	resTypeKinesisVideoSignalingChannel = "AWS::KinesisVideo::SignalingChannel"
)

// createKinesisVideoResource handles the KinesisVideo resource types listed
// above.
func (rc *ResourceCreator) createKinesisVideoResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeKinesisVideoStream:
		id, err := rc.createKinesisVideoStream(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeKinesisVideoSignalingChannel:
		id, err := rc.createKinesisVideoSignalingChannel(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteKinesisVideoResource handles deletion for the types created above.
// Both types delete by their ARN-shaped physicalID directly.
func (rc *ResourceCreator) deleteKinesisVideoResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.KinesisVideo == nil {
		switch resourceType {
		case resTypeKinesisVideoStream, resTypeKinesisVideoSignalingChannel:
			return true, nil
		default:
			return false, nil
		}
	}

	switch resourceType {
	case resTypeKinesisVideoStream:
		return true, rc.backends.KinesisVideo.Backend.DeleteStream(physicalID, "")
	case resTypeKinesisVideoSignalingChannel:
		return true, rc.backends.KinesisVideo.Backend.DeleteSignalingChannel(physicalID, "")
	default:
		return false, nil
	}
}

// ---- AWS::KinesisVideo::Stream ----
// Ref and Fn::GetAtt Arn both return the stream ARN: the CloudFormation
// Template Reference's Return values section documents only the Arn
// attribute and leaves Ref undocumented, the same pattern the AWS-published
// resource-provider schema confirms for AWS::KafkaConnect::Connector (Ref ==
// its sole ARN attribute, primaryIdentifier == readOnlyProperties[0]).

func (rc *ResourceCreator) createKinesisVideoStream(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KinesisVideo == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	storageConfig, _ := props["StreamStorageConfiguration"].(map[string]any)

	s, err := rc.backends.KinesisVideo.Backend.CreateStream(
		rc.backends.AccountID,
		rc.backends.Region,
		name,
		strProp(props, "DeviceName", params, physicalIDs),
		strProp(props, "MediaType", params, physicalIDs),
		strProp(props, "KmsKeyId", params, physicalIDs),
		strProp(storageConfig, "DefaultStorageTier", params, physicalIDs),
		int32Prop(props, "DataRetentionInHours", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create KinesisVideo stream %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = s.ARN

	return s.ARN, nil
}

// ---- AWS::KinesisVideo::SignalingChannel ----
// Ref and Fn::GetAtt Arn both return the channel ARN (see the Stream doc
// comment above for the Ref-attribution basis).

func (rc *ResourceCreator) createKinesisVideoSignalingChannel(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KinesisVideo == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	c, err := rc.backends.KinesisVideo.Backend.CreateSignalingChannel(
		rc.backends.AccountID,
		rc.backends.Region,
		name,
		strProp(props, "Type", params, physicalIDs),
		int32Prop(props, "MessageTtlSeconds", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create KinesisVideo signaling channel %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = c.ARN

	return c.ARN, nil
}
