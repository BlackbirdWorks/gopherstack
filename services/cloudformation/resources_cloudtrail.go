package cloudformation

import "fmt"

// ---- CloudTrail ----

func (rc *ResourceCreator) createCloudTrailTrail(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudTrail == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "TrailName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	s3Bucket := strProp(props, "S3BucketName", params, physicalIDs)
	s3KeyPrefix := strProp(props, "S3KeyPrefix", params, physicalIDs)
	snsTopicName := strProp(props, "SnsTopicName", params, physicalIDs)
	cwLogsLogGroupARN := strProp(props, "CloudWatchLogsLogGroupArn", params, physicalIDs)
	cwLogsRoleARN := strProp(props, "CloudWatchLogsRoleArn", params, physicalIDs)
	kmsKeyID := strProp(props, "KMSKeyId", params, physicalIDs)

	var includeGlobal, multiRegion, logValidation bool
	if v, ok := props["IncludeGlobalServiceEvents"].(bool); ok {
		includeGlobal = v
	}
	if v, ok := props["IsMultiRegionTrail"].(bool); ok {
		multiRegion = v
	}
	if v, ok := props["EnableLogFileValidation"].(bool); ok {
		logValidation = v
	}

	trail, err := rc.backends.CloudTrail.Backend.CreateTrail(
		name, s3Bucket, s3KeyPrefix, snsTopicName,
		cwLogsLogGroupARN, cwLogsRoleARN, kmsKeyID,
		includeGlobal, multiRegion, logValidation, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create CloudTrail trail %s: %w", name, err)
	}

	return trail.TrailARN, nil
}

func (rc *ResourceCreator) deleteCloudTrailTrail(arn string) error {
	if rc.backends.CloudTrail == nil {
		return nil
	}

	return rc.backends.CloudTrail.Backend.DeleteTrail(arn)
}
