package cloudformation

import (
	"errors"
	"fmt"
	"strings"
	"time"

	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

const (
	resTypeCWMetricStream    = "AWS::CloudWatch::MetricStream"
	resTypeCWAnomalyDetector = "AWS::CloudWatch::AnomalyDetector"
	resTypeCWInsightRule     = "AWS::CloudWatch::InsightRule"
)

func (rc *ResourceCreator) createCloudWatchMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeCWMetricStream:
		id, err := rc.createCWMetricStream(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeCWAnomalyDetector:
		id, err := rc.createCWAnomalyDetector(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeCWInsightRule:
		id, err := rc.createCWInsightRule(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteCloudWatchMoreResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeCWMetricStream:
		return true, rc.deleteCWMetricStream(physicalID)
	case resTypeCWInsightRule:
		return true, rc.deleteCWInsightRule(physicalID)
	default:
		return false, nil
	}
}

// ---- CloudWatch MetricStream ----

func (rc *ResourceCreator) createCWMetricStream(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	stream := &cloudwatchbackend.MetricStream{
		Name:         name,
		FirehoseArn:  strProp(props, "FirehoseArn", params, physicalIDs),
		RoleArn:      strProp(props, "RoleArn", params, physicalIDs),
		OutputFormat: strProp(props, "OutputFormat", params, physicalIDs),
	}

	if err := rc.backends.CloudWatch.Backend.PutMetricStream(stream); err != nil {
		return "", fmt.Errorf("create CloudWatch metric stream %s: %w", name, err)
	}

	created, err := rc.backends.CloudWatch.Backend.GetMetricStream(name)
	if err != nil {
		return "", fmt.Errorf("get CloudWatch metric stream %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = created.Arn
	physicalIDs[logicalID+"/CreationDate"] = created.CreationDate.UTC().Format(time.RFC3339)
	physicalIDs[logicalID+"/LastUpdateDate"] = created.LastUpdateDate.UTC().Format(time.RFC3339)
	physicalIDs[logicalID+"/State"] = created.State

	return created.Name, nil
}

func (rc *ResourceCreator) deleteCWMetricStream(name string) error {
	if rc.backends.CloudWatch == nil {
		return nil
	}

	err := rc.backends.CloudWatch.Backend.DeleteMetricStream(name)
	if errors.Is(err, cloudwatchbackend.ErrMetricStreamNotFound) {
		return nil
	}

	return err
}

// ---- CloudWatch AnomalyDetector ----

func anomalyDetectorDimensionsProp(
	props map[string]any, params, physicalIDs map[string]string,
) []cloudwatchbackend.Dimension {
	raw, ok := props["Dimensions"].([]any)
	if !ok {
		return nil
	}

	out := make([]cloudwatchbackend.Dimension, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		out = append(out, cloudwatchbackend.Dimension{
			Name:  strProp(m, "Name", params, physicalIDs),
			Value: strProp(m, "Value", params, physicalIDs),
		})
	}

	return out
}

func (rc *ResourceCreator) createCWAnomalyDetector(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatch == nil {
		return logicalID + "-stub", nil
	}

	detector := &cloudwatchbackend.AnomalyDetector{
		Namespace:  strProp(props, "Namespace", params, physicalIDs),
		MetricName: strProp(props, "MetricName", params, physicalIDs),
		Stat:       strProp(props, "Stat", params, physicalIDs),
		Dimensions: anomalyDetectorDimensionsProp(props, params, physicalIDs),
	}

	if err := rc.backends.CloudWatch.Backend.PutAnomalyDetector(detector); err != nil {
		return "", fmt.Errorf("create CloudWatch anomaly detector: %w", err)
	}

	return detector.ID, nil
}

// deleteCWAnomalyDetector needs Namespace/MetricName/Stat/Dimensions, none of
// which are embedded in the detector's physical ID (its opaque generated ID;
// AWS::CloudWatch::AnomalyDetector's own CloudFormation Return values section
// documents no Ref/GetAtt at all), so they come from props like
// deleteEKSAccessEntry.
func (rc *ResourceCreator) deleteCWAnomalyDetector(
	props map[string]any, stackPhysicalIDs map[string]string,
) error {
	if rc.backends.CloudWatch == nil {
		return nil
	}

	err := rc.backends.CloudWatch.Backend.DeleteAnomalyDetector(
		strProp(props, "Namespace", nil, stackPhysicalIDs),
		strProp(props, "MetricName", nil, stackPhysicalIDs),
		strProp(props, "Stat", nil, stackPhysicalIDs),
		anomalyDetectorDimensionsProp(props, nil, stackPhysicalIDs),
	)
	if errors.Is(err, cloudwatchbackend.ErrAnomalyDetectorNotFound) {
		return nil
	}

	return err
}

// ---- CloudWatch InsightRule ----

func (rc *ResourceCreator) createCWInsightRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "RuleName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	rule := &cloudwatchbackend.InsightRule{
		Name:       name,
		State:      strProp(props, "RuleState", params, physicalIDs),
		Definition: strProp(props, "RuleBody", params, physicalIDs),
		Schema:     `{"Name": "CloudWatchLogRule", "Version": 1}`,
	}

	if err := rc.backends.CloudWatch.Backend.PutInsightRule(rule); err != nil {
		return "", fmt.Errorf("create CloudWatch insight rule %s: %w", name, err)
	}

	created, err := rc.backends.CloudWatch.Backend.GetInsightRule(name)
	if err != nil {
		return "", fmt.Errorf("get CloudWatch insight rule %s: %w", name, err)
	}

	return created.Arn, nil
}

func (rc *ResourceCreator) deleteCWInsightRule(arnOrName string) error {
	if rc.backends.CloudWatch == nil {
		return nil
	}

	name := arnOrName
	if idx := strings.LastIndex(arnOrName, "/"); idx >= 0 {
		name = arnOrName[idx+1:]
	}

	// DeleteInsightRules reports an unknown name as a failure entry rather
	// than an error; a delete of an already-gone rule is treated as success,
	// matching the idempotent-delete convention used elsewhere in this file.
	_, err := rc.backends.CloudWatch.Backend.DeleteInsightRules([]string{name})

	return err
}
