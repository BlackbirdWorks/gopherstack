package cloudformation

import (
	"fmt"
	"strconv"

	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const (
	resTypeLogsDelivery            = "AWS::Logs::Delivery"
	resTypeLogsDeliveryDestination = "AWS::Logs::DeliveryDestination"
	resTypeLogsDeliverySource      = "AWS::Logs::DeliverySource"
	resTypeLogsIntegration         = "AWS::Logs::Integration"
	resTypeLogsAnomalyDetector     = "AWS::Logs::LogAnomalyDetector"
	resTypeLogsScheduledQuery      = "AWS::Logs::ScheduledQuery"
)

// createLogsMoreResource handles the CloudWatch Logs resource types listed
// above, added after resources_cloudwatch_logs.go's own cyclop budget was
// spent. Returns handled=false when resourceType isn't one of them.
func (rc *ResourceCreator) createLogsMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeLogsDelivery:
		id, err := rc.createLogsDelivery(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeLogsDeliveryDestination:
		id, err := rc.createLogsDeliveryDestination(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeLogsDeliverySource:
		id, err := rc.createLogsDeliverySource(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeLogsIntegration:
		id, err := rc.createLogsIntegration(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeLogsAnomalyDetector:
		id, err := rc.createLogsAnomalyDetector(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeLogsScheduledQuery:
		id, err := rc.createLogsScheduledQuery(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteLogsMoreResource handles deletion for the types described in
// createLogsMoreResource.
func (rc *ResourceCreator) deleteLogsMoreResource(resourceType, physicalID string) (bool, error) {
	if rc.backends.CloudWatchLogs == nil {
		switch resourceType {
		case resTypeLogsDelivery, resTypeLogsDeliveryDestination, resTypeLogsDeliverySource,
			resTypeLogsIntegration, resTypeLogsAnomalyDetector, resTypeLogsScheduledQuery:
			return true, nil
		default:
			return false, nil
		}
	}

	b := rc.backends.CloudWatchLogs.Backend

	switch resourceType {
	case resTypeLogsDelivery:
		return true, ignoreNotFound(b.DeleteDelivery(physicalID), cwlogsbackend.ErrDeliveryNotFound)
	case resTypeLogsDeliveryDestination:
		imb, ok := b.(*cwlogsbackend.InMemoryBackend)
		if !ok {
			return true, nil
		}

		err := imb.DeleteDeliveryDestination(physicalID)

		return true, ignoreNotFound(err, cwlogsbackend.ErrDeliveryDestinationNotFound)
	case resTypeLogsDeliverySource:
		imb, ok := b.(*cwlogsbackend.InMemoryBackend)
		if !ok {
			return true, nil
		}

		return true, ignoreNotFound(imb.DeleteDeliverySource(physicalID), cwlogsbackend.ErrDeliverySourceNotFound)
	case resTypeLogsIntegration:
		imb, ok := b.(*cwlogsbackend.InMemoryBackend)
		if !ok {
			return true, nil
		}

		return true, ignoreNotFound(imb.DeleteIntegration(physicalID), cwlogsbackend.ErrIntegrationNotFound)
	case resTypeLogsAnomalyDetector:
		return true, ignoreNotFound(b.DeleteLogAnomalyDetector(physicalID), cwlogsbackend.ErrLogAnomalyDetectorNotFound)
	case resTypeLogsScheduledQuery:
		return true, ignoreNotFound(b.DeleteScheduledQuery(physicalID), cwlogsbackend.ErrScheduledQueryNotFound)
	default:
		return false, nil
	}
}

// ---- AWS::Logs::Delivery ----
// Ref is undocumented (empty Ref section); DeliveryId is used as the
// physical ID, matching DeleteDelivery's own key and a documented GetAtt
// attribute -- see the task's "use the primary identifier" rule.

func (rc *ResourceCreator) createLogsDelivery(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	source := strProp(props, "DeliverySourceName", params, physicalIDs)

	d, err := rc.backends.CloudWatchLogs.Backend.CreateDelivery(
		source,
		strProp(props, "DeliveryDestinationArn", params, physicalIDs),
		strProp(props, "FieldDelimiter", params, physicalIDs),
		strSliceProp(props["RecordFields"], params, physicalIDs),
		nil,
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Logs delivery for source %s: %w", source, err)
	}

	physicalIDs[logicalID+"/Arn"] = d.Arn
	physicalIDs[logicalID+"/DeliveryDestinationType"] = d.DeliveryDestinationType

	return d.ID, nil
}

// ---- AWS::Logs::DeliveryDestination ----
// Ref is undocumented; Name is used as the physical ID, matching
// PutDeliveryDestination/DeleteDeliveryDestination's own key.

func (rc *ResourceCreator) createLogsDeliveryDestination(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	dest, err := imb.PutDeliveryDestination(
		name,
		strProp(props, "DestinationResourceArn", params, physicalIDs),
		strProp(props, "OutputFormat", params, physicalIDs),
		strProp(props, "DeliveryDestinationType", params, physicalIDs),
		tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Logs delivery destination %s: %w", name, err)
	}

	physicalIDs[logicalID+"/Arn"] = dest.Arn

	return name, nil
}

// ---- AWS::Logs::DeliverySource ----
// Ref is undocumented; Name is used as the physical ID, matching
// PutDeliverySource/DeleteDeliverySource's own key.

func (rc *ResourceCreator) createLogsDeliverySource(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var resourceArns []string
	if arn := strProp(props, "ResourceArn", params, physicalIDs); arn != "" {
		resourceArns = []string{arn}
	}

	src, err := imb.PutDeliverySource(
		name, strProp(props, "LogType", params, physicalIDs), resourceArns, tagListProp(props, params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Logs delivery source %s: %w", name, err)
	}

	// Status/StatusReason aren't tracked on the backend's DeliverySource
	// struct (the docs note they're "defined for selective log types" only),
	// so only the always-populated Arn/Service are stashed.
	physicalIDs[logicalID+"/Arn"] = src.Arn
	physicalIDs[logicalID+"/Service"] = src.Service

	return name, nil
}

// ---- AWS::Logs::Integration ----
// Ref is undocumented; IntegrationName is used as the physical ID, matching
// PutIntegration/DeleteIntegration's own key.

func (rc *ResourceCreator) createLogsIntegration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	imb, ok := rc.backends.CloudWatchLogs.Backend.(*cwlogsbackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "IntegrationName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	var cfg *cwlogsbackend.OpenSearchResourceConfig

	if rcProp, isMap := props["ResourceConfig"].(map[string]any); isMap {
		if os, ok2 := rcProp["OpenSearchResourceConfig"].(map[string]any); ok2 {
			cfg = &cwlogsbackend.OpenSearchResourceConfig{
				DataSourceRoleArn: strProp(os, "DataSourceRoleArn", params, physicalIDs),
				ApplicationArn:    strProp(os, "ApplicationARN", params, physicalIDs),
				KmsKeyArn:         strProp(os, "KmsKeyArn", params, physicalIDs),
				RetentionDays:     int32PtrProp(os, "RetentionDays", params, physicalIDs),
				DashboardViewerPrincipals: strSliceProp(
					os["DashboardViewerPrincipals"], params, physicalIDs,
				),
			}
		}
	}

	ig, err := imb.PutIntegration(
		name, strProp(props, "IntegrationType", params, physicalIDs), cfg,
	)
	if err != nil {
		return "", fmt.Errorf("create Logs integration %s: %w", name, err)
	}

	physicalIDs[logicalID+"/IntegrationStatus"] = ig.Status

	return name, nil
}

// ---- AWS::Logs::LogAnomalyDetector ----
// Ref is undocumented; AnomalyDetectorArn is used as the physical ID,
// matching CreateLogAnomalyDetector's return value and
// DeleteLogAnomalyDetector's own key.

func (rc *ResourceCreator) createLogsAnomalyDetector(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	detectorARN, err := rc.backends.CloudWatchLogs.Backend.CreateLogAnomalyDetector(
		strSliceProp(props["LogGroupArnList"], params, physicalIDs),
		strProp(props, "DetectorName", params, physicalIDs),
		strProp(props, "EvaluationFrequency", params, physicalIDs),
		strProp(props, "FilterPattern", params, physicalIDs),
		strProp(props, "KmsKeyId", params, physicalIDs),
		int64Prop(props, "AnomalyVisibilityTime", params, physicalIDs),
	)
	if err != nil {
		return "", fmt.Errorf("create Logs anomaly detector for %s: %w", logicalID, err)
	}

	if det, getErr := rc.backends.CloudWatchLogs.Backend.GetLogAnomalyDetector(detectorARN); getErr == nil {
		physicalIDs[logicalID+"/AnomalyDetectorStatus"] = det.AnomalyDetectorStatus
		physicalIDs[logicalID+"/CreationTimeStamp"] = strconv.FormatInt(det.CreationTimeStamp, 10)
		physicalIDs[logicalID+"/LastModifiedTimeStamp"] = strconv.FormatInt(det.LastModifiedTimeStamp, 10)
	}

	return detectorARN, nil
}

// ---- AWS::Logs::ScheduledQuery ----
// Ref is undocumented; ScheduledQueryArn is used as the physical ID,
// matching CreateScheduledQuery's return value and DeleteScheduledQuery's
// own key.

func (rc *ResourceCreator) createLogsScheduledQuery(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	sqArn, err := rc.backends.CloudWatchLogs.Backend.CreateScheduledQuery(cwlogsbackend.ScheduledQueryCreateParams{
		Name:                name,
		QueryString:         strProp(props, "QueryString", params, physicalIDs),
		QueryLanguage:       strProp(props, "QueryLanguage", params, physicalIDs),
		ScheduleExpression:  strProp(props, "ScheduleExpression", params, physicalIDs),
		ExecutionRoleArn:    strProp(props, "ExecutionRoleArn", params, physicalIDs),
		State:               strProp(props, "State", params, physicalIDs),
		Description:         strProp(props, "Description", params, physicalIDs),
		Timezone:            strProp(props, "Timezone", params, physicalIDs),
		LogGroupIdentifiers: strSliceProp(props["LogGroupIdentifiers"], params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create Logs scheduled query %s: %w", name, err)
	}

	if sq, getErr := rc.backends.CloudWatchLogs.Backend.GetScheduledQuery(sqArn); getErr == nil {
		physicalIDs[logicalID+"/CreationTime"] = strconv.FormatInt(sq.CreationTime, 10)
		physicalIDs[logicalID+"/LastExecutionStatus"] = sq.LastExecutionStatus
		physicalIDs[logicalID+"/LastTriggeredTime"] = strconv.FormatInt(sq.LastTriggeredTime, 10)
		physicalIDs[logicalID+"/LastUpdatedTime"] = strconv.FormatInt(sq.LastUpdatedTime, 10)
	}

	return sqArn, nil
}
