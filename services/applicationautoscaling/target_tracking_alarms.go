package applicationautoscaling

import (
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

const (
	targetTrackingAlarmPrefix = "TargetTracking-"
	alarmHighKind             = "AlarmHigh"
	alarmLowKind              = "AlarmLow"

	alarmPeriodSeconds   = 60
	scaleOutDatapoints   = 3
	scaleInDatapoints    = 15
	scaleInThresholdPart = 0.9
)

type metricShape struct {
	queries    []cwbackend.MetricDataQuery
	dims       []cwbackend.Dimension
	namespace  string
	metricName string
	statistic  string
	unit       string
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

func asString(v any) string {
	s, _ := v.(string)

	return s
}

func dimensionsOf(v any) []cwbackend.Dimension {
	raw, _ := v.([]any)
	out := make([]cwbackend.Dimension, 0, len(raw))

	for _, d := range raw {
		m := asMap(d)
		out = append(out, cwbackend.Dimension{Name: asString(m["Name"]), Value: asString(m["Value"])})
	}

	return out
}

// metricFor resolves the alarm metric for a target-tracking configuration; ok is false for
// predefined metric types with no known CloudWatch mapping.
func metricFor(resourceID string, cfg map[string]any) (metricShape, bool) {
	if custom := asMap(cfg["CustomizedMetricSpecification"]); custom != nil {
		return customMetric(custom), true
	}

	spec := asMap(cfg["PredefinedMetricSpecification"])
	if spec == nil {
		return metricShape{}, false
	}

	return predefinedMetric(asString(spec["PredefinedMetricType"]), resourceID, asString(spec["ResourceLabel"]))
}

func customMetric(c map[string]any) metricShape {
	if raw, ok := c["Metrics"].([]any); ok && len(raw) > 0 {
		queries := make([]cwbackend.MetricDataQuery, 0, len(raw))

		for _, q := range raw {
			m := asMap(q)
			ret := true

			if v, has := m["ReturnData"].(bool); has {
				ret = v
			}

			dq := cwbackend.MetricDataQuery{
				ID: asString(m["Id"]), Label: asString(m["Label"]), Expression: asString(m["Expression"]),
				ReturnData: ret, Period: alarmPeriodSeconds,
			}

			if ms := asMap(m["MetricStat"]); ms != nil {
				metric := asMap(ms["Metric"])
				dq.MetricStat = cwbackend.MetricStat{
					Namespace: asString(metric["Namespace"]), MetricName: asString(metric["MetricName"]),
					Dimensions: dimensionsOf(metric["Dimensions"]), Stat: asString(ms["Stat"]), Period: alarmPeriodSeconds,
				}
			}

			queries = append(queries, dq)
		}

		return metricShape{queries: queries}
	}

	stat := asString(c["Statistic"])
	if stat == "" {
		stat = "Average"
	}

	return metricShape{
		namespace: asString(c["Namespace"]), metricName: asString(c["MetricName"]),
		dims: dimensionsOf(c["Dimensions"]), statistic: stat, unit: asString(c["Unit"]),
	}
}

func dim(name, value string) cwbackend.Dimension { return cwbackend.Dimension{Name: name, Value: value} }

// segment returns the idx-th "/"-separated part of s, or "".
func segment(s string, idx int) string {
	parts := strings.Split(s, "/")
	if idx < len(parts) {
		return parts[idx]
	}

	return ""
}

func afterColon(s string) string {
	_, rest, _ := strings.Cut(s, ":")

	return rest
}

func simple(ns, name, stat string, dims ...cwbackend.Dimension) (metricShape, bool) {
	return metricShape{namespace: ns, metricName: name, statistic: stat, dims: dims}, true
}

//nolint:cyclop,gocyclo // flat lookup of predefined metric types
func predefinedMetric(metricType, resourceID, label string) (metricShape, bool) {
	switch metricType {
	case "ECSServiceAverageCPUUtilization":
		return simple("AWS/ECS", "CPUUtilization", "Average",
			dim("ClusterName", segment(resourceID, 1)), dim("ServiceName", segment(resourceID, 2)))
	case "ECSServiceAverageMemoryUtilization":
		return simple("AWS/ECS", "MemoryUtilization", "Average",
			dim("ClusterName", segment(resourceID, 1)), dim("ServiceName", segment(resourceID, 2)))
	case "ALBRequestCountPerTarget":
		parts := strings.Split(label, "/")
		if len(parts) < 6 { //nolint:mnd // app/<lb>/<id>/targetgroup/<tg>/<id>
			return metricShape{}, false
		}

		return simple("AWS/ApplicationELB", "RequestCountPerTarget", "Sum",
			dim("LoadBalancer", strings.Join(parts[:3], "/")), dim("TargetGroup", strings.Join(parts[3:6], "/")))
	case "RDSReaderAverageCPUUtilization":
		return simple("AWS/RDS", "CPUUtilization", "Average",
			dim("DBClusterIdentifier", afterColon(resourceID)), dim("Role", "READER"))
	case "RDSReaderAverageDatabaseConnections":
		return simple("AWS/RDS", "DatabaseConnections", "Average",
			dim("DBClusterIdentifier", afterColon(resourceID)), dim("Role", "READER"))
	case "NeptuneReaderAverageCPUUtilization":
		return simple("AWS/Neptune", "CPUUtilization", "Average",
			dim("DBClusterIdentifier", afterColon(resourceID)), dim("Role", "READER"))
	case "SageMakerVariantInvocationsPerInstance":
		return simple("AWS/SageMaker", "InvocationsPerInstance", "Sum",
			dim("EndpointName", segment(resourceID, 1)), dim("VariantName", segment(resourceID, 3))) //nolint:mnd // endpoint/<e>/variant/<v>
	case "EC2SpotFleetRequestAverageCPUUtilization":
		return simple("AWS/EC2Spot", "CPUUtilization", "Average", dim("FleetRequestId", segment(resourceID, 1)))
	case "EC2SpotFleetRequestAverageNetworkIn":
		return simple("AWS/EC2Spot", "NetworkIn", "Average", dim("FleetRequestId", segment(resourceID, 1)))
	case "EC2SpotFleetRequestAverageNetworkOut":
		return simple("AWS/EC2Spot", "NetworkOut", "Average", dim("FleetRequestId", segment(resourceID, 1)))
	case "LambdaProvisionedConcurrencyUtilization":
		fn := strings.SplitN(afterColon(resourceID), ":", 2) //nolint:mnd // function:<name>:<alias>

		return simple("AWS/Lambda", "ProvisionedConcurrencyUtilization", "Average",
			dim("FunctionName", fn[0]), dim("Resource", afterColon(resourceID)))
	case "ElastiCachePrimaryEngineCPUUtilization":
		return simple("AWS/ElastiCache", "EngineCPUUtilization", "Average",
			dim("ReplicationGroupId", segment(resourceID, 1)), dim("Role", "Primary"))
	case "ElastiCacheReplicaEngineCPUUtilization":
		return simple("AWS/ElastiCache", "EngineCPUUtilization", "Average",
			dim("ReplicationGroupId", segment(resourceID, 1)), dim("Role", "Replica"))
	case "AppStreamAverageCapacityUtilization":
		return simple("AWS/AppStream", "CapacityUtilization", "Average", dim("Fleet", segment(resourceID, 1)))
	case "DynamoDBReadCapacityUtilization":
		return dynamoDBUtilization(resourceID, "ConsumedReadCapacityUnits", "ProvisionedReadCapacityUnits")
	case "DynamoDBWriteCapacityUtilization":
		return dynamoDBUtilization(resourceID, "ConsumedWriteCapacityUnits", "ProvisionedWriteCapacityUnits")
	}

	return metricShape{}, false
}

// dynamoDBUtilization is consumed capacity as a percentage of provisioned capacity.
func dynamoDBUtilization(resourceID, consumed, provisioned string) (metricShape, bool) {
	dims := []cwbackend.Dimension{dim("TableName", segment(resourceID, 1))}
	if segment(resourceID, 2) == "index" { //nolint:mnd // table/<t>/index/<i>
		dims = append(dims, dim("GlobalSecondaryIndexName", segment(resourceID, 3))) //nolint:mnd // index name
	}

	stat := func(id, name, s string) cwbackend.MetricDataQuery {
		return cwbackend.MetricDataQuery{
			ID: id, Period: alarmPeriodSeconds,
			MetricStat: cwbackend.MetricStat{
				Namespace: "AWS/DynamoDB", MetricName: name, Dimensions: dims, Stat: s, Period: alarmPeriodSeconds,
			},
		}
	}

	return metricShape{queries: []cwbackend.MetricDataQuery{
		stat("m1", consumed, "Sum"),
		stat("m2", provisioned, "Average"),
		{ID: "e1", Expression: "(m1/(m2*60))*100", Label: "Utilization", ReturnData: true, Period: alarmPeriodSeconds},
	}}, true
}

func (b *InMemoryBackend) alarmSpec(p *ScalingPolicy, kind, name string, shape metricShape, threshold float64) *cwbackend.MetricAlarm {
	op, evals := "GreaterThanThreshold", int32(scaleOutDatapoints)
	if kind == alarmLowKind {
		op, evals = "LessThanThreshold", scaleInDatapoints
	}

	a := &cwbackend.MetricAlarm{
		AlarmName:          name,
		AlarmDescription:   "DO NOT EDIT OR DELETE. For TargetTrackingScaling policy " + p.ARN + ".",
		ComparisonOperator: op,
		Threshold:          threshold,
		Period:             alarmPeriodSeconds,
		EvaluationPeriods:  evals,
		DatapointsToAlarm:  evals,
		TreatMissingData:   "missing",
		AlarmActions:       []string{p.ARN},
		ActionsEnabled:     true,
	}

	if len(shape.queries) > 0 {
		a.Metrics = shape.queries
		a.Period = 0
		a.ThresholdMetricID = ""

		return a
	}

	a.Namespace, a.MetricName, a.Dimensions = shape.namespace, shape.metricName, shape.dims
	a.Statistic, a.Unit = shape.statistic, shape.unit

	return a
}

// targetTrackingAlarms returns the CloudWatch alarms AWS manages for policy p. Must not hold b.mu.
func targetTrackingAlarms(cw cwbackend.StorageBackend, p *ScalingPolicy) []cwbackend.MetricAlarm {
	var out []cwbackend.MetricAlarm

	token := ""

	for {
		metric, _, _, err := cw.DescribeAlarms(nil, nil, targetTrackingAlarmPrefix, "", token, 0, "", "", "")
		if err != nil {
			return out
		}

		for _, a := range metric.Data {
			if slices.Contains(a.AlarmActions, p.ARN) {
				out = append(out, a)
			}
		}

		if metric.Next == "" {
			return out
		}

		token = metric.Next
	}
}

// syncTargetTrackingAlarms creates, updates or removes the AlarmHigh/AlarmLow alarms backing p. Must not hold b.mu.
func (b *InMemoryBackend) syncTargetTrackingAlarms(p *ScalingPolicy) error {
	cw, ok := b.cloudWatchBackend()
	if !ok {
		return nil
	}

	existing := targetTrackingAlarms(cw, p)
	want := map[string]*cwbackend.MetricAlarm{}

	if p.PolicyType == policyTypeTargetTrackingScaling {
		if shape, mapped := metricFor(p.ResourceID, p.TargetTrackingConfig); mapped {
			target, _ := p.TargetTrackingConfig["TargetValue"].(float64)
			disableScaleIn, _ := p.TargetTrackingConfig["DisableScaleIn"].(bool)

			want[alarmHighKind] = b.alarmSpec(p, alarmHighKind, "", shape, target)
			if !disableScaleIn {
				want[alarmLowKind] = b.alarmSpec(p, alarmLowKind, "", shape, target*scaleInThresholdPart)
			}
		}
	}

	var stale []string

	for _, a := range existing {
		kind := alarmKind(a.AlarmName)
		if spec, keep := want[kind]; keep && spec.AlarmName == "" {
			spec.AlarmName = a.AlarmName
		} else {
			stale = append(stale, a.AlarmName)
		}
	}

	for kind, spec := range want {
		if spec.AlarmName == "" {
			spec.AlarmName = fmt.Sprintf("%s%s-%s-%s", targetTrackingAlarmPrefix, p.ResourceID, kind, uuid.NewString())
		}

		if err := cw.PutMetricAlarm(spec); err != nil {
			return fmt.Errorf("%w: creating %s alarm: %w", ErrValidation, kind, err)
		}
	}

	if len(stale) > 0 {
		return cw.DeleteAlarms(stale) //nolint:wrapcheck // cloudwatch error surfaces as-is
	}

	return nil
}

func alarmKind(name string) string {
	switch {
	case strings.Contains(name, "-"+alarmHighKind+"-"):
		return alarmHighKind
	case strings.Contains(name, "-"+alarmLowKind+"-"):
		return alarmLowKind
	}

	return ""
}

// deleteTargetTrackingAlarms removes the managed alarms of the policies with the given ARNs. Must not hold b.mu.
func (b *InMemoryBackend) deleteTargetTrackingAlarms(policyARNs ...string) {
	cw, ok := b.cloudWatchBackend()
	if !ok {
		return
	}

	for _, policyARN := range policyARNs {
		var names []string
		for _, a := range targetTrackingAlarms(cw, &ScalingPolicy{ARN: policyARN}) {
			names = append(names, a.AlarmName)
		}

		if len(names) > 0 {
			_ = cw.DeleteAlarms(names)
		}
	}
}
