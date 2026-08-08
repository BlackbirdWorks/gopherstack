package personalize

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// --- MetricAttribution ---

func (h *Handler) createMetricAttribution(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	metrics := extractMetricAttributes(input, "metrics")
	metricsOutputConfig, _ := input["metricsOutputConfig"].(map[string]any)
	tags := extractTags(input)

	ma, err := h.Backend.CreateMetricAttribution(name, datasetGroupArn, metrics, metricsOutputConfig, tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyMetricAttributionArn: ma.MetricAttributionArn}, nil
}

func (h *Handler) describeMetricAttribution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["metricAttributionArn"].(string)

	ma, err := h.Backend.DescribeMetricAttribution(nameOrArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"metricAttribution": metricAttributionToMap(ma)}, nil
}

func (h *Handler) updateMetricAttribution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["metricAttributionArn"].(string)
	addMetrics := extractMetricAttributes(input, "addMetrics")
	removeMetrics := strSlice(input, "removeMetrics")
	metricsOutputConfig, _ := input["metricsOutputConfig"].(map[string]any)

	ma, err := h.Backend.UpdateMetricAttribution(nameOrArn, addMetrics, removeMetrics, metricsOutputConfig)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyMetricAttributionArn: ma.MetricAttributionArn}, nil
}

func (h *Handler) deleteMetricAttribution(input map[string]any) (map[string]any, error) {
	nameOrArn, _ := input["metricAttributionArn"].(string)

	return map[string]any{}, h.Backend.DeleteMetricAttribution(nameOrArn)
}

func (h *Handler) listMetricAttributions(input map[string]any) (map[string]any, error) {
	datasetGroupArn, _ := input["datasetGroupArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	list, outToken := h.Backend.ListMetricAttributions(datasetGroupArn, maxResults, nextToken)

	summaries := make([]map[string]any, 0, len(list))
	for _, ma := range list {
		summaries = append(summaries, metricAttributionToMap(ma))
	}

	result := map[string]any{"metricAttributions": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func (h *Handler) listMetricAttributionMetrics(input map[string]any) (map[string]any, error) {
	metricAttributionArn, _ := input["metricAttributionArn"].(string)
	maxResults := intField(input, "maxResults")
	nextToken, _ := input["nextToken"].(string)

	metrics, outToken, err := h.Backend.ListMetricAttributionMetrics(metricAttributionArn, maxResults, nextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(metrics))
	for _, m := range metrics {
		summaries = append(summaries, metricAttributeToMap(m))
	}

	result := map[string]any{"metrics": summaries}
	if outToken != "" {
		result["nextToken"] = outToken
	}

	return result, nil
}

func metricAttributionToMap(ma *MetricAttribution) map[string]any {
	return map[string]any{
		keyMetricAttributionArn: ma.MetricAttributionArn,
		keyName:                 ma.Name,
		keyDatasetGroupArn:      ma.DatasetGroupArn,
		"metricsOutputConfig":   ma.MetricsOutputConfig,
		keyStatus:               ma.Status,
		keyCreationDateTime:     awstime.Epoch(ma.CreationDateTime),
		keyLastUpdatedDateTime:  awstime.Epoch(ma.LastUpdatedDateTime),
	}
}

func metricAttributeToMap(m MetricAttribute) map[string]any {
	return map[string]any{
		keyEventType: m.EventType,
		"expression": m.Expression,
		"metricName": m.MetricName,
	}
}

// extractMetricAttributes parses a []MetricAttribute wire list (each entry
// carrying eventType/expression/metricName) from input[key], used by
// CreateMetricAttribution's required "metrics" field and
// UpdateMetricAttribution's "addMetrics" field.
func extractMetricAttributes(input map[string]any, key string) []MetricAttribute {
	raw, ok := input[key].([]any)
	if !ok {
		return nil
	}

	out := make([]MetricAttribute, 0, len(raw))
	for _, item := range raw {
		entry, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		eventType, _ := entry[keyEventType].(string)
		expression, _ := entry["expression"].(string)
		metricName, _ := entry["metricName"].(string)
		out = append(out, MetricAttribute{EventType: eventType, Expression: expression, MetricName: metricName})
	}

	return out
}
