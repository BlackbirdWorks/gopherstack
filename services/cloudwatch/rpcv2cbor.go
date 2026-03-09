package cloudwatch

import (
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

const cborServicePath = "/service/GraniteServiceVersion20100801/operation/"

// nanosPerSecond is the number of nanoseconds in a second.
const nanosPerSecond = 1e9

const (
	cborOpListTagsForResource = "ListTagsForResource"
	cborOpTagResource         = "TagResource"
	cborOpUntagResource       = "UntagResource"
)

// isCBORRequest returns true when the request uses the rpc-v2-cbor (Smithy RPCv2) protocol.
func isCBORRequest(r *http.Request) bool {
	return r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, cborServicePath)
}

// extractCBOROperation returns the operation name from an rpc-v2-cbor request path.
func extractCBOROperation(path string) string {
	return strings.TrimPrefix(path, cborServicePath)
}

// handleCBOR dispatches rpc-v2-cbor requests.
func (h *Handler) handleCBOR(c *echo.Context) error {
	r := c.Request()
	op := extractCBOROperation(r.URL.Path)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "SerializationException", "cannot read body")
	}

	var input cbor.Map

	if len(body) > 0 {
		val, decErr := cbor.Decode(body)
		if decErr != nil {
			return h.cborError(c, http.StatusBadRequest, "SerializationException", "invalid CBOR body")
		}

		m, isCBORMap := val.(cbor.Map)
		if !isCBORMap {
			return h.cborError(c, http.StatusBadRequest, "SerializationException", "expected CBOR map")
		}

		input = m
	} else {
		input = cbor.Map{}
	}

	return h.dispatchCBOR(op, input, c)
}

// dispatchCBOR routes a decoded CBOR operation to the appropriate handler.
func (h *Handler) dispatchCBOR(op string, input cbor.Map, c *echo.Context) error {
	switch op {
	case "PutMetricData":
		return h.cborPutMetricData(input, c)
	case "GetMetricStatistics":
		return h.cborGetMetricStatistics(input, c)
	case "GetMetricData":
		return h.cborGetMetricData(input, c)
	case "ListMetrics":
		return h.cborListMetrics(input, c)
	case cborOpListTagsForResource, cborOpTagResource, cborOpUntagResource:
		return h.cborTagOperation(op, input, c)
	case "PutDashboard":
		return h.cborPutDashboard(input, c)
	case "GetDashboard":
		return h.cborGetDashboard(input, c)
	case "ListDashboards":
		return h.cborListDashboards(input, c)
	case "DeleteDashboards":
		return h.cborDeleteDashboards(input, c)
	default:
		return h.dispatchAlarmCBOR(op, input, c)
	}
}

// dispatchAlarmCBOR routes alarm-specific CBOR operations.
func (h *Handler) dispatchAlarmCBOR(op string, input cbor.Map, c *echo.Context) error {
	switch op {
	case "PutMetricAlarm":
		return h.cborPutMetricAlarm(input, c)
	case "PutCompositeAlarm":
		return h.cborPutCompositeAlarm(input, c)
	case "DescribeAlarms":
		return h.cborDescribeAlarms(input, c)
	case "DescribeAlarmsForMetric":
		return h.cborDescribeAlarmsForMetric(input, c)
	case "DescribeAlarmHistory":
		return h.cborDescribeAlarmHistory(input, c)
	case "DeleteAlarms":
		return h.cborDeleteAlarms(input, c)
	case "SetAlarmState":
		return h.cborSetAlarmState(input, c)
	case "EnableAlarmActions":
		return h.cborEnableAlarmActions(input, c)
	case "DisableAlarmActions":
		return h.cborDisableAlarmActions(input, c)
	default:
		return h.cborError(c, http.StatusBadRequest, "InvalidAction", "unknown operation: "+op)
	}
}

// writeCBOR writes a CBOR-encoded response with the Smithy-Protocol header.
func writeCBOR(c *echo.Context, v cbor.Value) error {
	c.Response().Header().Set("Content-Type", "application/cbor")
	c.Response().Header().Set("Smithy-Protocol", "rpc-v2-cbor")
	c.Response().WriteHeader(http.StatusOK)
	_, err := c.Response().Write(cbor.Encode(v))

	return err
}

// cborError writes a CBOR error response.
func (h *Handler) cborError(c *echo.Context, status int, code, message string) error {
	c.Response().Header().Set("Content-Type", "application/cbor")
	c.Response().Header().Set("Smithy-Protocol", "rpc-v2-cbor")
	c.Response().Header().Set("X-Amzn-Errortype", code)
	c.Response().WriteHeader(status)
	_, err := c.Response().Write(cbor.Encode(cbor.Map{
		"message": cbor.String(message),
	}))

	return err
}

// cborStr extracts a string value from a CBOR map by key.
func cborStr(m cbor.Map, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}

	s, isStr := v.(cbor.String)
	if !isStr {
		return ""
	}

	return string(s)
}

// cborFloat extracts a float64 from a CBOR map by key.
func cborFloat(m cbor.Map, key string) float64 {
	v, ok := m[key]
	if !ok {
		return 0
	}

	return cborValFloat(v)
}

func cborValFloat(v cbor.Value) float64 {
	switch f := v.(type) {
	case cbor.Float64:
		return float64(f)
	case cbor.Float32:
		return float64(f)
	case cbor.Uint:
		return float64(f)
	case cbor.NegInt:
		return -float64(f)
	}

	return 0
}

// cborInt32 extracts an int32 from a CBOR map by key.
func cborInt32(m cbor.Map, key string) int32 {
	v, ok := m[key]
	if !ok {
		return 0
	}

	switch i := v.(type) {
	case cbor.Uint:
		return int32(i) //nolint:gosec // CloudWatch period/evaluation values always fit in int32
	case cbor.NegInt:
		return -int32(i) //nolint:gosec // CloudWatch period/evaluation values always fit in int32
	case cbor.Float64:
		return int32(i)
	case cbor.Float32:
		return int32(i)
	}

	return 0
}

// cborTime extracts a [time.Time] from a CBOR map by key.
func cborTime(m cbor.Map, key string) time.Time {
	v, ok := m[key]
	if !ok {
		return time.Now().UTC()
	}

	return cborValTime(v)
}

func cborValTime(v cbor.Value) time.Time {
	// Tag(1, ...) means epoch timestamp per RFC 8949.
	// cbor.Decode returns *cbor.Tag (pointer) even though encoding uses cbor.Tag (value).
	if t, isTag := v.(*cbor.Tag); isTag {
		return cborValTime(t.Value)
	}

	secs := cborValFloat(v)
	sec := int64(secs)
	nsec := int64((secs - float64(sec)) * nanosPerSecond)

	return time.Unix(sec, nsec).UTC()
}

// cborStrList extracts a string list from a CBOR map by key.
func cborStrList(m cbor.Map, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}

	l, isList := v.(cbor.List)
	if !isList {
		return nil
	}

	result := make([]string, 0, len(l))

	for _, item := range l {
		s, isStr := item.(cbor.String)
		if !isStr {
			continue
		}

		result = append(result, string(s))
	}

	return result
}

// cborFromTime converts a [time.Time] to a CBOR Tag(1, float64) epoch timestamp.
func cborFromTime(t time.Time) cbor.Value {
	return cbor.Tag{ID: 1, Value: cbor.Float64(float64(t.Unix()))}
}

func (h *Handler) cborPutMetricData(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, "Namespace")
	if namespace == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Namespace is required")
	}

	var data []MetricDatum

	if listVal, hasData := input["MetricData"]; hasData {
		if list, isList := listVal.(cbor.List); isList {
			for _, item := range list {
				m, isMap := item.(cbor.Map)
				if !isMap {
					continue
				}

				val := cborFloat(m, "Value")
				ts := cborTime(m, "Timestamp")
				data = append(data, MetricDatum{
					MetricName: cborStr(m, "MetricName"),
					Value:      val,
					Unit:       cborStr(m, "Unit"),
					Timestamp:  ts,
					Count:      1,
					Sum:        val,
					Min:        val,
					Max:        val,
				})
			}
		}
	}

	if err := h.Backend.PutMetricData(namespace, data); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborGetMetricStatistics(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, "Namespace")
	metricName := cborStr(input, "MetricName")
	startTime := cborTime(input, "StartTime")
	endTime := cborTime(input, "EndTime")
	period := cborInt32(input, "Period")

	if period <= 0 {
		period = 60
	}

	statistics := cborStrList(input, "Statistics")

	dps, err := h.Backend.GetMetricStatistics(namespace, metricName, startTime, endTime, period, statistics)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	dpList := make(cbor.List, 0, len(dps))

	for _, dp := range dps {
		m := cbor.Map{"Timestamp": cborFromTime(dp.Timestamp)}

		if dp.Average != nil {
			m["Average"] = cbor.Float64(*dp.Average)
		}

		if dp.Sum != nil {
			m["Sum"] = cbor.Float64(*dp.Sum)
		}

		if dp.Minimum != nil {
			m["Minimum"] = cbor.Float64(*dp.Minimum)
		}

		if dp.Maximum != nil {
			m["Maximum"] = cbor.Float64(*dp.Maximum)
		}

		if dp.SampleCount != nil {
			m["SampleCount"] = cbor.Float64(*dp.SampleCount)
		}

		if dp.Unit != "" {
			m["Unit"] = cbor.String(dp.Unit)
		}

		dpList = append(dpList, m)
	}

	return writeCBOR(c, cbor.Map{
		"Label":      cbor.String(metricName),
		"Datapoints": dpList,
	})
}

// applyMetricStatToQuery fills q.MetricStat from a CBOR MetricStat map.
func applyMetricStatToQuery(q *MetricDataQuery, msMap cbor.Map) {
	q.MetricStat = MetricStat{
		Stat:   cborStr(msMap, "Stat"),
		Period: cborInt32(msMap, "Period"),
	}

	if mVal, hasMet := msMap["Metric"]; hasMet {
		if mMap, isMMap := mVal.(cbor.Map); isMMap {
			q.MetricStat.Namespace = cborStr(mMap, "Namespace")
			q.MetricStat.MetricName = cborStr(mMap, "MetricName")
		}
	}
}

// parseMetricDataQueries extracts MetricDataQueries from a CBOR map.
func parseMetricDataQueries(input cbor.Map) []MetricDataQuery {
	listVal, hasQueries := input["MetricDataQueries"]
	if !hasQueries {
		return nil
	}

	list, isList := listVal.(cbor.List)
	if !isList {
		return nil
	}

	queries := make([]MetricDataQuery, 0, len(list))

	for _, item := range list {
		m, isMap := item.(cbor.Map)
		if !isMap {
			continue
		}

		q := MetricDataQuery{
			ID:    cborStr(m, "Id"),
			Label: cborStr(m, "Label"),
		}

		if msVal, hasMS := m["MetricStat"]; hasMS {
			if msMap, isMSMap := msVal.(cbor.Map); isMSMap {
				applyMetricStatToQuery(&q, msMap)
			}
		}

		if q.ID != "" {
			queries = append(queries, q)
		}
	}

	return queries
}

func (h *Handler) cborGetMetricData(input cbor.Map, c *echo.Context) error {
	startTime := cborTime(input, "StartTime")
	endTime := cborTime(input, "EndTime")
	queries := parseMetricDataQueries(input)

	results, err := h.Backend.GetMetricData(queries, startTime, endTime)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	resultList := make(cbor.List, 0, len(results))

	for _, r := range results {
		tsList := make(cbor.List, 0, len(r.Timestamps))
		for _, ts := range r.Timestamps {
			tsList = append(tsList, cborFromTime(ts))
		}

		valList := make(cbor.List, 0, len(r.Values))
		for _, v := range r.Values {
			valList = append(valList, cbor.Float64(v))
		}

		resultList = append(resultList, cbor.Map{
			"Id":         cbor.String(r.ID),
			"Label":      cbor.String(r.Label),
			"StatusCode": cbor.String(r.StatusCode),
			"Timestamps": tsList,
			"Values":     valList,
		})
	}

	return writeCBOR(c, cbor.Map{
		"MetricDataResults": resultList,
	})
}

func (h *Handler) cborListMetrics(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, "Namespace")
	metricName := cborStr(input, "MetricName")
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.ListMetrics(namespace, metricName, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	mList := make(cbor.List, 0, len(p.Data))

	for _, m := range p.Data {
		mList = append(mList, cbor.Map{
			"Namespace":  cbor.String(m.Namespace),
			"MetricName": cbor.String(m.MetricName),
		})
	}

	resp := cbor.Map{
		"Metrics": mList,
	}
	if p.Next != "" {
		resp["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, resp)
}

func (h *Handler) cborPutMetricAlarm(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	if alarmName == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmName is required")
	}

	actionsEnabled := true
	if v, ok := input["ActionsEnabled"]; ok {
		if b, isBool := v.(cbor.Bool); isBool {
			actionsEnabled = bool(b)
		}
	}

	alarm := &MetricAlarm{
		AlarmName:               alarmName,
		Namespace:               cborStr(input, "Namespace"),
		MetricName:              cborStr(input, "MetricName"),
		ComparisonOperator:      cborStr(input, "ComparisonOperator"),
		Statistic:               cborStr(input, "Statistic"),
		AlarmDescription:        cborStr(input, "AlarmDescription"),
		Threshold:               cborFloat(input, "Threshold"),
		EvaluationPeriods:       cborInt32(input, "EvaluationPeriods"),
		Period:                  cborInt32(input, "Period"),
		ActionsEnabled:          actionsEnabled,
		AlarmActions:            cborStrList(input, "AlarmActions"),
		OKActions:               cborStrList(input, "OKActions"),
		InsufficientDataActions: cborStrList(input, "InsufficientDataActions"),
	}

	if err := h.Backend.PutMetricAlarm(alarm); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDescribeAlarms(input cbor.Map, c *echo.Context) error {
	alarmNames := cborStrList(input, "AlarmNames")
	alarmTypes := cborStrList(input, "AlarmTypes")
	stateValue := cborStr(input, "StateValue")
	nextToken := cborStr(input, "NextToken")
	maxRecords := int(cborInt32(input, "MaxRecords"))

	metricPage, compositePage, err := h.Backend.DescribeAlarms(
		alarmNames,
		alarmTypes,
		stateValue,
		nextToken,
		maxRecords,
	)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	alarmList := make(cbor.List, 0, len(metricPage.Data))

	for _, a := range metricPage.Data {
		m := cbor.Map{
			"AlarmName":          cbor.String(a.AlarmName),
			"AlarmArn":           cbor.String(a.AlarmArn),
			"Namespace":          cbor.String(a.Namespace),
			"MetricName":         cbor.String(a.MetricName),
			"ComparisonOperator": cbor.String(a.ComparisonOperator),
			"Statistic":          cbor.String(a.Statistic),
			"StateValue":         cbor.String(a.StateValue),
			"StateReason":        cbor.String(a.StateReason),
			"AlarmDescription":   cbor.String(a.AlarmDescription),
			"Threshold":          cbor.Float64(a.Threshold),
			"EvaluationPeriods": cbor.Uint(
				uint64(a.EvaluationPeriods), //nolint:gosec // EvaluationPeriods is always positive
			),
			"Period": cbor.Uint(
				uint64(a.Period), //nolint:gosec // Period is always positive
			),
			"ActionsEnabled": cbor.Bool(a.ActionsEnabled),
		}
		if len(a.AlarmActions) > 0 {
			m["AlarmActions"] = cborStringList(a.AlarmActions)
		}
		if len(a.OKActions) > 0 {
			m["OKActions"] = cborStringList(a.OKActions)
		}
		if len(a.InsufficientDataActions) > 0 {
			m["InsufficientDataActions"] = cborStringList(a.InsufficientDataActions)
		}
		alarmList = append(alarmList, m)
	}

	compositeList := make(cbor.List, 0, len(compositePage.Data))

	for _, a := range compositePage.Data {
		m := cbor.Map{
			"AlarmName":        cbor.String(a.AlarmName),
			"AlarmArn":         cbor.String(a.AlarmArn),
			"AlarmRule":        cbor.String(a.AlarmRule),
			"StateValue":       cbor.String(a.StateValue),
			"StateReason":      cbor.String(a.StateReason),
			"AlarmDescription": cbor.String(a.AlarmDescription),
			"ActionsEnabled":   cbor.Bool(a.ActionsEnabled),
		}
		if len(a.AlarmActions) > 0 {
			m["AlarmActions"] = cborStringList(a.AlarmActions)
		}
		if len(a.OKActions) > 0 {
			m["OKActions"] = cborStringList(a.OKActions)
		}
		if len(a.InsufficientDataActions) > 0 {
			m["InsufficientDataActions"] = cborStringList(a.InsufficientDataActions)
		}
		compositeList = append(compositeList, m)
	}

	resp := cbor.Map{
		"MetricAlarms":    alarmList,
		"CompositeAlarms": compositeList,
	}

	nextTok := metricPage.Next
	if nextTok == "" {
		nextTok = compositePage.Next
	}
	if nextTok != "" {
		resp["NextToken"] = cbor.String(nextTok)
	}

	return writeCBOR(c, resp)
}

func (h *Handler) cborDeleteAlarms(input cbor.Map, c *echo.Context) error {
	alarmNames := cborStrList(input, "AlarmNames")

	if err := h.Backend.DeleteAlarms(alarmNames); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

// cborTagOperation routes tag operations to their respective handlers.
func (h *Handler) cborTagOperation(op string, input cbor.Map, c *echo.Context) error {
	switch op {
	case cborOpListTagsForResource:
		return h.cborListTagsForResource(input, c)
	case cborOpTagResource:
		return h.cborTagResource(input, c)
	default: // UntagResource
		return h.cborUntagResource(input, c)
	}
}

func (h *Handler) cborListTagsForResource(input cbor.Map, c *echo.Context) error {
	arn := cborStr(input, "ResourceARN")
	tags := h.getTags(arn)
	tagList := make(cbor.List, 0, len(tags))

	for k, v := range tags {
		tagList = append(tagList, cbor.Map{
			"Key":   cbor.String(k),
			"Value": cbor.String(v),
		})
	}

	return writeCBOR(c, cbor.Map{"Tags": tagList})
}

func (h *Handler) cborTagResource(input cbor.Map, c *echo.Context) error {
	arn := cborStr(input, "ResourceARN")

	if tagList, ok := input["Tags"].(cbor.List); ok {
		kv := make(map[string]string, len(tagList))
		for _, item := range tagList {
			if m, isMap := item.(cbor.Map); isMap {
				kv[cborStr(m, "Key")] = cborStr(m, "Value")
			}
		}
		h.setTags(arn, kv)
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborUntagResource(input cbor.Map, c *echo.Context) error {
	arn := cborStr(input, "ResourceARN")

	if keyList, ok := input["TagKeys"].(cbor.List); ok {
		keys := make([]string, 0, len(keyList))
		for _, item := range keyList {
			if s, isStr := item.(cbor.String); isStr {
				keys = append(keys, string(s))
			}
		}
		h.removeTags(arn, keys)
	}

	return writeCBOR(c, cbor.Map{})
}

// cborStringList converts a []string to a cbor.List.
func cborStringList(ss []string) cbor.List {
	l := make(cbor.List, 0, len(ss))
	for _, s := range ss {
		l = append(l, cbor.String(s))
	}

	return l
}

func (h *Handler) cborPutCompositeAlarm(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	if alarmName == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmName is required")
	}
	alarmRule := cborStr(input, "AlarmRule")
	if alarmRule == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmRule is required")
	}

	actionsEnabled := true
	if v, ok := input["ActionsEnabled"]; ok {
		if b, isBool := v.(cbor.Bool); isBool {
			actionsEnabled = bool(b)
		}
	}

	alarm := &CompositeAlarm{
		AlarmName:               alarmName,
		AlarmRule:               alarmRule,
		AlarmDescription:        cborStr(input, "AlarmDescription"),
		ActionsEnabled:          actionsEnabled,
		AlarmActions:            cborStrList(input, "AlarmActions"),
		OKActions:               cborStrList(input, "OKActions"),
		InsufficientDataActions: cborStrList(input, "InsufficientDataActions"),
	}

	if err := h.Backend.PutCompositeAlarm(alarm); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDescribeAlarmsForMetric(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, "Namespace")
	metricName := cborStr(input, "MetricName")
	alarmNames := cborStrList(input, "AlarmNames")
	nextToken := cborStr(input, "NextToken")
	maxRecords := int(cborInt32(input, "MaxRecords"))

	p, err := h.Backend.DescribeAlarmsForMetric(namespace, metricName, alarmNames, nextToken, maxRecords)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	alarmList := make(cbor.List, 0, len(p.Data))
	for _, a := range p.Data {
		m := cbor.Map{
			"AlarmName":          cbor.String(a.AlarmName),
			"AlarmArn":           cbor.String(a.AlarmArn),
			"Namespace":          cbor.String(a.Namespace),
			"MetricName":         cbor.String(a.MetricName),
			"ComparisonOperator": cbor.String(a.ComparisonOperator),
			"Statistic":          cbor.String(a.Statistic),
			"StateValue":         cbor.String(a.StateValue),
			"Threshold":          cbor.Float64(a.Threshold),
			"EvaluationPeriods": cbor.Uint(
				uint64(a.EvaluationPeriods), //nolint:gosec // EvaluationPeriods is always positive
			),
			"Period":         cbor.Uint(uint64(a.Period)), //nolint:gosec // Period is always positive
			"ActionsEnabled": cbor.Bool(a.ActionsEnabled),
		}
		if len(a.AlarmActions) > 0 {
			m["AlarmActions"] = cborStringList(a.AlarmActions)
		}
		alarmList = append(alarmList, m)
	}

	resp := cbor.Map{"MetricAlarms": alarmList}
	if p.Next != "" {
		resp["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, resp)
}

func (h *Handler) cborDescribeAlarmHistory(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	historyItemType := cborStr(input, "HistoryItemType")
	nextToken := cborStr(input, "NextToken")
	maxRecords := int(cborInt32(input, "MaxRecords"))

	// Treat zero-value times as unset (cborTime returns now when key is missing).
	var sd, ed time.Time
	if _, hasStart := input["StartDate"]; hasStart {
		sd = cborTime(input, "StartDate")
	}
	if _, hasEnd := input["EndDate"]; hasEnd {
		ed = cborTime(input, "EndDate")
	}

	p, err := h.Backend.DescribeAlarmHistory(alarmName, historyItemType, nextToken, sd, ed, maxRecords)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	histList := make(cbor.List, 0, len(p.Data))
	for _, item := range p.Data {
		histList = append(histList, cbor.Map{
			"AlarmName":       cbor.String(item.AlarmName),
			"HistoryItemType": cbor.String(item.HistoryItemType),
			"HistorySummary":  cbor.String(item.HistorySummary),
			"HistoryData":     cbor.String(item.HistoryData),
			"Timestamp":       cborFromTime(item.Timestamp),
		})
	}

	resp := cbor.Map{"AlarmHistoryItems": histList}
	if p.Next != "" {
		resp["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, resp)
}

func (h *Handler) cborSetAlarmState(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	if alarmName == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmName is required")
	}

	if err := h.Backend.SetAlarmState(
		alarmName,
		cborStr(input, "StateValue"),
		cborStr(input, "StateReason"),
	); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborEnableAlarmActions(input cbor.Map, c *echo.Context) error {
	if err := h.Backend.EnableAlarmActions(cborStrList(input, "AlarmNames")); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDisableAlarmActions(input cbor.Map, c *echo.Context) error {
	if err := h.Backend.DisableAlarmActions(cborStrList(input, "AlarmNames")); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborPutDashboard(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, "DashboardName")
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "DashboardName is required")
	}

	body := cborStr(input, "DashboardBody")

	if err := h.Backend.PutDashboard(name, body); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{"DashboardValidationMessages": cbor.List{}})
}

func (h *Handler) cborGetDashboard(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, "DashboardName")
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "DashboardName is required")
	}

	entry, body, err := h.Backend.GetDashboard(name)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{
		"DashboardArn":  cbor.String(entry.DashboardArn),
		"DashboardBody": cbor.String(body),
		"DashboardName": cbor.String(entry.DashboardName),
	})
}

func (h *Handler) cborListDashboards(input cbor.Map, c *echo.Context) error {
	prefix := cborStr(input, "DashboardNamePrefix")
	nextToken := cborStr(input, "NextToken")

	p, err := h.Backend.ListDashboards(prefix, nextToken)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	entries := make(cbor.List, 0, len(p.Data))
	for _, e := range p.Data {
		entries = append(entries, cbor.Map{
			"DashboardArn":  cbor.String(e.DashboardArn),
			"DashboardName": cbor.String(e.DashboardName),
			"LastModified":  cborFromTime(e.LastModified),
			"Size":          cbor.Uint(uint64(e.Size)), //nolint:gosec // Size is always non-negative (len of body)
		})
	}

	resp := cbor.Map{"DashboardEntries": entries}
	if p.Next != "" {
		resp["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, resp)
}

func (h *Handler) cborDeleteDashboards(input cbor.Map, c *echo.Context) error {
	names := cborStrList(input, "DashboardNames")

	if err := h.Backend.DeleteDashboards(names); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}
