package cloudwatch

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

const (
	keyStateValue = "StateValue"
	keyState      = "State"
)

const (
	keyNamespace  = "Namespace"
	keyMetricName = "MetricName"
	keyName       = "Name"
	keyValue      = "Value"
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

// maxCBORBodyBytes caps CloudWatch rpc-v2-cbor request bodies. CloudWatch
// PutMetricData payloads are well below 1 MiB; cap conservatively to prevent
// unbounded io.ReadAll memory use.
const maxCBORBodyBytes = 1 << 20

// handleCBOR dispatches rpc-v2-cbor requests.
func (h *Handler) handleCBOR(c *echo.Context) error {
	r := c.Request()
	op := extractCBOROperation(r.URL.Path)

	body, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, maxCBORBodyBytes))
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "SerializationException", "cannot read body")
	}

	var input cbor.Map

	if len(body) > 0 {
		val, decErr := cbor.Decode(body)
		if decErr != nil {
			return h.cborError(
				c,
				http.StatusBadRequest,
				"SerializationException",
				"invalid CBOR body",
			)
		}

		m, isCBORMap := val.(cbor.Map)
		if !isCBORMap {
			return h.cborError(
				c,
				http.StatusBadRequest,
				"SerializationException",
				"expected CBOR map",
			)
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
	case opPutMetricData:
		return h.cborPutMetricData(input, c)
	case opGetMetricStatistics:
		return h.cborGetMetricStatistics(input, c)
	case opGetMetricData:
		return h.cborGetMetricData(input, c)
	case opListMetrics:
		return h.cborListMetrics(input, c)
	case cborOpListTagsForResource, cborOpTagResource, cborOpUntagResource:
		return h.cborTagOperation(op, input, c)
	default:
		return h.dispatchDashboardCBOR(op, input, c)
	}
}

func (h *Handler) dispatchDashboardCBOR(op string, input cbor.Map, c *echo.Context) error {
	switch op {
	case opPutDashboard:
		return h.cborPutDashboard(input, c)
	case opGetDashboard:
		return h.cborGetDashboard(input, c)
	case opListDashboards:
		return h.cborListDashboards(input, c)
	case opDeleteDashboards:
		return h.cborDeleteDashboards(input, c)
	default:
		return h.dispatchResourceManagementCBOR(op, input, c)
	}
}

func (h *Handler) dispatchResourceManagementCBOR(op string, input cbor.Map, c *echo.Context) error {
	switch op {
	case opPutAlarmMuteRule:
		return h.cborPutAlarmMuteRule(input, c)
	case opUpdateAlarmMuteRule:
		return h.cborUpdateAlarmMuteRule(input, c)
	case opPutInsightRule:
		return h.cborPutInsightRule(input, c)
	case opUpdateInsightRule:
		return h.cborUpdateInsightRule(input, c)
	case opPutMetricStream:
		return h.cborPutMetricStream(input, c)
	case opUpdateMetricStream:
		return h.cborUpdateMetricStream(input, c)
	case opGetAlarmMuteRule:
		return h.cborGetAlarmMuteRule(input, c)
	case opDeleteAlarmMuteRule:
		return h.cborDeleteAlarmMuteRule(input, c)
	default:
		return h.dispatchAlarmCBOR(op, input, c)
	}
}

// dispatchAlarmCBOR routes alarm-specific CBOR operations.
func (h *Handler) dispatchAlarmCBOR(op string, input cbor.Map, c *echo.Context) error {
	switch op {
	case opPutMetricAlarm:
		return h.cborPutMetricAlarm(input, c)
	case opPutCompositeAlarm:
		return h.cborPutCompositeAlarm(input, c)
	case opDescribeAlarms:
		return h.cborDescribeAlarms(input, c)
	case opDescribeAlarmsForMetric:
		return h.cborDescribeAlarmsForMetric(input, c)
	case opDescribeAlarmHistory:
		return h.cborDescribeAlarmHistory(input, c)
	case opDeleteAlarms:
		return h.cborDeleteAlarms(input, c)
	case opSetAlarmState:
		return h.cborSetAlarmState(input, c)
	case opEnableAlarmActions:
		return h.cborEnableAlarmActions(input, c)
	case opDisableAlarmActions:
		return h.cborDisableAlarmActions(input, c)
	default:
		return h.dispatchExtendedCBOR(op, input, c)
	}
}

// dispatchExtendedCBOR routes extended CloudWatch CBOR operations.
func (h *Handler) dispatchExtendedCBOR(op string, input cbor.Map, c *echo.Context) error {
	if handled, err := h.dispatchAnomalyMetricStreamCBOR(op, input, c); handled {
		return err
	}

	return h.dispatchInsightMetricFilterCBOR(op, input, c)
}

// dispatchAnomalyMetricStreamCBOR routes anomaly detector and metric stream CBOR operations.
func (h *Handler) dispatchAnomalyMetricStreamCBOR(
	op string,
	input cbor.Map,
	c *echo.Context,
) (bool, error) {
	switch op {
	case opPutAnomalyDetector:
		return true, h.cborPutAnomalyDetector(input, c)
	case opDeleteAnomalyDetector:
		return true, h.cborDeleteAnomalyDetector(input, c)
	case opDescribeAnomalyDetectors:
		return true, h.cborDescribeAnomalyDetectors(input, c)
	case opListMetricStreams:
		return true, h.cborListMetricStreams(input, c)
	case opGetMetricStream:
		return true, h.cborGetMetricStream(input, c)
	case opDeleteMetricStream:
		return true, h.cborDeleteMetricStream(input, c)
	case opDescribeAlarmContributors:
		return true, h.cborDescribeAlarmContributors(input, c)
	}

	return false, nil
}

// dispatchInsightMetricFilterCBOR routes insight rule and metric filter CBOR operations.
func (h *Handler) dispatchInsightMetricFilterCBOR(
	op string,
	input cbor.Map,
	c *echo.Context,
) error {
	switch op {
	case opDeleteInsightRules:
		return h.cborDeleteInsightRules(input, c)
	case opDescribeInsightRules:
		return h.cborDescribeInsightRules(input, c)
	case opDisableInsightRules:
		return h.cborDisableInsightRules(input, c)
	case opEnableInsightRules:
		return h.cborEnableInsightRules(input, c)
	case opGetInsightRuleReport:
		return h.cborGetInsightRuleReport(input, c)
	case opPutMetricFilter:
		return h.cborPutMetricFilter(input, c)
	case opDescribeMetricFilters:
		return h.cborDescribeMetricFilters(input, c)
	case opDeleteMetricFilter:
		return h.cborDeleteMetricFilter(input, c)
	case opTestMetricFilter:
		return h.cborTestMetricFilter(input, c)
	case opGetMetricWidgetImage:
		return h.cborGetMetricWidgetImage(input, c)
	case opListAlarmMuteRules:
		return h.cborListAlarmMuteRules(input, c)
	case opListManagedInsightRules:
		return h.cborListManagedInsightRules(input, c)
	case opPutManagedInsightRules:
		return h.cborPutManagedInsightRules(input, c)
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

// cborValInt64 extracts an int64 value from a raw cbor.Value without truncating to int32.
func cborValInt64(v cbor.Value) int64 {
	switch i := v.(type) {
	case cbor.Uint:
		if i > math.MaxInt64 {
			return math.MaxInt64
		}

		return int64(i)
	case cbor.NegInt:
		return -int64(i) //nolint:gosec // int64 covers all cbor.NegInt values
	case cbor.Float64:
		return int64(i)
	case cbor.Float32:
		return int64(i)
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

// cborDimensions extracts a list of Dimension from the "Dimensions" key of a CBOR map.
func cborDimensions(m cbor.Map) []Dimension {
	listVal, ok := m["Dimensions"]
	if !ok {
		return nil
	}
	list, isList := listVal.(cbor.List)
	if !isList {
		return nil
	}
	dims := make([]Dimension, 0, len(list))
	for _, item := range list {
		dm, isMap := item.(cbor.Map)
		if !isMap {
			continue
		}
		name := cborStr(dm, keyName)
		if name == "" {
			continue
		}
		dims = append(dims, Dimension{Name: name, Value: cborStr(dm, keyValue)})
	}

	return dims
}

// cborDecodeDatum parses a single MetricDatum from a CBOR map.
// StatisticValues takes precedence over Value when present.
func cborDecodeDatum(m cbor.Map) MetricDatum {
	ts := cborTime(m, "Timestamp")
	dims := cborDimensions(m)
	storageRes := cborInt32(m, "StorageResolution")
	name := cborStr(m, keyMetricName)
	unit := cborStr(m, "Unit")

	if ssMap, ok := cborStatisticValues(m); ok {
		return MetricDatum{
			MetricName:        name,
			Unit:              unit,
			Timestamp:         ts,
			Count:             cborFloat(ssMap, "SampleCount"),
			Sum:               cborFloat(ssMap, statSum),
			Min:               cborFloat(ssMap, "Minimum"),
			Max:               cborFloat(ssMap, "Maximum"),
			Dimensions:        dims,
			StorageResolution: storageRes,
		}
	}

	val := cborFloat(m, keyValue)

	return MetricDatum{
		MetricName:        name,
		Value:             val,
		Unit:              unit,
		Timestamp:         ts,
		Count:             1,
		Sum:               val,
		Min:               val,
		Max:               val,
		Dimensions:        dims,
		StorageResolution: storageRes,
	}
}

// cborStatisticValues returns the StatisticValues sub-map when present.
func cborStatisticValues(m cbor.Map) (cbor.Map, bool) {
	ssVal, hasSS := m["StatisticValues"]
	if !hasSS {
		return nil, false
	}
	ssMap, isSS := ssVal.(cbor.Map)

	return ssMap, isSS
}

func (h *Handler) cborPutMetricData(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, keyNamespace)
	if namespace == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace is required",
		)
	}

	var data []MetricDatum
	if listVal, hasData := input["MetricData"]; hasData {
		if list, isList := listVal.(cbor.List); isList {
			for _, item := range list {
				m, isMap := item.(cbor.Map)
				if !isMap {
					continue
				}
				data = append(data, cborDecodeDatum(m))
			}
		}
	}

	unprocessed, err := h.Backend.PutMetricData(namespace, data)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, buildUnprocessedCBORResponse(unprocessed))
}

// buildUnprocessedCBORResponse constructs the CBOR response map for PutMetricData.
func buildUnprocessedCBORResponse(unprocessed []UnprocessedMetricDatum) cbor.Map {
	resp := cbor.Map{}
	if len(unprocessed) == 0 {
		return resp
	}
	uList := make(cbor.List, 0, len(unprocessed))
	for _, u := range unprocessed {
		uList = append(uList, cbor.Map{
			"MetricName":   cbor.String(u.MetricName),
			"ErrorCode":    cbor.String(u.ErrorCode),
			"ErrorMessage": cbor.String(u.ErrorMessage),
		})
	}
	resp["UnprocessedMetricData"] = uList

	return resp
}

func (h *Handler) cborGetMetricStatistics(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, keyNamespace)
	metricName := cborStr(input, keyMetricName)
	startTime := cborTime(input, "StartTime")
	endTime := cborTime(input, "EndTime")
	period := cborInt32(input, "Period")

	if period <= 0 {
		period = 60
	}

	dimensions := cborDimensions(input)
	statistics := cborStrList(input, "Statistics")
	extendedStatistics := cborStrList(input, "ExtendedStatistics")

	dps, err := h.Backend.GetMetricStatistics(
		namespace,
		metricName,
		dimensions,
		startTime,
		endTime,
		period,
		statistics,
		extendedStatistics,
	)
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
			m[statSum] = cbor.Float64(*dp.Sum)
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
			q.MetricStat.Namespace = cborStr(mMap, keyNamespace)
			q.MetricStat.MetricName = cborStr(mMap, keyMetricName)
			q.MetricStat.Dimensions = cborDimensions(mMap)
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

		// ReturnData defaults to true; only suppress when caller explicitly passes false.
		returnData := true
		if rdVal, hasRD := m["ReturnData"]; hasRD {
			if rdBool, isBool := rdVal.(cbor.Bool); isBool {
				returnData = bool(rdBool)
			}
		}

		q := MetricDataQuery{
			ID:         cborStr(m, "Id"),
			Label:      cborStr(m, "Label"),
			Expression: cborStr(m, "Expression"),
			AccountID:  cborStr(m, "AccountId"),
			ReturnData: returnData,
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
	scanBy := cborStr(input, "ScanBy")
	queries := parseMetricDataQueries(input)

	var results []MetricDataResult
	var err error
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		results, err = bk.GetMetricDataWithOptions(queries, startTime, endTime, scanBy)
	} else {
		results, err = h.Backend.GetMetricData(queries, startTime, endTime)
	}
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
	namespace := cborStr(input, keyNamespace)
	metricName := cborStr(input, keyMetricName)
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))
	dimensions := cborDimensions(input)

	p, err := h.Backend.ListMetrics(namespace, metricName, dimensions, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	mList := make(cbor.List, 0, len(p.Data))

	for _, m := range p.Data {
		mm := cbor.Map{
			keyNamespace:  cbor.String(m.Namespace),
			keyMetricName: cbor.String(m.MetricName),
		}
		if len(m.Dimensions) > 0 {
			dimList := make(cbor.List, 0, len(m.Dimensions))
			for _, d := range m.Dimensions {
				dimList = append(dimList, cbor.Map{
					keyName:  cbor.String(d.Name),
					keyValue: cbor.String(d.Value),
				})
			}
			mm["Dimensions"] = dimList
		}
		mList = append(mList, mm)
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
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}

	actionsEnabled := true
	if v, ok := input["ActionsEnabled"]; ok {
		if b, isBool := v.(cbor.Bool); isBool {
			actionsEnabled = bool(b)
		}
	}

	alarm := &MetricAlarm{
		AlarmName:               alarmName,
		Namespace:               cborStr(input, keyNamespace),
		MetricName:              cborStr(input, keyMetricName),
		ComparisonOperator:      cborStr(input, "ComparisonOperator"),
		Statistic:               cborStr(input, "Statistic"),
		ExtendedStatistic:       cborStr(input, "ExtendedStatistic"),
		TreatMissingData:        cborStr(input, "TreatMissingData"),
		AlarmDescription:        cborStr(input, "AlarmDescription"),
		Threshold:               cborFloat(input, "Threshold"),
		EvaluationPeriods:       cborInt32(input, "EvaluationPeriods"),
		DatapointsToAlarm:       cborInt32(input, "DatapointsToAlarm"),
		Period:                  cborInt32(input, "Period"),
		ActionsEnabled:          actionsEnabled,
		AlarmActions:            cborStrList(input, "AlarmActions"),
		OKActions:               cborStrList(input, "OKActions"),
		InsufficientDataActions: cborStrList(input, "InsufficientDataActions"),
		Dimensions:              cborDimensions(input),
	}

	if err := h.Backend.PutMetricAlarm(alarm); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDescribeAlarms(input cbor.Map, c *echo.Context) error {
	alarmNames := cborStrList(input, "AlarmNames")
	alarmTypes := cborStrList(input, "AlarmTypes")
	alarmNamePrefix := cborStr(input, "AlarmNamePrefix")
	stateValue := cborStr(input, keyStateValue)
	nextToken := cborStr(input, "NextToken")
	maxRecords := int(cborInt32(input, "MaxRecords"))

	metricPage, compositePage, err := h.Backend.DescribeAlarms(
		alarmNames,
		alarmTypes,
		alarmNamePrefix,
		stateValue,
		nextToken,
		maxRecords,
	)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	alarmList := make(cbor.List, 0, len(metricPage.Data))
	for i := range metricPage.Data {
		alarmList = append(alarmList, buildMetricAlarmCBOR(&metricPage.Data[i]))
	}

	compositeList := make(cbor.List, 0, len(compositePage.Data))
	for i := range compositePage.Data {
		compositeList = append(compositeList, buildCompositeAlarmCBOR(&compositePage.Data[i]))
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

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		for _, a := range b.GetAlarmARNs(alarmNames) {
			h.deleteResourceTags(a)
		}
	}

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
			"Key":    cbor.String(k),
			keyValue: cbor.String(v),
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
				kv[cborStr(m, "Key")] = cborStr(m, keyValue)
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

// buildMetricAlarmCBOR converts a MetricAlarm to a cbor.Map.
func buildMetricAlarmCBOR(a *MetricAlarm) cbor.Map {
	m := cbor.Map{
		keyAlarmName:         cbor.String(a.AlarmName),
		keyAlarmArn:          cbor.String(a.AlarmArn),
		"AlarmType":          cbor.String("MetricAlarm"),
		keyNamespace:         cbor.String(a.Namespace),
		keyMetricName:        cbor.String(a.MetricName),
		"ComparisonOperator": cbor.String(a.ComparisonOperator),
		"Statistic":          cbor.String(a.Statistic),
		keyStateValue:        cbor.String(a.StateValue),
		"StateReason":        cbor.String(a.StateReason),
		keyAlarmDescription:  cbor.String(a.AlarmDescription),
		"Threshold":          cbor.Float64(a.Threshold),
		"EvaluationPeriods": cbor.Uint(
			uint64(a.EvaluationPeriods), //nolint:gosec // EvaluationPeriods is positive
		),
		"Period": cbor.Uint(
			uint64(a.Period), //nolint:gosec // Period is positive
		),
		"ActionsEnabled": cbor.Bool(a.ActionsEnabled),
	}
	if !a.StateTransitionedTimestamp.IsZero() {
		m["StateTransitionedTimestamp"] = cborFromTime(a.StateTransitionedTimestamp)
	}
	if !a.AlarmConfigurationUpdatedTimestamp.IsZero() {
		m["AlarmConfigurationUpdatedTimestamp"] = cborFromTime(a.AlarmConfigurationUpdatedTimestamp)
	}
	if !a.CreatedAt.IsZero() {
		m["AlarmCreatedAt"] = cborFromTime(a.CreatedAt)
	}
	if a.StateReasonData != "" {
		m["StateReasonData"] = cbor.String(a.StateReasonData)
	}
	if a.DatapointsToAlarm > 0 {
		m["DatapointsToAlarm"] = cbor.Uint(uint64(a.DatapointsToAlarm))
	}
	if a.TreatMissingData != "" {
		m["TreatMissingData"] = cbor.String(a.TreatMissingData)
	}
	if a.ExtendedStatistic != "" {
		m["ExtendedStatistic"] = cbor.String(a.ExtendedStatistic)
	}
	if len(a.Dimensions) > 0 {
		dims := make(cbor.List, 0, len(a.Dimensions))
		for _, d := range a.Dimensions {
			dims = append(dims, cbor.Map{
				keyName:  cbor.String(d.Name),
				keyValue: cbor.String(d.Value),
			})
		}
		m["Dimensions"] = dims
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

	return m
}

// buildCompositeAlarmCBOR converts a CompositeAlarm to a cbor.Map.
func buildCompositeAlarmCBOR(a *CompositeAlarm) cbor.Map {
	m := cbor.Map{
		keyAlarmName:        cbor.String(a.AlarmName),
		keyAlarmArn:         cbor.String(a.AlarmArn),
		"AlarmType":         cbor.String("CompositeAlarm"),
		"AlarmRule":         cbor.String(a.AlarmRule),
		keyStateValue:       cbor.String(a.StateValue),
		"StateReason":       cbor.String(a.StateReason),
		keyAlarmDescription: cbor.String(a.AlarmDescription),
		"ActionsEnabled":    cbor.Bool(a.ActionsEnabled),
	}
	if !a.CreatedAt.IsZero() {
		m["AlarmCreatedAt"] = cborFromTime(a.CreatedAt)
	}
	if !a.StateTransitionedTimestamp.IsZero() {
		m["StateTransitionedTimestamp"] = cborFromTime(a.StateTransitionedTimestamp)
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

	return m
}

func (h *Handler) cborPutCompositeAlarm(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	if alarmName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}
	alarmRule := cborStr(input, "AlarmRule")
	if alarmRule == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmRule is required",
		)
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
	namespace := cborStr(input, keyNamespace)
	metricName := cborStr(input, keyMetricName)
	dimensions := cborDimensions(input)
	alarmNames := cborStrList(input, "AlarmNames")
	nextToken := cborStr(input, "NextToken")
	maxRecords := int(cborInt32(input, "MaxRecords"))

	p, err := h.Backend.DescribeAlarmsForMetric(
		namespace,
		metricName,
		dimensions,
		alarmNames,
		nextToken,
		maxRecords,
	)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	alarmList := make(cbor.List, 0, len(p.Data))
	for i := range p.Data {
		alarmList = append(alarmList, buildMetricAlarmCBOR(&p.Data[i]))
	}

	resp := cbor.Map{"MetricAlarms": alarmList}
	if p.Next != "" {
		resp["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, resp)
}

func (h *Handler) cborDescribeAlarmHistory(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	alarmType := cborStr(input, "AlarmType")
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

	p, err := h.Backend.DescribeAlarmHistory(
		alarmName,
		alarmType,
		historyItemType,
		nextToken,
		sd,
		ed,
		maxRecords,
	)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	histList := make(cbor.List, 0, len(p.Data))
	for _, item := range p.Data {
		m := cbor.Map{
			keyAlarmName:      cbor.String(item.AlarmName),
			"HistoryItemType": cbor.String(item.HistoryItemType),
			"HistorySummary":  cbor.String(item.HistorySummary),
			"HistoryData":     cbor.String(item.HistoryData),
			"Timestamp":       cborFromTime(item.Timestamp),
		}
		if item.AlarmType != "" {
			m["AlarmType"] = cbor.String(item.AlarmType)
		}
		histList = append(histList, m)
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
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}

	if err := h.Backend.SetAlarmState(
		c.Request().Context(),
		alarmName,
		cborStr(input, keyStateValue),
		cborStr(input, "StateReason"),
		cborStr(input, "StateReasonData"),
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
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"DashboardName is required",
		)
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
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"DashboardName is required",
		)
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
			"Size": cbor.Uint(
				uint64(e.Size), //nolint:gosec // Size is always non-negative (len of body)
			),
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

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		for _, a := range b.GetDashboardARNs(names) {
			h.deleteResourceTags(a)
		}
	}

	if err := h.Backend.DeleteDashboards(names); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborPutAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	muteName := cborStr(input, "MuteName")
	if muteName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteName is required",
		)
	}

	rawDuration := cborValInt64(input["MuteDuration"])
	if rawDuration < 0 || rawDuration > math.MaxInt32 {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteDuration must be between 0 and 2147483647",
		)
	}

	rule := &AlarmMuteRule{
		MuteName:      muteName,
		Description:   cborStr(input, "Description"),
		AlarmNames:    cborStrList(input, "AlarmNames"),
		MuteDuration:  int32(rawDuration),
		MuteStartTime: time.Now().UTC(),
	}

	if start := cborTime(input, "MuteStartTime"); !start.IsZero() {
		rule.MuteStartTime = start.UTC()
	}

	if err := h.Backend.PutAlarmMuteRule(rule); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborUpdateAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	muteName := cborStr(input, "MuteName")
	if muteName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteName is required",
		)
	}
	if _, err := h.Backend.GetAlarmMuteRule(muteName); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return h.cborPutAlarmMuteRule(input, c)
}

func (h *Handler) cborGetAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	muteName := cborStr(input, "MuteName")
	if muteName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteName is required",
		)
	}

	rule, err := h.Backend.GetAlarmMuteRule(muteName)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	muteRule := cbor.Map{
		"MuteName":    cbor.String(rule.MuteName),
		"Description": cbor.String(rule.Description),
		"MuteDuration": cbor.Uint(
			uint64(rule.MuteDuration), //nolint:gosec // MuteDuration is always non-negative
		),
		"CreationTime": cborFromTime(rule.CreationTime),
		"AlarmNames":   cborStringList(rule.AlarmNames),
	}
	if !rule.MuteStartTime.IsZero() {
		muteRule["MuteStartTime"] = cborFromTime(rule.MuteStartTime)
	}

	return writeCBOR(c, cbor.Map{"MuteRule": muteRule})
}

func (h *Handler) cborDeleteAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	muteName := cborStr(input, "MuteName")
	if muteName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteName is required",
		)
	}

	if err := h.Backend.DeleteAlarmMuteRule(muteName); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborPutInsightRule(input cbor.Map, c *echo.Context) error {
	return h.cborPutInsightRuleWithName(cborStr(input, "RuleName"), input, c)
}

func (h *Handler) cborPutInsightRuleWithName(
	ruleName string,
	input cbor.Map,
	c *echo.Context,
) error {
	if ruleName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleName is required",
		)
	}

	if err := h.Backend.PutInsightRule(&InsightRule{
		Name:       ruleName,
		Definition: cborStr(input, "RuleDefinition"),
		State:      cborStr(input, "RuleState"),
	}); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborUpdateInsightRule(input cbor.Map, c *echo.Context) error {
	ruleName := cborStr(input, "RuleName")
	if ruleName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleName is required",
		)
	}
	if _, err := h.Backend.GetInsightRule(ruleName); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return h.cborPutInsightRuleWithName(ruleName, input, c)
}

// cborMetricStreamFilters extracts a list of MetricStreamFilter from a CBOR map key.
func cborMetricStreamFilters(input cbor.Map, key string) []MetricStreamFilter {
	listVal, ok := input[key]
	if !ok {
		return nil
	}
	list, isList := listVal.(cbor.List)
	if !isList {
		return nil
	}
	filters := make([]MetricStreamFilter, 0, len(list))
	for _, item := range list {
		fm, isMap := item.(cbor.Map)
		if !isMap {
			continue
		}
		ns := cborStr(fm, keyNamespace)
		if ns == "" {
			continue
		}
		metricNames := cborStrList(fm, "MetricNames")
		filters = append(filters, MetricStreamFilter{Namespace: ns, MetricNames: metricNames})
	}

	return filters
}

func (h *Handler) cborPutMetricStream(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, keyName)
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	if err := h.Backend.PutMetricStream(&MetricStream{
		Name:           name,
		FirehoseArn:    cborStr(input, "FirehoseArn"),
		RoleArn:        cborStr(input, "RoleArn"),
		OutputFormat:   cborStr(input, "OutputFormat"),
		State:          cborStr(input, keyState),
		IncludeFilters: cborMetricStreamFilters(input, "IncludeFilters"),
		ExcludeFilters: cborMetricStreamFilters(input, "ExcludeFilters"),
	}); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborUpdateMetricStream(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, keyName)
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}
	if _, err := h.Backend.GetMetricStream(name); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return h.cborPutMetricStream(input, c)
}

func (h *Handler) cborPutAnomalyDetector(input cbor.Map, c *echo.Context) error {
	namespace := ""
	metricName := ""
	stat := ""

	if smadRaw, hasSmad := input["SingleMetricAnomalyDetector"]; hasSmad {
		if smad, isMap := smadRaw.(cbor.Map); isMap {
			namespace = cborStr(smad, keyNamespace)
			metricName = cborStr(smad, keyMetricName)
			stat = cborStr(smad, "Stat")
		}
	}
	if namespace == "" {
		namespace = cborStr(input, keyNamespace)
	}
	if metricName == "" {
		metricName = cborStr(input, keyMetricName)
	}
	if stat == "" {
		stat = cborStr(input, "Stat")
	}

	if namespace == "" || metricName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace and MetricName are required",
		)
	}

	if err := h.Backend.PutAnomalyDetector(&AnomalyDetector{
		Namespace:  namespace,
		MetricName: metricName,
		Stat:       stat,
		StateValue: statusTrainedInsufficient,
	}); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDeleteAnomalyDetector(input cbor.Map, c *echo.Context) error {
	namespace := ""
	metricName := ""
	stat := ""

	var dimsD []Dimension
	if smadRaw, hasSmad := input["SingleMetricAnomalyDetector"]; hasSmad {
		if smad, isMap := smadRaw.(cbor.Map); isMap {
			namespace = cborStr(smad, keyNamespace)
			metricName = cborStr(smad, keyMetricName)
			stat = cborStr(smad, "Stat")
			dimsD = cborDimensions(smad)
		}
	}
	if namespace == "" {
		namespace = cborStr(input, keyNamespace)
	}
	if metricName == "" {
		metricName = cborStr(input, keyMetricName)
	}
	if stat == "" {
		stat = cborStr(input, "Stat")
	}
	if dimsD == nil {
		dimsD = cborDimensions(input)
	}

	if err := h.Backend.DeleteAnomalyDetector(namespace, metricName, stat, dimsD); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDescribeAnomalyDetectors(input cbor.Map, c *echo.Context) error {
	namespace := cborStr(input, keyNamespace)
	metricName := cborStr(input, keyMetricName)
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.DescribeAnomalyDetectors(namespace, metricName, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	members := make(cbor.List, 0, len(p.Data))
	for _, d := range p.Data {
		entry := cbor.Map{
			keyStateValue: cbor.String(d.StateValue),
			"SingleMetricAnomalyDetector": cbor.Map{
				keyNamespace:  cbor.String(d.Namespace),
				keyMetricName: cbor.String(d.MetricName),
				"Stat":        cbor.String(d.Stat),
			},
		}
		members = append(members, entry)
	}

	out := cbor.Map{
		"AnomalyDetectors": members,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborListMetricStreams(input cbor.Map, c *echo.Context) error {
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.ListMetricStreams(nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	entries := make(cbor.List, 0, len(p.Data))
	for _, s := range p.Data {
		entry := cbor.Map{
			keyName:        cbor.String(s.Name),
			"Arn":          cbor.String(s.Arn),
			"FirehoseArn":  cbor.String(s.FirehoseArn),
			keyState:       cbor.String(s.State),
			"OutputFormat": cbor.String(s.OutputFormat),
		}
		if !s.CreationDate.IsZero() {
			entry["CreationDate"] = cborFromTime(s.CreationDate)
		}
		if !s.LastUpdateDate.IsZero() {
			entry["LastUpdateDate"] = cborFromTime(s.LastUpdateDate)
		}
		entries = append(entries, entry)
	}

	out := cbor.Map{
		"Entries": entries,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborGetMetricStream(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, keyName)
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	stream, err := h.Backend.GetMetricStream(name)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	out := cbor.Map{
		keyName:        cbor.String(stream.Name),
		"Arn":          cbor.String(stream.Arn),
		"FirehoseArn":  cbor.String(stream.FirehoseArn),
		"RoleArn":      cbor.String(stream.RoleArn),
		keyState:       cbor.String(stream.State),
		"OutputFormat": cbor.String(stream.OutputFormat),
	}
	if !stream.CreationDate.IsZero() {
		out["CreationDate"] = cborFromTime(stream.CreationDate)
	}
	if !stream.LastUpdateDate.IsZero() {
		out["LastUpdateDate"] = cborFromTime(stream.LastUpdateDate)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborDeleteMetricStream(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, keyName)
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	if err := h.Backend.DeleteMetricStream(name); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDeleteInsightRules(input cbor.Map, c *echo.Context) error {
	ruleNames := cborStringSlice(input["RuleNames"])
	if len(ruleNames) == 0 {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.DeleteInsightRules(ruleNames)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

func (h *Handler) cborDescribeInsightRules(input cbor.Map, c *echo.Context) error {
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.DescribeInsightRules(nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	rules := make(cbor.List, 0, len(p.Data))
	for _, r := range p.Data {
		entry := cbor.Map{
			keyName:       cbor.String(r.Name),
			keyState:      cbor.String(r.State),
			"Schema":      cbor.String(r.Schema),
			"Definition":  cbor.String(r.Definition),
			"ManagedRule": cbor.Bool(r.ManagedRule),
		}
		if r.Arn != "" {
			entry["RuleArn"] = cbor.String(r.Arn)
		}
		if !r.CreatedAt.IsZero() {
			entry["CreatedAt"] = cborFromTime(r.CreatedAt)
		}
		rules = append(rules, entry)
	}

	out := cbor.Map{
		"InsightRules": rules,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborDisableInsightRules(input cbor.Map, c *echo.Context) error {
	ruleNames := cborStringSlice(input["RuleNames"])
	if len(ruleNames) == 0 {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.DisableInsightRules(ruleNames)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

func (h *Handler) cborEnableInsightRules(input cbor.Map, c *echo.Context) error {
	ruleNames := cborStringSlice(input["RuleNames"])
	if len(ruleNames) == 0 {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.EnableInsightRules(ruleNames)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

func (h *Handler) cborGetInsightRuleReport(input cbor.Map, c *echo.Context) error {
	ruleName := cborStr(input, "RuleName")
	if ruleName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleName is required",
		)
	}
	if _, err := h.Backend.GetInsightRule(ruleName); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	maxContributors := int(cborInt32(input, "MaxContributorCount"))
	if maxContributors <= 0 {
		maxContributors = 10
	}
	orderBy := cborStr(input, "OrderBy")

	startTime := time.Now().UTC().Add(-time.Hour)
	if _, ok := input["StartTime"]; ok {
		startTime = cborTime(input, "StartTime")
	}
	endTime := time.Now().UTC()
	if _, ok := input["EndTime"]; ok {
		endTime = cborTime(input, "EndTime")
	}

	var contributors []AlarmContributor
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		bk.mu.RLock("GetInsightRuleReport")
		var innerErr error
		contributors, innerErr = bk.GetInsightRuleContributors(
			ruleName,
			startTime,
			endTime,
			maxContributors,
			orderBy,
		)
		bk.mu.RUnlock()
		if innerErr != nil {
			return h.cborError(
				c,
				http.StatusBadRequest,
				"ResourceNotFoundException",
				innerErr.Error(),
			)
		}
	}

	contribList := make(cbor.List, 0, len(contributors))
	var aggregateValue float64
	for _, contrib := range contributors {
		keys := make(cbor.List, 0, len(contrib.Keys))
		for _, k := range contrib.Keys {
			keys = append(keys, cbor.String(k))
		}
		contribList = append(contribList, cbor.Map{
			"Keys":                      keys,
			"ApproximateAggregateValue": cbor.Float64(contrib.Sum),
		})
		aggregateValue += contrib.Sum
	}

	// KeyLabels: extract key expressions from the rule definition (best-effort).
	rule, _ := h.Backend.GetInsightRule(ruleName)
	keyLabels := extractInsightRuleKeyLabels(rule)

	return writeCBOR(c, cbor.Map{
		"KeyLabels":              keyLabels,
		"AggregationStatistic":   cbor.String("Sum"),
		"AggregateValue":         cbor.Float64(aggregateValue),
		"ApproximateUniqueCount": cbor.Uint(uint64(len(contributors))),
		"Contributors":           contribList,
		"MetricDatapoints":       cbor.List{},
	})
}

// extractInsightRuleKeyLabels returns a cbor.List of key-label strings from the
// Contribution.Keys array in the rule definition JSON. Returns an empty list if
// the rule is nil or the definition cannot be parsed.
func extractInsightRuleKeyLabels(rule *InsightRule) cbor.List {
	if rule == nil || rule.Definition == "" {
		return cbor.List{}
	}
	var def struct {
		Contribution struct {
			Keys []string `json:"Keys"`
		} `json:"Contribution"`
	}
	if err := json.Unmarshal([]byte(rule.Definition), &def); err != nil {
		return cbor.List{}
	}
	labels := make(cbor.List, 0, len(def.Contribution.Keys))
	for _, k := range def.Contribution.Keys {
		labels = append(labels, cbor.String(k))
	}

	return labels
}

func (h *Handler) cborPutMetricFilter(input cbor.Map, c *echo.Context) error {
	filterName := cborStr(input, "FilterName")
	if filterName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"FilterName is required",
		)
	}
	logGroupName := cborStr(input, "LogGroupName")
	if logGroupName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"LogGroupName is required",
		)
	}

	filter := &MetricFilter{
		FilterName:    filterName,
		LogGroupName:  logGroupName,
		FilterPattern: cborStr(input, "FilterPattern"),
	}

	if mtsRaw, hasMts := input["MetricTransformations"]; hasMts {
		if mtsList, isList := mtsRaw.(cbor.List); isList {
			for _, mtRaw := range mtsList {
				if mt, isMap := mtRaw.(cbor.Map); isMap {
					filter.MetricTransformations = append(
						filter.MetricTransformations,
						MetricTransformation{
							MetricName:      cborStr(mt, keyMetricName),
							MetricNamespace: cborStr(mt, "MetricNamespace"),
							MetricValue:     cborStr(mt, "MetricValue"),
							Unit:            cborStr(mt, "Unit"),
							DefaultValue:    cborFloat(mt, "DefaultValue"),
						},
					)
				}
			}
		}
	}

	if err := h.Backend.PutMetricFilter(filter); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDescribeMetricFilters(input cbor.Map, c *echo.Context) error {
	filterNamePrefix := cborStr(input, "FilterNamePrefix")
	logGroupName := cborStr(input, "LogGroupName")
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.DescribeMetricFilters(filterNamePrefix, logGroupName, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	filters := make(cbor.List, 0, len(p.Data))
	for _, f := range p.Data {
		entry := cbor.Map{
			"FilterName":    cbor.String(f.FilterName),
			"LogGroupName":  cbor.String(f.LogGroupName),
			"FilterPattern": cbor.String(f.FilterPattern),
			"CreationTime":  cbor.Uint(uint64(f.CreationTime.UnixMilli())),
		}
		if len(f.MetricTransformations) > 0 {
			mts := make(cbor.List, 0, len(f.MetricTransformations))
			for _, mt := range f.MetricTransformations {
				mts = append(mts, cbor.Map{
					keyMetricName:     cbor.String(mt.MetricName),
					"MetricNamespace": cbor.String(mt.MetricNamespace),
					"MetricValue":     cbor.String(mt.MetricValue),
					"Unit":            cbor.String(mt.Unit),
					"DefaultValue":    cbor.Float64(mt.DefaultValue),
				})
			}
			entry["MetricTransformations"] = mts
		}
		filters = append(filters, entry)
	}

	out := cbor.Map{
		"MetricFilters": filters,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborDeleteMetricFilter(input cbor.Map, c *echo.Context) error {
	filterName := cborStr(input, "FilterName")
	if filterName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"FilterName is required",
		)
	}
	logGroupName := cborStr(input, "LogGroupName")
	if logGroupName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"LogGroupName is required",
		)
	}

	if err := h.Backend.DeleteMetricFilter(filterName, logGroupName); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

// cborTestMetricFilter returns an empty matches response (log events are not stored by this emulator).
func (h *Handler) cborTestMetricFilter(_ cbor.Map, c *echo.Context) error {
	return writeCBOR(c, cbor.Map{
		"Matches": cbor.List{},
	})
}

// cborGetMetricWidgetImage returns a minimal placeholder PNG, mirroring the form handler.
func (h *Handler) cborGetMetricWidgetImage(_ cbor.Map, c *echo.Context) error {
	// The CBOR MetricWidgetImage member is a blob, so emit the raw PNG bytes
	// rather than the base64 text used by the XML/form response.
	img, err := base64.StdEncoding.DecodeString(minimalPNG1x1)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{
		"MetricWidgetImage": cbor.Slice(img),
	})
}

func (h *Handler) cborListAlarmMuteRules(input cbor.Map, c *echo.Context) error {
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxRecords"))
	if maxResults == 0 {
		maxResults = int(cborInt32(input, "MaxResults"))
	}

	p, err := h.Backend.ListAlarmMuteRules(nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	summaries := make(cbor.List, 0, len(p.Data))
	for _, rule := range p.Data {
		entry := cbor.Map{
			"AlarmMuteRuleArn": cbor.String(rule.MuteName),
			"Status":           cbor.String("active"),
		}
		if !rule.CreationTime.IsZero() {
			entry["LastUpdatedTimestamp"] = cborFromTime(rule.CreationTime)
		}
		summaries = append(summaries, entry)
	}

	out := cbor.Map{
		"AlarmMuteRuleSummaries": summaries,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborListManagedInsightRules(input cbor.Map, c *echo.Context) error {
	resourceARN := cborStr(input, "ResourceARN")
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxResults"))

	p, err := h.Backend.ListManagedInsightRules(resourceARN, nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	rules := make(cbor.List, 0, len(p.Data))
	for _, rule := range p.Data {
		entry := cbor.Map{
			"TemplateName": cbor.String(rule.Name),
		}
		if rule.Arn != "" {
			entry["ResourceARN"] = cbor.String(rule.Arn)
		}
		ruleState := cbor.Map{
			"RuleName": cbor.String(rule.Name),
		}
		if rule.State != "" {
			ruleState[keyState] = cbor.String(rule.State)
		}
		entry["RuleState"] = ruleState
		rules = append(rules, entry)
	}

	out := cbor.Map{
		"ManagedRules": rules,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

func (h *Handler) cborPutManagedInsightRules(input cbor.Map, c *echo.Context) error {
	var failures []InsightRuleFailure

	//nolint:nestif // nested CBOR decoding; structure mirrors the wire format
	if rulesRaw, ok := input["ManagedRules"]; ok {
		if rulesList, isList := rulesRaw.(cbor.List); isList {
			for _, ruleRaw := range rulesList {
				rule, isMap := ruleRaw.(cbor.Map)
				if !isMap {
					continue
				}
				ruleName := cborStr(rule, "RuleName")
				if ruleName == "" {
					continue
				}

				if err := h.Backend.PutInsightRule(&InsightRule{
					Name:        ruleName,
					State:       insightRuleStateEnabled,
					Definition:  cborStr(rule, "TemplateName"),
					Arn:         cborStr(rule, "ResourceARN"),
					ManagedRule: true,
				}); err != nil {
					failures = append(failures, InsightRuleFailure{
						RuleName:           ruleName,
						FailureCode:        "InternalFailure",
						FailureDescription: err.Error(),
					})
				}
			}
		}
	}

	return writeCBOR(c, buildInsightRuleFailureCBOR(failures))
}

func (h *Handler) cborDescribeAlarmContributors(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	if alarmName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}
	nextToken := cborStr(input, "NextToken")

	p, err := h.Backend.DescribeAlarmContributors(alarmName, nextToken)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	contributors := make(cbor.List, 0, len(p.Data))
	for _, contrib := range p.Data {
		keys := make(cbor.List, 0, len(contrib.Keys))
		for _, k := range contrib.Keys {
			keys = append(keys, cbor.String(k))
		}
		contributors = append(contributors, cbor.Map{
			"Keys":  keys,
			statSum: cbor.Float64(contrib.Sum),
		})
	}

	out := cbor.Map{
		"Contributors": contributors,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}

// cborStringSlice converts a cbor.Value to a []string slice.
func cborStringSlice(v cbor.Value) []string {
	list, isList := v.(cbor.List)
	if !isList {
		return nil
	}
	result := make([]string, 0, len(list))
	for _, item := range list {
		if s, isStr := item.(cbor.String); isStr {
			result = append(result, string(s))
		}
	}

	return result
}

// buildInsightRuleFailureCBOR builds a CBOR map for insight rule failure responses.
func buildInsightRuleFailureCBOR(failures []InsightRuleFailure) cbor.Map {
	failList := make(cbor.List, 0, len(failures))
	for _, f := range failures {
		failList = append(failList, cbor.Map{
			"RuleName":           cbor.String(f.RuleName),
			"FailureCode":        cbor.String(f.FailureCode),
			"FailureDescription": cbor.String(f.FailureDescription),
		})
	}

	return cbor.Map{
		"Failures": failList,
	}
}
