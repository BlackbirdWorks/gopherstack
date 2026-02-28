package cloudwatch

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const cloudwatchNS = "http://monitoring.amazonaws.com/doc/2010-08-01/"

// Handler is the Echo HTTP service handler for CloudWatch operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
	tags    map[string]map[string]string
	tagsMu  sync.RWMutex
}

// NewHandler creates a new CloudWatch handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log, tags: make(map[string]map[string]string)}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = make(map[string]string)
	}
	maps.Copy(h.tags[resourceID], kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	for _, k := range keys {
		delete(h.tags[resourceID], k)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock()
	defer h.tagsMu.RUnlock()
	result := make(map[string]string)
	maps.Copy(result, h.tags[resourceID])

	return result
}

// Name returns the service name.
func (h *Handler) Name() string { return "CloudWatch" }

// GetSupportedOperations returns all mocked CloudWatch operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"PutMetricData",
		"GetMetricStatistics",
		"GetMetricData",
		"ListMetrics",
		"PutMetricAlarm",
		"DescribeAlarms",
		"DeleteAlarms",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// RouteMatcher returns a matcher for CloudWatch query-protocol requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}

		// Match rpc-v2-cbor requests (AWS SDK v2 ≥ cloudwatch@v1.55)
		if isCBORRequest(r) {
			op := extractCBOROperation(r.URL.Path)

			return slices.Contains(h.GetSupportedOperations(), op)
		}

		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}

		body, err := httputil.ReadBody(r)
		if err != nil {
			return false
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		action := vals.Get("Action")

		return slices.Contains(h.GetSupportedOperations(), action)
	}
}

const cloudwatchMatchPriority = 80

// MatchPriority returns the routing priority for the CloudWatch handler.
func (h *Handler) MatchPriority() int { return cloudwatchMatchPriority }

// ExtractOperation extracts the operation name from the Action form field or CBOR path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()

	if isCBORRequest(r) {
		return extractCBOROperation(r.URL.Path)
	}

	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("Action")
}

// ExtractResource extracts the resource name (Namespace) from the form.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("Namespace")
}

// Handler returns the Echo handler function for CloudWatch requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()

		// Route rpc-v2-cbor requests (AWS SDK v2 ≥ cloudwatch@v1.55)
		if isCBORRequest(r) {
			return h.handleCBOR(c)
		}

		return h.handleFormRequest(c, r)
	}
}

// handleFormRequest handles the query-protocol (form-encoded) path for CloudWatch requests.
func (h *Handler) handleFormRequest(c *echo.Context, r *http.Request) error {
	// ParseForm is idempotent; RouteMatcher may have already called it.
	if err := r.ParseForm(); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "cannot parse form body")
	}
	action := r.Form.Get("Action")
	c.Response().Header().Set("Content-Type", "text/xml")

	switch action {
	case "PutMetricData":
		return h.handlePutMetricData(r.Form, c)
	case "GetMetricStatistics":
		return h.handleGetMetricStatistics(r.Form, c)
	case "GetMetricData":
		return h.handleGetMetricData(r.Form, c)
	case "ListMetrics":
		return h.handleListMetrics(r.Form, c)
	case "PutMetricAlarm":
		return h.handlePutMetricAlarm(r.Form, c)
	case "DescribeAlarms":
		return h.handleDescribeAlarms(r.Form, c)
	case "DeleteAlarms":
		return h.handleDeleteAlarms(r.Form, c)
	case "ListTagsForResource":
		return h.handleListTagsForResource(r.Form, c)
	case "TagResource":
		return h.handleTagResource(r.Form, c)
	case "UntagResource":
		return h.handleUntagResource(r.Form, c)
	default:
		return h.xmlError(c, http.StatusBadRequest, "InvalidAction", "unknown action: "+action)
	}
}

func (h *Handler) handleListTagsForResource(form url.Values, c *echo.Context) error {
	arn := form.Get("ResourceARN")
	tags := h.getTags(arn)
	type xmlCWTag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type listTagsForResourceResp struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		Result  struct {
			XMLName xml.Name   `xml:"ListTagsForResourceResult"`
			Tags    []xmlCWTag `xml:"Tags>member"`
		} `xml:"ListTagsForResourceResult"`
	}
	var resp listTagsForResourceResp
	for k, v := range tags {
		resp.Result.Tags = append(resp.Result.Tags, xmlCWTag{Key: k, Value: v})
	}

	return writeXML(c, resp)
}

func (h *Handler) handleTagResource(form url.Values, c *echo.Context) error {
	arn := form.Get("ResourceARN")
	newTags := make(map[string]string)

	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			break
		}

		newTags[k] = form.Get(fmt.Sprintf("Tags.member.%d.Value", i))
	}

	h.setTags(arn, newTags)

	return writeXML(c, struct {
		XMLName xml.Name `xml:"TagResourceResponse"`
	}{})
}

func (h *Handler) handleUntagResource(form url.Values, c *echo.Context) error {
	arn := form.Get("ResourceARN")

	var keys []string

	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			break
		}

		keys = append(keys, k)
	}

	h.removeTags(arn, keys)

	return writeXML(c, struct {
		XMLName xml.Name `xml:"UntagResourceResponse"`
	}{})
}

// xmlError writes an XML error response.
func (h *Handler) xmlError(c *echo.Context, status int, code, message string) error {
	type xmlErrorBody struct {
		XMLName   xml.Name `xml:"ErrorResponse"`
		Code      string   `xml:"Error>Code"`
		Message   string   `xml:"Error>Message"`
		RequestID string   `xml:"RequestId"`
	}
	w := c.Response()
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	enc := xml.NewEncoder(w)
	_ = enc.Encode(xmlErrorBody{Code: code, Message: message, RequestID: uuid.New().String()})

	return nil
}

// writeXML encodes v as XML to the response with HTTP 200.
func writeXML(c *echo.Context, v any) error {
	w := c.Response()
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`); err != nil {
		return err
	}

	return xml.NewEncoder(w).Encode(v)
}

// parseMetricDataFromForm parses MetricData.member.N.* form values.
func parseMetricDataFromForm(form url.Values) []MetricDatum {
	var data []MetricDatum
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("MetricData.member.%d.", i)
		name := form.Get(prefix + "MetricName")
		if name == "" {
			break
		}
		val, _ := strconv.ParseFloat(form.Get(prefix+"Value"), 64)
		unit := form.Get(prefix + "Unit")
		data = append(data, MetricDatum{
			MetricName: name,
			Value:      val,
			Unit:       unit,
			Timestamp:  time.Now(),
			Count:      1,
			Sum:        val,
			Min:        val,
			Max:        val,
		})
	}

	return data
}

// parseMemberList parses form values like "Prefix.member.1", "Prefix.member.2", ...
func parseMemberList(form url.Values, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("%smember.%d", prefix, i))
		if v == "" {
			break
		}
		result = append(result, v)
	}

	return result
}

func (h *Handler) handlePutMetricData(form url.Values, c *echo.Context) error {
	namespace := form.Get("Namespace")
	if namespace == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "Namespace is required")
	}
	data := parseMetricDataFromForm(form)
	if err := h.Backend.PutMetricData(namespace, data); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutMetricDataResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetMetricStatistics(form url.Values, c *echo.Context) error {
	namespace := form.Get("Namespace")
	metricName := form.Get("MetricName")
	startStr := form.Get("StartTime")
	endStr := form.Get("EndTime")
	periodStr := form.Get("Period")

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "invalid StartTime")
	}
	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "invalid EndTime")
	}
	period, err := strconv.ParseInt(periodStr, 10, 32)
	if err != nil || period <= 0 {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "invalid Period")
	}

	statistics := parseMemberList(form, "Statistics.")
	dps, berr := h.Backend.GetMetricStatistics(namespace, metricName, startTime, endTime, int32(period), statistics)
	if berr != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", berr.Error())
	}

	return writeXML(c, buildGetMetricStatisticsResponse(metricName, dps))
}

func buildGetMetricStatisticsResponse(metricName string, dps []Datapoint) any {
	type dpXML struct {
		Average     *float64 `xml:"Average,omitempty"`
		Sum         *float64 `xml:"Sum,omitempty"`
		Minimum     *float64 `xml:"Minimum,omitempty"`
		Maximum     *float64 `xml:"Maximum,omitempty"`
		SampleCount *float64 `xml:"SampleCount,omitempty"`
		Timestamp   string   `xml:"Timestamp"`
		Unit        string   `xml:"Unit,omitempty"`
	}
	members := make([]dpXML, 0, len(dps))
	for _, dp := range dps {
		members = append(members, dpXML{
			Timestamp:   dp.Timestamp.UTC().Format(time.RFC3339),
			Unit:        dp.Unit,
			Average:     dp.Average,
			Sum:         dp.Sum,
			Minimum:     dp.Minimum,
			Maximum:     dp.Maximum,
			SampleCount: dp.SampleCount,
		})
	}
	type result struct {
		Label      string  `xml:"Label"`
		Datapoints []dpXML `xml:"Datapoints>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetMetricStatisticsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"GetMetricStatisticsResult"`
	}

	return response{
		Xmlns:     cloudwatchNS,
		Result:    result{Datapoints: members, Label: metricName},
		RequestID: uuid.New().String(),
	}
}

func (h *Handler) handleListMetrics(form url.Values, c *echo.Context) error {
	namespace := form.Get("Namespace")
	metricName := form.Get("MetricName")
	metrics, err := h.Backend.ListMetrics(namespace, metricName)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type dimXML struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	}
	type metricXML struct {
		Namespace  string   `xml:"Namespace"`
		MetricName string   `xml:"MetricName"`
		Dimensions []dimXML `xml:"Dimensions>member,omitempty"`
	}
	members := make([]metricXML, 0, len(metrics))
	for _, m := range metrics {
		dims := make([]dimXML, 0, len(m.Dimensions))
		for _, d := range m.Dimensions {
			dims = append(dims, dimXML(d))
		}
		members = append(members, metricXML{Namespace: m.Namespace, MetricName: m.MetricName, Dimensions: dims})
	}

	type listResult struct {
		NextToken string      `xml:"NextToken"`
		Metrics   []metricXML `xml:"Metrics>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListMetricsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListMetricsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		Result:    listResult{Metrics: members},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handlePutMetricAlarm(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	if alarmName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmName is required")
	}

	threshold, _ := strconv.ParseFloat(form.Get("Threshold"), 64)
	evalPeriods, _ := strconv.ParseInt(form.Get("EvaluationPeriods"), 10, 32)
	period, _ := strconv.ParseInt(form.Get("Period"), 10, 32)

	alarm := &MetricAlarm{
		AlarmName:          alarmName,
		Namespace:          form.Get("Namespace"),
		MetricName:         form.Get("MetricName"),
		ComparisonOperator: form.Get("ComparisonOperator"),
		Statistic:          form.Get("Statistic"),
		AlarmDescription:   form.Get("AlarmDescription"),
		Threshold:          threshold,
		EvaluationPeriods:  int32(evalPeriods),
		Period:             int32(period),
	}
	if err := h.Backend.PutMetricAlarm(alarm); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutMetricAlarmResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeAlarms(form url.Values, c *echo.Context) error {
	alarmNames := parseMemberList(form, "AlarmNames.")
	stateValue := form.Get("StateValue")

	alarms, err := h.Backend.DescribeAlarms(alarmNames, stateValue)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type alarmXML struct {
		AlarmName          string  `xml:"AlarmName"`
		AlarmArn           string  `xml:"AlarmArn"`
		Namespace          string  `xml:"Namespace"`
		MetricName         string  `xml:"MetricName"`
		ComparisonOperator string  `xml:"ComparisonOperator"`
		Statistic          string  `xml:"Statistic"`
		StateValue         string  `xml:"StateValue"`
		StateReason        string  `xml:"StateReason,omitempty"`
		AlarmDescription   string  `xml:"AlarmDescription,omitempty"`
		Threshold          float64 `xml:"Threshold"`
		EvaluationPeriods  int32   `xml:"EvaluationPeriods"`
		Period             int32   `xml:"Period"`
	}
	members := make([]alarmXML, 0, len(alarms))
	for _, a := range alarms {
		members = append(members, alarmXML{
			AlarmName:          a.AlarmName,
			AlarmArn:           a.AlarmArn,
			Namespace:          a.Namespace,
			MetricName:         a.MetricName,
			ComparisonOperator: a.ComparisonOperator,
			EvaluationPeriods:  a.EvaluationPeriods,
			Period:             a.Period,
			Statistic:          a.Statistic,
			Threshold:          a.Threshold,
			StateValue:         a.StateValue,
			StateReason:        a.StateReason,
			AlarmDescription:   a.AlarmDescription,
		})
	}

	type descResult struct {
		MetricAlarms []alarmXML `xml:"MetricAlarms>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeAlarmsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeAlarmsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		Result:    descResult{MetricAlarms: members},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDeleteAlarms(form url.Values, c *echo.Context) error {
	alarmNames := parseMemberList(form, "AlarmNames.")
	if err := h.Backend.DeleteAlarms(alarmNames); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteAlarmsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetMetricData(form url.Values, c *echo.Context) error {
	startStr := form.Get("StartTime")
	endStr := form.Get("EndTime")

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		startTime = time.Now().UTC().Add(-time.Hour)
	}

	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		endTime = time.Now().UTC()
	}

	var queries []MetricDataQuery
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("MetricDataQueries.member.%d.", i)
		id := form.Get(prefix + "Id")
		if id == "" {
			break
		}

		period, _ := strconv.ParseInt(form.Get(prefix+"MetricStat.Period"), 10, 32)
		if period <= 0 {
			period = 60
		}

		queries = append(queries, MetricDataQuery{
			ID:    id,
			Label: form.Get(prefix + "Label"),
			MetricStat: MetricStat{
				Namespace:  form.Get(prefix + "MetricStat.Metric.Namespace"),
				MetricName: form.Get(prefix + "MetricStat.Metric.MetricName"),
				Stat:       form.Get(prefix + "MetricStat.Stat"),
				Period:     int32(period),
			},
		})
	}

	results, berr := h.Backend.GetMetricData(queries, startTime, endTime)
	if berr != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", berr.Error())
	}

	type resultEntry struct {
		XMLName    xml.Name  `xml:"member"`
		ID         string    `xml:"Id"`
		Label      string    `xml:"Label,omitempty"`
		StatusCode string    `xml:"StatusCode"`
		Timestamps []string  `xml:"Timestamps>member"`
		Values     []float64 `xml:"Values>member"`
	}

	type response struct {
		XMLName           xml.Name      `xml:"GetMetricDataResponse"`
		Xmlns             string        `xml:"xmlns,attr"`
		RequestID         string        `xml:"ResponseMetadata>RequestId"`
		MetricDataResults []resultEntry `xml:"GetMetricDataResult>MetricDataResults"`
	}

	resp := response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()}

	for _, r := range results {
		entry := resultEntry{
			ID:         r.ID,
			Label:      r.Label,
			StatusCode: r.StatusCode,
			Values:     r.Values,
		}
		for _, ts := range r.Timestamps {
			entry.Timestamps = append(entry.Timestamps, ts.UTC().Format(time.RFC3339))
		}

		resp.MetricDataResults = append(resp.MetricDataResults, entry)
	}

	return writeXML(c, resp)
}
