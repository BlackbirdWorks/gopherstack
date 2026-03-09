package cloudwatch

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const cloudwatchNS = "http://monitoring.amazonaws.com/doc/2010-08-01/"

// Handler is the Echo HTTP service handler for CloudWatch operations.
type Handler struct {
	Backend StorageBackend
	tags    map[string]*tags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new CloudWatch handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*tags.Tags),
		tagsMu:  lockmetrics.New("cloudwatch.tags"),
	}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("cloudwatch." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
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
		"PutCompositeAlarm",
		"DescribeAlarms",
		"DescribeAlarmsForMetric",
		"DescribeAlarmHistory",
		"DeleteAlarms",
		"SetAlarmState",
		"EnableAlarmActions",
		"DisableAlarmActions",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "monitoring" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CloudWatch instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

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

		body, err := httputils.ReadBody(r)
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

	return h.dispatchFormAction(action, r.Form, c)
}

// dispatchFormAction routes a form-encoded action to the appropriate handler.
func (h *Handler) dispatchFormAction(action string, form url.Values, c *echo.Context) error {
	switch action {
	case "PutMetricData":
		return h.handlePutMetricData(form, c)
	case "GetMetricStatistics":
		return h.handleGetMetricStatistics(form, c)
	case "GetMetricData":
		return h.handleGetMetricData(form, c)
	case "ListMetrics":
		return h.handleListMetrics(form, c)
	case "ListTagsForResource":
		return h.handleListTagsForResource(form, c)
	case "TagResource":
		return h.handleTagResource(form, c)
	case "UntagResource":
		return h.handleUntagResource(form, c)
	default:
		return h.dispatchAlarmFormAction(action, form, c)
	}
}

// dispatchAlarmFormAction routes alarm-specific form-encoded actions.
func (h *Handler) dispatchAlarmFormAction(action string, form url.Values, c *echo.Context) error {
	switch action {
	case "PutMetricAlarm":
		return h.handlePutMetricAlarm(form, c)
	case "PutCompositeAlarm":
		return h.handlePutCompositeAlarm(form, c)
	case "DescribeAlarms":
		return h.handleDescribeAlarms(form, c)
	case "DescribeAlarmsForMetric":
		return h.handleDescribeAlarmsForMetric(form, c)
	case "DescribeAlarmHistory":
		return h.handleDescribeAlarmHistory(form, c)
	case "DeleteAlarms":
		return h.handleDeleteAlarms(form, c)
	case "SetAlarmState":
		return h.handleSetAlarmState(form, c)
	case "EnableAlarmActions":
		return h.handleEnableAlarmActions(form, c)
	case "DisableAlarmActions":
		return h.handleDisableAlarmActions(form, c)
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
		XMLName   xml.Name   `xml:"ListTagsForResourceResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Tags      []xmlCWTag `xml:"ListTagsForResourceResult>Tags>member"`
	}
	resp := listTagsForResourceResp{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
	}
	for k, v := range tags {
		resp.Tags = append(resp.Tags, xmlCWTag{Key: k, Value: v})
	}

	return writeXML(c, resp)
}

func (h *Handler) handleTagResource(form url.Values, c *echo.Context) error {
	arn := form.Get("ResourceARN")
	newTags := parseCWTagsFromForm(form)
	h.setTags(arn, newTags)

	type tagResourceResp struct {
		XMLName   xml.Name `xml:"TagResourceResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, tagResourceResp{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleUntagResource(form url.Values, c *echo.Context) error {
	arn := form.Get("ResourceARN")
	keys := parseCWTagKeysFromForm(form)
	h.removeTags(arn, keys)

	type untagResourceResp struct {
		XMLName   xml.Name `xml:"UntagResourceResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, untagResourceResp{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
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
			return data
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
}

// parseMemberList parses form values like "Prefix.member.1", "Prefix.member.2", ...
func parseMemberList(form url.Values, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("%smember.%d", prefix, i))
		if v == "" {
			return result
		}
		result = append(result, v)
	}
}

// parseCWTagsFromForm reads Tags.member.N.Key/Value pairs from the form.
func parseCWTagsFromForm(form url.Values) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			return tags
		}
		tags[k] = form.Get(fmt.Sprintf("Tags.member.%d.Value", i))
	}
}

// parseCWTagKeysFromForm reads TagKeys.member.N values from the form.
func parseCWTagKeysFromForm(form url.Values) []string {
	var keys []string
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}

// parseMetricDataQueriesFromForm reads MetricDataQueries.member.N.* values from the form.
func parseMetricDataQueriesFromForm(form url.Values) []MetricDataQuery {
	var queries []MetricDataQuery
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("MetricDataQueries.member.%d.", i)
		id := form.Get(prefix + "Id")
		if id == "" {
			return queries
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
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.ListMetrics(namespace, metricName, nextToken, maxResults)
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
	members := make([]metricXML, 0, len(p.Data))
	for _, m := range p.Data {
		dims := make([]dimXML, 0, len(m.Dimensions))
		for _, d := range m.Dimensions {
			dims = append(dims, dimXML(d))
		}
		members = append(members, metricXML{Namespace: m.Namespace, MetricName: m.MetricName, Dimensions: dims})
	}

	type listResult struct {
		NextToken string      `xml:"NextToken,omitempty"`
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
		Result:    listResult{Metrics: members, NextToken: p.Next},
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
	actionsEnabled := form.Get("ActionsEnabled") != "false"

	alarm := &MetricAlarm{
		AlarmName:               alarmName,
		Namespace:               form.Get("Namespace"),
		MetricName:              form.Get("MetricName"),
		ComparisonOperator:      form.Get("ComparisonOperator"),
		Statistic:               form.Get("Statistic"),
		AlarmDescription:        form.Get("AlarmDescription"),
		Threshold:               threshold,
		EvaluationPeriods:       int32(evalPeriods),
		Period:                  int32(period),
		ActionsEnabled:          actionsEnabled,
		AlarmActions:            parseMemberList(form, "AlarmActions."),
		OKActions:               parseMemberList(form, "OKActions."),
		InsufficientDataActions: parseMemberList(form, "InsufficientDataActions."),
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

// metricAlarmToXML converts a MetricAlarm to its XML representation.
func metricAlarmToXML(a MetricAlarm) metricAlarmXML {
	return metricAlarmXML{
		AlarmName:               a.AlarmName,
		AlarmArn:                a.AlarmArn,
		Namespace:               a.Namespace,
		MetricName:              a.MetricName,
		ComparisonOperator:      a.ComparisonOperator,
		EvaluationPeriods:       a.EvaluationPeriods,
		Period:                  a.Period,
		Statistic:               a.Statistic,
		Threshold:               a.Threshold,
		StateValue:              a.StateValue,
		StateReason:             a.StateReason,
		AlarmDescription:        a.AlarmDescription,
		AlarmActions:            a.AlarmActions,
		OKActions:               a.OKActions,
		InsufficientDataActions: a.InsufficientDataActions,
		ActionsEnabled:          a.ActionsEnabled,
	}
}

// compositeAlarmToXML converts a CompositeAlarm to its XML representation.
func compositeAlarmToXML(a CompositeAlarm) compositeAlarmXMLType {
	return compositeAlarmXMLType{
		AlarmName:               a.AlarmName,
		AlarmArn:                a.AlarmArn,
		AlarmRule:               a.AlarmRule,
		StateValue:              a.StateValue,
		StateReason:             a.StateReason,
		AlarmDescription:        a.AlarmDescription,
		AlarmActions:            a.AlarmActions,
		OKActions:               a.OKActions,
		InsufficientDataActions: a.InsufficientDataActions,
		ActionsEnabled:          a.ActionsEnabled,
	}
}

// metricAlarmXML is the XML representation of a MetricAlarm.
type metricAlarmXML struct {
	AlarmDescription        string   `xml:"AlarmDescription,omitempty"`
	Namespace               string   `xml:"Namespace"`
	MetricName              string   `xml:"MetricName"`
	ComparisonOperator      string   `xml:"ComparisonOperator"`
	Statistic               string   `xml:"Statistic"`
	AlarmArn                string   `xml:"AlarmArn"`
	StateValue              string   `xml:"StateValue"`
	AlarmName               string   `xml:"AlarmName"`
	StateReason             string   `xml:"StateReason,omitempty"`
	AlarmActions            []string `xml:"AlarmActions>member,omitempty"`
	InsufficientDataActions []string `xml:"InsufficientDataActions>member,omitempty"`
	OKActions               []string `xml:"OKActions>member,omitempty"`
	Threshold               float64  `xml:"Threshold"`
	Period                  int32    `xml:"Period"`
	EvaluationPeriods       int32    `xml:"EvaluationPeriods"`
	ActionsEnabled          bool     `xml:"ActionsEnabled"`
}

// compositeAlarmXMLType is the XML representation of a CompositeAlarm.
type compositeAlarmXMLType struct {
	AlarmName               string   `xml:"AlarmName"`
	AlarmArn                string   `xml:"AlarmArn"`
	AlarmRule               string   `xml:"AlarmRule"`
	StateValue              string   `xml:"StateValue"`
	StateReason             string   `xml:"StateReason,omitempty"`
	AlarmDescription        string   `xml:"AlarmDescription,omitempty"`
	AlarmActions            []string `xml:"AlarmActions>member,omitempty"`
	OKActions               []string `xml:"OKActions>member,omitempty"`
	InsufficientDataActions []string `xml:"InsufficientDataActions>member,omitempty"`
	ActionsEnabled          bool     `xml:"ActionsEnabled"`
}

func (h *Handler) handleDescribeAlarms(form url.Values, c *echo.Context) error {
	alarmNames := parseMemberList(form, "AlarmNames.")
	alarmTypes := parseMemberList(form, "AlarmTypes.")
	stateValue := form.Get("StateValue")
	nextToken := form.Get("NextToken")
	maxRecords, _ := strconv.Atoi(form.Get("MaxRecords"))

	metricPage, compositePage, err := h.Backend.DescribeAlarms(
		alarmNames,
		alarmTypes,
		stateValue,
		nextToken,
		maxRecords,
	)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	metricMembers := make([]metricAlarmXML, 0, len(metricPage.Data))
	for _, a := range metricPage.Data {
		metricMembers = append(metricMembers, metricAlarmToXML(a))
	}

	compositeMembers := make([]compositeAlarmXMLType, 0, len(compositePage.Data))
	for _, a := range compositePage.Data {
		compositeMembers = append(compositeMembers, compositeAlarmToXML(a))
	}

	nextTok := metricPage.Next
	if nextTok == "" {
		nextTok = compositePage.Next
	}

	type descResult struct {
		NextToken       string                  `xml:"NextToken,omitempty"`
		MetricAlarms    []metricAlarmXML        `xml:"MetricAlarms>member"`
		CompositeAlarms []compositeAlarmXMLType `xml:"CompositeAlarms>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeAlarmsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeAlarmsResult"`
	}

	return writeXML(c, response{
		Xmlns: cloudwatchNS,
		Result: descResult{
			MetricAlarms:    metricMembers,
			CompositeAlarms: compositeMembers,
			NextToken:       nextTok,
		},
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

	queries := parseMetricDataQueriesFromForm(form)

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

func (h *Handler) handlePutCompositeAlarm(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	if alarmName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmName is required")
	}
	alarmRule := form.Get("AlarmRule")
	if alarmRule == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmRule is required")
	}

	actionsEnabled := form.Get("ActionsEnabled") != "false"

	alarm := &CompositeAlarm{
		AlarmName:               alarmName,
		AlarmRule:               alarmRule,
		AlarmDescription:        form.Get("AlarmDescription"),
		ActionsEnabled:          actionsEnabled,
		AlarmActions:            parseMemberList(form, "AlarmActions."),
		OKActions:               parseMemberList(form, "OKActions."),
		InsufficientDataActions: parseMemberList(form, "InsufficientDataActions."),
	}
	if err := h.Backend.PutCompositeAlarm(alarm); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutCompositeAlarmResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeAlarmsForMetric(form url.Values, c *echo.Context) error {
	namespace := form.Get("Namespace")
	metricName := form.Get("MetricName")
	alarmNames := parseMemberList(form, "AlarmNames.")
	nextToken := form.Get("NextToken")
	maxRecords, _ := strconv.Atoi(form.Get("MaxRecords"))

	p, err := h.Backend.DescribeAlarmsForMetric(namespace, metricName, alarmNames, nextToken, maxRecords)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	members := make([]metricAlarmXML, 0, len(p.Data))
	for _, a := range p.Data {
		members = append(members, metricAlarmToXML(a))
	}

	type descResult struct {
		NextToken    string           `xml:"NextToken,omitempty"`
		MetricAlarms []metricAlarmXML `xml:"MetricAlarms>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeAlarmsForMetricResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeAlarmsForMetricResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		Result:    descResult{MetricAlarms: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeAlarmHistory(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	historyItemType := form.Get("HistoryItemType")
	nextToken := form.Get("NextToken")
	maxRecords, _ := strconv.Atoi(form.Get("MaxRecords"))

	var startDate, endDate time.Time
	if s := form.Get("StartDate"); s != "" {
		startDate, _ = time.Parse(time.RFC3339, s)
	}
	if e := form.Get("EndDate"); e != "" {
		endDate, _ = time.Parse(time.RFC3339, e)
	}

	p, err := h.Backend.DescribeAlarmHistory(alarmName, historyItemType, nextToken, startDate, endDate, maxRecords)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type historyItemXML struct {
		AlarmName       string `xml:"AlarmName"`
		HistoryItemType string `xml:"HistoryItemType"`
		HistorySummary  string `xml:"HistorySummary"`
		HistoryData     string `xml:"HistoryData,omitempty"`
		Timestamp       string `xml:"Timestamp"`
	}
	members := make([]historyItemXML, 0, len(p.Data))
	for _, item := range p.Data {
		members = append(members, historyItemXML{
			AlarmName:       item.AlarmName,
			HistoryItemType: item.HistoryItemType,
			HistorySummary:  item.HistorySummary,
			HistoryData:     item.HistoryData,
			Timestamp:       item.Timestamp.UTC().Format(time.RFC3339),
		})
	}

	type descResult struct {
		NextToken         string           `xml:"NextToken,omitempty"`
		AlarmHistoryItems []historyItemXML `xml:"AlarmHistoryItems>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeAlarmHistoryResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeAlarmHistoryResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		Result:    descResult{AlarmHistoryItems: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleSetAlarmState(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	if alarmName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmName is required")
	}
	stateValue := form.Get("StateValue")
	stateReason := form.Get("StateReason")

	if err := h.Backend.SetAlarmState(alarmName, stateValue, stateReason); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"SetAlarmStateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleEnableAlarmActions(form url.Values, c *echo.Context) error {
	alarmNames := parseMemberList(form, "AlarmNames.")
	if err := h.Backend.EnableAlarmActions(alarmNames); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"EnableAlarmActionsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDisableAlarmActions(form url.Values, c *echo.Context) error {
	alarmNames := parseMemberList(form, "AlarmNames.")
	if err := h.Backend.DisableAlarmActions(alarmNames); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DisableAlarmActionsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}
