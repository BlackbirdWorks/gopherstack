package cloudwatch

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	opGetMetricWidgetImage    = "GetMetricWidgetImage"
	opListAlarmMuteRules      = "ListAlarmMuteRules"
	opListManagedInsightRules = "ListManagedInsightRules"
	opPutManagedInsightRules  = "PutManagedInsightRules"
	opStartMetricStreams      = "StartMetricStreams"
	opStopMetricStreams       = "StopMetricStreams"
)

const (
	opSetAlarmState       = "SetAlarmState"
	opUpdateAlarmMuteRule = "UpdateAlarmMuteRule"
	opUpdateInsightRule   = "UpdateInsightRule"
	opUpdateMetricStream  = "UpdateMetricStream"
	opTestMetricFilter    = "TestMetricFilter"
)

const (
	opPutMetricData        = "PutMetricData"
	opGetMetricStatistics  = "GetMetricStatistics"
	opGetMetricData        = "GetMetricData"
	opListMetrics          = "ListMetrics"
	opPutMetricAlarm       = "PutMetricAlarm"
	opPutCompositeAlarm    = "PutCompositeAlarm"
	opPutDashboard         = "PutDashboard"
	opListDashboards       = "ListDashboards"
	opPutAlarmMuteRule     = "PutAlarmMuteRule"
	opPutAnomalyDetector   = "PutAnomalyDetector"
	opPutInsightRule       = "PutInsightRule"
	opGetInsightRuleReport = "GetInsightRuleReport"
	opPutMetricStream      = "PutMetricStream"
	opListMetricStreams    = "ListMetricStreams"
	opGetMetricStream      = "GetMetricStream"
	opPutMetricFilter      = "PutMetricFilter"
)

const (
	opDescribeAlarms            = "DescribeAlarms"
	opDescribeAlarmsForMetric   = "DescribeAlarmsForMetric"
	opDescribeAlarmHistory      = "DescribeAlarmHistory"
	opDeleteAlarms              = "DeleteAlarms"
	opEnableAlarmActions        = "EnableAlarmActions"
	opDisableAlarmActions       = "DisableAlarmActions"
	opDeleteDashboards          = "DeleteDashboards"
	opDeleteAlarmMuteRule       = "DeleteAlarmMuteRule"
	opDeleteAnomalyDetector     = "DeleteAnomalyDetector"
	opDeleteInsightRules        = "DeleteInsightRules"
	opDeleteMetricStream        = "DeleteMetricStream"
	opDescribeMetricFilters     = "DescribeMetricFilters"
	opDeleteMetricFilter        = "DeleteMetricFilter"
	opDescribeAlarmContributors = "DescribeAlarmContributors"
	opDescribeAnomalyDetectors  = "DescribeAnomalyDetectors"
	opDescribeInsightRules      = "DescribeInsightRules"
	opDisableInsightRules       = "DisableInsightRules"
	opEnableInsightRules        = "EnableInsightRules"
	opGetAlarmMuteRule          = "GetAlarmMuteRule"
	opGetDashboard              = "GetDashboard"
)

const cloudwatchNS = "http://monitoring.amazonaws.com/doc/2010-08-01/"

// errCodeInternalFailure is the AWS error code for an unclassified server-side failure.
const errCodeInternalFailure = "InternalFailure"

// formFalse is the string value "false" as submitted in form-encoded CloudWatch requests.
const formFalse = "false"

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

// deleteResourceTags removes the entire tag entry for a resource ARN and closes
// the underlying Tags instance to deregister its Prometheus lockmetrics entry.
func (h *Handler) deleteResourceTags(resourceARN string) {
	h.tagsMu.Lock("deleteResourceTags")
	t := h.tags[resourceARN]
	delete(h.tags, resourceARN)
	h.tagsMu.Unlock()
	if t != nil {
		t.Close()
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

// StartWorker starts the background janitor for metric sweeping.
// It implements service.BackgroundWorker.
func (h *Handler) StartWorker(ctx context.Context) error {
	if cwBk, ok := h.Backend.(*InMemoryBackend); ok {
		janitor := NewJanitor(cwBk)
		go janitor.Run(ctx)
	}

	return nil
}

// GetSupportedOperations returns all mocked CloudWatch operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opPutMetricData,
		opGetMetricStatistics,
		opGetMetricData,
		opListMetrics,
		opPutMetricAlarm,
		opPutCompositeAlarm,
		opDescribeAlarms,
		opDescribeAlarmsForMetric,
		opDescribeAlarmHistory,
		opDeleteAlarms,
		opSetAlarmState,
		opEnableAlarmActions,
		opDisableAlarmActions,
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		opPutDashboard,
		opGetDashboard,
		opListDashboards,
		opDeleteDashboards,
		opPutAlarmMuteRule,
		opDeleteAlarmMuteRule,
		opUpdateAlarmMuteRule,
		opPutAnomalyDetector,
		opDeleteAnomalyDetector,
		opPutInsightRule,
		opDeleteInsightRules,
		opUpdateInsightRule,
		opGetInsightRuleReport,
		opPutMetricStream,
		opListMetricStreams,
		opGetMetricStream,
		opDeleteMetricStream,
		opUpdateMetricStream,
		opPutMetricFilter,
		opDescribeMetricFilters,
		opDeleteMetricFilter,
		opTestMetricFilter,
		opDescribeAlarmContributors,
		opDescribeAnomalyDetectors,
		opDescribeInsightRules,
		opDisableInsightRules,
		opEnableInsightRules,
		opGetAlarmMuteRule,
		opGetMetricWidgetImage,
		opListAlarmMuteRules,
		opListManagedInsightRules,
		opPutManagedInsightRules,
		opStartMetricStreams,
		opStopMetricStreams,
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

		if target := extractTargetOperation(r.Header.Get("X-Amz-Target")); target != "" {
			return slices.Contains(h.GetSupportedOperations(), target)
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

func extractTargetOperation(target string) string {
	if target == "" {
		return ""
	}

	parts := strings.Split(target, ".")
	if len(parts) == 0 {
		return ""
	}

	return parts[len(parts)-1]
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

	if target := extractTargetOperation(r.Header.Get("X-Amz-Target")); target != "" {
		return target
	}

	if err := r.ParseForm(); err != nil {
		body, readErr := httputils.ReadBody(r)
		if readErr != nil {
			return ""
		}

		vals, parseErr := url.ParseQuery(string(body))
		if parseErr != nil {
			return ""
		}

		return vals.Get("Action")
	}

	if action := r.Form.Get("Action"); action != "" {
		return action
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return ""
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	return vals.Get("Action")
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
	if handled, err := h.handleTargetRequest(c, r); handled {
		return err
	}

	// ParseForm is idempotent; RouteMatcher may have already called it.
	if err := r.ParseForm(); err != nil {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"cannot parse form body",
		)
	}
	action := r.Form.Get("Action")
	c.Response().Header().Set("Content-Type", "text/xml")

	return h.dispatchFormAction(action, r.Form, c)
}

func (h *Handler) handleTargetRequest(c *echo.Context, r *http.Request) (bool, error) {
	target := extractTargetOperation(r.Header.Get("X-Amz-Target"))
	if target == "" {
		return false, nil
	}

	input := cbor.Map{}
	body, err := httputils.ReadBody(r)
	if err != nil || len(body) == 0 {
		return true, h.dispatchCBOR(target, input, c)
	}

	val, decErr := cbor.Decode(body)
	if decErr != nil {
		return true, h.dispatchCBOR(target, input, c)
	}

	m, ok := val.(cbor.Map)
	if !ok {
		return true, h.dispatchCBOR(target, input, c)
	}

	return true, h.dispatchCBOR(target, m, c)
}

// dispatchFormAction routes a form-encoded action to the appropriate handler.
func (h *Handler) dispatchFormAction(action string, form url.Values, c *echo.Context) error {
	switch action {
	case opPutMetricData:
		return h.handlePutMetricData(form, c)
	case opGetMetricStatistics:
		return h.handleGetMetricStatistics(form, c)
	case opGetMetricData:
		return h.handleGetMetricData(form, c)
	case opListMetrics:
		return h.handleListMetrics(form, c)
	case "ListTagsForResource":
		return h.handleListTagsForResource(form, c)
	case "TagResource":
		return h.handleTagResource(form, c)
	case "UntagResource":
		return h.handleUntagResource(form, c)
	case opPutDashboard:
		return h.handlePutDashboard(form, c)
	case opGetDashboard:
		return h.handleGetDashboard(form, c)
	case opListDashboards:
		return h.handleListDashboards(form, c)
	case opDeleteDashboards:
		return h.handleDeleteDashboards(form, c)
	default:
		return h.dispatchExtendedFormAction(action, form, c)
	}
}

// dispatchExtendedFormAction routes extended CloudWatch actions added after the initial implementation.
func (h *Handler) dispatchExtendedFormAction(
	action string,
	form url.Values,
	c *echo.Context,
) error {
	if handled, err := h.dispatchAnomalyInsightFormAction(action, form, c); handled {
		return err
	}

	switch action {
	case opDeleteAlarmMuteRule:
		return h.handleDeleteAlarmMuteRule(form, c)
	case opGetAlarmMuteRule:
		return h.handleGetAlarmMuteRule(form, c)
	case opListMetricStreams:
		return h.handleListMetricStreams(form, c)
	case opGetMetricStream:
		return h.handleGetMetricStream(form, c)
	case opDeleteMetricStream:
		return h.handleDeleteMetricStream(form, c)
	case opPutMetricFilter:
		return h.handlePutMetricFilter(form, c)
	case opDescribeMetricFilters:
		return h.handleDescribeMetricFilters(form, c)
	case opDeleteMetricFilter:
		return h.handleDeleteMetricFilter(form, c)
	case opTestMetricFilter:
		return h.handleTestMetricFilter(form, c)
	case opDescribeAlarmContributors:
		return h.handleDescribeAlarmContributors(form, c)
	default:
		return h.dispatchResourceUpsertFormAction(action, form, c)
	}
}

// dispatchAnomalyInsightFormAction routes anomaly-detector and insight-rule form actions.
// Returns (true, err) when the action was handled, (false, nil) otherwise.
func (h *Handler) dispatchAnomalyInsightFormAction(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case opPutAnomalyDetector:
		return true, h.handlePutAnomalyDetector(form, c)
	case opDeleteAnomalyDetector:
		return true, h.handleDeleteAnomalyDetector(form, c)
	case opDescribeAnomalyDetectors:
		return true, h.handleDescribeAnomalyDetectors(form, c)
	case opDeleteInsightRules:
		return true, h.handleDeleteInsightRules(form, c)
	case opDescribeInsightRules:
		return true, h.handleDescribeInsightRules(form, c)
	case opDisableInsightRules:
		return true, h.handleDisableInsightRules(form, c)
	case opEnableInsightRules:
		return true, h.handleEnableInsightRules(form, c)
	case opGetInsightRuleReport:
		return true, h.handleGetInsightRuleReport(form, c)
	}

	return false, nil
}

func (h *Handler) dispatchResourceUpsertFormAction(
	action string,
	form url.Values,
	c *echo.Context,
) error {
	switch action {
	case opPutAlarmMuteRule:
		return h.handlePutAlarmMuteRule(form, c)
	case opUpdateAlarmMuteRule:
		return h.handleUpdateAlarmMuteRule(form, c)
	case opPutInsightRule:
		return h.handlePutInsightRule(form, c)
	case opUpdateInsightRule:
		return h.handleUpdateInsightRule(form, c)
	case opPutMetricStream:
		return h.handlePutMetricStream(form, c)
	case opUpdateMetricStream:
		return h.handleUpdateMetricStream(form, c)
	case opGetMetricWidgetImage:
		return h.handleGetMetricWidgetImage(form, c)
	case opListAlarmMuteRules:
		return h.handleListAlarmMuteRules(form, c)
	case opListManagedInsightRules:
		return h.handleListManagedInsightRules(form, c)
	case opPutManagedInsightRules:
		return h.handlePutManagedInsightRules(form, c)
	case opStartMetricStreams:
		return h.handleStartMetricStreams(form, c)
	case opStopMetricStreams:
		return h.handleStopMetricStreams(form, c)
	default:
		return h.dispatchAlarmFormAction(action, form, c)
	}
}

// dispatchAlarmFormAction routes alarm-specific form-encoded actions.
func (h *Handler) dispatchAlarmFormAction(action string, form url.Values, c *echo.Context) error {
	switch action {
	case opPutMetricAlarm:
		return h.handlePutMetricAlarm(form, c)
	case opPutCompositeAlarm:
		return h.handlePutCompositeAlarm(form, c)
	case opDescribeAlarms:
		return h.handleDescribeAlarms(form, c)
	case opDescribeAlarmsForMetric:
		return h.handleDescribeAlarmsForMetric(form, c)
	case opDescribeAlarmHistory:
		return h.handleDescribeAlarmHistory(form, c)
	case opDeleteAlarms:
		return h.handleDeleteAlarms(form, c)
	case opSetAlarmState:
		return h.handleSetAlarmState(form, c)
	case opEnableAlarmActions:
		return h.handleEnableAlarmActions(form, c)
	case opDisableAlarmActions:
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
// Supports the Value field, the StatisticSet (pre-aggregated) path, and the
// Values/Counts array path. Also parses Dimensions and StorageResolution.
// Each datum's Has* flags record which shape (if any) was present on the wire;
// validateMetricDatum uses those flags to reject ambiguous combinations rather
// than guessing from zero values.
func parseMetricDataFromForm(form url.Values) []MetricDatum {
	var data []MetricDatum
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("MetricData.member.%d.", i)
		name := form.Get(prefix + "MetricName")
		if name == "" {
			return data
		}

		datum := parseMetricDatumBaseFields(form, prefix, name)
		applyMetricDatumShape(form, prefix, &datum)
		data = append(data, datum)
	}
}

// parseMetricDatumBaseFields parses the shape-independent fields of a single
// MetricData.member.N entry: MetricName, Unit, Timestamp, StorageResolution,
// and Dimensions.
func parseMetricDatumBaseFields(form url.Values, prefix, name string) MetricDatum {
	unit := form.Get(prefix + "Unit")

	// Parse optional Timestamp (fall back to now).
	ts := time.Now().UTC()
	if tsStr := form.Get(prefix + "Timestamp"); tsStr != "" {
		if t, err := time.Parse(time.RFC3339, tsStr); err == nil {
			ts = t.UTC()
		}
	}

	storageRes, _ := strconv.ParseInt(form.Get(prefix+"StorageResolution"), 10, 32)
	dims := parseDimensionsFromForm(form, prefix+"Dimensions.")

	return MetricDatum{
		MetricName:        name,
		Unit:              unit,
		Timestamp:         ts,
		Dimensions:        dims,
		StorageResolution: int32(storageRes),
	}
}

// applyMetricDatumShape parses whichever of the three mutually-exclusive
// PutMetricData input shapes (Values/Counts, StatisticValues, or Value) is
// present in the form for this entry and populates datum accordingly.
func applyMetricDatumShape(form url.Values, prefix string, datum *MetricDatum) {
	if values, hasValues := parseFloatMemberList(form, prefix+"Values."); hasValues {
		applyValuesArrayShape(form, prefix, datum, values)

		return
	}

	if ssCount := form.Get(prefix + "StatisticValues.SampleCount"); ssCount != "" {
		applyStatisticSetShape(form, prefix, datum, ssCount)

		return
	}

	rawValue := form.Get(prefix + "Value")
	val, _ := strconv.ParseFloat(rawValue, 64)
	datum.HasValue = rawValue != ""
	datum.Value = val
	datum.Count = 1
	datum.Sum = val
	datum.Min = val
	datum.Max = val
}

// applyValuesArrayShape populates datum from a Values/Counts array pair,
// defaulting Counts to all-1s when the caller omits it. The backend
// (storeDatum) derives Sum/SampleCount/Min/Max from Values/Counts at store
// time, so this only needs to carry the raw arrays through.
func applyValuesArrayShape(form url.Values, prefix string, datum *MetricDatum, values []float64) {
	counts, hasCounts := parseFloatMemberList(form, prefix+"Counts.")
	if !hasCounts {
		counts = make([]float64, len(values))
		for j := range counts {
			counts[j] = 1
		}
	}

	datum.HasValuesArray = true
	datum.Values = values
	datum.Counts = counts
}

// applyStatisticSetShape populates datum from a StatisticValues input. A
// caller may also (invalidly) supply Value alongside it; that is preserved so
// validateMetricDatum can reject the combination.
func applyStatisticSetShape(form url.Values, prefix string, datum *MetricDatum, ssCount string) {
	count, _ := strconv.ParseFloat(ssCount, 64)
	sum, _ := strconv.ParseFloat(form.Get(prefix+"StatisticValues.Sum"), 64)
	minimum, _ := strconv.ParseFloat(form.Get(prefix+"StatisticValues.Minimum"), 64)
	maximum, _ := strconv.ParseFloat(form.Get(prefix+"StatisticValues.Maximum"), 64)
	datum.HasStatisticSet = true
	datum.Count = count
	datum.Sum = sum
	datum.Min = minimum
	datum.Max = maximum

	if rawValue := form.Get(prefix + "Value"); rawValue != "" {
		datum.HasValue = true
		datum.Value, _ = strconv.ParseFloat(rawValue, 64)
	}
}

// parseFloatMemberList parses a "Prefix.member.N" list of floats (e.g.
// "MetricData.member.1.Values.member.1"). Returns ok=false when no members are
// present so callers can distinguish an absent list from an explicit empty one.
func parseFloatMemberList(form url.Values, prefix string) ([]float64, bool) {
	var vals []float64

	for i := 1; ; i++ {
		raw := form.Get(fmt.Sprintf("%smember.%d", prefix, i))
		if raw == "" {
			return vals, len(vals) > 0
		}

		f, _ := strconv.ParseFloat(raw, 64)
		vals = append(vals, f)
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

// parseDimensionsFromForm reads Dimensions.member.N.Name/Value pairs from the form.
// The prefix argument should be e.g. "Dimensions." so keys are "Dimensions.member.N.Name".
func parseDimensionsFromForm(form url.Values, prefix string) []Dimension {
	var dims []Dimension
	for i := 1; ; i++ {
		name := form.Get(fmt.Sprintf("%smember.%d.Name", prefix, i))
		if name == "" {
			return dims
		}
		dims = append(
			dims,
			Dimension{Name: name, Value: form.Get(fmt.Sprintf("%smember.%d.Value", prefix, i))},
		)
	}
}

// parseMetricTransformationsFromForm reads MetricTransformations.member.N.* from the form.
func parseMetricTransformationsFromForm(form url.Values) []MetricTransformation {
	var transformations []MetricTransformation
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("MetricTransformations.member.%d.", i)
		name := form.Get(prefix + "MetricName")
		if name == "" {
			return transformations
		}
		defaultValue, _ := strconv.ParseFloat(form.Get(prefix+"DefaultValue"), 64)
		transformations = append(transformations, MetricTransformation{
			MetricName:      name,
			MetricNamespace: form.Get(prefix + "MetricNamespace"),
			MetricValue:     form.Get(prefix + "MetricValue"),
			DefaultValue:    defaultValue,
			Unit:            form.Get(prefix + "Unit"),
		})
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

		// ReturnData defaults to true; only set false when caller passes "false".
		returnData := form.Get(prefix+"ReturnData") != formFalse

		dims := parseDimensionsFromForm(form, prefix+"MetricStat.Metric.Dimensions.")

		queries = append(queries, MetricDataQuery{
			ID:         id,
			Label:      form.Get(prefix + "Label"),
			Expression: form.Get(prefix + "Expression"),
			AccountID:  form.Get(prefix + "AccountId"),
			ReturnData: returnData,
			MetricStat: MetricStat{
				Namespace:  form.Get(prefix + "MetricStat.Metric.Namespace"),
				MetricName: form.Get(prefix + "MetricStat.Metric.MetricName"),
				Stat:       form.Get(prefix + "MetricStat.Stat"),
				Period:     int32(period),
				Dimensions: dims,
			},
		})
	}
}

func (h *Handler) handlePutMetricData(form url.Values, c *echo.Context) error {
	namespace := form.Get("Namespace")
	if namespace == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace is required",
		)
	}
	data := parseMetricDataFromForm(form)
	if err := h.Backend.PutMetricData(namespace, data); err != nil {
		return h.xmlError(c, putMetricDataErrorStatus(err), putMetricDataErrorCode(err), err.Error())
	}

	// PutMetricDataOutput has no members besides the request ID: CloudWatch has
	// no partial-success concept for this operation, so a 200 always means every
	// datum in the request was accepted.
	type response struct {
		XMLName   xml.Name `xml:"PutMetricDataResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// putMetricDataErrorCode maps a PutMetricData validation error to its AWS error
// code. Order matters: more specific sentinels must be checked before the
// generic ErrValidation they may also match via errors.Is chains.
func putMetricDataErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrValueAndStatisticSet):
		return "InvalidParameterCombination"
	case errors.Is(err, ErrMetricSeriesLimitExceeded):
		return "LimitExceeded"
	case errors.Is(err, ErrValuesCountsLengthMismatch),
		errors.Is(err, ErrTooManyValues),
		errors.Is(err, ErrInvalidMetricValue),
		errors.Is(err, ErrValidation):
		return "InvalidParameterValue"
	default:
		return errCodeInternalFailure
	}
}

// putMetricDataErrorStatus maps a PutMetricData validation error to its HTTP status.
func putMetricDataErrorStatus(err error) int {
	if putMetricDataErrorCode(err) == errCodeInternalFailure {
		return http.StatusInternalServerError
	}

	return http.StatusBadRequest
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

	dimensions := parseDimensionsFromForm(form, "Dimensions.")
	statistics := parseMemberList(form, "Statistics.")
	extendedStatistics := parseMemberList(form, "ExtendedStatistics.")
	dps, berr := h.Backend.GetMetricStatistics(
		namespace,
		metricName,
		dimensions,
		startTime,
		endTime,
		int32(period),
		statistics,
		extendedStatistics,
	)
	if berr != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", berr.Error())
	}

	return writeXML(c, buildGetMetricStatisticsResponse(metricName, dps))
}

func buildGetMetricStatisticsResponse(metricName string, dps []Datapoint) any {
	type extStatXML struct {
		Key   string  `xml:"Name"`
		Value float64 `xml:"Value"`
	}
	type dpXML struct {
		Average            *float64     `xml:"Average,omitempty"`
		Sum                *float64     `xml:"Sum,omitempty"`
		Minimum            *float64     `xml:"Minimum,omitempty"`
		Maximum            *float64     `xml:"Maximum,omitempty"`
		SampleCount        *float64     `xml:"SampleCount,omitempty"`
		Timestamp          string       `xml:"Timestamp"`
		Unit               string       `xml:"Unit,omitempty"`
		ExtendedStatistics []extStatXML `xml:"ExtendedStatistics>member,omitempty"`
	}
	members := make([]dpXML, 0, len(dps))
	for _, dp := range dps {
		d := dpXML{
			Timestamp:   dp.Timestamp.UTC().Format(time.RFC3339),
			Unit:        dp.Unit,
			Average:     dp.Average,
			Sum:         dp.Sum,
			Minimum:     dp.Minimum,
			Maximum:     dp.Maximum,
			SampleCount: dp.SampleCount,
		}
		for k, v := range dp.ExtendedStatistics {
			d.ExtendedStatistics = append(d.ExtendedStatistics, extStatXML{Key: k, Value: v})
		}
		members = append(members, d)
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
	recentlyActive := form.Get("RecentlyActive")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))
	dimensions := parseDimensionsFromForm(form, "Dimensions.")

	p, err := h.Backend.ListMetrics(namespace, metricName, dimensions, recentlyActive, nextToken, maxResults)
	if err != nil {
		if errors.Is(err, ErrValidation) {
			return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

		return h.xmlError(c, http.StatusInternalServerError, errCodeInternalFailure, err.Error())
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
		members = append(
			members,
			metricXML{Namespace: m.Namespace, MetricName: m.MetricName, Dimensions: dims},
		)
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
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}

	threshold, _ := strconv.ParseFloat(form.Get("Threshold"), 64)
	evalPeriods, _ := strconv.ParseInt(form.Get("EvaluationPeriods"), 10, 32)
	datapointsToAlarm, _ := strconv.ParseInt(form.Get("DatapointsToAlarm"), 10, 32)
	period, _ := strconv.ParseInt(form.Get("Period"), 10, 32)
	actionsEnabled := form.Get("ActionsEnabled") != formFalse

	alarm := &MetricAlarm{
		AlarmName:               alarmName,
		Namespace:               form.Get("Namespace"),
		MetricName:              form.Get("MetricName"),
		ComparisonOperator:      form.Get("ComparisonOperator"),
		Statistic:               form.Get("Statistic"),
		ExtendedStatistic:       form.Get("ExtendedStatistic"),
		TreatMissingData:        form.Get("TreatMissingData"),
		AlarmDescription:        form.Get("AlarmDescription"),
		ThresholdMetricID:       form.Get("ThresholdMetricId"),
		Threshold:               threshold,
		EvaluationPeriods:       int32(evalPeriods),
		DatapointsToAlarm:       int32(datapointsToAlarm),
		Period:                  int32(period),
		ActionsEnabled:          actionsEnabled,
		AlarmActions:            parseMemberList(form, "AlarmActions."),
		OKActions:               parseMemberList(form, "OKActions."),
		InsufficientDataActions: parseMemberList(form, "InsufficientDataActions."),
		Dimensions:              parseDimensionsFromForm(form, "Dimensions."),
		Metrics:                 parseMetricDataQueriesFromForm(form),
	}
	if err := h.Backend.PutMetricAlarm(alarm); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

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
	x := metricAlarmXML{
		AlarmName:               a.AlarmName,
		AlarmArn:                a.AlarmArn,
		Namespace:               a.Namespace,
		MetricName:              a.MetricName,
		ComparisonOperator:      a.ComparisonOperator,
		EvaluationPeriods:       a.EvaluationPeriods,
		DatapointsToAlarm:       a.DatapointsToAlarm,
		Period:                  a.Period,
		Statistic:               a.Statistic,
		ExtendedStatistic:       a.ExtendedStatistic,
		TreatMissingData:        a.TreatMissingData,
		ThresholdMetricID:       a.ThresholdMetricID,
		Threshold:               a.Threshold,
		StateValue:              a.StateValue,
		StateReason:             a.StateReason,
		StateReasonData:         a.StateReasonData,
		AlarmDescription:        a.AlarmDescription,
		AlarmActions:            a.AlarmActions,
		OKActions:               a.OKActions,
		InsufficientDataActions: a.InsufficientDataActions,
		ActionsEnabled:          a.ActionsEnabled,
	}
	if !a.StateTransitionedTimestamp.IsZero() {
		x.StateTransitionedTimestamp = a.StateTransitionedTimestamp.UTC().Format(time.RFC3339)
	}
	if !a.AlarmConfigurationUpdatedTimestamp.IsZero() {
		x.AlarmConfigurationUpdatedTimestamp = a.AlarmConfigurationUpdatedTimestamp.UTC().
			Format(time.RFC3339)
	}
	for _, d := range a.Dimensions {
		x.Dimensions = append(x.Dimensions, struct {
			Name  string `xml:"Name"`
			Value string `xml:"Value"`
		}{Name: d.Name, Value: d.Value})
	}

	return x
}

// compositeAlarmToXML converts a CompositeAlarm to its XML representation.
func compositeAlarmToXML(a CompositeAlarm) compositeAlarmXMLType {
	x := compositeAlarmXMLType{
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
	if !a.StateTransitionedTimestamp.IsZero() {
		x.StateTransitionedTimestamp = a.StateTransitionedTimestamp.UTC().Format(time.RFC3339)
	}

	return x
}

// metricAlarmXML is the XML representation of a MetricAlarm.
type metricAlarmXML struct {
	AlarmConfigurationUpdatedTimestamp string   `xml:"AlarmConfigurationUpdatedTimestamp,omitempty"`
	StateTransitionedTimestamp         string   `xml:"StateTransitionedTimestamp,omitempty"`
	AlarmDescription                   string   `xml:"AlarmDescription,omitempty"`
	Namespace                          string   `xml:"Namespace,omitempty"`
	MetricName                         string   `xml:"MetricName,omitempty"`
	ComparisonOperator                 string   `xml:"ComparisonOperator"`
	Statistic                          string   `xml:"Statistic,omitempty"`
	ExtendedStatistic                  string   `xml:"ExtendedStatistic,omitempty"`
	TreatMissingData                   string   `xml:"TreatMissingData,omitempty"`
	ThresholdMetricID                  string   `xml:"ThresholdMetricId,omitempty"`
	AlarmArn                           string   `xml:"AlarmArn"`
	StateValue                         string   `xml:"StateValue"`
	AlarmName                          string   `xml:"AlarmName"`
	StateReason                        string   `xml:"StateReason,omitempty"`
	StateReasonData                    string   `xml:"StateReasonData,omitempty"`
	AlarmActions                       []string `xml:"AlarmActions>member,omitempty"`
	InsufficientDataActions            []string `xml:"InsufficientDataActions>member,omitempty"`
	OKActions                          []string `xml:"OKActions>member,omitempty"`
	Dimensions                         []struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	} `xml:"Dimensions>member,omitempty"`
	Threshold         float64 `xml:"Threshold"`
	Period            int32   `xml:"Period,omitempty"`
	EvaluationPeriods int32   `xml:"EvaluationPeriods"`
	DatapointsToAlarm int32   `xml:"DatapointsToAlarm,omitempty"`
	ActionsEnabled    bool    `xml:"ActionsEnabled"`
}

// compositeAlarmXMLType is the XML representation of a CompositeAlarm.
type compositeAlarmXMLType struct {
	StateTransitionedTimestamp string   `xml:"StateTransitionedTimestamp,omitempty"`
	AlarmName                  string   `xml:"AlarmName"`
	AlarmArn                   string   `xml:"AlarmArn"`
	AlarmRule                  string   `xml:"AlarmRule"`
	StateValue                 string   `xml:"StateValue"`
	StateReason                string   `xml:"StateReason,omitempty"`
	AlarmDescription           string   `xml:"AlarmDescription,omitempty"`
	AlarmActions               []string `xml:"AlarmActions>member,omitempty"`
	OKActions                  []string `xml:"OKActions>member,omitempty"`
	InsufficientDataActions    []string `xml:"InsufficientDataActions>member,omitempty"`
	ActionsEnabled             bool     `xml:"ActionsEnabled"`
}

func (h *Handler) handleDescribeAlarms(form url.Values, c *echo.Context) error {
	alarmNames := parseMemberList(form, "AlarmNames.")
	alarmTypes := parseMemberList(form, "AlarmTypes.")
	alarmNamePrefix := form.Get("AlarmNamePrefix")
	stateValue := form.Get("StateValue")
	nextToken := form.Get("NextToken")
	maxRecords, _ := strconv.Atoi(form.Get("MaxRecords"))

	metricPage, compositePage, err := h.Backend.DescribeAlarms(
		alarmNames,
		alarmTypes,
		alarmNamePrefix,
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

	// Collect alarm ARNs before deleting so we can clean up their tag entries.
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		for _, arn := range b.GetAlarmARNs(alarmNames) {
			h.deleteResourceTags(arn)
		}
	}

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

	scanBy := form.Get("ScanBy")
	nextToken := form.Get("NextToken")
	maxDatapoints, _ := strconv.Atoi(form.Get("MaxDatapoints"))
	queries := parseMetricDataQueriesFromForm(form)

	var pageResult GetMetricDataPage
	var berr error
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		pageResult, berr = bk.GetMetricDataPaged(
			queries, startTime, endTime, scanBy, nextToken, maxDatapoints,
		)
	} else {
		var results []MetricDataResult
		results, berr = h.Backend.GetMetricData(queries, startTime, endTime)
		pageResult.Results = results
	}
	if berr != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", berr.Error())
	}

	type messageXML struct {
		Code  string `xml:"Code"`
		Value string `xml:"Value"`
	}
	type resultEntry struct {
		XMLName    xml.Name     `xml:"member"`
		ID         string       `xml:"Id"`
		Label      string       `xml:"Label,omitempty"`
		StatusCode string       `xml:"StatusCode"`
		Timestamps []string     `xml:"Timestamps>member"`
		Values     []float64    `xml:"Values>member"`
		Messages   []messageXML `xml:"Messages>member,omitempty"`
	}

	type response struct {
		XMLName           xml.Name      `xml:"GetMetricDataResponse"`
		Xmlns             string        `xml:"xmlns,attr"`
		RequestID         string        `xml:"ResponseMetadata>RequestId"`
		NextToken         string        `xml:"GetMetricDataResult>NextToken,omitempty"`
		MetricDataResults []resultEntry `xml:"GetMetricDataResult>MetricDataResults"`
		Messages          []messageXML  `xml:"GetMetricDataResult>Messages>member,omitempty"`
	}

	resp := response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		NextToken: pageResult.NextToken,
	}

	for _, r := range pageResult.Results {
		entry := resultEntry{
			ID:         r.ID,
			Label:      r.Label,
			StatusCode: r.StatusCode,
			Values:     r.Values,
		}
		for _, ts := range r.Timestamps {
			entry.Timestamps = append(entry.Timestamps, ts.UTC().Format(time.RFC3339))
		}
		for _, m := range r.Messages {
			entry.Messages = append(entry.Messages, messageXML(m))
		}

		resp.MetricDataResults = append(resp.MetricDataResults, entry)
	}

	for _, m := range pageResult.Messages {
		resp.Messages = append(resp.Messages, messageXML(m))
	}

	return writeXML(c, resp)
}

func (h *Handler) handlePutCompositeAlarm(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	if alarmName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}
	alarmRule := form.Get("AlarmRule")
	if alarmRule == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmRule is required",
		)
	}

	actionsEnabled := form.Get("ActionsEnabled") != formFalse

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
	dimensions := parseDimensionsFromForm(form, "Dimensions.")
	alarmNames := parseMemberList(form, "AlarmNames.")
	nextToken := form.Get("NextToken")
	maxRecords, _ := strconv.Atoi(form.Get("MaxRecords"))

	p, err := h.Backend.DescribeAlarmsForMetric(
		namespace,
		metricName,
		dimensions,
		alarmNames,
		nextToken,
		maxRecords,
	)
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
	alarmType := form.Get("AlarmType")
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

	p, err := h.Backend.DescribeAlarmHistory(
		alarmName,
		alarmType,
		historyItemType,
		nextToken,
		startDate,
		endDate,
		maxRecords,
	)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type historyItemXML struct {
		AlarmName       string `xml:"AlarmName"`
		AlarmType       string `xml:"AlarmType,omitempty"`
		HistoryItemType string `xml:"HistoryItemType"`
		HistorySummary  string `xml:"HistorySummary"`
		HistoryData     string `xml:"HistoryData,omitempty"`
		Timestamp       string `xml:"Timestamp"`
	}
	members := make([]historyItemXML, 0, len(p.Data))
	for _, item := range p.Data {
		members = append(members, historyItemXML{
			AlarmName:       item.AlarmName,
			AlarmType:       item.AlarmType,
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
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}
	stateValue := form.Get("StateValue")
	stateReason := form.Get("StateReason")
	stateReasonData := form.Get("StateReasonData")

	if err := h.Backend.SetAlarmState(
		c.Request().Context(),
		alarmName,
		stateValue,
		stateReason,
		stateReasonData,
	); err != nil {
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

func (h *Handler) handlePutDashboard(form url.Values, c *echo.Context) error {
	name := form.Get("DashboardName")
	if name == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterInput",
			"DashboardName is required",
		)
	}
	body := form.Get("DashboardBody")

	if err := h.Backend.PutDashboard(name, body); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		DashboardValidationMessages struct{} `xml:"PutDashboardResult>DashboardValidationMessages"`
		XMLName                     xml.Name `xml:"PutDashboardResponse"`
		Xmlns                       string   `xml:"xmlns,attr"`
		RequestID                   string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetDashboard(form url.Values, c *echo.Context) error {
	name := form.Get("DashboardName")
	if name == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterInput",
			"DashboardName is required",
		)
	}

	entry, body, err := h.Backend.GetDashboard(name)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type result struct {
		DashboardArn  string `xml:"DashboardArn"`
		DashboardBody string `xml:"DashboardBody"`
		DashboardName string `xml:"DashboardName"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetDashboardResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"GetDashboardResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result: result{
			DashboardArn:  entry.DashboardArn,
			DashboardBody: body,
			DashboardName: entry.DashboardName,
		},
	})
}

func (h *Handler) handleListDashboards(form url.Values, c *echo.Context) error {
	prefix := form.Get("DashboardNamePrefix")
	nextToken := form.Get("NextToken")

	p, err := h.Backend.ListDashboards(prefix, nextToken)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type entryXML struct {
		DashboardArn  string `xml:"DashboardArn"`
		DashboardName string `xml:"DashboardName"`
		LastModified  string `xml:"LastModified"`
		Size          int64  `xml:"Size"`
	}
	members := make([]entryXML, 0, len(p.Data))
	for _, e := range p.Data {
		members = append(members, entryXML{
			DashboardArn:  e.DashboardArn,
			DashboardName: e.DashboardName,
			LastModified:  e.LastModified.UTC().Format(time.RFC3339),
			Size:          e.Size,
		})
	}

	type listResult struct {
		NextToken        string     `xml:"NextToken,omitempty"`
		DashboardEntries []entryXML `xml:"DashboardEntries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListDashboardsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListDashboardsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    listResult{DashboardEntries: members, NextToken: p.Next},
	})
}

func (h *Handler) handleDeleteDashboards(form url.Values, c *echo.Context) error {
	names := parseMemberList(form, "DashboardNames.")

	// Clean up tag entries for dashboards being deleted.
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		for _, arn := range b.GetDashboardARNs(names) {
			h.deleteResourceTags(arn)
		}
	}

	if err := h.Backend.DeleteDashboards(names); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteDashboardsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// insightRuleFailureXML is the XML representation of a failed insight rule operation.
type insightRuleFailureXML struct {
	RuleName           string `xml:"RuleName"`
	FailureCode        string `xml:"FailureCode"`
	FailureDescription string `xml:"FailureDescription,omitempty"`
}

// insightRuleFailResult holds the failures portion of insight rule batch operation responses.
type insightRuleFailResult struct {
	Failures []insightRuleFailureXML `xml:"Failures>member"`
}

// buildInsightRuleFailResult converts backend failures into the XML result struct.
func buildInsightRuleFailResult(failures []InsightRuleFailure) insightRuleFailResult {
	if len(failures) == 0 {
		return insightRuleFailResult{}
	}

	members := make([]insightRuleFailureXML, 0, len(failures))
	for _, f := range failures {
		members = append(members, insightRuleFailureXML(f))
	}

	return insightRuleFailResult{Failures: members}
}

// insightRuleXML is the XML representation of an InsightRule.
type insightRuleXML struct {
	CreatedAt   string `xml:"CreatedAt,omitempty"`
	Name        string `xml:"Name"`
	State       string `xml:"State"`
	Schema      string `xml:"Schema,omitempty"`
	Definition  string `xml:"Definition,omitempty"`
	Arn         string `xml:"RuleArn,omitempty"`
	ManagedRule bool   `xml:"ManagedRule"`
}

func (h *Handler) putAlarmMuteRuleFromForm(form url.Values, c *echo.Context) error {
	muteName := form.Get("MuteName")
	if muteName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "MuteName is required")
	}

	var muteDuration int64
	if rawDuration := form.Get("MuteDuration"); rawDuration != "" {
		parsedDuration, err := strconv.ParseInt(rawDuration, 10, 64)
		if err != nil {
			return h.xmlError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValue",
				"MuteDuration must be an integer",
			)
		}
		if parsedDuration < 0 || parsedDuration > math.MaxInt32 {
			return h.xmlError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValue",
				"MuteDuration must be between 0 and 2147483647",
			)
		}
		muteDuration = parsedDuration
	}

	rule := &AlarmMuteRule{
		MuteName:      muteName,
		Description:   form.Get("Description"),
		MuteDuration:  int32(muteDuration), //nolint:gosec // bounds checked above (0..MaxInt32)
		AlarmNames:    parseMemberList(form, "AlarmNames."),
		MuteStartTime: time.Now().UTC(),
	}

	if rawStart := form.Get("MuteStartTime"); rawStart != "" {
		start, err := time.Parse(time.RFC3339, rawStart)
		if err != nil {
			return h.xmlError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValue",
				"MuteStartTime must be RFC3339",
			)
		}
		rule.MuteStartTime = start.UTC()
	}

	if err := h.Backend.PutAlarmMuteRule(rule); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return nil
}

func (h *Handler) handlePutAlarmMuteRule(form url.Values, c *echo.Context) error {
	if err := h.putAlarmMuteRuleFromForm(form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name `xml:"PutAlarmMuteRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleUpdateAlarmMuteRule(form url.Values, c *echo.Context) error {
	muteName := form.Get("MuteName")
	if muteName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "MuteName is required")
	}
	if _, err := h.Backend.GetAlarmMuteRule(muteName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	if err := h.putAlarmMuteRuleFromForm(form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name `xml:"UpdateAlarmMuteRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeleteAlarmMuteRule(form url.Values, c *echo.Context) error {
	muteName := form.Get("MuteName")
	if muteName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "MuteName is required")
	}

	if err := h.Backend.DeleteAlarmMuteRule(muteName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteAlarmMuteRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetAlarmMuteRule(form url.Values, c *echo.Context) error {
	muteName := form.Get("MuteName")
	if muteName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "MuteName is required")
	}

	rule, err := h.Backend.GetAlarmMuteRule(muteName)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type muteRuleXML struct {
		MuteName      string   `xml:"MuteName"`
		Description   string   `xml:"Description,omitempty"`
		CreationTime  string   `xml:"CreationTime"`
		MuteStartTime string   `xml:"MuteStartTime,omitempty"`
		AlarmNames    []string `xml:"AlarmNames>member,omitempty"`
		MuteDuration  int32    `xml:"MuteDuration,omitempty"`
	}
	type result struct {
		MuteRule muteRuleXML `xml:"MuteRule"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetAlarmMuteRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"GetAlarmMuteRuleResult"`
	}

	mr := muteRuleXML{
		MuteName:     rule.MuteName,
		Description:  rule.Description,
		AlarmNames:   rule.AlarmNames,
		CreationTime: rule.CreationTime.UTC().Format(time.RFC3339),
		MuteDuration: rule.MuteDuration,
	}
	if !rule.MuteStartTime.IsZero() {
		mr.MuteStartTime = rule.MuteStartTime.UTC().Format(time.RFC3339)
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    result{MuteRule: mr},
	})
}

func (h *Handler) handleDeleteAnomalyDetector(form url.Values, c *echo.Context) error {
	namespace := form.Get("SingleMetricAnomalyDetector.Namespace")
	if namespace == "" {
		namespace = form.Get("Namespace")
	}

	metricName := form.Get("SingleMetricAnomalyDetector.MetricName")
	if metricName == "" {
		metricName = form.Get("MetricName")
	}

	stat := form.Get("SingleMetricAnomalyDetector.Stat")
	if stat == "" {
		stat = form.Get("Stat")
	}

	if namespace == "" || metricName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace and MetricName are required",
		)
	}

	dimsD := parseDimensionsFromForm(form, "SingleMetricAnomalyDetector.Dimensions")
	if len(dimsD) == 0 {
		dimsD = parseDimensionsFromForm(form, "Dimensions")
	}

	if err := h.Backend.DeleteAnomalyDetector(namespace, metricName, stat, dimsD); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	// Clean up tags for the deleted anomaly detector's ARN.
	detectorARN := buildAnomalyDetectorARN(namespace, metricName, stat)
	h.deleteResourceTags(detectorARN)

	type response struct {
		XMLName   xml.Name `xml:"DeleteAnomalyDetectorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// buildAnomalyDetectorARN constructs a synthetic ARN for an anomaly detector (for tag storage).
func buildAnomalyDetectorARN(namespace, metricName, stat string) string {
	return "arn:aws:cloudwatch::anomaly-detector:" + namespace + "/" + metricName + "/" + stat
}

func (h *Handler) handleDescribeAnomalyDetectors(form url.Values, c *echo.Context) error {
	namespace := form.Get("Namespace")
	metricName := form.Get("MetricName")
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.DescribeAnomalyDetectors(namespace, metricName, nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type detectorXML struct {
		Namespace  string `xml:"SingleMetricAnomalyDetector>Namespace"`
		MetricName string `xml:"SingleMetricAnomalyDetector>MetricName"`
		Stat       string `xml:"SingleMetricAnomalyDetector>Stat"`
		StateValue string `xml:"StateValue"`
	}
	members := make([]detectorXML, 0, len(p.Data))
	for _, d := range p.Data {
		members = append(members, detectorXML{
			Namespace:  d.Namespace,
			MetricName: d.MetricName,
			Stat:       d.Stat,
			StateValue: d.StateValue,
		})
	}

	type descResult struct {
		NextToken        string        `xml:"NextToken,omitempty"`
		AnomalyDetectors []detectorXML `xml:"AnomalyDetectors>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeAnomalyDetectorsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeAnomalyDetectorsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    descResult{AnomalyDetectors: members, NextToken: p.Next},
	})
}

func (h *Handler) handlePutInsightRule(form url.Values, c *echo.Context) error {
	if err := h.putInsightRule(form.Get("RuleName"), form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name `xml:"PutInsightRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) putInsightRule(ruleName string, form url.Values, c *echo.Context) error {
	if ruleName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "RuleName is required")
	}

	if err := h.Backend.PutInsightRule(&InsightRule{
		Name:       ruleName,
		Definition: form.Get("RuleDefinition"),
		State:      form.Get("RuleState"),
	}); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return nil
}

func (h *Handler) handleUpdateInsightRule(form url.Values, c *echo.Context) error {
	ruleName := form.Get("RuleName")
	if ruleName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "RuleName is required")
	}
	if _, err := h.Backend.GetInsightRule(ruleName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	if err := h.putInsightRule(ruleName, form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name `xml:"UpdateInsightRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeleteInsightRules(form url.Values, c *echo.Context) error {
	ruleNames := parseMemberList(form, "RuleNames.")
	if len(ruleNames) == 0 {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.DeleteInsightRules(ruleNames)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name              `xml:"DeleteInsightRulesResponse"`
		Xmlns     string                `xml:"xmlns,attr"`
		RequestID string                `xml:"ResponseMetadata>RequestId"`
		Result    insightRuleFailResult `xml:"DeleteInsightRulesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    buildInsightRuleFailResult(failures),
	})
}

func (h *Handler) handleDescribeInsightRules(form url.Values, c *echo.Context) error {
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.DescribeInsightRules(nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	members := make([]insightRuleXML, 0, len(p.Data))
	for _, r := range p.Data {
		members = append(members, insightRuleXML{
			Name:        r.Name,
			State:       r.State,
			Schema:      r.Schema,
			Definition:  r.Definition,
			ManagedRule: r.ManagedRule,
			Arn:         r.Arn,
			CreatedAt:   formatTimeOmitZero(r.CreatedAt),
		})
	}

	type descResult struct {
		NextToken    string           `xml:"NextToken,omitempty"`
		InsightRules []insightRuleXML `xml:"InsightRules>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeInsightRulesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeInsightRulesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    descResult{InsightRules: members, NextToken: p.Next},
	})
}

func (h *Handler) handleDisableInsightRules(form url.Values, c *echo.Context) error {
	ruleNames := parseMemberList(form, "RuleNames.")
	if len(ruleNames) == 0 {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.DisableInsightRules(ruleNames)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name              `xml:"DisableInsightRulesResponse"`
		Xmlns     string                `xml:"xmlns,attr"`
		RequestID string                `xml:"ResponseMetadata>RequestId"`
		Result    insightRuleFailResult `xml:"DisableInsightRulesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    buildInsightRuleFailResult(failures),
	})
}

func (h *Handler) handleEnableInsightRules(form url.Values, c *echo.Context) error {
	ruleNames := parseMemberList(form, "RuleNames.")
	if len(ruleNames) == 0 {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"RuleNames is required",
		)
	}

	failures, err := h.Backend.EnableInsightRules(ruleNames)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name              `xml:"EnableInsightRulesResponse"`
		Xmlns     string                `xml:"xmlns,attr"`
		RequestID string                `xml:"ResponseMetadata>RequestId"`
		Result    insightRuleFailResult `xml:"EnableInsightRulesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    buildInsightRuleFailResult(failures),
	})
}

// parseMetricStreamFiltersFromForm parses IncludeFilters.member.N.* or ExcludeFilters.member.N.* form values.
func parseMetricStreamFiltersFromForm(form url.Values, listPrefix string) []MetricStreamFilter {
	var filters []MetricStreamFilter
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("%smember.%d.", listPrefix, i)
		ns := form.Get(prefix + "Namespace")
		if ns == "" {
			return filters
		}
		var metricNames []string
		for j := 1; ; j++ {
			mn := form.Get(fmt.Sprintf("%sMetricNames.member.%d", prefix, j))
			if mn == "" {
				break
			}
			metricNames = append(metricNames, mn)
		}
		filters = append(filters, MetricStreamFilter{Namespace: ns, MetricNames: metricNames})
	}
}

func (h *Handler) putMetricStreamFromForm(form url.Values, c *echo.Context) error {
	name := form.Get("Name")
	if name == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	if err := h.Backend.PutMetricStream(&MetricStream{
		Name:           name,
		FirehoseArn:    form.Get("FirehoseArn"),
		RoleArn:        form.Get("RoleArn"),
		OutputFormat:   form.Get("OutputFormat"),
		State:          form.Get("State"),
		IncludeFilters: parseMetricStreamFiltersFromForm(form, "IncludeFilters."),
		ExcludeFilters: parseMetricStreamFiltersFromForm(form, "ExcludeFilters."),
	}); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return nil
}

func (h *Handler) handlePutMetricStream(form url.Values, c *echo.Context) error {
	if err := h.putMetricStreamFromForm(form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name `xml:"PutMetricStreamResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleUpdateMetricStream(form url.Values, c *echo.Context) error {
	name := form.Get("Name")
	if name == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}
	if _, err := h.Backend.GetMetricStream(name); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	if err := h.putMetricStreamFromForm(form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name `xml:"UpdateMetricStreamResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeleteMetricStream(form url.Values, c *echo.Context) error {
	name := form.Get("Name")
	if name == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	if err := h.Backend.DeleteMetricStream(name); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	// Clean up tags for the deleted metric stream's ARN.
	streamARN := "arn:aws:cloudwatch::metric-stream/" + name
	h.deleteResourceTags(streamARN)

	type response struct {
		XMLName   xml.Name `xml:"DeleteMetricStreamResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeAlarmContributors(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	if alarmName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}

	nextToken := form.Get("NextToken")

	p, err := h.Backend.DescribeAlarmContributors(alarmName, nextToken)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type contributorXML struct {
		Keys []string `xml:"Keys>member"`
		Sum  float64  `xml:"Sum"`
	}
	members := make([]contributorXML, 0, len(p.Data))
	for _, contrib := range p.Data {
		members = append(members, contributorXML(contrib))
	}

	type descResult struct {
		NextToken    string           `xml:"NextToken,omitempty"`
		Contributors []contributorXML `xml:"Contributors>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeAlarmContributorsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeAlarmContributorsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    descResult{Contributors: members, NextToken: p.Next},
	})
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}

	h.tagsMu.Lock("Reset")
	h.tags = make(map[string]*tags.Tags)
	h.tagsMu.Unlock()
}

// formatTimeOmitZero formats t as RFC3339 or returns "" for the zero value.
func formatTimeOmitZero(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format(time.RFC3339)
}

func (h *Handler) handlePutAnomalyDetector(form url.Values, c *echo.Context) error {
	namespace := form.Get("SingleMetricAnomalyDetector.Namespace")
	if namespace == "" {
		namespace = form.Get("Namespace")
	}
	metricName := form.Get("SingleMetricAnomalyDetector.MetricName")
	if metricName == "" {
		metricName = form.Get("MetricName")
	}
	stat := form.Get("SingleMetricAnomalyDetector.Stat")
	if stat == "" {
		stat = form.Get("Stat")
	}

	if namespace == "" || metricName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"Namespace and MetricName are required",
		)
	}

	// Parse dimensions for the anomaly detector.
	dims := parseDimensionsFromForm(form, "SingleMetricAnomalyDetector.Dimensions")
	if len(dims) == 0 {
		dims = parseDimensionsFromForm(form, "Dimensions")
	}

	// Parse optional band width.
	bandWidth := 0.0
	if bwStr := form.Get("Configuration.BandWidth"); bwStr != "" {
		bandWidth, _ = strconv.ParseFloat(bwStr, 64)
	}

	if err := h.Backend.PutAnomalyDetector(&AnomalyDetector{
		Namespace:  namespace,
		MetricName: metricName,
		Stat:       stat,
		Dimensions: dims,
		BandWidth:  bandWidth,
		StateValue: statusTrainedInsufficient,
	}); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutAnomalyDetectorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleListMetricStreams(form url.Values, c *echo.Context) error {
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.ListMetricStreams(nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type entryXML struct {
		Name           string `xml:"Name"`
		Arn            string `xml:"Arn"`
		FirehoseArn    string `xml:"FirehoseArn"`
		State          string `xml:"State"`
		OutputFormat   string `xml:"OutputFormat"`
		CreationDate   string `xml:"CreationDate,omitempty"`
		LastUpdateDate string `xml:"LastUpdateDate,omitempty"`
	}
	members := make([]entryXML, 0, len(p.Data))
	for _, s := range p.Data {
		members = append(members, entryXML{
			Name:           s.Name,
			Arn:            s.Arn,
			FirehoseArn:    s.FirehoseArn,
			State:          s.State,
			OutputFormat:   s.OutputFormat,
			CreationDate:   formatTimeOmitZero(s.CreationDate),
			LastUpdateDate: formatTimeOmitZero(s.LastUpdateDate),
		})
	}

	type listResult struct {
		NextToken     string     `xml:"NextToken,omitempty"`
		MetricStreams []entryXML `xml:"Entries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListMetricStreamsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListMetricStreamsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    listResult{MetricStreams: members, NextToken: p.Next},
	})
}

func (h *Handler) handleGetMetricStream(form url.Values, c *echo.Context) error {
	name := form.Get("Name")
	if name == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	stream, err := h.Backend.GetMetricStream(name)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type result struct {
		Name           string `xml:"Name"`
		Arn            string `xml:"Arn"`
		FirehoseArn    string `xml:"FirehoseArn"`
		RoleArn        string `xml:"RoleArn"`
		State          string `xml:"State"`
		OutputFormat   string `xml:"OutputFormat"`
		CreationDate   string `xml:"CreationDate,omitempty"`
		LastUpdateDate string `xml:"LastUpdateDate,omitempty"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetMetricStreamResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"GetMetricStreamResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result: result{
			Name:           stream.Name,
			Arn:            stream.Arn,
			FirehoseArn:    stream.FirehoseArn,
			RoleArn:        stream.RoleArn,
			State:          stream.State,
			OutputFormat:   stream.OutputFormat,
			CreationDate:   formatTimeOmitZero(stream.CreationDate),
			LastUpdateDate: formatTimeOmitZero(stream.LastUpdateDate),
		},
	})
}

func (h *Handler) handlePutMetricFilter(form url.Values, c *echo.Context) error {
	filterName := form.Get("FilterName")
	if filterName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"FilterName is required",
		)
	}
	logGroupName := form.Get("LogGroupName")
	if logGroupName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"LogGroupName is required",
		)
	}

	filter := &MetricFilter{
		FilterName:            filterName,
		LogGroupName:          logGroupName,
		FilterPattern:         form.Get("FilterPattern"),
		MetricTransformations: parseMetricTransformationsFromForm(form),
	}
	if err := h.Backend.PutMetricFilter(filter); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutMetricFilterResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeMetricFilters(form url.Values, c *echo.Context) error {
	filterNamePrefix := form.Get("FilterNamePrefix")
	logGroupName := form.Get("LogGroupName")
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.DescribeMetricFilters(filterNamePrefix, logGroupName, nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type metricTransXML struct {
		MetricName      string  `xml:"MetricName"`
		MetricNamespace string  `xml:"MetricNamespace"`
		MetricValue     string  `xml:"MetricValue"`
		Unit            string  `xml:"Unit,omitempty"`
		DefaultValue    float64 `xml:"DefaultValue,omitempty"`
	}
	type filterXML struct {
		FilterName            string           `xml:"FilterName"`
		LogGroupName          string           `xml:"LogGroupName"`
		FilterPattern         string           `xml:"FilterPattern,omitempty"`
		MetricTransformations []metricTransXML `xml:"MetricTransformations>member,omitempty"`
		CreationTime          int64            `xml:"CreationTime"`
	}

	members := make([]filterXML, 0, len(p.Data))
	for _, f := range p.Data {
		fx := filterXML{
			FilterName:    f.FilterName,
			LogGroupName:  f.LogGroupName,
			FilterPattern: f.FilterPattern,
			CreationTime:  f.CreationTime.UnixMilli(),
		}
		for _, t := range f.MetricTransformations {
			fx.MetricTransformations = append(fx.MetricTransformations, metricTransXML(t))
		}
		members = append(members, fx)
	}

	type descResult struct {
		NextToken     string      `xml:"NextToken,omitempty"`
		MetricFilters []filterXML `xml:"MetricFilters>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeMetricFiltersResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeMetricFiltersResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    descResult{MetricFilters: members, NextToken: p.Next},
	})
}

func (h *Handler) handleDeleteMetricFilter(form url.Values, c *echo.Context) error {
	filterName := form.Get("FilterName")
	if filterName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"FilterName is required",
		)
	}
	logGroupName := form.Get("LogGroupName")
	if logGroupName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"LogGroupName is required",
		)
	}

	if err := h.Backend.DeleteMetricFilter(filterName, logGroupName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteMetricFilterResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// handleTestMetricFilter is a stub that returns empty matches (log events not stored by this emulator).
func (h *Handler) handleTestMetricFilter(_ url.Values, c *echo.Context) error {
	type match struct {
		ExtractedValues struct{} `xml:"ExtractedValues"`
		EventMessage    string   `xml:"EventMessage"`
		EventNumber     int64    `xml:"EventNumber"`
	}
	type response struct {
		XMLName   xml.Name `xml:"TestMetricFilterResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Matches   []match  `xml:"TestMetricFilterResult>Matches>member"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// handleGetInsightRuleReport returns a contributor insights report by aggregating
// metric data grouped by dimension values for the named rule's log group.
func (h *Handler) handleGetInsightRuleReport(form url.Values, c *echo.Context) error {
	ruleName := form.Get("RuleName")
	if ruleName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "RuleName is required")
	}
	if _, err := h.Backend.GetInsightRule(ruleName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	maxContributors, _ := strconv.Atoi(form.Get("MaxContributorCount"))
	if maxContributors <= 0 {
		maxContributors = 10
	}
	orderBy := form.Get("OrderBy")
	startStr := form.Get("StartTime")
	endStr := form.Get("EndTime")

	startTime := time.Now().UTC().Add(-time.Hour)
	if t, err := time.Parse(time.RFC3339, startStr); err == nil {
		startTime = t
	}
	endTime := time.Now().UTC()
	if t, err := time.Parse(time.RFC3339, endStr); err == nil {
		endTime = t
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
			return h.xmlError(
				c,
				http.StatusBadRequest,
				"ResourceNotFoundException",
				innerErr.Error(),
			)
		}
	}

	type keyXML struct {
		Keys []string `xml:"Keys>member"`
		Sum  float64  `xml:"ApproximateSum"`
	}
	type result struct {
		Contributors []keyXML `xml:"Contributors>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetInsightRuleReportResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"GetInsightRuleReportResult"`
	}

	resp := response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()}
	for _, c := range contributors {
		resp.Result.Contributors = append(resp.Result.Contributors, keyXML(c))
	}

	return writeXML(c, resp)
}

// minimalPNG1x1 is a base64-encoded 1×1 white PNG. It is retained as a last-resort
// fallback for the (unreachable) case where widget rendering fails.
const minimalPNG1x1 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAAAAAA6fptVAAAACklEQVQI12NgAAAAAgAB4iG8MwAAAABJRU5ErkJggg=="

// renderWidgetImageBase64 renders the MetricWidget JSON into a base64-encoded PNG.
// On any failure it falls back to the 1×1 placeholder so the response is always a
// decodable image.
func (h *Handler) renderWidgetImageBase64(form url.Values) string {
	widgetJSON := form.Get("MetricWidget")
	bk, _ := h.Backend.(*InMemoryBackend)

	img, err := renderMetricWidgetPNG(bk, widgetJSON, time.Now().UTC())
	if err != nil || len(img) == 0 {
		return minimalPNG1x1
	}

	return base64.StdEncoding.EncodeToString(img)
}

func (h *Handler) handleGetMetricWidgetImage(form url.Values, c *echo.Context) error {
	type response struct {
		MetricWidgetImage string   `xml:"GetMetricWidgetImageResult>MetricWidgetImage"`
		XMLName           xml.Name `xml:"GetMetricWidgetImageResponse"`
		Xmlns             string   `xml:"xmlns,attr"`
		RequestID         string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:             cloudwatchNS,
		RequestID:         uuid.New().String(),
		MetricWidgetImage: h.renderWidgetImageBase64(form),
	})
}

func (h *Handler) handleListAlarmMuteRules(form url.Values, c *echo.Context) error {
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.ListAlarmMuteRules(nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type muteRuleXML struct {
		MuteName      string   `xml:"MuteName"`
		Description   string   `xml:"Description,omitempty"`
		CreationTime  string   `xml:"CreationTime"`
		MuteStartTime string   `xml:"MuteStartTime,omitempty"`
		AlarmNames    []string `xml:"AlarmNames>member,omitempty"`
		MuteDuration  int32    `xml:"MuteDuration,omitempty"`
	}
	type listResult struct {
		NextToken string        `xml:"NextToken,omitempty"`
		MuteRules []muteRuleXML `xml:"MuteRules>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListAlarmMuteRulesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListAlarmMuteRulesResult"`
	}

	members := make([]muteRuleXML, 0, len(p.Data))
	for _, rule := range p.Data {
		mr := muteRuleXML{
			MuteName:     rule.MuteName,
			Description:  rule.Description,
			AlarmNames:   rule.AlarmNames,
			MuteDuration: rule.MuteDuration,
			CreationTime: rule.CreationTime.UTC().Format(time.RFC3339),
		}
		if !rule.MuteStartTime.IsZero() {
			mr.MuteStartTime = rule.MuteStartTime.UTC().Format(time.RFC3339)
		}
		members = append(members, mr)
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    listResult{MuteRules: members, NextToken: p.Next},
	})
}

func (h *Handler) handleListManagedInsightRules(form url.Values, c *echo.Context) error {
	resourceARN := form.Get("ResourceARN")
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.ListManagedInsightRules(resourceARN, nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type managedRuleXML struct {
		RuleName     string `xml:"RuleName"`
		ResourceARN  string `xml:"ResourceARN,omitempty"`
		RuleState    string `xml:"RuleState>Value,omitempty"`
		TemplateName string `xml:"TemplateName,omitempty"`
	}
	type listResult struct {
		NextToken    string           `xml:"NextToken,omitempty"`
		ManagedRules []managedRuleXML `xml:"ManagedRules>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListManagedInsightRulesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListManagedInsightRulesResult"`
	}

	members := make([]managedRuleXML, 0, len(p.Data))
	for _, rule := range p.Data {
		members = append(members, managedRuleXML{
			RuleName:    rule.Name,
			ResourceARN: rule.Arn,
			RuleState:   rule.State,
		})
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    listResult{ManagedRules: members, NextToken: p.Next},
	})
}

func (h *Handler) handlePutManagedInsightRules(form url.Values, c *echo.Context) error {
	type failureXML struct {
		RuleName           string `xml:"RuleName"`
		FailureCode        string `xml:"FailureCode"`
		FailureDescription string `xml:"FailureDescription,omitempty"`
	}
	type putResult struct {
		Failures []failureXML `xml:"Failures>member,omitempty"`
	}
	type response struct {
		XMLName   xml.Name  `xml:"PutManagedInsightRulesResponse"`
		Xmlns     string    `xml:"xmlns,attr"`
		RequestID string    `xml:"ResponseMetadata>RequestId"`
		Result    putResult `xml:"PutManagedInsightRulesResult"`
	}

	var failures []failureXML
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("ManagedRules.member.%d.", i)
		ruleName := form.Get(prefix + "RuleName")
		if ruleName == "" {
			break
		}
		templateName := form.Get(prefix + "TemplateName")
		resourceARN := form.Get(prefix + "ResourceARN")

		if err := h.Backend.PutInsightRule(&InsightRule{
			Name:        ruleName,
			State:       insightRuleStateEnabled,
			Definition:  templateName,
			Arn:         resourceARN,
			ManagedRule: true,
		}); err != nil {
			failures = append(failures, failureXML{
				RuleName:           ruleName,
				FailureCode:        errCodeInternalFailure,
				FailureDescription: err.Error(),
			})
		}
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    putResult{Failures: failures},
	})
}

func (h *Handler) handleStartMetricStreams(form url.Values, c *echo.Context) error {
	names := parseMemberList(form, "Names.")
	if err := h.Backend.StartMetricStreams(names); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"StartMetricStreamsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleStopMetricStreams(form url.Values, c *echo.Context) error {
	names := parseMemberList(form, "Names.")
	if err := h.Backend.StopMetricStreams(names); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"StopMetricStreamsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}
