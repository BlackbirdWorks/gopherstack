package cloudwatch

import (
	"io"
	"math"
	"net/http"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyStateValue = "StateValue"
	keyState      = "State"
	keyStatus     = "Status"
)

const (
	keyNamespace  = "Namespace"
	keyMetricName = "MetricName"
	keyName       = "Name"
	keyValue      = "Value"
)

const (
	keyAlarmType      = "AlarmType"
	keyStateReason    = "StateReason"
	keyActionsEnabled = "ActionsEnabled"
	keyArn            = "Arn"
)

const cborServicePath = "/service/GraniteServiceVersion20100801/operation/"

// nanosPerSecond is the number of nanoseconds in a second.
const nanosPerSecond = 1e9

const (
	cborOpListTagsForResource = "ListTagsForResource"
	cborOpTagResource         = "TagResource"
	cborOpUntagResource       = "UntagResource"
)

// isCBORRequest returns true when the request uses the rpc-v2-cbor (Smithy
// RPCv2) protocol. Delegates to pkgs/service, shared with AppStream (the
// only other rpc-v2-cbor service).
func isCBORRequest(r *http.Request) bool {
	return service.IsRPCv2CBORRequest(r, cborServicePath)
}

// extractCBOROperation returns the operation name from an rpc-v2-cbor request path.
func extractCBOROperation(path string) string {
	return service.ExtractRPCv2CBOROperation(path, cborServicePath)
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
	case opPutInsightRule:
		return h.cborPutInsightRule(input, c)
	case opPutMetricStream:
		return h.cborPutMetricStream(input, c)
	case opGetAlarmMuteRule:
		return h.cborGetAlarmMuteRule(input, c)
	case opDeleteAlarmMuteRule:
		return h.cborDeleteAlarmMuteRule(input, c)
	case opGetDataset:
		return h.cborGetDataset(input, c)
	case opAssociateDatasetKmsKey:
		return h.cborAssociateDatasetKmsKey(input, c)
	case opDisassociateDatasetKmsKey:
		return h.cborDisassociateDatasetKmsKey(input, c)
	case opGetOTelEnrichment:
		return h.cborGetOTelEnrichment(input, c)
	case opStartOTelEnrichment:
		return h.cborStartOTelEnrichment(input, c)
	case opStopOTelEnrichment:
		return h.cborStopOTelEnrichment(input, c)
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
	case opPutLogAlarm:
		return h.cborPutLogAlarm(input, c)
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

	return h.dispatchInsightRuleCBOR(op, input, c)
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

// dispatchInsightRuleCBOR routes insight rule CBOR operations.
func (h *Handler) dispatchInsightRuleCBOR(
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
// Delegates to pkgs/service, shared with AppStream (the only other
// rpc-v2-cbor service).
func writeCBOR(c *echo.Context, v cbor.Value) error {
	return service.WriteRPCv2CBORResponse(c, v)
}

// cborError writes a CBOR error response. See
// [service.WriteRPCv2CBORError] for why the CBOR body itself (not just the
// X-Amzn-Errortype header) must carry the exception name.
func (h *Handler) cborError(c *echo.Context, status int, code, message string) error {
	return service.WriteRPCv2CBORError(c, status, code, message)
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

// cborTagsFromInput extracts a Tags list (`[]{Key,Value}`) from a CBOR
// operation input, matching how CreateOrUpdateTags-style ops and every
// Tags-accepting Put* op encode tags on the wire.
func cborTagsFromInput(input cbor.Map) map[string]string {
	tagList, ok := input["Tags"].(cbor.List)
	if !ok {
		return nil
	}

	kv := make(map[string]string, len(tagList))
	for _, item := range tagList {
		if m, isMap := item.(cbor.Map); isMap {
			kv[cborStr(m, "Key")] = cborStr(m, keyValue)
		}
	}

	return kv
}

// applyCreationTags stores any Tags present on a Put*/Create* CBOR input
// against resourceARN. Every tag-accepting Put* op (PutMetricAlarm,
// PutCompositeAlarm, PutLogAlarm, PutDashboard, PutInsightRule,
// PutMetricStream, PutAlarmMuteRule) must call this after the resource is
// created, or Tags supplied at creation are silently dropped
// (gopherstack-2mwl).
func (h *Handler) applyCreationTags(input cbor.Map, resourceARN string) {
	if kv := cborTagsFromInput(input); len(kv) > 0 {
		h.setTags(resourceARN, kv)
	}
}

func (h *Handler) cborTagResource(input cbor.Map, c *echo.Context) error {
	arn := cborStr(input, "ResourceARN")
	if kv := cborTagsFromInput(input); kv != nil {
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
