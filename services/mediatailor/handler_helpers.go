package mediatailor

import (
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// addTimestamps writes CreationTime/LastModifiedTime into out as
// unixTimestamp (epoch-seconds JSON number) values, the wire shape every
// MediaTailor timestamp uses — skipping either field when it is the zero
// value (never populated), matching how a real Describe/List response omits
// timestamps for a resource type that never sets them.
func addTimestamps(out map[string]any, created, modified time.Time) {
	if !created.IsZero() {
		out["CreationTime"] = awstime.Epoch(created)
	}

	if !modified.IsZero() {
		out["LastModifiedTime"] = awstime.Epoch(modified)
	}
}

// extractPaginationParams reads MaxResults/NextToken from the query string.
// The real MediaTailor model is inconsistent about casing: ListChannels,
// ListSourceLocations, ListVodSources, ListLiveSources, and
// GetChannelSchedule bind them as lowercase "maxResults"/"nextToken", while
// ListPlaybackConfigurations and ListFunctions bind them PascalCase
// "MaxResults"/"NextToken" (confirmed against aws-sdk-go-v2's httpbinding
// serializers and botocore's service-2.json). Checking both keys makes this
// helper correct for every query-string-based List op regardless of casing.
func extractPaginationParams(c *echo.Context) (int, string) {
	q := c.Request().URL.Query()
	maxResults := 0

	s := q.Get("MaxResults")
	if s == "" {
		s = q.Get("maxResults")
	}

	if s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxResults = n
		}
	}

	nextToken := q.Get("NextToken")
	if nextToken == "" {
		nextToken = q.Get("nextToken")
	}

	return maxResults, nextToken
}

// extractBodyPaginationParams reads MaxResults/NextToken from a decoded JSON
// request body. ListPrefetchSchedules is a POST operation that carries its
// pagination parameters in the request body rather than the query string
// (confirmed against aws-sdk-go-v2's serializer), unlike every other List op.
func extractBodyPaginationParams(body map[string]any) (int, string) {
	maxResults := 0
	if f, ok := body["MaxResults"].(float64); ok {
		maxResults = int(f)
	}

	nextToken, _ := body["NextToken"].(string)

	return maxResults, nextToken
}

func extractTags(body map[string]any) map[string]string {
	raw, _ := body[keyTags].(map[string]any)
	if len(raw) == 0 {
		return nil
	}

	tags := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}

	return tags
}

func extractBaseURL(body map[string]any) string {
	httpConfig, _ := body["HttpConfiguration"].(map[string]any)
	if httpConfig == nil {
		return ""
	}

	baseURL, _ := httpConfig[keyBaseURL].(string)

	return baseURL
}

func extractAccessConfiguration(body map[string]any) *AccessConfiguration {
	raw, _ := body["AccessConfiguration"].(map[string]any)
	if raw == nil {
		return nil
	}

	cfg := &AccessConfiguration{AccessType: stringField(raw, "AccessType")}

	if smat, ok := raw["SecretsManagerAccessTokenConfiguration"].(map[string]any); ok {
		cfg.SecretsManagerAccessTokenConfiguration = &SecretsManagerAccessTokenConfiguration{
			HeaderName:      stringField(smat, "HeaderName"),
			SecretArn:       stringField(smat, "SecretArn"),
			SecretStringKey: stringField(smat, "SecretStringKey"),
		}
	}

	return cfg
}

func extractDefaultSegmentDeliveryConfiguration(body map[string]any) *DefaultSegmentDeliveryConfiguration {
	raw, _ := body["DefaultSegmentDeliveryConfiguration"].(map[string]any)
	if raw == nil {
		return nil
	}

	return &DefaultSegmentDeliveryConfiguration{BaseURL: stringField(raw, keyBaseURL)}
}

func extractSegmentDeliveryConfigurations(body map[string]any) []SegmentDeliveryConfiguration {
	raw, _ := body["SegmentDeliveryConfigurations"].([]any)
	if len(raw) == 0 {
		return nil
	}

	out := make([]SegmentDeliveryConfiguration, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, SegmentDeliveryConfiguration{
			BaseURL: stringField(m, keyBaseURL),
			Name:    stringField(m, "Name"),
		})
	}

	return out
}

func extractOutputs(body map[string]any) []OutputItem {
	raw, _ := body["Outputs"].([]any)
	if len(raw) == 0 {
		return nil
	}

	outputs := make([]OutputItem, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out := OutputItem{
			ManifestName: stringField(m, "ManifestName"),
			SourceGroup:  stringField(m, keySourceGroup),
		}

		if hls, hlsOk := m["HlsPlaylistSettings"].(map[string]any); hlsOk {
			sec, _ := hls["ManifestWindowSeconds"].(float64)
			out.HlsPlaylistSettings = &HlsPlaylistSettings{
				ManifestWindowSeconds: int(sec),
			}
		}

		outputs = append(outputs, out)
	}

	return outputs
}

func extractHTTPPackageConfigurations(body map[string]any) []HTTPPackageConfiguration {
	raw, _ := body[keyHTTPPackageConfigs].([]any)
	if len(raw) == 0 {
		return nil
	}

	cfgs := make([]HTTPPackageConfiguration, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		cfgs = append(cfgs, HTTPPackageConfiguration{
			Path:        stringField(m, "Path"),
			SourceGroup: stringField(m, keySourceGroup),
			Type:        stringField(m, "Type"),
		})
	}

	return cfgs
}

func httpPackageConfigurationsWire(cfgs []HTTPPackageConfiguration) []map[string]any {
	out := make([]map[string]any, 0, len(cfgs))
	for _, cfg := range cfgs {
		out = append(out, map[string]any{
			"Path":         cfg.Path,
			keySourceGroup: cfg.SourceGroup,
			"Type":         cfg.Type,
		})
	}

	return out
}

func stringField(m map[string]any, key string) string {
	v, _ := m[key].(string)

	return v
}

func extractFillerSlate(body map[string]any) *SlateSource {
	raw, _ := body["FillerSlate"].(map[string]any)
	if raw == nil {
		return nil
	}

	return &SlateSource{
		SourceLocationName: stringField(raw, keySourceLocationName),
		VodSourceName:      stringField(raw, keyVodSourceName),
	}
}

// epochSecondsToTime converts a wire-format epoch-seconds value (as produced
// by smithytime.FormatEpochSeconds / awstime.Epoch) back into a time.Time.
func epochSecondsToTime(sec float64) time.Time {
	return time.Unix(0, int64(sec*float64(time.Second))).UTC()
}

// extractPrefetchRetrieval reads the Retrieval.StartTime/EndTime timestamps.
// These are unixTimestamp shapes on the wire (JSON number of seconds since
// epoch, per aws-sdk-go-v2's serializers and botocore's service-2.json), so a
// real SDK client sends a JSON number here, not an RFC3339 string.
func extractPrefetchRetrieval(body map[string]any) *PrefetchRetrieval {
	raw, _ := body["Retrieval"].(map[string]any)
	if raw == nil {
		return nil
	}

	r := &PrefetchRetrieval{}

	if f, ok := raw["StartTime"].(float64); ok {
		r.StartTime = epochSecondsToTime(f)
	}

	if f, ok := raw["EndTime"].(float64); ok {
		r.EndTime = epochSecondsToTime(f)
	}

	if dv, _ := raw["DynamicVariables"].(map[string]any); len(dv) > 0 {
		r.DynamicVariables = make(map[string]string, len(dv))
		for k, v := range dv {
			if sv, ok := v.(string); ok {
				r.DynamicVariables[k] = sv
			}
		}
	}

	return r
}

// extractPrefetchConsumption reads the Consumption.StartTime/EndTime
// timestamps; see extractPrefetchRetrieval for the epoch-seconds wire format.
func extractPrefetchConsumption(body map[string]any) *PrefetchConsumption {
	raw, _ := body["Consumption"].(map[string]any)
	if raw == nil {
		return nil
	}

	c := &PrefetchConsumption{}

	if f, ok := raw["StartTime"].(float64); ok {
		c.StartTime = epochSecondsToTime(f)
	}

	if f, ok := raw["EndTime"].(float64); ok {
		c.EndTime = epochSecondsToTime(f)
	}

	return c
}

func nilToEmpty(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return m
}

func nilToEmptyStrings(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}

func extractStringSlice(body map[string]any, key string) []string {
	raw, _ := body[key].([]any)
	if len(raw) == 0 {
		return nil
	}

	result := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}

	return result
}

func int64Field(m map[string]any, key string) int64 {
	f, _ := m[key].(float64)

	return int64(f)
}

func extractTimeShiftConfiguration(body map[string]any) *TimeShiftConfiguration {
	raw, _ := body["TimeShiftConfiguration"].(map[string]any)
	if raw == nil {
		return nil
	}

	sec, _ := raw["MaxTimeDelaySeconds"].(float64)

	return &TimeShiftConfiguration{MaxTimeDelaySeconds: int(sec)}
}

func extractKeyValuePairs(raw []any) []KeyValuePair {
	if len(raw) == 0 {
		return nil
	}

	out := make([]KeyValuePair, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, KeyValuePair{Key: stringField(m, "Key"), Value: stringField(m, "Value")})
	}

	return out
}

func extractMapSlice(raw []any) []map[string]any {
	if len(raw) == 0 {
		return nil
	}

	out := make([]map[string]any, 0, len(raw))

	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}

	return out
}

func extractClipRange(m map[string]any) *ClipRange {
	raw, _ := m["ClipRange"].(map[string]any)
	if raw == nil {
		return nil
	}

	return &ClipRange{
		StartOffsetMillis: int64Field(raw, "StartOffsetMillis"),
		EndOffsetMillis:   int64Field(raw, "EndOffsetMillis"),
	}
}

// int32Field reads key as a JSON number and truncates it to int32. The
// SCTE-35 counters it feeds (AvailNum, SpliceEventId, ...) are documented by
// the real SDK as small bounded values ("default value is 0", "must be
// between 0 and 256"), so a wider-than-int32 input is malformed input, not a
// security-relevant overflow.
//
//nolint:gosec // G115: bounded SCTE-35 counters, see comment above
func int32Field(m map[string]any, key string) int32 {
	return int32(int64Field(m, key))
}

func extractAdBreaks(body map[string]any) []AdBreak {
	raw, _ := body["AdBreaks"].([]any)
	if len(raw) == 0 {
		return nil
	}

	out := make([]AdBreak, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, extractAdBreak(m))
	}

	return out
}

func extractAdBreak(m map[string]any) AdBreak {
	ab := AdBreak{
		OffsetMillis:    int64Field(m, "OffsetMillis"),
		MessageType:     stringField(m, "MessageType"),
		AdBreakMetadata: extractKeyValuePairs(toAnySlice(m["AdBreakMetadata"])),
	}

	if slate, slateOK := m["Slate"].(map[string]any); slateOK {
		ab.Slate = &SlateSource{
			SourceLocationName: stringField(slate, keySourceLocationName),
			VodSourceName:      stringField(slate, keyVodSourceName),
		}
	}

	if sim, simOK := m["SpliceInsertMessage"].(map[string]any); simOK {
		ab.SpliceInsertMessage = &SpliceInsertMessage{
			AvailNum:        int32Field(sim, "AvailNum"),
			AvailsExpected:  int32Field(sim, "AvailsExpected"),
			SpliceEventID:   int32Field(sim, "SpliceEventId"),
			UniqueProgramID: int32Field(sim, "UniqueProgramId"),
		}
	}

	if tsm, tsmOK := m["TimeSignalMessage"].(map[string]any); tsmOK {
		ab.TimeSignalMessage = &TimeSignalMessage{
			SegmentationDescriptors: extractMapSlice(toAnySlice(tsm["SegmentationDescriptors"])),
		}
	}

	return ab
}

func toAnySlice(v any) []any {
	s, _ := v.([]any)

	return s
}

func extractAlternateMedia(raw []any) []AlternateMedia {
	if len(raw) == 0 {
		return nil
	}

	out := make([]AlternateMedia, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, AlternateMedia{
			SourceLocationName:       stringField(m, keySourceLocationName),
			VodSourceName:            stringField(m, keyVodSourceName),
			LiveSourceName:           stringField(m, keyLiveSourceName),
			ScheduledStartTimeMillis: int64Field(m, "ScheduledStartTimeMillis"),
			DurationMillis:           int64Field(m, "DurationMillis"),
			ClipRange:                extractClipRange(m),
			AdBreaks:                 extractAdBreaksFromList(toAnySlice(m["AdBreaks"])),
		})
	}

	return out
}

// extractAdBreaksFromList is extractAdBreaks's body, factored out so
// AlternateMedia (which nests AdBreaks under a raw []any rather than a
// top-level request body map) can reuse the same parsing logic.
func extractAdBreaksFromList(raw []any) []AdBreak {
	return extractAdBreaks(map[string]any{"AdBreaks": raw})
}

func extractAudienceMedia(body map[string]any) []AudienceMedia {
	raw, _ := body["AudienceMedia"].([]any)
	if len(raw) == 0 {
		return nil
	}

	out := make([]AudienceMedia, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, AudienceMedia{
			Audience:       stringField(m, "Audience"),
			AlternateMedia: extractAlternateMedia(toAnySlice(m["AlternateMedia"])),
		})
	}

	return out
}

func extractTransition(m map[string]any) Transition {
	return Transition{
		RelativePosition:         stringField(m, "RelativePosition"),
		Type:                     stringField(m, "Type"),
		RelativeProgram:          stringField(m, "RelativeProgram"),
		DurationMillis:           int64Field(m, "DurationMillis"),
		ScheduledStartTimeMillis: int64Field(m, "ScheduledStartTimeMillis"),
	}
}

func extractScheduleConfiguration(body map[string]any) *ScheduleConfiguration {
	raw, _ := body["ScheduleConfiguration"].(map[string]any)
	if raw == nil {
		return nil
	}

	sc := &ScheduleConfiguration{ClipRange: extractClipRange(raw)}

	if tr, ok := raw["Transition"].(map[string]any); ok {
		sc.Transition = extractTransition(tr)
	}

	return sc
}

func extractUpdateProgramScheduleConfiguration(body map[string]any) *UpdateProgramScheduleConfiguration {
	raw, _ := body["ScheduleConfiguration"].(map[string]any)
	if raw == nil {
		return nil
	}

	sc := &UpdateProgramScheduleConfiguration{ClipRange: extractClipRange(raw)}

	if tr, ok := raw["Transition"].(map[string]any); ok {
		sc.Transition = &UpdateProgramTransition{
			DurationMillis:           int64Field(tr, "DurationMillis"),
			ScheduledStartTimeMillis: int64Field(tr, "ScheduledStartTimeMillis"),
		}
	}

	return sc
}

// isExtraConfigHandledKey reports whether k is a PutPlaybackConfigurationInput
// member extractExtraConfig must NOT pass through, because
// handlePutPlaybackConfiguration already reads it individually (Name,
// AdDecisionServerUrl, VideoContentSourceUrl) or extractTags already reads
// it (keyTags).
func isExtraConfigHandledKey(k string) bool {
	switch k {
	case keyName, keyAdDecisionServerURL, keyVideoContentSourceURL, keyTags:
		return true
	default:
		return false
	}
}

// extractExtraConfig reads every PutPlaybackConfigurationInput member
// handlePutPlaybackConfiguration doesn't parse individually
// (AdConditioningConfiguration, AvailSuppression, Bumper, CdnConfiguration,
// DashConfiguration, ManifestProcessingRules, AdsPersonalizationConcurrency,
// AdsPersonalizationTimeouts, etc.) and stores/echoes them back verbatim
// without interpreting them. Real MediaTailor validates/consumes these
// during ad-decision-server calls and manifest personalization, which
// gopherstack's playback-configuration CRUD emulation does not perform;
// storing them as decoded-JSON pass-through preserves exact wire round-trip
// fidelity (what a client PUTs is exactly what a client GETs back) without
// hand-modeling every nested SCTE/ADS field. Excluding by the handled set
// rather than enumerating the pass-through set means a future SDK bump that
// adds another optional sub-config survives without a code change here.
func extractExtraConfig(body map[string]any) map[string]any {
	extra := make(map[string]any, len(body))

	for k, v := range body {
		if isExtraConfigHandledKey(k) {
			continue
		}

		extra[k] = v
	}

	if len(extra) == 0 {
		return nil
	}

	return extra
}
