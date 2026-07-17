package mediatailor

import (
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
)

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

	baseURL, _ := httpConfig["BaseUrl"].(string)

	return baseURL
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
	raw, _ := body["HttpPackageConfigurations"].([]any)
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
		SourceLocationName: stringField(raw, "SourceLocationName"),
		VodSourceName:      stringField(raw, "VodSourceName"),
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
