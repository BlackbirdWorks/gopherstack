package rum

import (
	"slices"
	"sort"
)

const (
	evtNavigation = "com.amazon.rum.performance_navigation_event"
	evtResource   = "com.amazon.rum.performance_resource_event"
	evtCLS        = "com.amazon.rum.cumulative_layout_shift_event"
	evtFID        = "com.amazon.rum.first_input_delay_event"
	evtLCP        = "com.amazon.rum.largest_contentful_paint_event"
	evtJSError    = "com.amazon.rum.js_error_event"
	evtHTTP       = "com.amazon.rum.http_event"
	evtSession    = "com.amazon.rum.session_start_event"
	evtPageView   = "com.amazon.rum.page_view_event"

	namespaceRUM       = "AWS/RUM"
	namespaceCustom    = "RUM/CustomMetrics/"
	dimApplicationName = "application_name"

	keyEventDetails = "event_details"
	keyEventType    = "event_type"
	keyDuration     = "duration"
	keyResponse     = "response"
	keyValue        = "value"

	pathDetailsDuration = keyEventDetails + "." + keyDuration
	pathDetailsValue    = keyEventDetails + "." + keyValue

	thresholdSatisfied  = 2000
	thresholdFrustrated = 8000
	status4xxMin        = 400
	status5xxMin        = 500
	status5xxMax        = 599

	unitCount        = "Count"
	unitMilliseconds = "Milliseconds"
	unitNone         = "None"
)

// extendedSpec describes one extended metric: the event it counts and the value it reads.
type extendedSpec struct {
	pattern         map[string]any
	eventType       string
	valueKey        string
	unit            string
	detailsRequired []string
}

func numericPattern(eventType string, details map[string]any) map[string]any {
	return map[string]any{keyEventType: []any{eventType}, keyEventDetails: details}
}

func eventOnly(eventType string) map[string]any {
	return map[string]any{keyEventType: []any{eventType}}
}

func duration(rules ...any) map[string]any {
	return map[string]any{keyDuration: []any{map[string]any{opNumeric: rules}}}
}

func status(rules ...any) map[string]any {
	return map[string]any{keyResponse: map[string]any{"status": []any{map[string]any{opNumeric: rules}}}}
}

var extendedSpecs = map[string]extendedSpec{ //nolint:gochecknoglobals // immutable lookup table
	"PerformanceNavigationDuration": {
		eventType: evtNavigation, valueKey: pathDetailsDuration, unit: unitMilliseconds,
		pattern: eventOnly(evtNavigation),
	},
	"PerformanceResourceDuration": {
		eventType: evtResource, valueKey: pathDetailsDuration, unit: unitMilliseconds,
		pattern: eventOnly(evtResource),
	},
	"NavigationSatisfiedTransaction": {
		eventType: evtNavigation, unit: unitCount, detailsRequired: []string{keyDuration},
		pattern: numericPattern(evtNavigation, duration(">", thresholdSatisfied)),
	},
	"NavigationToleratedTransaction": {
		eventType: evtNavigation, unit: unitCount, detailsRequired: []string{keyDuration},
		pattern: numericPattern(evtNavigation, duration(">=", thresholdSatisfied, "<", thresholdFrustrated)),
	},
	"NavigationFrustratedTransaction": {
		eventType: evtNavigation, unit: unitCount, detailsRequired: []string{keyDuration},
		pattern: numericPattern(evtNavigation, duration(">=", thresholdFrustrated)),
	},
	"WebVitalsCumulativeLayoutShift": {
		eventType: evtCLS, valueKey: pathDetailsValue, unit: unitNone, pattern: eventOnly(evtCLS),
	},
	"WebVitalsFirstInputDelay": {
		eventType: evtFID, valueKey: pathDetailsValue, unit: unitMilliseconds, pattern: eventOnly(evtFID),
	},
	"WebVitalsLargestContentfulPaint": {
		eventType: evtLCP, valueKey: pathDetailsValue, unit: unitMilliseconds, pattern: eventOnly(evtLCP),
	},
	"JsErrorCount":   {eventType: evtJSError, unit: unitCount, pattern: eventOnly(evtJSError)},
	"HttpErrorCount": {eventType: evtHTTP, unit: unitCount, pattern: eventOnly(evtHTTP)},
	"SessionCount":   {eventType: evtSession, unit: unitCount, pattern: eventOnly(evtSession)},
	"PageViewCount":  {eventType: evtPageView, unit: unitCount, pattern: eventOnly(evtPageView)},
	"Http4xxCount": {
		eventType: evtHTTP, unit: unitCount, detailsRequired: []string{keyResponse},
		pattern: numericPattern(evtHTTP, status(">=", status4xxMin, "<", status5xxMin)),
	},
	"Http5xxCount": {
		eventType: evtHTTP, unit: unitCount, detailsRequired: []string{keyResponse},
		pattern: numericPattern(evtHTTP, status(">=", status5xxMin, "<=", status5xxMax)),
	},
}

// extendedDimensions maps the event field path to the CloudWatch dimension for extended metrics.
var extendedDimensions = map[string]string{ //nolint:gochecknoglobals // immutable lookup table
	"metadata.pageId":        "PageId",
	"metadata.browserName":   "BrowserName",
	"metadata.deviceType":    "DeviceType",
	"metadata.osName":        "OSName",
	"metadata.countryCode":   "CountryCode",
	"event_details.fileType": "FileType",
}

var topLevelKeys = []string{ //nolint:gochecknoglobals // immutable lookup table
	"account_id", "application_Id", "application_version", "application_name", "batch_id", "event_details",
	"event_id", "event_interaction", "event_timestamp", "event_type", "event_version", "log_stream", "metadata",
	"sessionId", "user_details", "userId",
}

func extendedNames() []string {
	names := make([]string, 0, len(extendedSpecs))
	for n := range extendedSpecs {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}

func validTopLevel(path string) bool {
	first := path
	for i := range path {
		if path[i] == '.' {
			first = path[:i]

			break
		}
	}

	return slices.Contains(topLevelKeys, first)
}

const (
	telemetryErrors      = "errors"
	telemetryPerformance = "performance"
	telemetryHTTP        = "http"
)

// telemetryOfEvent maps an event type to the AppMonitorConfiguration telemetry that gates it
// (errors: unhandled JS errors, performance: load/render data incl. Core Web Vitals, http: HTTP errors);
// other event types are not gated.
var telemetryOfEvent = map[string]string{ //nolint:gochecknoglobals // immutable lookup table
	evtJSError:    telemetryErrors,
	evtNavigation: telemetryPerformance,
	evtResource:   telemetryPerformance,
	evtCLS:        telemetryPerformance,
	evtFID:        telemetryPerformance,
	evtLCP:        telemetryPerformance,
	evtHTTP:       telemetryHTTP,
}

// collects reports whether the monitor's Telemetries allow the event type; no Telemetries means no gating.
func (m *AppMonitor) collects(eventType string) bool {
	if m.Config == nil || len(m.Config.Telemetries) == 0 {
		return true
	}

	t, gated := telemetryOfEvent[eventType]

	return !gated || slices.Contains(m.Config.Telemetries, t)
}
