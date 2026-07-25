package cloudwatch

import (
	"encoding/json"
	"fmt"
	"strings"
)

// knownDashboardWidgetTypes are the CloudWatch dashboard widget "type" values
// documented for the dashboard body JSON schema. An unrecognized type is a
// warning (the dashboard is still created, but that widget may not render), not
// a hard validation error.
//
//nolint:gochecknoglobals // read-only lookup table, mirrors a fixed AWS enum
var knownDashboardWidgetTypes = map[string]bool{
	"text":     true,
	"metric":   true,
	"log":      true,
	"alarm":    true,
	"explorer": true,
	"custom":   true,
}

// knownDashboardPeriodOverrides are the two documented values for the
// dashboard body's optional top-level "periodOverride" field.
//
//nolint:gochecknoglobals // read-only lookup table, mirrors a fixed AWS enum
var knownDashboardPeriodOverrides = map[string]bool{
	"auto":    true,
	"inherit": true,
}

// dashboardValidationHasErrors reports whether any message in msgs is an error
// (as opposed to a warning). PutDashboard fails the whole call when true.
func dashboardValidationHasErrors(msgs []DashboardValidationMessage) bool {
	for _, m := range msgs {
		if m.IsError {
			return true
		}
	}

	return false
}

// validateDashboardBody validates a PutDashboard DashboardBody against
// CloudWatch's documented dashboard JSON schema, returning zero or more
// DashboardValidationMessage entries (see DashboardValidationMessage.IsError
// for the error-vs-warning distinction). An empty result means the body is
// fully valid with nothing to report.
func validateDashboardBody(body string) []DashboardValidationMessage {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return []DashboardValidationMessage{{
			DataPath: "/",
			Message:  "DashboardBody parameter is required",
			IsError:  true,
		}}
	}

	dec := json.NewDecoder(strings.NewReader(trimmed))

	var raw any
	if err := dec.Decode(&raw); err != nil {
		return []DashboardValidationMessage{{
			DataPath: "/",
			Message:  fmt.Sprintf("The dashboard body is not valid JSON: %s", err.Error()),
			IsError:  true,
		}}
	}

	if dec.More() {
		return []DashboardValidationMessage{{
			DataPath: "/",
			Message:  "The dashboard body contains trailing content after the JSON value",
			IsError:  true,
		}}
	}

	root, ok := raw.(map[string]any)
	if !ok {
		return []DashboardValidationMessage{{
			DataPath: "/",
			Message:  "The dashboard body must be a JSON object",
			IsError:  true,
		}}
	}

	msgs := validateDashboardWidgetsField(root)
	msgs = append(msgs, validateDashboardPeriodOverride(root)...)

	return msgs
}

// validateDashboardWidgetsField validates the top-level "widgets" field, if present.
func validateDashboardWidgetsField(root map[string]any) []DashboardValidationMessage {
	widgetsRaw, hasWidgets := root["widgets"]
	if !hasWidgets {
		return nil
	}

	widgets, ok := widgetsRaw.([]any)
	if !ok {
		return []DashboardValidationMessage{{
			DataPath: "/widgets",
			Message:  "widgets must be an array",
			IsError:  true,
		}}
	}

	var msgs []DashboardValidationMessage
	for i, w := range widgets {
		msgs = append(msgs, validateDashboardWidget(i, w)...)
	}

	return msgs
}

// validateDashboardPeriodOverride validates the optional top-level "periodOverride" field.
func validateDashboardPeriodOverride(root map[string]any) []DashboardValidationMessage {
	poRaw, hasPO := root["periodOverride"]
	if !hasPO {
		return nil
	}

	s, isStr := poRaw.(string)
	if !isStr || !knownDashboardPeriodOverrides[s] {
		return []DashboardValidationMessage{{
			DataPath: "/periodOverride",
			Message:  `periodOverride must be one of: "auto", "inherit"`,
			IsError:  true,
		}}
	}

	return nil
}

// validateDashboardWidget validates a single entry of the top-level "widgets" array.
func validateDashboardWidget(idx int, w any) []DashboardValidationMessage {
	base := fmt.Sprintf("/widgets/%d", idx)

	obj, ok := w.(map[string]any)
	if !ok {
		return []DashboardValidationMessage{{
			DataPath: base,
			Message:  "widget entries must be JSON objects",
			IsError:  true,
		}}
	}

	msgs := validateDashboardWidgetType(base, obj)
	msgs = append(msgs, validateDashboardWidgetProperties(base, obj)...)
	msgs = append(msgs, validateDashboardWidgetLayout(base, obj)...)

	return msgs
}

// validateDashboardWidgetType validates a widget's required "type" field.
func validateDashboardWidgetType(base string, obj map[string]any) []DashboardValidationMessage {
	typeRaw, hasType := obj["type"]
	if !hasType {
		return []DashboardValidationMessage{{
			DataPath: base + "/type",
			Message:  "widget type is required",
			IsError:  true,
		}}
	}

	typeStr, isStr := typeRaw.(string)
	if !isStr || typeStr == "" {
		return []DashboardValidationMessage{{
			DataPath: base + "/type",
			Message:  "widget type must be a non-empty string",
			IsError:  true,
		}}
	}

	if !knownDashboardWidgetTypes[typeStr] {
		return []DashboardValidationMessage{{
			DataPath: base + "/type",
			Message:  fmt.Sprintf("unrecognized widget type %q; this widget may not render", typeStr),
			IsError:  false,
		}}
	}

	return nil
}

// validateDashboardWidgetProperties validates a widget's "properties" object,
// including metric-widget-specific checks for "properties.metrics".
func validateDashboardWidgetProperties(base string, obj map[string]any) []DashboardValidationMessage {
	propsRaw, hasProps := obj["properties"]
	if !hasProps {
		return []DashboardValidationMessage{{
			DataPath: base + "/properties",
			Message:  "widget is missing properties and may not render",
			IsError:  false,
		}}
	}

	typeStr, _ := obj["type"].(string)
	if typeStr != "metric" {
		return nil
	}

	props, ok := propsRaw.(map[string]any)
	if !ok {
		return []DashboardValidationMessage{{
			DataPath: base + "/properties",
			Message:  "widget properties must be a JSON object",
			IsError:  true,
		}}
	}

	metricsRaw, hasMetrics := props["metrics"]
	if !hasMetrics {
		return []DashboardValidationMessage{{
			DataPath: base + "/properties/metrics",
			Message:  "metric widget is missing properties.metrics and may not render",
			IsError:  false,
		}}
	}

	metrics, ok := metricsRaw.([]any)
	if !ok || len(metrics) == 0 {
		return []DashboardValidationMessage{{
			DataPath: base + "/properties/metrics",
			Message:  "metric widget properties.metrics must be a non-empty array",
			IsError:  false,
		}}
	}

	return nil
}

// validateDashboardWidgetLayout validates a widget's optional numeric
// layout fields (x, y, width, height).
func validateDashboardWidgetLayout(base string, obj map[string]any) []DashboardValidationMessage {
	var msgs []DashboardValidationMessage

	for _, field := range []string{"x", "y", "width", "height"} {
		v, present := obj[field]
		if !present {
			continue
		}

		if _, isNum := v.(float64); !isNum {
			msgs = append(msgs, DashboardValidationMessage{
				DataPath: base + "/" + field,
				Message:  field + " must be a number",
				IsError:  true,
			})
		}
	}

	return msgs
}
