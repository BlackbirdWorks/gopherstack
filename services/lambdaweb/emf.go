package lambdaweb

import (
	"encoding/json/v2"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const emfMarker = `"_aws"`

type emfMetric struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

type emfDirective struct {
	Namespace  string      `json:"Namespace"`
	Dimensions [][]string  `json:"Dimensions"`
	Metrics    []emfMetric `json:"Metrics"`
}

type emfEnvelope struct {
	CloudWatchMetrics []emfDirective `json:"CloudWatchMetrics"`
}

// parseEMF extracts the metric points an embedded-metric-format line declares; ok is false for any other line.
func parseEMF(line string) ([]cwmetric.Point, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "{") || !strings.Contains(trimmed, emfMarker) {
		return nil, false
	}

	var root map[string]any
	if json.Unmarshal([]byte(trimmed), &root, jsonx.V1()) != nil {
		return nil, false
	}

	raw, ok := root["_aws"].(map[string]any)
	if !ok {
		return nil, false
	}

	encoded, err := json.Marshal(raw, jsonx.V1())
	if err != nil {
		return nil, false
	}

	var env emfEnvelope
	if json.Unmarshal(encoded, &env, jsonx.V1()) != nil || len(env.CloudWatchMetrics) == 0 {
		return nil, false
	}

	var points []cwmetric.Point

	for _, d := range env.CloudWatchMetrics {
		points = append(points, directivePoints(d, root)...)
	}

	return points, true
}

func directivePoints(d emfDirective, root map[string]any) []cwmetric.Point {
	if d.Namespace == "" {
		return nil
	}

	sets := d.Dimensions
	if len(sets) == 0 {
		sets = [][]string{nil}
	}

	var out []cwmetric.Point

	for _, set := range sets {
		dims, ok := emfDimensions(set, root)
		if !ok {
			continue
		}

		for _, m := range d.Metrics {
			for _, v := range emfValues(root[m.Name]) {
				unit := m.Unit
				if unit == "" {
					unit = "None"
				}

				out = append(out, cwmetric.Point{
					Namespace: d.Namespace, Name: m.Name, Unit: unit, Value: v, Dimensions: dims,
				})
			}
		}
	}

	return out
}

func emfDimensions(names []string, root map[string]any) ([]cwmetric.Dimension, bool) {
	dims := make([]cwmetric.Dimension, 0, len(names))

	for _, n := range names {
		v, ok := root[n].(string)
		if !ok {
			return nil, false
		}

		dims = append(dims, cwmetric.Dimension{Name: n, Value: v})
	}

	return dims, true
}

// emfValues reads a metric member: one number, or an array of numbers each recorded as an observation.
func emfValues(v any) []float64 {
	switch t := v.(type) {
	case float64:
		return []float64{t}
	case []any:
		out := make([]float64, 0, len(t))

		for _, x := range t {
			if f, ok := x.(float64); ok {
				out = append(out, f)
			}
		}

		return out
	default:
		return nil
	}
}
