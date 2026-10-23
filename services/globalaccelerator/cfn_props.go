package globalaccelerator

import (
	"errors"
	"maps"
	"math"
	"strconv"
)

func isErr(err, target error) bool { return errors.Is(err, target) }

func cloneTags(m map[string]string) map[string]string { return maps.Clone(m) }

func cfnString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int:
		return strconv.Itoa(t)
	}

	return ""
}

func cfnOptString(v any) *string {
	if v == nil {
		return nil
	}

	s := cfnString(v)

	return &s
}

func cfnBool(v any) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		b, err := strconv.ParseBool(t)

		return b, err == nil
	}

	return false, false
}

func cfnFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(t, 64)

		return f, err == nil
	}

	return 0, false
}

func cfnOptInt32(v any) *int32 {
	f, ok := cfnFloat(v)
	if !ok {
		return nil
	}

	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}

	// bitSize 32 rejects out-of-range values and is the bound CodeQL recognizes.
	i, err := strconv.ParseInt(strconv.FormatFloat(math.Trunc(f), 'f', 0, 64), 10, 32)
	if err != nil {
		return nil
	}

	n := int32(i)

	return &n
}

func cfnOptFloat32(v any) *float32 {
	f, ok := cfnFloat(v)
	if !ok {
		return nil
	}

	n := float32(f)

	return &n
}

func cfnInt32(v any) int32 {
	if p := cfnOptInt32(v); p != nil {
		return *p
	}

	return 0
}

func cfnStrings(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))

	for _, e := range list {
		out = append(out, cfnString(e))
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func cfnMaps(v any) []map[string]any {
	list, _ := v.([]any)
	out := make([]map[string]any, 0, len(list))

	for _, e := range list {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m)
		}
	}

	return out
}

func cfnTags(v any) map[string]string {
	out := map[string]string{}

	for _, m := range cfnMaps(v) {
		out[cfnString(m["Key"])] = cfnString(m["Value"])
	}

	return out
}

func cfnPortRanges(v any) []PortRange {
	items := cfnMaps(v)
	out := make([]PortRange, 0, len(items))

	for _, m := range items {
		out = append(out, PortRange{FromPort: cfnInt32(m["FromPort"]), ToPort: cfnInt32(m["ToPort"])})
	}

	return out
}

func cfnPortOverrides(v any) []PortOverride {
	items := cfnMaps(v)
	out := make([]PortOverride, 0, len(items))

	for _, m := range items {
		out = append(out, PortOverride{
			ListenerPort: cfnInt32(m["ListenerPort"]), EndpointPort: cfnInt32(m["EndpointPort"]),
		})
	}

	return out
}

func cfnEndpointConfigs(v any) []EndpointConfig {
	items := cfnMaps(v)
	out := make([]EndpointConfig, 0, len(items))

	for _, m := range items {
		c := EndpointConfig{
			EndpointID:    cfnString(m["EndpointId"]),
			AttachmentARN: cfnString(m["AttachmentArn"]),
			Weight:        cfnOptInt32(m["Weight"]),
		}
		if p, ok := cfnBool(m["ClientIPPreservationEnabled"]); ok {
			c.ClientIPPreservation = &p
		}

		out = append(out, c)
	}

	return out
}

func cfnResources(v any) []Resource {
	items := cfnMaps(v)
	out := make([]Resource, 0, len(items))

	for _, m := range items {
		out = append(out, Resource{
			EndpointID: cfnString(m["EndpointId"]), Region: cfnString(m["Region"]), Cidr: cfnString(m["Cidr"]),
		})
	}

	return out
}
