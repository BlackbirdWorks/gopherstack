package azureeventgrid

import (
	"encoding/json"
	"maps"
	"net/http"
	"strings"
)

const (
	propMaxEventsPerBatch  = "maxEventsPerBatch"
	propPreferredBatchKB   = "preferredBatchSizeInKilobytes"
	propAttrMappings       = "deliveryAttributeMappings"
	attrStatic             = "Static"
	attrDynamic            = "Dynamic"
	defaultMaxEventsBatch  = 1
	maxEventsPerBatchLimit = 5000
	defaultBatchKB         = 64
	maxBatchKB             = 1024
	bytesPerKB             = 1024
)

type attrMapping struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Properties struct {
		Value       *string `json:"value"`
		SourceField string  `json:"sourceField"`
		IsSecret    bool    `json:"isSecret"`
	} `json:"properties"`
}

func (d Destination) mappings() ([]attrMapping, error) {
	raw, ok := d.Properties[propAttrMappings]
	if !ok || raw == nil {
		return nil, nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, invalidSpec("deliveryAttributeMappings is not valid JSON")
	}

	var out []attrMapping
	if err = json.Unmarshal(data, &out); err != nil {
		return nil, invalidSpec("deliveryAttributeMappings is malformed: %v", err)
	}

	return out, nil
}

func validateBatching(d Destination) error {
	limits := map[string]int{propMaxEventsPerBatch: maxEventsPerBatchLimit, propPreferredBatchKB: maxBatchKB}

	for key, limit := range limits {
		v, ok := d.Properties[key]
		if !ok || v == nil {
			continue
		}

		f, isNum := toFloat(v)
		if !isNum || f < 1 || int(f) > limit {
			return invalidSpec("%s must be between 1 and %d", key, limit)
		}
	}

	return nil
}

func validateMappings(d Destination) error {
	ms, err := d.mappings()
	if err != nil {
		return err
	}

	for _, m := range ms {
		if !validHeaderName(m.Name) {
			return invalidSpec("delivery attribute %q is not a valid header name", m.Name)
		}

		switch {
		case strings.EqualFold(m.Type, attrStatic) && m.Properties.Value != nil:
		case strings.EqualFold(m.Type, attrDynamic) && m.Properties.SourceField != "":
		default:
			return invalidSpec("delivery attribute %q needs a Static value or a Dynamic sourceField", m.Name)
		}
	}

	return nil
}

func (d Destination) batchLimits() (int, int) {
	events, kb := defaultMaxEventsBatch, defaultBatchKB

	if v, ok := toFloat(d.Properties[propMaxEventsPerBatch]); ok && v >= 1 {
		events = int(v)
	}

	if v, ok := toFloat(d.Properties[propPreferredBatchKB]); ok && v >= 1 {
		kb = int(v)
	}

	return events, kb * bytesPerKB
}

// webhookHeaders renders the delivery attribute mappings as headers; a Dynamic
// mapping reads the first event of the request.
func (d Destination) webhookHeaders(first []byte) http.Header {
	ms, _ := d.mappings()
	if len(ms) == 0 {
		return nil
	}

	fields := decodeFields(first)
	h := http.Header{}

	for _, m := range ms {
		switch {
		case strings.EqualFold(m.Type, attrStatic) && m.Properties.Value != nil:
			h.Set(m.Name, *m.Properties.Value)
		case strings.EqualFold(m.Type, attrDynamic):
			if v, ok := lookup(fields, m.Properties.SourceField); ok && v != nil {
				h.Set(m.Name, headerValue(v))
			}
		}
	}

	return h
}

func headerValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}

	raw, _ := json.Marshal(v)

	return string(raw)
}

// withholdSecrets copies the mappings of a destination's read-back properties
// without the values of secret static attributes.
func withholdSecrets(props map[string]any) {
	list, ok := props[propAttrMappings].([]any)
	if !ok {
		return
	}

	out := make([]any, 0, len(list))

	for _, item := range list {
		m, isMap := item.(map[string]any)
		p, _ := m["properties"].(map[string]any)

		if isMap && p != nil && p["isSecret"] == true {
			m = maps.Clone(m)
			p = maps.Clone(p)
			delete(p, "value")
			m["properties"] = p
			item = m
		}

		out = append(out, item)
	}

	props[propAttrMappings] = out
}

// DeliveryAttributes returns an event subscription's delivery attribute
// mappings with secret values included (getDeliveryAttributes).
func (b *InMemoryBackend) DeliveryAttributes(sc Scope, name string) ([]map[string]any, error) {
	b.mu.RLock("DeliveryAttributes")
	defer b.mu.RUnlock()

	_, s, err := b.subAtLocked(sc, name)
	if err != nil {
		return nil, err
	}

	raw, ok := s.Spec.Destination.Properties[propAttrMappings].([]any)
	out := make([]map[string]any, 0, len(raw))

	if ok {
		for _, item := range raw {
			if m, isMap := item.(map[string]any); isMap {
				out = append(out, maps.Clone(m))
			}
		}
	}

	return out, nil
}

func validHeaderName(n string) bool {
	if n == "" {
		return false
	}

	for _, r := range n {
		ok := r == '-' || r == '_' || r == '.' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z')
		if !ok {
			return false
		}
	}

	return true
}
