package azureeventgrid

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// MappingField maps one Event Grid event property from a field of the published event; DefaultValue is used when
// SourceField is unset or the event has no such property.
type MappingField struct {
	SourceField  string `json:"sourceField,omitempty"`
	DefaultValue string `json:"defaultValue,omitempty"`
}

// InputMapping is a custom topic's JSON input schema mapping.
type InputMapping struct {
	ID          MappingField `json:"id"`
	Topic       MappingField `json:"topic"`
	EventTime   MappingField `json:"eventTime"`
	Subject     MappingField `json:"subject"`
	EventType   MappingField `json:"eventType"`
	DataVersion MappingField `json:"dataVersion"`
}

const (
	mappingTypeJSON = "Json"
	fieldData       = "data"
)

func mappingField(props map[string]any, key string, withDefault bool) (MappingField, error) {
	raw, ok := props[key]
	if !ok || raw == nil {
		return MappingField{}, nil
	}

	obj, isObj := raw.(map[string]any)
	if !isObj {
		return MappingField{}, invalidSpec("inputSchemaMapping.properties.%s must be an object", key)
	}

	src, _ := obj["sourceField"].(string)
	def, _ := obj["defaultValue"].(string)

	if !withDefault && def != "" {
		return MappingField{}, invalidSpec("inputSchemaMapping.properties.%s takes only a sourceField", key)
	}

	if withDefault && src == "" && def == "" {
		return MappingField{}, invalidSpec(
			"inputSchemaMapping.properties.%s needs a sourceField or a defaultValue", key)
	}

	return MappingField{SourceField: src, DefaultValue: def}, nil
}

// ParseInputMapping reads the ARM inputSchemaMapping object (JsonInputSchemaMapping); nil in, nil out.
func ParseInputMapping(raw map[string]any) (*InputMapping, error) {
	if len(raw) == 0 {
		return nil, nil //nolint:nilnil // absent mapping
	}

	if t, _ := raw["inputSchemaMappingType"].(string); t != "" && t != mappingTypeJSON {
		return nil, invalidSpec("inputSchemaMappingType must be %s", mappingTypeJSON)
	}

	props, _ := raw["properties"].(map[string]any)
	m := &InputMapping{}

	for _, f := range []struct {
		dst         *MappingField
		key         string
		withDefault bool
	}{
		{&m.ID, "id", false}, {&m.Topic, propTopic, false}, {&m.EventTime, "eventTime", false},
		{&m.Subject, "subject", true}, {&m.EventType, "eventType", true}, {&m.DataVersion, "dataVersion", true},
	} {
		v, err := mappingField(props, f.key, f.withDefault)
		if err != nil {
			return nil, err
		}

		*f.dst = v
	}

	return m, nil
}

func scalarString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(x), true
	}

	return "", false
}

// resolve returns the target value: the source field, else the default, else the same-named property.
func (f MappingField) resolve(fields map[string]any, target string) (string, bool) {
	if f.SourceField != "" {
		if s, ok := scalarString(fields[f.SourceField]); ok && s != "" {
			return s, true
		}
	}

	if f.DefaultValue != "" {
		return f.DefaultValue, true
	}

	if f.SourceField == "" {
		if s, ok := scalarString(fields[target]); ok && s != "" {
			return s, true
		}
	}

	return "", false
}

// mapEvent builds the Event Grid event for a custom-schema payload; the payload becomes its data.
func (m *InputMapping) mapEvent(ev Event, topicRID string, now time.Time) (Event, error) {
	if m == nil {
		m = &InputMapping{}
	}

	out := map[string]any{fieldData: ev.Fields, "metadataVersion": "1", propTopic: topicRID}

	for _, f := range []struct {
		field    MappingField
		target   string
		required bool
	}{
		{m.ID, "id", false}, {m.Topic, "topic", false}, {m.EventTime, "eventTime", false},
		{m.Subject, "subject", true}, {m.EventType, "eventType", true}, {m.DataVersion, "dataVersion", true},
	} {
		v, ok := f.field.resolve(ev.Fields, f.target)

		switch {
		case ok:
			out[f.target] = v
		case f.required:
			return Event{}, invalidEvent("required property '%s' not found in JSON", f.target)
		}
	}

	if _, ok := out["id"]; !ok {
		out["id"] = uuid.NewString()
	}

	if v, ok := out["eventTime"].(string); !ok {
		out["eventTime"] = now.Format(time.RFC3339Nano)
	} else if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
		return Event{}, invalidEvent("property 'eventTime' is not a valid date and time")
	}

	raw, err := json.Marshal(out)
	if err != nil {
		return Event{}, invalidEvent("the event cannot be encoded")
	}

	id, _ := out["id"].(string)

	return Event{Fields: out, ID: id, Raw: raw, Original: ev.Raw}, nil
}

// ParseCustomEvents decodes a publish body for a CustomEventSchema topic: any JSON object is an event.
func ParseCustomEvents(body []byte) ([]Event, error) {
	raws, err := splitEvents(body)
	if err != nil {
		return nil, err
	}

	out := make([]Event, 0, len(raws))

	for i, raw := range raws {
		var fields map[string]any
		if json.Unmarshal(raw, &fields) != nil || fields == nil {
			return nil, fmt.Errorf("event %d: %w", i, invalidEvent("the event is not a JSON object"))
		}

		out = append(out, Event{Fields: fields, Raw: raw})
	}

	return out, nil
}
