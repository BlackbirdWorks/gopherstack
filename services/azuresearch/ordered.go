package azuresearch

import (
	"bytes"
	"encoding/json"
)

// ordered is a JSON object that keeps insertion order, as the service lists fields in schema order.
type ordered struct {
	vals map[string]any
	keys []string
}

func newOrdered() *ordered { return &ordered{vals: map[string]any{}} }

func (o *ordered) set(k string, v any) {
	if _, ok := o.vals[k]; !ok {
		o.keys = append(o.keys, k)
	}

	o.vals[k] = v
}

// MarshalJSON implements json.Marshaler.
func (o *ordered) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte('{')

	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}

		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}

		vb, err := json.Marshal(o.vals[k])
		if err != nil {
			return nil, err
		}

		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(vb)
	}

	buf.WriteByte('}')

	return buf.Bytes(), nil
}
