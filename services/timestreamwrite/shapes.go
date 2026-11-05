package timestreamwrite

import (
	"sort"
	"strings"
)

// RecordShape is the schema footprint of an accepted record: its measure, value type and the
// dimension and multi-measure attribute names it carried. Shapes outlive the records themselves.
type RecordShape struct {
	MeasureName      string         `json:"measure_name"`
	MeasureValueType string         `json:"measure_value_type"`
	Dimensions       []string       `json:"dimensions,omitempty"`
	Attributes       []ShapeMeasure `json:"attributes,omitempty"`
}

// ShapeMeasure is one named, typed attribute of a MULTI record shape.
type ShapeMeasure struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func shapeOf(r Record) RecordShape {
	sh := RecordShape{MeasureName: r.MeasureName, MeasureValueType: strings.ToUpper(r.MeasureValueType)}

	for _, d := range r.Dimensions {
		sh.Dimensions = append(sh.Dimensions, d.Name)
	}

	for _, m := range r.MeasureValues {
		sh.Attributes = append(sh.Attributes, ShapeMeasure{Name: m.Name, Type: strings.ToUpper(m.Type)})
	}

	return sh
}

func (s RecordShape) key() string {
	dims := append([]string(nil), s.Dimensions...)
	sort.Strings(dims)

	attrs := make([]string, 0, len(s.Attributes))
	for _, a := range s.Attributes {
		attrs = append(attrs, a.Name+"\x01"+a.Type)
	}

	sort.Strings(attrs)

	return strings.Join([]string{s.MeasureName, s.MeasureValueType, strings.Join(dims, "\x00"), strings.Join(attrs, "\x00")}, "\x02")
}

// noteShape remembers r's shape in the slot. Caller holds the slot write lock.
func (t *tableRecords) noteShape(r Record) {
	if t.shapeIndex == nil {
		t.shapeIndex = make(map[string]struct{})
	}

	sh := shapeOf(r)
	k := sh.key()

	if _, ok := t.shapeIndex[k]; ok {
		return
	}

	t.shapeIndex[k] = struct{}{}
	t.shapes = append(t.shapes, sh)
}

// rebuildShapes seeds shapes from restored shapes plus the retained records.
func (t *tableRecords) rebuildShapes(saved []RecordShape) {
	t.shapes, t.shapeIndex = nil, make(map[string]struct{})

	for _, sh := range saved {
		if _, ok := t.shapeIndex[sh.key()]; !ok {
			t.shapeIndex[sh.key()] = struct{}{}
			t.shapes = append(t.shapes, sh)
		}
	}

	for _, r := range t.records {
		t.noteShape(r)
	}
}

// TableShapes returns every record shape ever accepted by the table, in first-seen order.
func (b *InMemoryBackend) TableShapes(dbName, tblName string) []RecordShape {
	b.mu.RLock("TableShapes")
	defer b.mu.RUnlock()

	slot := b.records[dbName][tblName]
	if slot == nil {
		return nil
	}

	slot.mu.RLock("TableShapes")
	defer slot.mu.RUnlock()

	return append([]RecordShape(nil), slot.shapes...)
}
