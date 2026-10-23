// Package gremlin is an in-process Apache TinkerPop Gremlin server over the shared openCypher property graph.
//
// It speaks the Gremlin Server WebSocket protocol (GraphSON v2/v3 and GraphBinary v1) and the HTTP endpoint,
// and runs Gremlin scripts and bytecode against the same nodes and relationships as openCypher and Bolt.
package gremlin

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

// Token is a TinkerPop enum value such as T.id, Order.desc or Direction.OUT.
type Token struct {
	Kind string
	Name string
}

// Token kinds.
const (
	kindT           = "T"
	kindOrder       = "Order"
	kindDirection   = "Direction"
	kindScope       = "Scope"
	kindColumn      = "Column"
	kindPop         = "Pop"
	kindCardinality = "Cardinality"
	kindOperator    = "Operator"
	kindMerge       = "Merge"
	kindWithOptions = "WithOptions"
)

// Pred is a P or TextP predicate tree.
type Pred struct {
	Op       string
	Args     []any
	Children []Pred
}

// Prop is a vertex property, a meta view of an edge property, or a property key/value.
type Prop struct {
	Owner any
	Val   any
	Key   string
	VP    bool
}

// ID is the stable, derived id of a vertex property.
func (p *Prop) ID() int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(elementID(p.Owner) + "\x00" + p.Key))

	return int64(h.Sum64() >> 1)
}

// Map is an insertion-ordered map whose keys may be any value.
type Map struct {
	keys []any
	vals []any
}

// NewMap returns an empty Map.
func NewMap() *Map { return &Map{} }

// Len is the number of entries.
func (m *Map) Len() int { return len(m.keys) }

// Entry returns the i-th key and value.
func (m *Map) Entry(i int) (any, any) { return m.keys[i], m.vals[i] }

// Put sets key to val, replacing an equal key.
func (m *Map) Put(k, v any) {
	for i, e := range m.keys {
		if equalValues(e, k) {
			m.vals[i] = v

			return
		}
	}

	m.keys = append(m.keys, k)
	m.vals = append(m.vals, v)
}

// Get returns the value for k.
func (m *Map) Get(k any) (any, bool) {
	for i, e := range m.keys {
		if equalValues(e, k) {
			return m.vals[i], true
		}
	}

	return nil, false
}

// Set is an ordered collection with set semantics on the wire.
type Set []any

// Path is a traversal path.
type Path struct {
	Labels  [][]string
	Objects []any
}

func elementID(v any) string {
	switch t := v.(type) {
	case *opencypher.Node:
		return t.ID
	case *opencypher.Rel:
		return t.ID
	case *Prop:
		return strconv.FormatInt(t.ID(), 10)
	}

	return ""
}

func vertexLabel(n *opencypher.Node) string {
	if len(n.Labels) == 0 {
		return "vertex"
	}

	return strings.Join(n.Labels, "::")
}

func isNumber(v any) bool {
	switch v.(type) {
	case int64, int, int32, float64, float32:
		return true
	}

	return false
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case int64:
		return float64(t)
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case float32:
		return float64(t)
	case float64:
		return t
	}

	return math.NaN()
}

func isInt(v any) bool {
	switch v.(type) {
	case int64, int, int32:
		return true
	}

	return false
}

func toInt(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case int32:
		return int64(t)
	}

	return int64(toFloat(v))
}

// normalizeNumber folds the numeric Go types to int64 and float64, the types the graph stores.
func normalizeNumber(v any) any {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int16:
		return int64(t)
	case int8:
		return int64(t)
	case uint8:
		return int64(t)
	case float32:
		return float64(t)
	}

	return v
}

func equalValues(a, b any) bool {
	if isNumber(a) && isNumber(b) {
		if isInt(a) && isInt(b) {
			return toInt(a) == toInt(b)
		}

		return toFloat(a) == toFloat(b)
	}

	return keyOf(a) == keyOf(b)
}

// compareValues orders numbers, strings and booleans; ok is false for incomparable operands.
func compareValues(a, b any) (int, bool) {
	if isNumber(a) && isNumber(b) {
		return compareNumbers(a, b), true
	}

	if as, ok := a.(string); ok {
		bs, bok := b.(string)

		return strings.Compare(as, bs), bok
	}

	ab, aok := a.(bool)
	bb, bok := b.(bool)

	if aok && bok {
		return compareBools(ab, bb), true
	}

	return 0, false
}

func compareNumbers(a, b any) int {
	if isInt(a) && isInt(b) {
		x, y := toInt(a), toInt(b)

		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}

		return 0
	}

	x, y := toFloat(a), toFloat(b)

	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}

	return 0
}

func compareBools(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	}

	return 1
}

// keyOf is a canonical string for dedup, grouping and equality of arbitrary values.
func keyOf(v any) string {
	switch t := v.(type) {
	case nil:
		return "n"
	case string:
		return "s:" + t
	case bool:
		return "b:" + strconv.FormatBool(t)
	case *opencypher.Node:
		return "v:" + t.ID
	case *opencypher.Rel:
		return "e:" + t.ID
	case *Prop:
		return "p:" + elementID(t.Owner) + "/" + t.Key
	case Token:
		return "t:" + t.Kind + "." + t.Name
	case []any:
		return "l:" + joinKeys(t)
	case Set:
		return "S:" + joinKeys(t)
	case *Map:
		return "m:" + mapKey(t)
	case *Path:
		return "P:" + joinKeys(t.Objects)
	}

	if isInt(v) {
		return "i:" + strconv.FormatInt(toInt(v), 10)
	}

	if isNumber(v) {
		return "f:" + strconv.FormatFloat(toFloat(v), 'g', -1, 64)
	}

	return fmt.Sprintf("o:%v", v)
}

func joinKeys(vs []any) string {
	parts := make([]string, len(vs))
	for i, e := range vs {
		parts[i] = keyOf(e)
	}

	return "[" + strings.Join(parts, ",") + "]"
}

func mapKey(m *Map) string {
	parts := make([]string, m.Len())
	for i := range parts {
		k, v := m.Entry(i)
		parts[i] = keyOf(k) + "=" + keyOf(v)
	}

	sort.Strings(parts)

	return "{" + strings.Join(parts, ",") + "}"
}

// idString renders an id the way the graph stores it.
func idString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case *opencypher.Node:
		return t.ID
	case *opencypher.Rel:
		return t.ID
	}

	if isInt(v) {
		return strconv.FormatInt(toInt(v), 10)
	}

	return fmt.Sprint(v)
}

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
