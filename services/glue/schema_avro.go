package glue

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

var errAvroSchema = errors.New("invalid AVRO schema")

// Avro resolution: https://avro.apache.org/docs/current/specification/#schema-resolution.
// A missing writer field is also accepted when the reader field is a union with null (Glue docs).

const (
	avroNull    = "null"
	avroBoolean = "boolean"
	avroInt     = "int"
	avroLong    = "long"
	avroFloat   = "float"
	avroDouble  = "double"
	avroBytes   = "bytes"
	avroString  = "string"
	avroUnion   = "union"
	avroRecord  = "record"
	avroEnum    = "enum"
	avroFixed   = "fixed"
	avroArray   = "array"
	avroMap     = "map"
)

type avroNode struct {
	items    *avroNode
	values   *avroNode
	kind     string
	name     string
	union    []*avroNode
	fields   []avroField
	symbols  []string
	aliases  []string
	size     float64
	enumDflt bool
}

type avroField struct {
	typ     *avroNode
	name    string
	aliases []string
	hasDef  bool
}

type avroParser struct {
	named map[string]*avroNode
}

func avroIsPrimitive(t string) bool {
	switch t {
	case avroNull, avroBoolean, avroInt, avroLong, avroFloat, avroDouble, avroBytes, avroString:
		return true
	}

	return false
}

func parseAvro(def string) (*avroNode, error) {
	var v any
	if err := json.Unmarshal([]byte(def), &v); err != nil {
		return nil, fmt.Errorf("schema is not valid JSON: %w", err)
	}

	p := &avroParser{named: map[string]*avroNode{}}

	return p.parse(v)
}

func (p *avroParser) parse(v any) (*avroNode, error) {
	switch t := v.(type) {
	case string:
		if avroIsPrimitive(t) {
			return &avroNode{kind: t}, nil
		}

		if n, ok := p.named[t]; ok {
			return n, nil
		}

		return nil, fmt.Errorf("%w: AVRO schema references undefined type %q", errAvroSchema, t)
	case []any:
		return p.parseUnion(t)
	case map[string]any:
		return p.parseObject(t)
	}

	return nil, fmt.Errorf("%w: AVRO schema must be a JSON string, array or object", errAvroSchema)
}

func (p *avroParser) parseUnion(arr []any) (*avroNode, error) {
	if len(arr) == 0 {
		return nil, fmt.Errorf("%w: AVRO union must have at least one branch", errAvroSchema)
	}

	n := &avroNode{kind: avroUnion}

	for _, b := range arr {
		c, err := p.parse(b)
		if err != nil {
			return nil, err
		}

		n.union = append(n.union, c)
	}

	return n, nil
}

func (p *avroParser) parseObject(m map[string]any) (*avroNode, error) {
	t, ok := m["type"]
	if !ok {
		return nil, fmt.Errorf("%w: AVRO schema must have a 'type' field", errAvroSchema)
	}

	ts, isStr := t.(string)
	if !isStr {
		return p.parse(t)
	}

	switch ts {
	case avroRecord, "error":
		return p.parseRecord(m)
	case avroEnum:
		return p.parseEnum(m)
	case avroFixed:
		return p.parseFixed(m)
	case avroArray:
		items, err := p.parse(m["items"])
		if err != nil {
			return nil, fmt.Errorf("array items: %w", err)
		}

		return &avroNode{kind: avroArray, items: items}, nil
	case avroMap:
		vals, err := p.parse(m["values"])
		if err != nil {
			return nil, fmt.Errorf("map values: %w", err)
		}

		return &avroNode{kind: avroMap, values: vals}, nil
	}

	return p.parse(ts)
}

func avroName(m map[string]any) (string, error) {
	name, _ := m["name"].(string)
	if name == "" {
		return "", fmt.Errorf("%w: AVRO named type requires a 'name'", errAvroSchema)
	}

	return name, nil
}

func avroAliases(m map[string]any) []string {
	var out []string

	arr, _ := m["aliases"].([]any)
	for _, a := range arr {
		if s, isStr := a.(string); isStr {
			out = append(out, s)
		}
	}

	return out
}

func (p *avroParser) parseRecord(m map[string]any) (*avroNode, error) {
	name, err := avroName(m)
	if err != nil {
		return nil, err
	}

	fields, isList := m["fields"].([]any)
	if !isList {
		return nil, fmt.Errorf("%w: AVRO record %q requires a 'fields' array", errAvroSchema, name)
	}

	n := &avroNode{kind: avroRecord, name: name, aliases: avroAliases(m)}
	p.named[name] = n

	for _, f := range fields {
		field, ferr := p.parseField(name, f)
		if ferr != nil {
			return nil, ferr
		}

		n.fields = append(n.fields, field)
	}

	return n, nil
}

func (p *avroParser) parseField(record string, f any) (avroField, error) {
	fm, ok := f.(map[string]any)
	if !ok {
		return avroField{}, fmt.Errorf("%w: AVRO record %q field must be an object", errAvroSchema, record)
	}

	fname, err := avroName(fm)
	if err != nil {
		return avroField{}, fmt.Errorf("record %q field: %w", record, err)
	}

	ft, err := p.parse(fm["type"])
	if err != nil {
		return avroField{}, fmt.Errorf("record %q field %q: %w", record, fname, err)
	}

	_, hasDef := fm["default"]

	return avroField{name: fname, typ: ft, aliases: avroAliases(fm), hasDef: hasDef}, nil
}

func (p *avroParser) parseEnum(m map[string]any) (*avroNode, error) {
	name, err := avroName(m)
	if err != nil {
		return nil, err
	}

	syms, isList := m["symbols"].([]any)
	if !isList || len(syms) == 0 {
		return nil, fmt.Errorf("%w: AVRO enum %q requires a non-empty 'symbols' array", errAvroSchema, name)
	}

	n := &avroNode{kind: avroEnum, name: name, aliases: avroAliases(m)}

	for _, s := range syms {
		str, isStr := s.(string)
		if !isStr {
			return nil, fmt.Errorf("%w: AVRO enum %q symbols must be strings", errAvroSchema, name)
		}

		n.symbols = append(n.symbols, str)
	}

	_, n.enumDflt = m["default"]
	p.named[name] = n

	return n, nil
}

func (p *avroParser) parseFixed(m map[string]any) (*avroNode, error) {
	name, err := avroName(m)
	if err != nil {
		return nil, err
	}

	size, ok := m["size"].(float64)
	if !ok {
		return nil, fmt.Errorf("%w: AVRO fixed %q requires a numeric 'size'", errAvroSchema, name)
	}

	n := &avroNode{kind: avroFixed, name: name, size: size, aliases: avroAliases(m)}
	p.named[name] = n

	return n, nil
}

func validateAvroSchema(def string) (bool, string) {
	if _, err := parseAvro(def); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func avroCanRead(readerDef, writerDef string) error {
	r, err := parseAvro(readerDef)
	if err != nil {
		return err
	}

	w, err := parseAvro(writerDef)
	if err != nil {
		return err
	}

	return avroResolve(r, w, map[[2]*avroNode]bool{})
}

func avroPromotes(from, to string) bool {
	switch from {
	case avroInt:
		return to == avroLong || to == avroFloat || to == avroDouble
	case avroLong:
		return to == avroFloat || to == avroDouble
	case avroFloat:
		return to == avroDouble
	case avroString:
		return to == avroBytes
	case avroBytes:
		return to == avroString
	}

	return false
}

func avroResolve(r, w *avroNode, seen map[[2]*avroNode]bool) error {
	key := [2]*avroNode{r, w}
	if seen[key] {
		return nil
	}

	seen[key] = true

	if w.kind == avroUnion {
		for _, wb := range w.union {
			if err := avroResolve(r, wb, seen); err != nil {
				return err
			}
		}

		return nil
	}

	if r.kind == avroUnion {
		for _, rb := range r.union {
			if avroResolve(rb, w, map[[2]*avroNode]bool{}) == nil {
				return nil
			}
		}

		return schemaCompatErr("no reader union branch can read writer type %s", avroDesc(w))
	}

	if r.kind != w.kind {
		if avroPromotes(w.kind, r.kind) {
			return nil
		}

		return schemaCompatErr("reader type %s cannot read writer type %s", avroDesc(r), avroDesc(w))
	}

	return avroResolveSame(r, w, seen)
}

func avroDesc(n *avroNode) string {
	if n.name != "" {
		return n.kind + " " + n.name
	}

	return n.kind
}

func avroNameMatches(r, w *avroNode) bool {
	return r.name == w.name || slices.Contains(r.aliases, w.name)
}

func avroResolveSame(r, w *avroNode, seen map[[2]*avroNode]bool) error {
	switch r.kind {
	case avroArray:
		return avroResolve(r.items, w.items, seen)
	case avroMap:
		return avroResolve(r.values, w.values, seen)
	case avroRecord:
		return avroResolveRecord(r, w, seen)
	case avroEnum:
		return avroResolveEnum(r, w)
	case avroFixed:
		if !avroNameMatches(r, w) || r.size != w.size {
			return schemaCompatErr("fixed %s differs in name or size", w.name)
		}
	}

	return nil
}

func avroResolveEnum(r, w *avroNode) error {
	if !avroNameMatches(r, w) {
		return schemaCompatErr("enum name %s does not match %s", r.name, w.name)
	}

	if r.enumDflt {
		return nil
	}

	for _, s := range w.symbols {
		if !slices.Contains(r.symbols, s) {
			return schemaCompatErr("enum %s symbol %q is not in the reader schema", w.name, s)
		}
	}

	return nil
}

func avroResolveRecord(r, w *avroNode, seen map[[2]*avroNode]bool) error {
	if !avroNameMatches(r, w) {
		return schemaCompatErr("record name %s does not match %s", r.name, w.name)
	}

	for _, rf := range r.fields {
		wf, ok := avroFindField(w, rf)
		if !ok {
			if rf.hasDef || avroOptional(rf.typ) {
				continue
			}

			return schemaCompatErr("required field %q of record %s is missing from the writer schema", rf.name, r.name)
		}

		if err := avroResolve(rf.typ, wf.typ, seen); err != nil {
			return fmt.Errorf("field %q: %w", rf.name, err)
		}
	}

	return nil
}

func avroFindField(w *avroNode, rf avroField) (avroField, bool) {
	for _, wf := range w.fields {
		if wf.name == rf.name || slices.Contains(rf.aliases, wf.name) {
			return wf, true
		}
	}

	return avroField{}, false
}

func avroOptional(n *avroNode) bool {
	if n.kind != avroUnion {
		return false
	}

	for _, b := range n.union {
		if b.kind == avroNull {
			return true
		}
	}

	return false
}
