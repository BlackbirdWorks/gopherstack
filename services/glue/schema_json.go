package glue

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
)

var errJSONSchema = errors.New("invalid JSON schema")

// Draft-07 subset; per the Glue docs, adding a property is backward compatible only when the
// old schema is closed (additionalProperties: false).

const (
	jsonSchemaMaxDepth = 32
	jsonKeyNot         = "not"
	jsonTypeArray      = "array"
)

func jsonKnownType(t string) bool {
	switch t {
	case "null", "boolean", "object", jsonTypeArray, "number", "integer", "string":
		return true
	}

	return false
}

type jsonSchemaDoc map[string]any

func parseJSONSchema(def string) (jsonSchemaDoc, error) {
	var v any
	if err := json.Unmarshal([]byte(def), &v); err != nil {
		return nil, fmt.Errorf("schema is not valid JSON: %w", err)
	}

	switch t := v.(type) {
	case map[string]any:
		return t, nil
	case bool:
		if t {
			return jsonSchemaDoc{}, nil
		}

		return jsonSchemaDoc{jsonKeyNot: map[string]any{}}, nil
	}

	return nil, fmt.Errorf("%w: JSON schema must be a JSON object or boolean", errJSONSchema)
}

func validateJSONSchema(def string) (bool, string) {
	doc, err := parseJSONSchema(def)
	if err != nil {
		return false, err.Error()
	}

	if err = validateJSONSchemaKeywords(doc, 0); err != nil {
		return false, err.Error()
	}

	return true, ""
}

func validateJSONSchemaKeywords(doc map[string]any, depth int) error {
	if depth > jsonSchemaMaxDepth {
		return nil
	}

	if err := validateJSONSchemaType(doc["type"]); err != nil {
		return err
	}

	if err := validateJSONRequired(doc["required"]); err != nil {
		return err
	}

	if err := validateJSONProperties(doc["properties"], depth); err != nil {
		return err
	}

	for _, kw := range []string{"items", "additionalProperties", jsonKeyNot} {
		if sub, ok := doc[kw]; ok {
			if err := validateJSONSubschema(sub, depth); err != nil {
				return fmt.Errorf("%s: %w", kw, err)
			}
		}
	}

	return nil
}

func validateJSONRequired(req any) error {
	if req == nil {
		return nil
	}

	arr, isArr := req.([]any)
	if !isArr {
		return fmt.Errorf("%w: 'required' must be an array of strings", errJSONSchema)
	}

	for _, r := range arr {
		if _, isStr := r.(string); !isStr {
			return fmt.Errorf("%w: 'required' must be an array of strings", errJSONSchema)
		}
	}

	return nil
}

func validateJSONProperties(props any, depth int) error {
	if props == nil {
		return nil
	}

	pm, isMap := props.(map[string]any)
	if !isMap {
		return fmt.Errorf("%w: 'properties' must be an object", errJSONSchema)
	}

	for name, sub := range pm {
		if err := validateJSONSubschema(sub, depth); err != nil {
			return fmt.Errorf("property %q: %w", name, err)
		}
	}

	return nil
}

func validateJSONSubschema(sub any, depth int) error {
	switch t := sub.(type) {
	case bool:
		return nil
	case map[string]any:
		return validateJSONSchemaKeywords(t, depth+1)
	}

	return fmt.Errorf("%w: subschema must be an object or boolean", errJSONSchema)
}

func validateJSONSchemaType(t any) error {
	if t == nil {
		return nil
	}

	names := jsonTypeNames(t)
	if len(names) == 0 {
		return fmt.Errorf("%w: JSON schema 'type' must be a string or array of strings", errJSONSchema)
	}

	for _, n := range names {
		if !jsonKnownType(n) {
			return fmt.Errorf("%w: JSON schema has unknown type %q", errJSONSchema, n)
		}
	}

	return nil
}

func jsonTypeNames(t any) []string {
	switch v := t.(type) {
	case string:
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))

		for _, e := range v {
			s, ok := e.(string)
			if !ok {
				return nil
			}

			out = append(out, s)
		}

		return out
	}

	return nil
}

func jsonSchemaCanRead(readerDef, writerDef string) error {
	r, err := parseJSONSchema(readerDef)
	if err != nil {
		return err
	}

	w, err := parseJSONSchema(writerDef)
	if err != nil {
		return err
	}

	c := &jsonCompat{rootR: r, rootW: w}

	return c.subset(r, w, "$", 0)
}

type jsonCompat struct {
	rootR jsonSchemaDoc
	rootW jsonSchemaDoc
}

func jsonAsSchema(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case nil:
		return map[string]any{}, true
	case bool:
		if t {
			return map[string]any{}, true
		}

		return map[string]any{jsonKeyNot: map[string]any{}}, true
	case map[string]any:
		return t, true
	}

	return nil, false
}

func jsonResolveRef(root, s map[string]any) map[string]any {
	for range jsonSchemaMaxDepth {
		ref, ok := s["$ref"].(string)
		if !ok || !strings.HasPrefix(ref, "#/") {
			return s
		}

		var cur any = root

		for seg := range strings.SplitSeq(strings.TrimPrefix(ref, "#/"), "/") {
			m, isMap := cur.(map[string]any)
			if !isMap {
				return s
			}

			cur = m[strings.ReplaceAll(strings.ReplaceAll(seg, "~1", "/"), "~0", "~")]
		}

		next, ok := cur.(map[string]any)
		if !ok {
			return s
		}

		s = next
	}

	return s
}

// subset reports whether every document valid under w is valid under r.
func (c *jsonCompat) subset(r, w map[string]any, path string, depth int) error {
	if depth > jsonSchemaMaxDepth || reflect.DeepEqual(r, w) {
		return nil
	}

	r = jsonResolveRef(c.rootR, r)
	w = jsonResolveRef(c.rootW, w)

	if jsonIsFalse(w) {
		return nil
	}

	if jsonIsFalse(r) {
		return schemaCompatErr("%s: reader schema accepts no values here but the writer schema does", path)
	}

	if done, err := c.subsetCombinators(r, w, path, depth); done {
		return err
	}

	checks := []func(r, w map[string]any, path string) error{
		jsonCheckTypes, jsonCheckEnum, jsonCheckStringBounds, jsonCheckNumberBounds, jsonCheckItemBounds,
	}
	for _, chk := range checks {
		if err := chk(r, w, path); err != nil {
			return err
		}
	}

	if err := c.subsetArray(r, w, path, depth); err != nil {
		return err
	}

	return c.subsetObject(r, w, path, depth)
}

func (c *jsonCompat) subsetCombinators(r, w map[string]any, path string, depth int) (bool, error) {
	for _, kw := range []string{"anyOf", "oneOf"} {
		if branches, ok := w[kw].([]any); ok {
			return true, c.subsetEveryWriterBranch(r, jsonWithout(w, kw), branches, path+"."+kw, depth)
		}
	}

	if branches, ok := w["allOf"].([]any); ok {
		return true, c.subsetAnyBranch(r, jsonWithout(w, "allOf"), branches, path, depth, false)
	}

	for _, kw := range []string{"anyOf", "oneOf"} {
		if branches, ok := r[kw].([]any); ok {
			return true, c.subsetAnyBranch(jsonWithout(r, kw), w, branches, path, depth, true)
		}
	}

	if branches, ok := r["allOf"].([]any); ok {
		return true, c.subsetEveryReaderBranch(jsonWithout(r, "allOf"), w, branches, path, depth)
	}

	return false, nil
}

func (c *jsonCompat) subsetEveryWriterBranch(r, rest map[string]any, branches []any, path string, depth int) error {
	for i, b := range branches {
		bs, ok := jsonAsSchema(b)
		if !ok {
			continue
		}

		if err := c.subset(r, jsonMerge(rest, bs), fmt.Sprintf("%s[%d]", path, i), depth+1); err != nil {
			return err
		}
	}

	return nil
}

func (c *jsonCompat) subsetEveryReaderBranch(rest, w map[string]any, branches []any, path string, depth int) error {
	for _, b := range branches {
		bs, ok := jsonAsSchema(b)
		if !ok {
			continue
		}

		if err := c.subset(jsonMerge(rest, bs), w, path, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// subsetAnyBranch succeeds when at least one branch, merged into the reader
// (readerSide) or writer, makes the pair compatible.
func (c *jsonCompat) subsetAnyBranch(
	r, w map[string]any,
	branches []any,
	path string,
	depth int,
	readerSide bool,
) error {
	for _, b := range branches {
		bs, ok := jsonAsSchema(b)
		if !ok {
			continue
		}

		var err error
		if readerSide {
			err = c.subset(jsonMerge(r, bs), w, path, depth+1)
		} else {
			err = c.subset(r, jsonMerge(w, bs), path, depth+1)
		}

		if err == nil {
			return nil
		}
	}

	return schemaCompatErr("%s: no combinator branch is compatible", path)
}

func jsonWithout(m map[string]any, key string) map[string]any {
	out := make(map[string]any, len(m))

	for k, v := range m {
		if k != key {
			out[k] = v
		}
	}

	return out
}

func jsonMerge(base, over map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(over))
	maps.Copy(out, base)
	maps.Copy(out, over)

	return out
}

func jsonTypeSet(s map[string]any) []string {
	if t, ok := s["type"]; ok {
		return jsonTypeNames(t)
	}

	return nil
}

func jsonCheckTypes(r, w map[string]any, path string) error {
	rt := jsonTypeSet(r)
	if rt == nil {
		return nil
	}

	wt := jsonTypeSet(w)
	if wt == nil {
		return schemaCompatErr("%s: writer allows any type but reader requires %v", path, rt)
	}

	for _, t := range wt {
		if slices.Contains(rt, t) || (t == "integer" && slices.Contains(rt, "number")) {
			continue
		}

		return schemaCompatErr("%s: type %q is not accepted by the reader schema", path, t)
	}

	return nil
}

func jsonCheckEnum(r, w map[string]any, path string) error {
	re, ok := r["enum"].([]any)
	if !ok {
		return nil
	}

	we, ok := w["enum"].([]any)
	if !ok {
		if c, hasConst := w["const"]; hasConst {
			we, ok = []any{c}, true
		}
	}

	if !ok {
		return schemaCompatErr("%s: reader restricts values with enum but writer does not", path)
	}

	for _, v := range we {
		if !slices.ContainsFunc(re, func(x any) bool { return reflect.DeepEqual(x, v) }) {
			return schemaCompatErr("%s: enum value %v is not accepted by the reader schema", path, v)
		}
	}

	return nil
}

func jsonNum(m map[string]any, key string) (float64, bool) {
	f, ok := m[key].(float64)

	return f, ok
}

func jsonCheckStringBounds(r, w map[string]any, path string) error {
	if err := jsonLowerBound(r, w, "minLength", path); err != nil {
		return err
	}

	if err := jsonUpperBound(r, w, "maxLength", path); err != nil {
		return err
	}

	for _, kw := range []string{"pattern", "format"} {
		if rv, ok := r[kw]; ok && !reflect.DeepEqual(rv, w[kw]) {
			return schemaCompatErr("%s: reader %s %v is not guaranteed by the writer schema", path, kw, rv)
		}
	}

	return nil
}

func jsonCheckNumberBounds(r, w map[string]any, path string) error {
	for _, kw := range []string{"minimum", "exclusiveMinimum"} {
		if err := jsonLowerBound(r, w, kw, path); err != nil {
			return err
		}
	}

	for _, kw := range []string{"maximum", "exclusiveMaximum"} {
		if err := jsonUpperBound(r, w, kw, path); err != nil {
			return err
		}
	}

	if rm, ok := jsonNum(r, "multipleOf"); ok {
		wm, has := jsonNum(w, "multipleOf")
		if !has || rm == 0 || float64(int64(wm/rm))*rm != wm {
			return schemaCompatErr("%s: reader multipleOf %v is not guaranteed by the writer schema", path, rm)
		}
	}

	return nil
}

func jsonCheckItemBounds(r, w map[string]any, path string) error {
	if err := jsonLowerBound(r, w, "minItems", path); err != nil {
		return err
	}

	if err := jsonUpperBound(r, w, "maxItems", path); err != nil {
		return err
	}

	if err := jsonLowerBound(r, w, "minProperties", path); err != nil {
		return err
	}

	return jsonUpperBound(r, w, "maxProperties", path)
}

func jsonLowerBound(r, w map[string]any, kw, path string) error {
	rv, ok := jsonNum(r, kw)
	if !ok {
		return nil
	}

	if wv, has := jsonNum(w, kw); !has || wv < rv {
		return schemaCompatErr("%s: reader %s %v is not guaranteed by the writer schema", path, kw, rv)
	}

	return nil
}

func jsonUpperBound(r, w map[string]any, kw, path string) error {
	rv, ok := jsonNum(r, kw)
	if !ok {
		return nil
	}

	if wv, has := jsonNum(w, kw); !has || wv > rv {
		return schemaCompatErr("%s: reader %s %v is not guaranteed by the writer schema", path, kw, rv)
	}

	return nil
}

func (c *jsonCompat) subsetArray(r, w map[string]any, path string, depth int) error {
	ri, ok := r["items"]
	if !ok {
		return nil
	}

	rs, rok := jsonAsSchema(ri)
	ws, wok := jsonAsSchema(w["items"])

	if !rok || !wok {
		return nil
	}

	return c.subset(rs, ws, path+"[]", depth+1)
}

func (c *jsonCompat) subsetObject(r, w map[string]any, path string, depth int) error {
	wReq := jsonStringList(w["required"])

	for _, name := range jsonStringList(r["required"]) {
		if !slices.Contains(wReq, name) {
			return schemaCompatErr("%s: reader requires property %q but the writer schema does not", path, name)
		}
	}

	rProps, _ := r["properties"].(map[string]any)
	wProps, _ := w["properties"].(map[string]any)
	rAP, _ := jsonAsSchema(jsonAdditional(r))
	wAP, _ := jsonAsSchema(jsonAdditional(w))

	for name, rp := range rProps {
		wSchema := wAP
		if src, found := wProps[name]; found {
			wSchema, _ = jsonAsSchema(src)
		}

		if err := c.subsetProperty(rp, wSchema, path+"."+name, depth); err != nil {
			return err
		}
	}

	for name, wp := range wProps {
		if _, ok := rProps[name]; ok {
			continue
		}

		if err := c.subsetProperty(rAP, wp, path+"."+name, depth); err != nil {
			return err
		}
	}

	return c.subset(rAP, wAP, path+".<additional>", depth+1)
}

func (c *jsonCompat) subsetProperty(r, w any, path string, depth int) error {
	rs, rok := jsonAsSchema(r)
	ws, wok := jsonAsSchema(w)

	if !rok || !wok {
		return nil
	}

	return c.subset(rs, ws, path, depth+1)
}

func jsonAdditional(s map[string]any) any {
	if v, ok := s["additionalProperties"]; ok {
		return v
	}

	return true
}

func jsonStringList(v any) []string {
	arr, _ := v.([]any)
	out := make([]string, 0, len(arr))

	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func jsonIsFalse(s map[string]any) bool {
	n, ok := s[jsonKeyNot].(map[string]any)

	return ok && len(n) == 0 && len(s) == 1
}
