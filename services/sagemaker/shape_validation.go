package sagemaker

import (
	"bytes"
	_ "embed" // shape_table.json
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var errShapeInvalid = errors.New("invalid document")

// shapeTable transcribes the required-member and nesting rules of the pinned
// aws-sdk-go-v2/service/sagemaker validators.go for opaque config documents.
//
//go:embed shape_table.json
var shapeTableJSON []byte

type shapeRule struct {
	Fields map[string]string `json:"fields"`
	Union  map[string]string `json:"union"`
	List   string            `json:"list"`
	Req    []string          `json:"req"`
}

type shapeTableDoc struct {
	Roots  map[string]string    `json:"roots"`
	Shapes map[string]shapeRule `json:"shapes"`
}

//nolint:gochecknoglobals // lazily parsed read-only schema
var loadShapeTable = sync.OnceValue(func() shapeTableDoc {
	var doc shapeTableDoc
	if err := json.Unmarshal(shapeTableJSON, &doc); err != nil {
		panic(fmt.Sprintf("sagemaker: invalid shape_table.json: %v", err))
	}

	return doc
})

// validateRequestShapes validates every json.RawMessage field of req that has
// a rule registered for op in shape_table.json; absent documents are skipped.
func validateRequestShapes(op string, req any) error {
	v := reflect.ValueOf(req).Elem()
	table := loadShapeTable()

	for i := range v.NumField() {
		f := v.Type().Field(i)
		if f.Type != reflect.TypeFor[json.RawMessage]() {
			continue
		}

		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")

		shape, ok := table.Roots[op+"."+name]
		if !ok {
			continue
		}

		raw, _ := reflect.TypeAssert[json.RawMessage](v.Field(i))
		if isAbsentJSON(raw) {
			continue
		}

		if err := validateShape(table, raw, shape, name); err != nil {
			return fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	return nil
}

func isAbsentJSON(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)

	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

func validateShape(table shapeTableDoc, raw json.RawMessage, shape, path string) error {
	rule, ok := table.Shapes[shape]
	if !ok {
		return nil
	}

	if rule.List != "" {
		return validateShapeList(table, raw, rule.List, path)
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("%w: %s must be an object", errShapeInvalid, path)
	}

	for _, name := range rule.Req {
		if isAbsentJSON(obj[name]) {
			return fmt.Errorf("%w: %s.%s is required", errShapeInvalid, path, name)
		}
	}

	for member, memberShape := range rule.Fields {
		if err := validateShapeMember(table, obj[member], memberShape, path+"."+member); err != nil {
			return err
		}
	}

	for member, memberShape := range rule.Union {
		if err := validateShapeMember(table, obj[member], memberShape, path+"."+member); err != nil {
			return err
		}
	}

	return nil
}

func validateShapeMember(table shapeTableDoc, raw json.RawMessage, shape, path string) error {
	if isAbsentJSON(raw) {
		return nil
	}

	return validateShape(table, raw, shape, path)
}

func validateShapeList(table shapeTableDoc, raw json.RawMessage, elem, path string) error {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return fmt.Errorf("%w: %s must be a list", errShapeInvalid, path)
	}

	for i, item := range items {
		if err := validateShape(table, item, elem, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
	}

	return nil
}
