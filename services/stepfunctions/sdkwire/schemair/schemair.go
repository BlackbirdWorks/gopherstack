// Package schemair is a serialized form of an SDK service's smithy-go
// schema graph, so the graph can be rebuilt on first use instead of by the
// SDK schemas package's init functions.
package schemair

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/prelude"
	"github.com/aws/smithy-go/traits"
)

// ErrModel marks an invalid or mismatched schema model.
var ErrModel = errors.New("schemair: invalid model")

func errf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrModel, fmt.Sprintf(format, args...))
}

// Model is one service's schema graph.
type Model struct {
	Ops     map[string]Op `json:"ops"`
	Version string        `json:"version"`
	Shapes  []Shape       `json:"shapes"`
	Service int           `json:"service"`
}

// Shape is a schema: a prelude shape when Prelude is set, else a declared one.
type Shape struct {
	Prelude   string   `json:"prelude,omitempty"`
	Namespace string   `json:"ns,omitempty"`
	Name      string   `json:"name,omitempty"`
	Type      string   `json:"type,omitempty"`
	Traits    []Trait  `json:"traits,omitempty"`
	Members   []Member `json:"members,omitempty"`
}

// Member is a member schema; Traits are those declared on the member itself.
type Member struct {
	Name   string  `json:"name"`
	Traits []Trait `json:"traits,omitempty"`
	Target int     `json:"to"`
}

// Op references an operation's schemas by Shapes index; -1 means none.
type Op struct {
	Token  string `json:"token,omitempty"`
	Schema int    `json:"op"`
	In     int    `json:"in"`
	Out    int    `json:"out"`
}

// Trait is a smithy-go trait by codec name and JSON-encoded fields.
type Trait struct {
	Name  string          `json:"n"`
	Value json.RawMessage `json:"v,omitempty"`
}

// Operation is a decoded operation.
type Operation struct {
	Schema *smithy.OperationSchema
	Token  string
}

// Service is a decoded service.
type Service struct {
	Schema *smithy.ServiceSchema
	Ops    map[string]Operation
}

type codec struct {
	probe func(s *smithy.Schema, direct bool) (smithy.Trait, bool)
	typ   reflect.Type
}

func entry[T smithy.Trait]() codec {
	return codec{
		typ: reflect.TypeFor[T]().Elem(),
		probe: func(s *smithy.Schema, direct bool) (smithy.Trait, bool) {
			if direct {
				return smithy.SchemaDirectTrait[T](s)
			}

			return smithy.SchemaTrait[T](s)
		},
	}
}

func codecs() map[string]codec {
	return map[string]codec{
		"AWSQueryCompatible": entry[*traits.AWSQueryCompatible](),
		"AWSQueryError":      entry[*traits.AWSQueryError](),
		"ContextParam":       entry[*traits.ContextParam](),
		"EC2QueryName":       entry[*traits.EC2QueryName](),
		"Error":              entry[*traits.Error](),
		"EventHeader":        entry[*traits.EventHeader](),
		"EventPayload":       entry[*traits.EventPayload](),
		"HTTP":               entry[*traits.HTTP](),
		"HTTPError":          entry[*traits.HTTPError](),
		"HTTPHeader":         entry[*traits.HTTPHeader](),
		"HTTPLabel":          entry[*traits.HTTPLabel](),
		"HTTPPayload":        entry[*traits.HTTPPayload](),
		"HTTPPrefixHeaders":  entry[*traits.HTTPPrefixHeaders](),
		"HTTPQuery":          entry[*traits.HTTPQuery](),
		"HTTPQueryParams":    entry[*traits.HTTPQueryParams](),
		"HTTPResponseCode":   entry[*traits.HTTPResponseCode](),
		"HostLabel":          entry[*traits.HostLabel](),
		"JSONName":           entry[*traits.JSONName](),
		"MediaType":          entry[*traits.MediaType](),
		"Sensitive":          entry[*traits.Sensitive](),
		"Streaming":          entry[*traits.Streaming](),
		"TimestampFormat":    entry[*traits.TimestampFormat](),
		"UnitShape":          entry[*traits.UnitShape](),
		"XMLAttribute":       entry[*traits.XMLAttribute](),
		"XMLFlattened":       entry[*traits.XMLFlattened](),
		"XMLName":            entry[*traits.XMLName](),
		"XMLNamespace":       entry[*traits.XMLNamespace](),
	}
}

func shapeTypes() map[string]smithy.ShapeType {
	return map[string]smithy.ShapeType{
		"blob": smithy.ShapeTypeBlob, "boolean": smithy.ShapeTypeBoolean, "string": smithy.ShapeTypeString,
		"timestamp": smithy.ShapeTypeTimestamp, "byte": smithy.ShapeTypeByte, "short": smithy.ShapeTypeShort,
		"integer": smithy.ShapeTypeInteger, "long": smithy.ShapeTypeLong, "float": smithy.ShapeTypeFloat,
		"document": smithy.ShapeTypeDocument, "double": smithy.ShapeTypeDouble,
		"bigDecimal": smithy.ShapeTypeBigDecimal, "bigInteger": smithy.ShapeTypeBigInteger,
		"enum": smithy.ShapeTypeEnum, "intEnum": smithy.ShapeTypeIntEnum, "list": smithy.ShapeTypeList,
		"set": smithy.ShapeTypeSet, "map": smithy.ShapeTypeMap, "structure": smithy.ShapeTypeStructure,
		"union": smithy.ShapeTypeUnion, "member": smithy.ShapeTypeMember, "service": smithy.ShapeTypeService,
		"resource": smithy.ShapeTypeResource, "operation": smithy.ShapeTypeOperation,
	}
}

func preludeShapes() map[string]*smithy.Schema {
	return map[string]*smithy.Schema{
		"String": prelude.String, "Blob": prelude.Blob, "Boolean": prelude.Boolean, "Byte": prelude.Byte,
		"Short": prelude.Short, "Integer": prelude.Integer, "Long": prelude.Long, "Float": prelude.Float,
		"Double": prelude.Double, "Timestamp": prelude.Timestamp, "Document": prelude.Document,
		"Unit": prelude.Unit, "PrimitiveBoolean": prelude.PrimitiveBoolean, "PrimitiveByte": prelude.PrimitiveByte,
		"PrimitiveShort": prelude.PrimitiveShort, "PrimitiveInteger": prelude.PrimitiveInteger,
		"PrimitiveLong": prelude.PrimitiveLong, "PrimitiveFloat": prelude.PrimitiveFloat,
		"PrimitiveDouble": prelude.PrimitiveDouble, "BigInteger": prelude.BigInteger,
		"BigDecimal": prelude.BigDecimal,
	}
}

// Read decodes a gzipped JSON model.
func Read(r io.Reader) (*Model, error) {
	zr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("schemair: %w", err)
	}

	defer zr.Close()

	var m Model
	if derr := json.NewDecoder(zr).Decode(&m); derr != nil {
		return nil, fmt.Errorf("schemair: %w", derr)
	}

	return &m, nil
}

// Build rebuilds the schema graph.
func (m *Model) Build() (*Service, error) {
	cs, types, pre := codecs(), shapeTypes(), preludeShapes()
	schemas := make([]*smithy.Schema, len(m.Shapes))

	for i := range m.Shapes {
		sh := &m.Shapes[i]
		if sh.Prelude != "" {
			p, ok := pre[sh.Prelude]
			if !ok {
				return nil, errf("unknown prelude shape %q", sh.Prelude)
			}

			schemas[i] = p

			continue
		}

		typ, ok := types[sh.Type]
		if !ok {
			return nil, errf("unknown shape type %q", sh.Type)
		}

		ts, err := decodeTraits(cs, sh.Traits)
		if err != nil {
			return nil, err
		}

		schemas[i] = smithy.NewSchema(
			smithy.ShapeID{Namespace: sh.Namespace, Name: sh.Name}, typ, len(sh.Members), ts...)
	}

	for i := range m.Shapes {
		for _, mem := range m.Shapes[i].Members {
			ts, err := decodeTraits(cs, mem.Traits)
			if err != nil {
				return nil, err
			}

			schemas[i].AddMember(mem.Name, schemas[mem.Target], ts...)
		}
	}

	svc := &Service{
		Schema: smithy.NewServiceSchema(schemas[m.Service], m.Version),
		Ops:    make(map[string]Operation, len(m.Ops)),
	}

	for name, op := range m.Ops {
		svc.Ops[name] = Operation{
			Schema: smithy.NewOperationSchema(schemas[op.Schema], at(schemas, op.In), at(schemas, op.Out)),
			Token:  op.Token,
		}
	}

	return svc, nil
}

func at(schemas []*smithy.Schema, i int) *smithy.Schema {
	if i < 0 {
		return nil
	}

	return schemas[i]
}

func decodeTraits(cs map[string]codec, in []Trait) ([]smithy.Trait, error) {
	out := make([]smithy.Trait, 0, len(in))

	for _, t := range in {
		c, ok := cs[t.Name]
		if !ok {
			return nil, errf("unknown trait %q", t.Name)
		}

		v := reflect.New(c.typ)
		if len(t.Value) > 0 {
			if err := json.Unmarshal(t.Value, v.Interface()); err != nil {
				return nil, fmt.Errorf("schemair: trait %s: %w", t.Name, err)
			}
		}

		tr, ok := reflect.TypeAssert[smithy.Trait](v)
		if !ok {
			return nil, errf("%s is not a trait", t.Name)
		}

		out = append(out, tr)
	}

	return out, nil
}
