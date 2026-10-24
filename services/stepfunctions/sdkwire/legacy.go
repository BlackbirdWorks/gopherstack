package sdkwire

import (
	"compress/gzip"
	"embed"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/traits"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire/ir"
)

//go:embed legacy/*.json.gz
var legacyModels embed.FS

// protocolFor builds the smithy-go protocol for a service schema.
type protocolFor func(*smithy.ServiceSchema) smithyhttp.ClientProtocol

// legacyService builds the Service for key from its embedded model, for
// services whose pinned SDK client has no schemas package.
func legacyService(key, signing, userAgentAPI string, protocol protocolFor) *Service {
	f, err := legacyModels.Open("legacy/" + key + ".json.gz")
	if err != nil {
		panic(fmt.Sprintf("sdkwire: no model for %s: %v", key, err))
	}

	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		panic(fmt.Sprintf("sdkwire: model %s: %v", key, err))
	}

	var m ir.Model
	if derr := json.NewDecoder(zr).Decode(&m); derr != nil {
		panic(fmt.Sprintf("sdkwire: model %s: %v", key, derr))
	}

	svcSchema, ops := buildSchemas(key, &m)

	return &Service{
		Protocol: protocol(svcSchema), Ops: ops, SigningName: signing, UserAgentAPI: userAgentAPI,
	}
}

type schemaBuilder struct {
	model   *ir.Model
	shapes  map[string]*smithy.Schema
	scalars map[string]*smithy.Schema
	ns      string
}

var scalarTypes = map[string]smithy.ShapeType{ //nolint:gochecknoglobals // read-only lookup
	ir.String: smithy.ShapeTypeString, ir.Boolean: smithy.ShapeTypeBoolean, ir.Byte: smithy.ShapeTypeByte,
	ir.Short: smithy.ShapeTypeShort, ir.Integer: smithy.ShapeTypeInteger, ir.Long: smithy.ShapeTypeLong,
	ir.Float: smithy.ShapeTypeFloat, ir.Double: smithy.ShapeTypeDouble, ir.Timestamp: smithy.ShapeTypeTimestamp,
	ir.Blob: smithy.ShapeTypeBlob, ir.Document: smithy.ShapeTypeDocument,
}

func buildSchemas(ns string, m *ir.Model) (*smithy.ServiceSchema, map[string]*Op) {
	b := &schemaBuilder{
		model: m, ns: ns, shapes: map[string]*smithy.Schema{}, scalars: map[string]*smithy.Schema{},
	}

	for kind, typ := range scalarTypes {
		b.scalars[kind] = smithy.NewSchema(smithy.ShapeID{Namespace: "smithy.api", Name: kind}, typ, 0)
	}

	names := make([]string, 0, len(m.Shapes))
	for name := range m.Shapes {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		b.shapes[name] = smithy.NewSchema(smithy.ShapeID{Namespace: ns, Name: name}, shapeType(m.Shapes[name].Type), 0)
	}

	for _, name := range names {
		b.addMembers(b.shapes[name], m.Shapes[name])
	}

	ops := make(map[string]*Op, len(m.Ops))

	for name, op := range m.Ops {
		opSchema := smithy.NewSchema(smithy.ShapeID{Namespace: ns, Name: name}, smithy.ShapeTypeOperation, 0,
			&traits.HTTP{Method: op.Method, URI: op.URI})
		ops[name] = &Op{Schema: smithy.NewOperationSchema(opSchema, b.shapes[op.In], b.shapes[op.Out]), Token: op.Token}
	}

	target := m.Target
	if target == "" {
		target = ns
	}

	svc := smithy.NewSchema(smithy.ShapeID{Namespace: ns, Name: target}, smithy.ShapeTypeService, 0)

	return smithy.NewServiceSchema(svc, m.Version), ops
}

func shapeType(kind string) smithy.ShapeType {
	switch kind {
	case ir.List:
		return smithy.ShapeTypeList
	case ir.Map:
		return smithy.ShapeTypeMap
	case ir.Union:
		return smithy.ShapeTypeUnion
	default:
		return smithy.ShapeTypeStructure
	}
}

func (b *schemaBuilder) target(name string) *smithy.Schema {
	if s, ok := b.scalars[name]; ok {
		return s
	}

	return b.shapes[name]
}

func (b *schemaBuilder) addMembers(s *smithy.Schema, sh *ir.Shape) {
	switch sh.Type {
	case ir.List:
		s.AddMember("member", b.target(sh.Item.Target), b.wireTraits(sh.Item, "member")...)
	case ir.Map:
		s.AddMember("key", b.target(sh.Key.Target), b.wireTraits(sh.Key, "key")...)
		s.AddMember("value", b.target(sh.Value.Target), b.wireTraits(sh.Value, "value")...)
	default:
		for i := range sh.Members {
			m := &sh.Members[i]
			ts := b.wireTraits(m, m.Name)

			if m.Flat {
				ts = append(ts, &traits.XMLFlattened{})
			}

			s.AddMember(m.Name, b.target(m.Target), ts...)
		}
	}
}

// wireTraits names the member on the wire: query-family serializers take the
// request name from xmlName (or ec2QueryName), deserializers the response name.
func (b *schemaBuilder) wireTraits(m *ir.Member, fallback string) []smithy.Trait {
	var ts []smithy.Trait

	switch b.model.Protocol {
	case "awsjson10", "awsjson11":
		return nil
	case "restjson1":
		return restJSONTraits(m)
	case "ec2query":
		if m.Ser != "" {
			ts = append(ts, &traits.EC2QueryName{Name: m.Ser})
		}

		if m.De != "" {
			ts = append(ts, &traits.XMLName{Name: m.De})
		}
	default:
		name := m.De
		if name == "" {
			name = m.Ser
		}

		if name == "" && fallback != m.Name {
			name = fallback
		}

		if name != "" {
			ts = append(ts, &traits.XMLName{Name: name})
		}
	}

	return ts
}

// restJSONTraits binds a member to its JSON key or HTTP location.
func restJSONTraits(m *ir.Member) []smithy.Trait {
	var ts []smithy.Trait

	if name := cmpOr(m.De, m.Ser); name != "" && name != m.Name {
		ts = append(ts, &traits.JSONName{Name: name})
	}

	switch m.Loc {
	case "label":
		ts = append(ts, &traits.HTTPLabel{})
	case "query":
		ts = append(ts, &traits.HTTPQuery{Name: m.LocName})
	case "header":
		ts = append(ts, &traits.HTTPHeader{Name: m.LocName})
	case "prefix":
		ts = append(ts, &traits.HTTPPrefixHeaders{Prefix: m.LocName})
	}

	return ts
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}

	return b
}
