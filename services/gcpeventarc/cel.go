package gcpeventarc

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"google.golang.org/protobuf/types/known/structpb"
)

var errCELResult = errors.New("unexpected CEL result")

func celMap() *cel.Type { return cel.MapType(cel.StringType, cel.DynType) }

// newMessageEnv declares `message` (the CloudEvent as a map) and the transformation functions documented on
// Pipeline.Mediation.Transformation.
func newMessageEnv() (*cel.Env, error) {
	env, err := cel.NewEnv(cel.Variable("message", celMap()), cel.Lib(messageLib{}))
	if err != nil {
		return nil, fmt.Errorf("build CEL environment: %w", err)
	}

	return env, nil
}

type messageLib struct{}

func (messageLib) ProgramOptions() []cel.ProgramOption { return nil }

func (messageLib) LibraryName() string { return "gcpeventarc.message" }

func (messageLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("merge", cel.MemberOverload("map_merge", []*cel.Type{celMap(), celMap()}, celMap(),
			cel.BinaryBinding(func(a, b ref.Val) ref.Val {
				x, y := toNativeMap(a), toNativeMap(b)

				return toVal(mergeMaps(x, y))
			}))),
		cel.Function("denormalize", cel.MemberOverload("map_denormalize", []*cel.Type{celMap()}, celMap(),
			cel.UnaryBinding(func(a ref.Val) ref.Val {
				out := map[string]any{}
				flatten("", toNativeMap(a), out)

				return toVal(out)
			}))),
		cel.Function("setField", cel.MemberOverload("map_setfield",
			[]*cel.Type{celMap(), cel.StringType, cel.DynType}, celMap(),
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				m := toNativeMap(args[0])
				key, _ := args[1].Value().(string)
				setPath(m, strings.Split(key, "."), toNative(args[2]))

				return toVal(m)
			}))),
		cel.Function("removeFields", cel.MemberOverload("map_removefields",
			[]*cel.Type{celMap(), cel.ListType(cel.StringType)}, celMap(),
			cel.BinaryBinding(func(a, b ref.Val) ref.Val {
				m := toNativeMap(a)

				if keys, ok := toNative(b).([]any); ok {
					for _, k := range keys {
						if s, isStr := k.(string); isStr {
							removePath(m, strings.Split(s, "."))
						}
					}
				}

				return toVal(m)
			}))),
		cel.Function("toMap", cel.MemberOverload("list_tomap", []*cel.Type{cel.ListType(celMap())}, celMap(),
			cel.UnaryBinding(func(a ref.Val) ref.Val {
				out := map[string]any{}

				if items, ok := toNative(a).([]any); ok {
					for _, it := range items {
						if m, isMap := it.(map[string]any); isMap {
							out = mergeMaps(out, m)
						}
					}
				}

				return toVal(out)
			}))),
		cel.Function("toJsonString", cel.MemberOverload("dyn_tojsonstring", []*cel.Type{cel.DynType}, cel.StringType,
			cel.UnaryBinding(func(a ref.Val) ref.Val {
				raw, err := json.Marshal(toNative(a))
				if err != nil {
					return types.NewErr("toJsonString: %v", err)
				}

				return types.String(raw)
			}))),
		cel.Function("toDestinationPayloadFormat", cel.MemberOverload("dyn_todestformat",
			[]*cel.Type{cel.DynType}, cel.DynType,
			cel.UnaryBinding(func(a ref.Val) ref.Val { return a }))),
		cel.Function("toCloudEventJsonWithPayloadFormat", cel.MemberOverload("map_tocejson",
			[]*cel.Type{celMap()}, celMap(),
			cel.UnaryBinding(func(a ref.Val) ref.Val { return a }))),
	}
}

func toNative(v ref.Val) any {
	if out, err := v.ConvertToNative(reflect.TypeFor[*structpb.Value]()); err == nil {
		if sv, ok := out.(*structpb.Value); ok {
			return sv.AsInterface()
		}
	}

	return v.Value()
}

func toNativeMap(v ref.Val) map[string]any {
	if m, ok := toNative(v).(map[string]any); ok {
		return m
	}

	return map[string]any{}
}

func toVal(v any) ref.Val { return types.DefaultTypeAdapter.NativeToValue(v) }

func mergeMaps(a, b map[string]any) map[string]any {
	out := maps.Clone(a)
	if out == nil {
		out = map[string]any{}
	}

	for k, bv := range b {
		if am, ok := out[k].(map[string]any); ok {
			if bm, isMap := bv.(map[string]any); isMap {
				out[k] = mergeMaps(am, bm)

				continue
			}
		}

		out[k] = bv
	}

	return out
}

func flatten(prefix string, in map[string]any, out map[string]any) {
	for k, v := range in {
		key := prefix + k

		switch t := v.(type) {
		case map[string]any:
			flatten(key+".", t, out)
		case []any:
			for i, e := range t {
				ek := key + "." + strconv.Itoa(i)
				if em, ok := e.(map[string]any); ok {
					flatten(ek+".", em, out)
				} else {
					out[ek] = e
				}
			}
		default:
			out[key] = v
		}
	}
}

func setPath(m map[string]any, path []string, v any) {
	if len(path) == 1 {
		m[path[0]] = v

		return
	}

	child, ok := m[path[0]].(map[string]any)
	if !ok {
		child = map[string]any{}
		m[path[0]] = child
	}

	setPath(child, path[1:], v)
}

func removePath(m map[string]any, path []string) {
	if len(path) == 1 {
		delete(m, path[0])

		return
	}

	if child, ok := m[path[0]].(map[string]any); ok {
		removePath(child, path[1:])
	}
}

// compileMessageExpr compiles src against the message environment; want is the required output type or nil.
func (b *InMemoryBackend) compileMessageExpr(src string, want *cel.Type) (cel.Program, error) {
	ast, iss := b.msgEnv.Compile(src)
	if iss.Err() != nil {
		return nil, iss.Err() //nolint:wrapcheck // message is returned to the caller verbatim
	}

	if want != nil && ast.OutputType() != want && ast.OutputType() != cel.DynType {
		return nil, fmt.Errorf("%w: want %s, got %s", errCELResult, want, ast.OutputType())
	}

	return b.msgEnv.Program(ast)
}

func evalMessage(prg cel.Program, msg map[string]any) (ref.Val, error) {
	out, _, err := prg.Eval(map[string]any{"message": msg})
	if err != nil {
		return nil, err //nolint:wrapcheck // surfaced as the delivery error
	}

	return out, nil
}
