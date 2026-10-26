package gcpdatastore

import (
	"math"
	"strings"
	"time"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func splitPath(p string) []string { return strings.Split(p, ".") }

// transform applies property transforms in order and returns each one's result.
func (c *commitCtx) transform(
	e *datastorepb.Entity, pts []*datastorepb.PropertyTransform,
) ([]*datastorepb.Value, error) {
	if len(pts) == 0 {
		return nil, nil
	}

	out := make([]*datastorepb.Value, 0, len(pts))

	for _, pt := range pts {
		if pt.GetProperty() == "" || isReserved(pt.GetProperty()) {
			return nil, errInvalid("Invalid property transform path %q.", pt.GetProperty())
		}

		parent := ensureParent(e, splitPath(pt.GetProperty()))
		name := splitPath(pt.GetProperty())[len(splitPath(pt.GetProperty()))-1]

		res, err := applyTransform(parent.GetProperties()[name], pt, c.at)
		if err != nil {
			return nil, err
		}

		if res.set != nil {
			parent.GetProperties()[name] = res.set
		}

		out = append(out, res.result)
	}

	return out, nil
}

func ensureParent(e *datastorepb.Entity, path []string) *datastorepb.Entity {
	cur := e
	if cur.GetProperties() == nil {
		cur.Properties = map[string]*datastorepb.Value{}
	}

	for _, head := range path[:len(path)-1] {
		v := cur.GetProperties()[head]
		ev, ok := v.GetValueType().(*datastorepb.Value_EntityValue)

		if !ok {
			ev = &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{}}
			cur.GetProperties()[head] = &datastorepb.Value{ValueType: ev}
		}

		if ev.EntityValue.GetProperties() == nil {
			ev.EntityValue.Properties = map[string]*datastorepb.Value{}
		}

		cur = ev.EntityValue
	}

	return cur
}

type transformResult struct {
	set    *datastorepb.Value
	result *datastorepb.Value
}

func isNumber(v *datastorepb.Value) bool {
	switch v.GetValueType().(type) {
	case *datastorepb.Value_IntegerValue, *datastorepb.Value_DoubleValue:
		return true
	default:
		return false
	}
}

func asFloat(v *datastorepb.Value) float64 {
	if _, ok := v.GetValueType().(*datastorepb.Value_DoubleValue); ok {
		return v.GetDoubleValue()
	}

	return float64(v.GetIntegerValue())
}

func isDouble(v *datastorepb.Value) bool {
	_, ok := v.GetValueType().(*datastorepb.Value_DoubleValue)

	return ok
}

func addChecked(a, b int64) (int64, bool) {
	s := a + b
	if (a > 0 && b > 0 && s < 0) || (a < 0 && b < 0 && s >= 0) {
		return s, true
	}

	return s, false
}

func addInts(a, b int64) int64 {
	s, over := addChecked(a, b)
	if !over {
		return s
	}

	if a > 0 {
		return math.MaxInt64
	}

	return math.MinInt64
}

func applyTransform(
	cur *datastorepb.Value, pt *datastorepb.PropertyTransform, at time.Time,
) (transformResult, error) {
	switch t := pt.GetTransformType().(type) {
	case *datastorepb.PropertyTransform_SetToServerValue:
		if t.SetToServerValue != datastorepb.PropertyTransform_REQUEST_TIME {
			return transformResult{}, errInvalid("Unsupported server value for property %q.", pt.GetProperty())
		}

		v := timestampValue(timestamppb.New(at))

		return transformResult{set: v, result: v}, nil
	case *datastorepb.PropertyTransform_Increment:
		return numericTransform(cur, t.Increment, "increment")
	case *datastorepb.PropertyTransform_Maximum:
		return numericTransform(cur, t.Maximum, "maximum")
	case *datastorepb.PropertyTransform_Minimum:
		return numericTransform(cur, t.Minimum, "minimum")
	case *datastorepb.PropertyTransform_AppendMissingElements:
		return arrayTransform(cur, t.AppendMissingElements.GetValues(), true), nil
	case *datastorepb.PropertyTransform_RemoveAllFromArray:
		return arrayTransform(cur, t.RemoveAllFromArray.GetValues(), false), nil
	default:
		return transformResult{}, errInvalid("Property transform %q has no transform type.", pt.GetProperty())
	}
}

func numericTransform(cur, arg *datastorepb.Value, op string) (transformResult, error) {
	if !isNumber(arg) {
		return transformResult{}, errInvalid("The %s transform requires an integer or double value.", op)
	}

	if !isNumber(cur) {
		return transformResult{set: arg, result: arg}, nil
	}

	var v *datastorepb.Value

	switch op {
	case "increment":
		if isDouble(cur) || isDouble(arg) {
			v = doubleValue(asFloat(cur) + asFloat(arg))
		} else {
			v = intValue(addInts(cur.GetIntegerValue(), arg.GetIntegerValue()))
		}
	default:
		v = pickExtreme(cur, arg, op == "maximum")
	}

	return transformResult{set: v, result: v}, nil
}

func pickExtreme(cur, arg *datastorepb.Value, wantMax bool) *datastorepb.Value {
	a, b := asFloat(cur), asFloat(arg)

	switch {
	case math.IsNaN(a):
		return cur
	case math.IsNaN(b):
		return arg
	case a == b:
		return cur
	case (a > b) == wantMax:
		return cur
	default:
		return arg
	}
}

func arrayTransform(cur *datastorepb.Value, args []*datastorepb.Value, appendMissing bool) transformResult {
	existing := arrayElems(cur)
	next := make([]*datastorepb.Value, 0, len(existing)+len(args))

	if appendMissing {
		next = append(next, existing...)

		for _, a := range args {
			if !containsValue(next, a) {
				next = append(next, a)
			}
		}
	} else {
		for _, e := range existing {
			if !containsValue(args, e) {
				next = append(next, e)
			}
		}
	}

	return transformResult{
		set: &datastorepb.Value{
			ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: next}},
		},
		result: nullValue(),
	}
}

func containsValue(list []*datastorepb.Value, v *datastorepb.Value) bool {
	for _, e := range list {
		if bucketOf(e) == bucketOther || bucketOf(v) == bucketOther {
			if proto.Equal(e, v) {
				return true
			}

			continue
		}

		if compareValues(e, v) == 0 {
			return true
		}
	}

	return false
}
