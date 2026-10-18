package restroute

import (
	"context"
	"reflect"
	"strconv"
)

type binding struct {
	name  string
	index []int
	query bool
}

// Typed adapts fn to an OpFunc. Fields tagged `path:"Label"` or `query:"name"` are filled from the URL;
// every other exported field comes from the JSON body, so URL-bound fields must also carry `json:"-"`.
func Typed[I, O any](fn func(ctx context.Context, in *I) (*O, error)) OpFunc {
	bindings := bindingsOf(reflect.TypeFor[I]())

	return func(ctx context.Context, req *Request) (any, int, error) {
		var in I

		if err := req.Decode(&in); err != nil {
			return nil, 0, err
		}

		v := reflect.ValueOf(&in).Elem()

		for _, b := range bindings {
			var raw []string

			if b.query {
				raw = req.Query[b.name]
			} else if p, ok := req.Params[b.name]; ok {
				raw = []string{p}
			}

			if len(raw) == 0 {
				continue
			}

			if err := setField(v.FieldByIndex(b.index), raw); err != nil {
				return nil, 0, Validation("%s is not valid", b.name)
			}
		}

		out, err := fn(ctx, &in)
		if err != nil || out == nil {
			return nil, 0, err
		}

		return out, 0, nil
	}
}

func bindingsOf(t reflect.Type) []binding {
	var out []binding

	for i := range t.NumField() {
		f := t.Field(i)

		switch name, isPath := f.Tag.Lookup("path"); {
		case isPath:
			out = append(out, binding{name: name, index: []int{i}})
		case f.Anonymous && f.Type.Kind() == reflect.Struct:
			for _, inner := range bindingsOf(f.Type) {
				inner.index = append([]int{i}, inner.index...)
				out = append(out, inner)
			}
		default:
			if q, isQuery := f.Tag.Lookup("query"); isQuery {
				out = append(out, binding{name: q, index: []int{i}, query: true})
			}
		}
	}

	return out
}

func setField(f reflect.Value, raw []string) error {
	switch f.Kind() {
	case reflect.Pointer:
		elem := reflect.New(f.Type().Elem())
		if err := setField(elem.Elem(), raw); err != nil {
			return err
		}

		f.Set(elem)
	case reflect.Slice:
		s := reflect.MakeSlice(f.Type(), len(raw), len(raw))
		for i, r := range raw {
			if err := setField(s.Index(i), []string{r}); err != nil {
				return err
			}
		}

		f.Set(s)
	case reflect.String:
		f.SetString(raw[0])
	case reflect.Bool:
		b, err := strconv.ParseBool(raw[0])
		if err != nil {
			return err
		}

		f.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw[0], 10, f.Type().Bits())
		if err != nil {
			return err
		}

		f.SetInt(n)
	default:
		return strconv.ErrSyntax
	}

	return nil
}
