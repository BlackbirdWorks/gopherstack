package parquetio

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/blackbirdworks/gopherstack/pkgs/hivetype"
)

// Definition level steps: an optional group adds one level, a repeated entry with an optional group adds two.
const (
	defOptional = 1
	defRepeated = 2
)

// nnode is the write-side shape of one nested column: how values shred into leaf columns.
type nnode struct {
	enc      cellEnc
	path     string
	children []*nnode
	names    []string
	kind     hivetype.Kind
	col      int
	maxRep   int
	required bool
	leaf     bool
}

func leafType(t *hivetype.Type) string {
	switch t.Kind {
	case hivetype.Decimal:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision, t.Scale)
	case hivetype.Boolean:
		return typeBoolean
	case hivetype.Tinyint:
		return typeTinyint
	case hivetype.Smallint:
		return typeSmall
	case hivetype.Int:
		return typeInt
	case hivetype.Bigint:
		return typeBigint
	case hivetype.Float:
		return typeFloat
	case hivetype.Double:
		return typeDouble
	case hivetype.Binary:
		return typeBinary
	case hivetype.Date:
		return typeDate
	case hivetype.Timestamp:
		return typeTS
	default:
		return typeString
	}
}

// buildNested returns the Parquet node and shredding plan for a nested column type.
func buildNested(
	t *hivetype.Type,
	path []string,
	lpath string,
	ids map[string]int,
	rep int,
	required bool,
) (parquet.Node, *nnode, error) {
	n := &nnode{kind: t.Kind, path: strings.Join(path, "."), required: required, maxRep: rep}

	switch t.Kind {
	case hivetype.Array:
		elemNode, elem, err := buildNested(
			t.Elem, append(slices.Clip(path), "list", "element"), lpath+".element", ids, rep+1, false)
		if err != nil {
			return nil, nil, err
		}

		n.children, n.maxRep = []*nnode{elem}, rep+1

		return withID(optionalUnless(parquet.List(elemNode), required), ids, lpath), n, nil
	case hivetype.Map:
		keyNode, key, err := buildNested(
			t.Key, append(slices.Clip(path), "key_value", "key"), lpath+".key", ids, rep+1, true)
		if err != nil {
			return nil, nil, err
		}

		valNode, val, err := buildNested(
			t.Value, append(slices.Clip(path), "key_value", "value"), lpath+".value", ids, rep+1, false)
		if err != nil {
			return nil, nil, err
		}

		n.children, n.maxRep = []*nnode{key, val}, rep+1

		return withID(optionalUnless(parquet.Map(keyNode, valNode), required), ids, lpath), n, nil
	case hivetype.Struct:
		group := newOrderedGroup()

		for _, f := range t.Fields {
			fn, child, err := buildNested(
				f.Type, append(slices.Clip(path), f.Name), lpath+"."+f.Name, ids, rep, false)
			if err != nil {
				return nil, nil, err
			}

			group.add(f.Name, fn)

			n.children, n.names = append(n.children, child), append(n.names, f.Name)
		}

		return withID(optionalUnless(group, required), ids, lpath), n, nil
	default:
		enc, err := encoderFor(leafType(t))
		if err != nil {
			return nil, nil, err
		}

		n.leaf, n.enc = true, enc.enc

		return withID(optionalUnless(enc.node, required), ids, lpath), n, nil
	}
}

func withID(n parquet.Node, ids map[string]int, lpath string) parquet.Node {
	if id := ids[lpath]; id > 0 {
		return parquet.FieldID(n, id)
	}

	return n
}

func optionalUnless(n parquet.Node, required bool) parquet.Node {
	if required {
		return n
	}

	return parquet.Optional(n)
}

// bind records the column index of every leaf under n from the schema's column paths.
func (n *nnode) bind(pos map[string]int) {
	if n.leaf {
		n.col = pos[n.path]

		return
	}

	for _, c := range n.children {
		c.bind(pos)
	}
}

type shredOut map[int][]parquet.Value

func (n *nnode) nullLeaves(out shredOut, rep, def int) {
	if n.leaf {
		out[n.col] = append(out[n.col], parquet.NullValue().Level(rep, def, n.col))

		return
	}

	for _, c := range n.children {
		c.nullLeaves(out, rep, def)
	}
}

// shred emits the leaf values of v with repetition rep and definition level def of the parent.
func (n *nnode) shred(out shredOut, v any, rep, def int) error {
	if v == nil {
		n.nullLeaves(out, rep, def)

		return nil
	}

	switch n.kind {
	case hivetype.Array:
		return n.shredList(out, v, rep, def)
	case hivetype.Map:
		return n.shredMap(out, v, rep, def)
	case hivetype.Struct:
		return n.shredStruct(out, v, rep, def)
	default:
		pv, err := n.enc(v)
		if err != nil {
			return err
		}

		lvl := def
		if !n.required {
			lvl++
		}

		out[n.col] = append(out[n.col], pv.Level(rep, lvl, n.col))

		return nil
	}
}

func (n *nnode) shredStruct(out shredOut, v any, rep, def int) error {
	obj, err := asObject(v)
	if err != nil {
		return err
	}

	for i, c := range n.children {
		var fv any

		if x, ok := obj[n.names[i]]; ok {
			fv = x
		} else {
			for k, x := range obj {
				if strings.EqualFold(k, n.names[i]) {
					fv = x
				}
			}
		}

		if err = c.shred(out, fv, rep, def+defOptional); err != nil {
			return err
		}
	}

	return nil
}

func (n *nnode) shredList(out shredOut, v any, rep, def int) error {
	arr, err := asArray(v)
	if err != nil {
		return err
	}

	if len(arr) == 0 {
		n.nullLeaves(out, rep, def+defOptional)

		return nil
	}

	for i, e := range arr {
		r := rep
		if i > 0 {
			r = n.maxRep
		}

		if err = n.children[0].shred(out, e, r, def+defRepeated); err != nil {
			return err
		}
	}

	return nil
}

func (n *nnode) shredMap(out shredOut, v any, rep, def int) error {
	obj, err := asObject(v)
	if err != nil {
		return err
	}

	if len(obj) == 0 {
		n.nullLeaves(out, rep, def+defOptional)

		return nil
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for i, k := range keys {
		r := rep
		if i > 0 {
			r = n.maxRep
		}

		if err = n.children[0].shred(out, k, r, def+defRepeated); err != nil {
			return err
		}

		if err = n.children[1].shred(out, obj[k], r, def+defRepeated); err != nil {
			return err
		}
	}

	return nil
}
