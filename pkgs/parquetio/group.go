package parquetio

import (
	"reflect"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding"
)

// orderedGroup is a root group that keeps column order; parquet.Group is a
// map and sorts its fields by name.
type orderedGroup struct {
	fields []parquet.Field
}

type orderedField struct {
	parquet.Node
	name string
}

func (f *orderedField) Name() string { return f.name }

func (f *orderedField) Value(base reflect.Value) reflect.Value { return base }

func newOrderedGroup() *orderedGroup { return &orderedGroup{} }

func (g *orderedGroup) add(name string, n parquet.Node) {
	g.fields = append(g.fields, &orderedField{Node: n, name: name})
}

func (g *orderedGroup) ID() int                     { return 0 }
func (g *orderedGroup) String() string              { return "group" }
func (g *orderedGroup) Type() parquet.Type          { return parquet.Group{}.Type() }
func (g *orderedGroup) Optional() bool              { return false }
func (g *orderedGroup) Repeated() bool              { return false }
func (g *orderedGroup) Required() bool              { return true }
func (g *orderedGroup) Leaf() bool                  { return false }
func (g *orderedGroup) Fields() []parquet.Field     { return g.fields }
func (g *orderedGroup) Encoding() encoding.Encoding { return nil }
func (g *orderedGroup) Compression() compress.Codec { return nil }
func (g *orderedGroup) GoType() reflect.Type        { return reflect.TypeOf(struct{}{}) }
