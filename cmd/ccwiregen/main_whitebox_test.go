package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire"
)

func TestGeneratedFilesUpToDate(t *testing.T) {
	t.Parallel()

	for _, s := range allSpecs() {
		t.Run(s.module, func(t *testing.T) {
			t.Parallel()

			want, err := render(s)
			require.NoError(t, err)

			got, err := os.ReadFile(filepath.Join("..", "..", s.outFile()))
			require.NoError(t, err)
			assert.Equal(t, string(want), string(got), "run `go run ./cmd/ccwiregen` from the repo root")
		})
	}
}

func TestWireStructsMatchSchemaMembers(t *testing.T) {
	t.Parallel()

	for _, s := range wireSpecs() {
		t.Run(s.module, func(t *testing.T) {
			t.Parallel()

			src, err := render(s)
			require.NoError(t, err)

			f, err := parser.ParseFile(token.NewFileSet(), "gen.go", src, 0)
			require.NoError(t, err)

			fields := map[string][]string{}

			for _, d := range f.Decls {
				gd, ok := d.(*ast.GenDecl)
				if !ok || gd.Tok != token.TYPE {
					continue
				}

				ts, _ := gd.Specs[0].(*ast.TypeSpec)
				if st, isStruct := ts.Type.(*ast.StructType); isStruct {
					for _, fld := range st.Fields.List {
						fields[ts.Name.Name] = append(fields[ts.Name.Name], strings.ToLower(fld.Names[0].Name))
					}
				}
			}

			svc, ok := sdkwire.Lookup(s.module)
			require.True(t, ok)

			for _, op := range s.ops {
				o, found := svc.Ops[op]
				require.True(t, found, op)

				for suffix, schema := range map[string]*smithy.Schema{"Input": o.Schema.Input, "Output": o.Schema.Output} {
					var members []string

					if schema != nil {
						for _, m := range schema.Members() {
							members = append(members, strings.ToLower(m.MemberName()))
						}
					}

					want := slices.DeleteFunc(members, func(m string) bool {
						return slices.ContainsFunc(s.unions, func(u string) bool { return strings.EqualFold(u, m) })
					})

					assert.ElementsMatch(t, want, fields[op+suffix], "%s%s", op, suffix)
				}
			}
		})
	}
}
