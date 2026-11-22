package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sqliteImport = "modernc.org/sqlite"
	vtabImport   = "modernc.org/sqlite/vtab"
)

// sqliteEnginePrefixes maps each package that registers process-global SQLite functions to its own prefix.
// The driver registry is shared by every sql.Open("sqlite") in the process and keyed by name alone, so a plain
// name shadows a built-in or another engine's function.
func sqliteEnginePrefixes() map[string]string {
	return map[string]string{
		"services/athena":            "athena_",
		"services/gcpbigquery/bqsql": "bq_",
		"services/timestreamquery":   "ts_",
	}
}

// sqlitePrefixHelpers are functions whose result is always prefixed by the calling package.
func sqlitePrefixHelpers() map[string]string {
	return map[string]string{
		"engineFunc":       "services/timestreamquery: prepends funcPrefix",
		"record":           "services/athena: funcRegistry.record returns prefixedFuncName(name)",
		"prefixedFuncName": "services/athena: prepends funcPrefix",
	}
}

// sqliteDynamicExemptions lists registrations whose name is a parameter, keyed "dir/file.go:func".
func sqliteDynamicExemptions() map[string]string {
	return map[string]string{
		"services/gcpbigquery/bqsql/udf.go:register": "every caller passes \"bq_\"+name",
	}
}

func sqliteRegisterFuncs() []string {
	return []string{
		"RegisterFunction", "MustRegisterFunction",
		"RegisterScalarFunction", "MustRegisterScalarFunction",
		"RegisterDeterministicScalarFunction", "MustRegisterDeterministicScalarFunction",
		"RegisterCollationUtf8", "MustRegisterCollationUtf8", "RegisterModule",
	}
}

type sqliteReg struct {
	where string
	name  string
	err   string
}

type sqlitePkg struct {
	fset  *token.FileSet
	dir   string
	files []*ast.File
}

func parsePkg(t *testing.T, dir string, sources map[string]string) sqlitePkg {
	t.Helper()

	p := sqlitePkg{fset: token.NewFileSet(), dir: dir}

	if sources == nil {
		paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
		require.NoError(t, err)

		sources = map[string]string{}

		for _, path := range paths {
			if !strings.HasSuffix(path, "_test.go") {
				sources[path] = ""
			}
		}
	}

	for path, src := range sources {
		var in any
		if src != "" {
			in = src
		}

		f, err := parser.ParseFile(p.fset, path, in, parser.SkipObjectResolution)
		require.NoError(t, err)

		p.files = append(p.files, f)
	}

	return p
}

func sqliteAliases(f *ast.File) map[string]bool {
	out := map[string]bool{}

	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		if path != sqliteImport && path != vtabImport {
			continue
		}

		name := filepath.Base(path)
		if imp.Name != nil {
			name = imp.Name.Name
		}

		out[name] = true
	}

	return out
}

func stringConsts(files []*ast.File) map[string]string {
	out := map[string]string{}

	for _, f := range files {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}

			for _, s := range gd.Specs {
				vs, _ := s.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}

					if lit, isLit := vs.Values[i].(*ast.BasicLit); isLit && lit.Kind == token.STRING {
						out[n.Name], _ = strconv.Unquote(lit.Value)
					}
				}
			}
		}
	}

	return out
}

// staticName resolves the leading literal of a registered name; ok=false means it is not statically known.
func staticName(e ast.Expr, consts map[string]string) (string, bool) {
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind == token.STRING {
			s, _ := strconv.Unquote(x.Value)

			return s, true
		}
	case *ast.BinaryExpr:
		if x.Op == token.ADD {
			return staticName(x.X, consts)
		}
	case *ast.Ident:
		s, ok := consts[x.Name]

		return s, ok
	case *ast.ParenExpr:
		return staticName(x.X, consts)
	}

	return "", false
}

func callsPrefixHelper(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}

	var name string

	switch fn := call.Fun.(type) {
	case *ast.Ident:
		name = fn.Name
	case *ast.SelectorExpr:
		name = fn.Sel.Name
	}

	_, isHelper := sqlitePrefixHelpers()[name]

	return isHelper
}

// assignedPrefixed reports whether every value assigned to ident in fn is a literal carrying prefix.
func assignedPrefixed(fn *ast.FuncDecl, ident, prefix string) bool {
	found, allOK := false, true

	ast.Inspect(fn, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}

		for i, lhs := range as.Lhs {
			if id, isIdent := lhs.(*ast.Ident); !isIdent || id.Name != ident || i >= len(as.Rhs) {
				continue
			}

			found = true

			if s, known := staticName(as.Rhs[i], nil); !known || !strings.HasPrefix(s, prefix) {
				allOK = false
			}
		}

		return true
	})

	return found && allOK
}

// rangedKeysPrefixed reports whether ident is the key of a range over a map literal in fn whose keys all carry prefix.
func rangedKeysPrefixed(fn *ast.FuncDecl, ident, prefix string) bool {
	found, allOK := false, true

	ast.Inspect(fn, func(n ast.Node) bool {
		rs, ok := n.(*ast.RangeStmt)
		if !ok {
			return true
		}

		if k, isIdent := rs.Key.(*ast.Ident); !isIdent || k.Name != ident {
			return true
		}

		lit, isLit := rs.X.(*ast.CompositeLit)
		if !isLit {
			return true
		}

		found = true

		for _, el := range lit.Elts {
			kv, isKV := el.(*ast.KeyValueExpr)
			if !isKV {
				allOK = false

				continue
			}

			if s, known := staticName(kv.Key, nil); !known || !strings.HasPrefix(s, prefix) {
				allOK = false
			}
		}

		return true
	})

	return found && allOK
}

func registrationName(sel *ast.SelectorExpr, call *ast.CallExpr) (ast.Expr, bool) {
	if !slices.Contains(sqliteRegisterFuncs(), sel.Sel.Name) {
		return nil, false
	}

	idx := 0
	if sel.Sel.Name == "RegisterModule" {
		idx = 1
	}

	if len(call.Args) <= idx {
		return nil, false
	}

	return call.Args[idx], true
}

// sqliteRegistrations lists the process-global registrations in p with the reason each one is unsafe, if any.
func sqliteRegistrations(p sqlitePkg, prefix string, exempt map[string]string) []sqliteReg {
	consts := stringConsts(p.files)

	var out []sqliteReg

	for _, f := range p.files {
		aliases := sqliteAliases(f)

		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}

				sel, isSel := call.Fun.(*ast.SelectorExpr)
				if !isSel {
					return true
				}

				if id, isIdent := sel.X.(*ast.Ident); !isIdent || !aliases[id.Name] {
					return true
				}

				pos := p.fset.Position(call.Pos())
				where := pos.Filename + ":" + strconv.Itoa(pos.Line)

				if sel.Sel.Name == "RegisterConnectionHook" {
					out = append(
						out,
						sqliteReg{
							where: where,
							err:   "RegisterConnectionHook applies to every connection in the process",
						},
					)

					return true
				}

				if nameArg, isReg := registrationName(sel, call); isReg {
					key := pos.Filename + ":" + fn.Name.Name
					out = append(out, classify(prefix, where, key, fn, nameArg, consts, exempt))
				}

				return true
			})
		}
	}

	return out
}

func classify(
	prefix, where, exemptKey string, fn *ast.FuncDecl, nameArg ast.Expr, consts, exempt map[string]string,
) sqliteReg {
	if prefix == "" {
		return sqliteReg{
			where: where,
			err:   "package registers process-global SQLite functions but has no sqliteEnginePrefixes entry",
		}
	}

	if callsPrefixHelper(nameArg) {
		return sqliteReg{where: where}
	}

	if s, ok := staticName(nameArg, consts); ok {
		if strings.HasPrefix(s, prefix) {
			return sqliteReg{where: where, name: s}
		}

		return sqliteReg{where: where, name: s, err: "name " + strconv.Quote(s) + " lacks prefix " + prefix}
	}

	if id, isIdent := nameArg.(*ast.Ident); isIdent &&
		(rangedKeysPrefixed(fn, id.Name, prefix) || assignedPrefixed(fn, id.Name, prefix)) {
		return sqliteReg{where: where}
	}

	if _, ok := exempt[exemptKey]; ok {
		return sqliteReg{where: where}
	}

	return sqliteReg{where: where, err: "name is not statically prefixed with " + prefix}
}

func TestSqliteRegistrationClassifier(t *testing.T) {
	t.Parallel()

	const head = "package p\nimport sqlitedriver \"modernc.org/sqlite\"\nconst modName = \"ts_mod\"\n"

	tests := []struct {
		exempt  map[string]string
		name    string
		body    string
		wantErr bool
		none    bool
	}{
		{name: "prefixed literal", body: `func f() { sqlitedriver.MustRegisterScalarFunction("ts_a", 1, nil) }`},
		{
			name:    "plain literal",
			body:    `func f() { sqlitedriver.MustRegisterScalarFunction("repeat", 1, nil) }`,
			wantErr: true,
		},
		{
			name:    "builtin override",
			body:    `func f() { sqlitedriver.MustRegisterDeterministicScalarFunction("replace", 3, nil) }`,
			wantErr: true,
		},
		{
			name:    "other engine prefix",
			body:    `func f() { sqlitedriver.MustRegisterFunction("bq_x", nil) }`,
			wantErr: true,
		},
		{name: "helper", body: `func f() { sqlitedriver.MustRegisterFunction(engineFunc("x"), nil) }`},
		{name: "concat prefixed", body: `func f(n string) { sqlitedriver.MustRegisterFunction("ts_"+n, nil) }`},
		{
			name:    "concat plain",
			body:    `func f(n string) { sqlitedriver.MustRegisterFunction(n+"_x", nil) }`,
			wantErr: true,
		},
		{name: "const", body: `func f() { sqlitedriver.RegisterScalarFunction(modName, 0, nil) }`},
		{
			name: "ranged prefixed keys",
			body: `func f() { for n := range map[string]int{"ts_a": 1, "ts_b": 2} {
				sqlitedriver.MustRegisterFunction(n, nil) } }`,
		},
		{
			name: "ranged plain key",
			body: `func f() { for n := range map[string]int{"ts_a": 1, "b": 2} {
				sqlitedriver.MustRegisterFunction(n, nil) } }`,
			wantErr: true,
		},
		{name: "dynamic", body: `func f(n string) { sqlitedriver.MustRegisterFunction(n, nil) }`, wantErr: true},
		{
			name:   "dynamic exempt",
			body:   `func f(n string) { sqlitedriver.MustRegisterFunction(n, nil) }`,
			exempt: map[string]string{"x/a.go:f": "caller prefixes"},
		},
		{name: "global hook", body: `func f() { sqlitedriver.RegisterConnectionHook(nil) }`, wantErr: true},
		{
			name: "unrelated receiver",
			body: `func f(r interface{ MustRegisterFunction(string) }) { r.MustRegisterFunction("repeat") }`,
			none: true,
		},
		{
			name: "assigned prefixed",
			body: `func f() { n := "ts_a"; n = "ts_b"; sqlitedriver.MustRegisterFunction(n, nil) }`,
		},
		{
			name:    "assigned plain",
			body:    `func f() { n := "ts_a"; n = "b"; sqlitedriver.MustRegisterFunction(n, nil) }`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := parsePkg(t, "x", map[string]string{"x/a.go": head + tt.body})
			regs := sqliteRegistrations(p, "ts_", tt.exempt)

			if tt.none {
				assert.Empty(t, regs)

				return
			}

			require.NotEmpty(t, regs, "the registration must be found")

			var errs []string

			for _, r := range regs {
				if r.err != "" {
					errs = append(errs, r.err)
				}
			}

			if tt.wantErr {
				assert.NotEmpty(t, errs)
			} else {
				assert.Empty(t, errs)
			}
		})
	}
}

func TestSqliteFunctionsArePrefixedPerPackage(t *testing.T) {
	t.Parallel()

	prefixes := sqliteEnginePrefixes()

	for a, pa := range prefixes {
		for b, pb := range prefixes {
			if a != b {
				assert.False(t, strings.HasPrefix(pa, pb), "%s and %s prefixes overlap", a, b)
			}
		}
	}

	var offenders []string

	owner := map[string]string{}

	err := filepath.WalkDir("services", func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}

		p := parsePkg(t, path, nil)

		for _, r := range sqliteRegistrations(p, prefixes[filepath.ToSlash(path)], sqliteDynamicExemptions()) {
			if r.err != "" {
				offenders = append(offenders, r.where+": "+r.err)
			}

			if prev, dup := owner[r.name]; r.name != "" && dup && prev != path {
				offenders = append(offenders, r.where+": "+r.name+" is also registered by "+prev)
			}

			if r.name != "" {
				owner[r.name] = path
			}
		}

		return nil
	})
	require.NoError(t, err)

	assert.Empty(t, offenders)
}
