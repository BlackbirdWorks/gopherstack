package persistence_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errNoVersionStruct = errors.New("no struct with an int-typed Version field")

var errPersistenceFileMissing = errors.New("persistence.go missing after parse")

// updateGolden is package-level because flag.Bool must register before
// TestMain/go test parses os.Args; there is no per-test alternative.
//
//nolint:gochecknoglobals // test flag, required by flag.Bool's registration model
var updateGolden = flag.Bool("update", false, "regenerate pkgs/persistence/testdata/snapshot_inventory.json")

// TestSnapshotVersionGuard is the enforcement mechanism for the mistake described in
// bd issue gopherstack-s8bk: an agent adds a field to a service's backendSnapshot and
// reflexively bumps that service's snapshot-version constant. encoding/json already
// decodes an older snapshot missing a new field fine (the field zero-values); bumping
// the version instead sends Restore down the ResetAll path and discards every user's
// persisted state on the exact upgrade that was only meant to extend it.
//
// The guard walks every services/*/persistence.go, extracts the <service>SnapshotVersion
// constant, and then recursively expands the fields of every "*Snapshot"-suffixed struct
// in the *whole service package* (not just persistence.go, and not just one level deep --
// gopherstack-5i6p: apigateway's additive Tags field landed on the nested stageSnapshot,
// invisible to a scan limited to the top-level struct). Recursion also follows every type
// registered via store.Register(reg, name, store.New(keyFn)): most services persist the
// domain model directly (e.g. eks's Cluster) rather than a dedicated *Snapshot DTO, and
// [store.Registry.SnapshotAll] erases that model to json.RawMessage -- a field-type string
// like "Tables map[string]json.RawMessage" never changes no matter what happens inside
// Cluster, so without resolving store.New's type parameter back to Cluster and expanding
// its fields too, a shape change there is invisible to this scan (gopherstack-hjdd: this is
// exactly how the eks NetworkingConfig/KubernetesNetworkConfig merge got through undetected
// even though the guard was already live). See scanServiceDir for the resolution details and
// its known blind spots (cross-package types, unresolved keyFn expressions).
//
// The combined field set is compared against the checked-in golden at
// testdata/snapshot_inventory.json. If the version changed and the new field set is a
// pure superset of the golden one (every old field still present, unchanged; only
// additions) that is exactly the destructive pattern -- the test fails unconditionally,
// even under -update, so there is no way to silence it by just refreshing the golden.
//
// A genuine incompatible change (a field removed or retyped) is not a superset, so it
// passes this check; run with -update to refresh the golden once you've confirmed the
// bump is warranted:
//
//	go test ./pkgs/persistence/... -run TestSnapshotVersionGuard -update
func TestSnapshotVersionGuard(t *testing.T) {
	t.Parallel()

	live, err := scanServiceSnapshots(servicesDir(t))
	require.NoError(t, err)

	golden := loadGolden(t)

	violations := diffSnapshots(live, golden)

	if *updateGolden {
		hardFailed := false

		for _, v := range violations {
			if strings.Contains(v, "PURELY ADDITIVE") {
				t.Error(v)

				hardFailed = true
			}
		}

		require.False(t, hardFailed, "refusing to write golden: at least one purely-additive version bump detected")

		writeGolden(t, live)

		return
	}

	sort.Strings(violations)

	for _, v := range violations {
		t.Error(v)
	}
}

// diffSnapshots compares the live-scanned snapshot info against the checked-in
// golden and returns one violation string per problem found. A version change
// with an unchanged field list (want.Version != got.Version but the fields
// are identical) is still a violation: it means either a nested type used by
// the snapshot changed independently of the version-carrying struct (which
// this scan cannot see), or the version was bumped for no reason -- either
// way it must be confirmed, not silently absorbed by the next -update.
func diffSnapshots(live, golden map[string]snapshotInfo) []string {
	var violations []string

	for name, want := range live {
		got, ok := golden[name]

		switch {
		case !ok:
			violations = append(violations, fmt.Sprintf(
				"%s: no golden entry (new persistence.go?); run with -update", name))
		case want.Version != got.Version:
			switch {
			case isPureAddition(got.Fields, want.Fields):
				violations = append(violations, fmt.Sprintf(
					"%s: version bumped %d -> %d for a PURELY ADDITIVE field change "+
						"(added: %v). encoding/json decodes an older snapshot missing "+
						"a new field fine -- do not bump the version constant for this. "+
						"Revert the bump; the field addition alone is safe.",
					name, got.Version, want.Version, addedFields(got.Fields, want.Fields)))
			case !fieldsEqual(got.Fields, want.Fields):
				violations = append(violations, fmt.Sprintf(
					"%s: version bumped %d -> %d with an incompatible struct change; "+
						"golden is out of date, run with -update to accept it", name, got.Version, want.Version))
			default:
				violations = append(violations, fmt.Sprintf(
					"%s: version bumped %d -> %d but the version-carrying struct's own "+
						"fields are unchanged; if a nested or dirty-table type changed "+
						"independently, confirm an older snapshot is still unsafe to "+
						"decode as this shape before accepting -- run with -update once "+
						"confirmed", name, got.Version, want.Version))
			}
		case !fieldsEqual(got.Fields, want.Fields):
			violations = append(violations, fmt.Sprintf(
				"%s: backendSnapshot fields changed without a version bump; golden is "+
					"out of date, run with -update to refresh it (this is bookkeeping, "+
					"not a version-bump case: additive fields never need a bump)", name))
		}
	}

	for name := range golden {
		if _, ok := live[name]; !ok {
			violations = append(violations, fmt.Sprintf(
				"%s: golden entry has no matching persistence.go; run with -update to remove it", name))
		}
	}

	return violations
}

func TestDiffSnapshots(t *testing.T) {
	t.Parallel()

	fieldsV1 := []string{
		"Account *Account `json:\"account,omitempty\"`",
		"Tables map[string]json.RawMessage `json:\"tables\"`",
	}
	fieldsV1WithExtra := append(append([]string{}, fieldsV1...),
		"UsageOverrides map[string]map[string]int64 `json:\"usageOverrides,omitempty\"`")
	fieldsV1Retyped := []string{
		"Account *Account `json:\"account,omitempty\"`",
		"Tables map[string]string `json:\"tables\"`",
	}

	tests := []struct {
		live    map[string]snapshotInfo
		golden  map[string]snapshotInfo
		name    string
		wantErr string
	}{
		{
			name:   "unchanged",
			live:   map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 1}},
			golden: map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 1}},
		},
		{
			name:    "new service no golden entry",
			live:    map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 1}},
			golden:  map[string]snapshotInfo{},
			wantErr: "no golden entry",
		},
		{
			name:    "stale golden entry",
			live:    map[string]snapshotInfo{},
			golden:  map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 1}},
			wantErr: "no matching persistence.go",
		},
		{
			name:    "fields changed without version bump",
			live:    map[string]snapshotInfo{"svc": {Fields: fieldsV1WithExtra, Version: 1}},
			golden:  map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 1}},
			wantErr: "without a version bump",
		},
		{
			name:    "purely additive field bumped version",
			live:    map[string]snapshotInfo{"svc": {Fields: fieldsV1WithExtra, Version: 2}},
			golden:  map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 1}},
			wantErr: "PURELY ADDITIVE",
		},
		{
			name:    "incompatible change bumped version",
			live:    map[string]snapshotInfo{"svc": {Fields: fieldsV1Retyped, Version: 2}},
			golden:  map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 1}},
			wantErr: "incompatible struct change",
		},
		{
			name:    "version bumped with fields unchanged",
			live:    map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 2}},
			golden:  map[string]snapshotInfo{"svc": {Fields: fieldsV1, Version: 1}},
			wantErr: "fields are unchanged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := diffSnapshots(tt.live, tt.golden)

			if tt.wantErr == "" {
				require.Empty(t, got)

				return
			}

			require.Len(t, got, 1)
			assert.Contains(t, got[0], tt.wantErr)
		})
	}
}

const goldenFile = "testdata/snapshot_inventory.json"

type snapshotInfo struct {
	Fields  []string `json:"fields"`
	Version int      `json:"version"`
}

func servicesDir(t *testing.T) string {
	t.Helper()

	dir, err := filepath.Abs(filepath.Join("..", "..", "services"))
	require.NoError(t, err)

	_, statErr := os.Stat(dir)
	require.NoError(t, statErr, "services dir not found at %s -- test assumes it runs from pkgs/persistence", dir)

	return dir
}

func loadGolden(t *testing.T) map[string]snapshotInfo {
	t.Helper()

	data, err := os.ReadFile(goldenFile)
	if os.IsNotExist(err) {
		return map[string]snapshotInfo{}
	}

	require.NoError(t, err)

	var golden map[string]snapshotInfo

	require.NoError(t, json.Unmarshal(data, &golden))

	return golden
}

func writeGolden(t *testing.T, live map[string]snapshotInfo) {
	t.Helper()

	data, err := json.MarshalIndent(live, "", "  ")
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(goldenFile, append(data, '\n'), 0o644))
}

func fieldsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// isPureAddition reports whether new is old plus zero or more extra fields, with every
// old field unchanged (same name, type and tag). A field rename, retype, or removal
// means new no longer contains that exact old descriptor, so this reports false.
func isPureAddition(oldFields, newFields []string) bool {
	if len(newFields) <= len(oldFields) {
		return false
	}

	present := make(map[string]bool, len(newFields))
	for _, f := range newFields {
		present[f] = true
	}

	for _, f := range oldFields {
		if !present[f] {
			return false
		}
	}

	return true
}

func addedFields(oldFields, newFields []string) []string {
	old := make(map[string]bool, len(oldFields))
	for _, f := range oldFields {
		old[f] = true
	}

	var added []string

	for _, f := range newFields {
		if !old[f] {
			added = append(added, f)
		}
	}

	return added
}

var versionConstRE = regexp.MustCompile(`(?i)snapshotversion$`)

// scanServiceSnapshots parses every services/*/persistence.go (plus the rest of that
// service's package, for type resolution -- see scanServiceDir) and extracts, per
// service, the version constant and the recursively-expanded fields reachable from its
// persisted state.
func scanServiceSnapshots(servicesDir string) (map[string]snapshotInfo, error) {
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return nil, err
	}

	result := make(map[string]snapshotInfo)

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		dir := filepath.Join(servicesDir, e.Name())
		if _, statErr := os.Stat(filepath.Join(dir, "persistence.go")); statErr != nil {
			continue
		}

		info, ok, scanErr := scanServiceDir(dir)
		if scanErr != nil {
			return nil, fmt.Errorf("%s: %w", dir, scanErr)
		}

		if ok {
			result[e.Name()] = info
		}
	}

	return result, nil
}

// scanServiceDir parses every non-test .go file in a service package and produces its
// snapshotInfo. persistence.go alone supplies the version constant (repo convention: it
// always lives there). Field expansion starts from every "*Snapshot"-suffixed struct
// declared anywhere in the package, plus every type resolved from a
// store.Register(reg, name, store.New(keyFn)) call anywhere in the package (this is what
// recovers domain models like eks's Cluster that are persisted directly rather than
// through a dedicated DTO), and recurses into any locally-declared named struct type a
// field refers to.
//
// Known blind spots, left as such rather than papered over: a keyFn passed as anything
// other than a bare identifier or an inline func literal (e.g. a method value) cannot be
// resolved and its table's contents stay invisible, exactly as before this change; and a
// field naming a type from another package cannot be expanded past its printed type
// string, since only this package's declarations are parsed.
func scanServiceDir(dir string) (snapshotInfo, bool, error) {
	fset, files, err := parseServiceDir(dir)
	if err != nil {
		return snapshotInfo{}, false, err
	}

	pfile, ok := files["persistence.go"]
	if !ok {
		return snapshotInfo{}, false, fmt.Errorf("%s: %w", dir, errPersistenceFileMissing)
	}

	version, hasVersion, err := findVersionConst(pfile)
	if err != nil {
		return snapshotInfo{}, false, err
	}

	if !hasVersion {
		return snapshotInfo{}, false, nil
	}

	structDecls := collectStructDecls(files)
	funcLookup := collectFuncLookup(files)

	var snapshotRoots []string

	for name := range structDecls {
		if snapshotStructRE.MatchString(name) {
			snapshotRoots = append(snapshotRoots, name)
		}
	}

	registeredRoots, _ := collectRegisteredTypeRoots(files, funcLookup)

	hasVersionStruct := false

	for _, root := range snapshotRoots {
		if hasIntVersionField(structDecls[root]) {
			hasVersionStruct = true
		}
	}

	if !hasVersionStruct {
		return snapshotInfo{}, false, fmt.Errorf(
			"found a *SnapshotVersion const but no matching int-typed Version struct field: %w", errNoVersionStruct)
	}

	visited := make(map[string]bool)

	var fields []string

	sort.Strings(snapshotRoots)

	for _, root := range snapshotRoots {
		expandFields(fset, root, structDecls, visited, &fields)
	}

	for _, root := range registeredRoots {
		expandFields(fset, root, structDecls, visited, &fields)
	}

	sort.Strings(fields)

	return snapshotInfo{Version: version, Fields: fields}, true, nil
}

// parseServiceDir parses every non-test .go file directly inside dir (no recursion into
// subpackages) into a shared *token.FileSet, keyed by base filename.
func parseServiceDir(dir string) (*token.FileSet, map[string]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}

	fset := token.NewFileSet()
	files := make(map[string]*ast.File)

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		f, parseErr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if parseErr != nil {
			return nil, nil, parseErr
		}

		files[name] = f
	}

	return fset, files, nil
}

// collectStructDecls indexes every package-level struct type declaration across files by
// its type name.
func collectStructDecls(files map[string]*ast.File) map[string]*ast.StructType {
	out := make(map[string]*ast.StructType)

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}

			for _, spec := range gd.Specs {
				ts, isTypeSpec := spec.(*ast.TypeSpec)
				if !isTypeSpec {
					continue
				}

				if st, isStructType := ts.Type.(*ast.StructType); isStructType {
					out[ts.Name.Name] = st
				}
			}
		}
	}

	return out
}

// collectFuncLookup indexes every package-level function signature by name: both plain
// func declarations and var-bound function literals (`var fooKeyFn = func(v *Foo) string
// {...}`), since keyFns are written both ways across services.
func collectFuncLookup(files map[string]*ast.File) map[string]*ast.FuncType {
	out := make(map[string]*ast.FuncType)

	for _, f := range files {
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil {
					out[d.Name.Name] = d.Type
				}
			case *ast.GenDecl:
				if d.Tok != token.VAR {
					continue
				}

				for _, spec := range d.Specs {
					vs, isValueSpec := spec.(*ast.ValueSpec)
					if !isValueSpec || len(vs.Names) != 1 || len(vs.Values) != 1 {
						continue
					}

					if fl, isFuncLit := vs.Values[0].(*ast.FuncLit); isFuncLit {
						out[vs.Names[0].Name] = fl.Type
					}
				}
			}
		}
	}

	return out
}

// collectRegisteredTypeRoots finds every store.New(keyFn) call in files and resolves
// keyFn's single parameter type back to a type name -- the V in the Table[V] that call
// constructs. unresolved counts calls whose keyFn expression or parameter type this AST-only
// resolution could not follow (a method value, or a type from another package).
func collectRegisteredTypeRoots(files map[string]*ast.File, funcLookup map[string]*ast.FuncType) ([]string, int) {
	seen := make(map[string]bool)
	unresolved := 0

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, isCall := n.(*ast.CallExpr)
			if !isCall {
				return true
			}

			sel, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}

			xIdent, isIdent := sel.X.(*ast.Ident)
			if !isIdent || xIdent.Name != "store" || sel.Sel.Name != "New" || len(call.Args) != 1 {
				return true
			}

			ft, ok := funcTypeOf(call.Args[0], funcLookup)
			if !ok || ft.Params == nil || len(ft.Params.List) != 1 {
				unresolved++

				return true
			}

			name, ok := baseTypeName(ft.Params.List[0].Type)
			if !ok {
				unresolved++

				return true
			}

			seen[name] = true

			return true
		})
	}

	roots := make([]string, 0, len(seen))
	for name := range seen {
		roots = append(roots, name)
	}

	sort.Strings(roots)

	return roots, unresolved
}

func funcTypeOf(arg ast.Expr, funcLookup map[string]*ast.FuncType) (*ast.FuncType, bool) {
	switch a := arg.(type) {
	case *ast.Ident:
		ft, ok := funcLookup[a.Name]

		return ft, ok
	case *ast.FuncLit:
		return a.Type, true
	default:
		return nil, false
	}
}

// baseTypeName strips pointer indirection and returns a same-package type's name; a
// cross-package type (*ast.SelectorExpr, e.g. *pkg.Foo) is not resolvable this way and
// returns false.
func baseTypeName(expr ast.Expr) (string, bool) {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name, true
	case *ast.StarExpr:
		return baseTypeName(e.X)
	default:
		return "", false
	}
}

// namedTypeRefs returns the same-package type names a field's type expression refers to,
// unwrapping pointers, slices/arrays and maps. A name that turns out to be a builtin or a
// non-struct type simply won't be found in structDecls by the caller and expansion stops
// there; a cross-package selector is not returned at all.
func namedTypeRefs(expr ast.Expr) []string {
	switch e := expr.(type) {
	case *ast.Ident:
		return []string{e.Name}
	case *ast.StarExpr:
		return namedTypeRefs(e.X)
	case *ast.ArrayType:
		return namedTypeRefs(e.Elt)
	case *ast.MapType:
		return append(namedTypeRefs(e.Key), namedTypeRefs(e.Value)...)
	default:
		return nil
	}
}

// expandFields appends root's own field descriptors (via structFieldDescriptors, which
// already excludes the Version field) prefixed with root's type name, then recurses into
// every same-package named type any of root's fields refer to. visited is shared across
// the whole service scan so a type reachable from multiple roots is only expanded once.
func expandFields(
	fset *token.FileSet,
	root string,
	structDecls map[string]*ast.StructType,
	visited map[string]bool,
	out *[]string,
) {
	if visited[root] {
		return
	}

	st, ok := structDecls[root]
	if !ok {
		return
	}

	visited[root] = true

	for _, d := range structFieldDescriptors(fset, st) {
		*out = append(*out, root+"."+d)
	}

	for _, field := range st.Fields.List {
		for _, ref := range namedTypeRefs(field.Type) {
			expandFields(fset, ref, structDecls, visited, out)
		}
	}
}

func findVersionConst(f *ast.File) (int, bool, error) {
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}

		for _, spec := range gd.Specs {
			vs, isValueSpec := spec.(*ast.ValueSpec)
			if !isValueSpec || len(vs.Names) != 1 || len(vs.Values) != 1 {
				continue
			}

			if !versionConstRE.MatchString(vs.Names[0].Name) {
				continue
			}

			lit, isBasicLit := vs.Values[0].(*ast.BasicLit)
			if !isBasicLit || lit.Kind != token.INT {
				continue
			}

			n, err := strconv.Atoi(lit.Value)
			if err != nil {
				return 0, false, fmt.Errorf("const %s: %w", vs.Names[0].Name, err)
			}

			return n, true, nil
		}
	}

	return 0, false, nil
}

// snapshotStructRE matches the codebase-wide convention (156 of 158
// persistence.go files, gopherstack-5i6p) of naming every persisted DTO
// "*Snapshot": backendSnapshot itself plus nested ones like stageSnapshot or
// invalidationSnapshot that hold fields the top-level struct never sees directly.
var snapshotStructRE = regexp.MustCompile(`(?i)snapshot$`)

func hasIntVersionField(st *ast.StructType) bool {
	for _, field := range st.Fields.List {
		if !fieldNamed(field, "Version") {
			continue
		}

		id, ok := field.Type.(*ast.Ident)
		if ok && id.Name == "int" {
			return true
		}
	}

	return false
}

func fieldNamed(field *ast.Field, name string) bool {
	for _, n := range field.Names {
		if n.Name == name {
			return true
		}
	}

	return false
}

func structFieldDescriptors(fset *token.FileSet, st *ast.StructType) []string {
	var out []string

	for _, field := range st.Fields.List {
		if fieldNamed(field, "Version") {
			continue
		}

		typeStr := exprString(fset, field.Type)

		tag := ""
		if field.Tag != nil {
			tag = " " + field.Tag.Value
		}

		for _, n := range field.Names {
			out = append(out, fmt.Sprintf("%s %s%s", n.Name, typeStr, tag))
		}
	}

	return out
}

func exprString(fset *token.FileSet, expr ast.Expr) string {
	var buf bytes.Buffer

	if err := format.Node(&buf, fset, expr); err != nil {
		return fmt.Sprintf("<unprintable:%v>", err)
	}

	return buf.String()
}
