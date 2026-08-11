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

	"github.com/stretchr/testify/require"
)

var errNoVersionStruct = errors.New("no struct with an int-typed Version field")

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
// constant and the sibling fields of its version-carrying struct, and compares them
// against the checked-in golden at testdata/snapshot_inventory.json. If the version
// changed and the new field set is a pure superset of the golden one (every old field
// still present, unchanged; only additions) that is exactly the destructive pattern --
// the test fails unconditionally, even under -update, so there is no way to silence it
// by just refreshing the golden.
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

	var violations []string

	for name, want := range live {
		got, ok := golden[name]

		switch {
		case !ok:
			violations = append(violations, fmt.Sprintf(
				"%s: no golden entry (new persistence.go?); run with -update", name))
		case want.Version != got.Version:
			if isPureAddition(got.Fields, want.Fields) {
				violations = append(violations, fmt.Sprintf(
					"%s: version bumped %d -> %d for a PURELY ADDITIVE field change "+
						"(added: %v). encoding/json decodes an older snapshot missing "+
						"a new field fine -- do not bump the version constant for this. "+
						"Revert the bump; the field addition alone is safe.",
					name, got.Version, want.Version, addedFields(got.Fields, want.Fields)))
			} else if !fieldsEqual(got.Fields, want.Fields) {
				violations = append(violations, fmt.Sprintf(
					"%s: version bumped %d -> %d with an incompatible struct change; "+
						"golden is out of date, run with -update to accept it", name, got.Version, want.Version))
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

// scanServiceSnapshots parses every services/*/persistence.go and extracts, per
// service, the version constant and the sibling fields of its version-carrying
// struct (the struct with an `int`-typed field literally named Version).
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

		path := filepath.Join(servicesDir, e.Name(), "persistence.go")
		if _, statErr := os.Stat(path); statErr != nil {
			continue
		}

		info, ok, scanErr := scanPersistenceFile(path)
		if scanErr != nil {
			return nil, fmt.Errorf("%s: %w", path, scanErr)
		}

		if ok {
			result[e.Name()] = info
		}
	}

	return result, nil
}

func scanPersistenceFile(path string) (snapshotInfo, bool, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return snapshotInfo{}, false, err
	}

	version, hasVersion, err := findVersionConst(f)
	if err != nil {
		return snapshotInfo{}, false, err
	}

	if !hasVersion {
		return snapshotInfo{}, false, nil
	}

	fields, err := findVersionStructFields(fset, f)
	if err != nil {
		return snapshotInfo{}, false, fmt.Errorf(
			"found a *SnapshotVersion const but no matching int-typed Version struct field: %w", err)
	}

	sort.Strings(fields)

	return snapshotInfo{Version: version, Fields: fields}, true, nil
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

// findVersionStructFields locates the struct type declared in f that has a field
// literally named "Version" of type "int", and returns the descriptors ("Name Type
// `tag`") of its other fields, sorted.
func findVersionStructFields(fset *token.FileSet, f *ast.File) ([]string, error) {
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

			st, isStructType := ts.Type.(*ast.StructType)
			if !isStructType || !hasIntVersionField(st) {
				continue
			}

			return structFieldDescriptors(fset, st), nil
		}
	}

	return nil, errNoVersionStruct
}

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
