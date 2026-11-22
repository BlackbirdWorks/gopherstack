package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// taggedListingMethods are the method names a service uses to hand its tagged resources to the aggregation.
func taggedListingMethods() []string { return []string{"TaggedResources", "TaggedStreams"} }

// inventoryExemptions lists packages that expose a tagged listing without an InventoryResources method.
func inventoryExemptions() map[string]string {
	return map[string]string{
		"services/s3":             "TaggedResources already lists every bucket, tagged or not",
		"services/resourcegroups": "serviceSource.TaggedResources reads the tagging aggregation, it is not a provider",
	}
}

func declaredMethods(t *testing.T, dir string) map[string]bool {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	fset := token.NewFileSet()
	out := map[string]bool{}

	for _, e := range entries {
		if e.IsDir() || !isGuardedSource(e.Name()) {
			continue
		}

		f, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		require.NoError(t, perr)

		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv != nil {
				out[fn.Name.Name] = true
			}
		}
	}

	return out
}

// TestTaggedListingsHaveInventory fails when a service package lists tagged resources for the tagging
// aggregation without an InventoryResources listing, which hides its untagged resources from Resource Explorer.
func TestTaggedListingsHaveInventory(t *testing.T) {
	t.Parallel()

	exempt := inventoryExemptions()

	dirs, err := filepath.Glob("services/*")
	require.NoError(t, err)
	sort.Strings(dirs)

	var offenders []string

	for _, dir := range dirs {
		if info, serr := os.Stat(dir); serr != nil || !info.IsDir() || skipGuardDir(filepath.Base(dir)) {
			continue
		}

		methods := declaredMethods(t, dir)
		listsTagged := false

		for _, name := range taggedListingMethods() {
			listsTagged = listsTagged || methods[name]
		}

		if !listsTagged || methods["InventoryResources"] {
			continue
		}

		if _, ok := exempt[dir]; !ok {
			offenders = append(offenders, dir)
		}
	}

	require.Empty(t, offenders,
		"packages with a tagged listing but no InventoryResources: %s", strings.Join(offenders, ", "))
}

// TestInventoryExemptionsAreStillNeeded keeps the exemption table honest.
func TestInventoryExemptionsAreStillNeeded(t *testing.T) {
	t.Parallel()

	for dir, reason := range inventoryExemptions() {
		require.NotEmpty(t, reason, dir)

		methods := declaredMethods(t, dir)
		require.False(t, methods["InventoryResources"], "%s now has InventoryResources; drop its exemption", dir)
	}
}
