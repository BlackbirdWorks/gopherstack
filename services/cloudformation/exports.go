package cloudformation

import (
	"fmt"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ListExports returns all exported output values across all stacks.
func (b *InMemoryBackend) ListExports(nextToken string) (page.Page[Export], error) {
	b.mu.RLock("ListExports")
	defer b.mu.RUnlock()

	exports := make([]Export, 0, b.exports.Len())
	for _, exp := range b.exports.All() {
		exports = append(exports, *exp)
	}

	sort.Slice(exports, func(i, j int) bool { return exports[i].Name < exports[j].Name })

	return page.New(exports, nextToken, 0, cfnDefaultPageSize), nil
}

// ListImports returns the names of stacks that import the given export.
func (b *InMemoryBackend) ListImports(exportName, nextToken string) (page.Page[string], error) {
	b.mu.RLock("ListImports")
	defer b.mu.RUnlock()

	if !b.exports.Has(exportName) {
		return page.Page[string]{}, ErrExportNotFound
	}

	var stackNames []string

	for _, stack := range b.stacks.All() {
		if stack.StackStatus == statusDeleteComplete {
			continue
		}

		// Build resolved params to handle non-literal import names like {"Ref": "Param"}.
		var resolvedParams map[string]string
		if tmpl, err := ParseTemplate(stack.TemplateBody); err == nil {
			resolvedParams = ResolveParameters(tmpl, stack.Parameters)
		}

		refs := collectImportValues(stack.TemplateBody, resolvedParams)
		if slices.Contains(refs, exportName) {
			stackNames = append(stackNames, stack.StackName)
		}
	}

	sort.Strings(stackNames)

	return page.New(stackNames, nextToken, 0, cfnDefaultPageSize), nil
}

// registerExports upserts exports for a stack from the given export map.
// It returns ErrDuplicateExport if an export name is already owned by a different stack.
func (b *InMemoryBackend) registerExports(stackID string, exportMap map[string]string) error {
	for name, value := range exportMap {
		if existing, ok := b.exports.Get(name); ok && existing.ExportingStackID != stackID {
			return fmt.Errorf("%w: %s", ErrDuplicateExport, name)
		}

		b.exports.Put(&Export{
			ExportingStackID: stackID,
			Name:             name,
			Value:            value,
		})
	}

	return nil
}

// removeExports removes all exports owned by the given stack.
func (b *InMemoryBackend) removeExports(stackID string) {
	b.exports.Range(func(exp *Export) bool {
		if exp.ExportingStackID == stackID {
			b.exports.Delete(exp.Name)
		}

		return true
	})
}

// stackExportsInUse reports whether any export currently owned by stackID would
// be dropped by prospectiveExports (the export set the caller is about to apply)
// while still being referenced via Fn::ImportValue by another active stack. AWS
// refuses to delete a stack — or update away one of its outputs — while an
// export it owns is still imported elsewhere ("Export X cannot be deleted as it
// is in use by Y"). Pass a nil prospectiveExports map to check full removal
// (DeleteStack, where no exports survive).
func (b *InMemoryBackend) stackExportsInUse(
	stackID string,
	prospectiveExports map[string]string,
) (string, string, bool) {
	var owned []string
	b.exports.Range(func(exp *Export) bool {
		if exp.ExportingStackID != stackID {
			return true
		}
		if _, stillExported := prospectiveExports[exp.Name]; stillExported {
			return true // export survives the change — nothing to block
		}
		owned = append(owned, exp.Name)

		return true
	})
	if len(owned) == 0 {
		return "", "", false
	}
	sort.Strings(owned)

	for _, other := range b.stacks.Snapshot() {
		if other.StackID == stackID || other.StackStatus == statusDeleteComplete {
			continue
		}

		var resolvedParams map[string]string
		if tmpl, err := ParseTemplate(other.TemplateBody); err == nil {
			resolvedParams = ResolveParameters(tmpl, other.Parameters)
		}

		refs := collectImportValues(other.TemplateBody, resolvedParams)
		for _, exportName := range owned {
			if slices.Contains(refs, exportName) {
				return exportName, other.StackName, true
			}
		}
	}

	return "", "", false
}

// validateExportsStillInUse is the UpdateStack pre-flight check: it computes the
// export set the new template would produce (using the pre-update physicalIDs
// snapshot) and fails if that would drop an export still imported by another
// stack.
func (b *InMemoryBackend) validateExportsStillInUse(
	stack *Stack,
	tmpl *Template,
	resolvedParams, physicalIDs map[string]string,
) error {
	resourceTypes := make(map[string]string, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		resourceTypes[logicalID] = res.Type
	}

	previewCtx := resolveCtx{
		params:        resolvedParams,
		physicalIDs:   physicalIDs,
		resourceTypes: resourceTypes,
		exports:       b.buildExportsMap(),
		conditions:    evaluateConditions(tmpl.Conditions, resolvedParams, physicalIDs),
		mappings:      tmpl.Mappings,
		accountID:     b.accountID,
		region:        b.region,
		stackName:     stack.StackName,
	}

	_, previewExports := resolveOutputsWithContext(tmpl, previewCtx)

	if name, importer, inUse := b.stackExportsInUse(stack.StackID, previewExports); inUse {
		return fmt.Errorf("%w: Export %s cannot be deleted as it is in use by %s", ErrExportInUse, name, importer)
	}

	return nil
}

// buildExportsMap builds a name→value map of all current exports (for Fn::ImportValue resolution).
func (b *InMemoryBackend) buildExportsMap() map[string]string {
	m := make(map[string]string, b.exports.Len())
	b.exports.Range(func(exp *Export) bool {
		m[exp.Name] = exp.Value

		return true
	})

	return m
}
