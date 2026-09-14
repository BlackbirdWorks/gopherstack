package iam

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dispatchSubtable names one of buildDispatchTable's (handler.go) merge
// inputs for TestBuildDispatchTable_NoUndocumentedDuplicates' error output.
type dispatchSubtable struct {
	fn   func(h *Handler) map[string]iamActionFn
	name string
}

// dispatchSubtablesInMergeOrder mirrors buildDispatchTable's subtables slice
// exactly, in merge order (last write wins). Keep this in sync with
// handler.go's buildDispatchTable -- the recurrence guard below is only as
// good as this mirror.
//
//nolint:gochecknoglobals // read-only test fixture, mirrors a read-only production table
var dispatchSubtablesInMergeOrder = []dispatchSubtable{
	{name: "iamUserDispatchTable", fn: (*Handler).iamUserDispatchTable},
	{name: "iamRoleDispatchTable", fn: (*Handler).iamRoleDispatchTable},
	{name: "iamPolicyBasicDispatchTable", fn: (*Handler).iamPolicyBasicDispatchTable},
	{name: "iamPolicyAttachDispatchTable", fn: (*Handler).iamPolicyAttachDispatchTable},
	{name: "iamGroupAttachedPolicyDispatchTable", fn: (*Handler).iamGroupAttachedPolicyDispatchTable},
	{name: "iamInlinePolicyDispatchTable", fn: (*Handler).iamInlinePolicyDispatchTable},
	{name: "iamPermissionBoundaryDispatchTable", fn: (*Handler).iamPermissionBoundaryDispatchTable},
	{name: "iamOtherOperationsDispatchTable", fn: (*Handler).iamOtherOperationsDispatchTable},
	{name: "iamReportingDispatchTable", fn: (*Handler).iamReportingDispatchTable},
	{name: "iamGroupDispatchTable", fn: (*Handler).iamGroupDispatchTable},
	{name: "iamAccessKeyDispatchTable", fn: (*Handler).iamAccessKeyDispatchTable},
	{name: "iamInstanceProfileDispatchTable", fn: (*Handler).iamInstanceProfileDispatchTable},
	{name: "iamTagDispatchTable", fn: (*Handler).iamTagDispatchTable},
	{name: "iamSAMLProviderDispatchTable", fn: (*Handler).iamSAMLProviderDispatchTable},
	{name: "iamOIDCProviderDispatchTable", fn: (*Handler).iamOIDCProviderDispatchTable},
	{name: "iamLoginProfileDispatchTable", fn: (*Handler).iamLoginProfileDispatchTable},
	{name: "iamMiscDispatchTable", fn: (*Handler).iamMiscDispatchTable},
	{name: "iamNewOpsDispatchTable", fn: (*Handler).iamNewOpsDispatchTable},
	{name: "iamRefinementDispatchTable", fn: (*Handler).iamRefinementDispatchTable},
	{name: "iamRefinement2DispatchTable", fn: (*Handler).iamRefinement2DispatchTable},
	{name: "iamCompletenessDispatchTable", fn: (*Handler).iamCompletenessDispatchTable},
	{name: "iamComprehensiveDispatchTable", fn: (*Handler).iamComprehensiveDispatchTable},
}

// intentionalDispatchOverrides lists operations legitimately registered in
// more than one dispatchSubtablesInMergeOrder entry, each with why the
// later-merged table's entry should win. Any duplicate key NOT in this list
// is an accidental shadow of the gopherstack-qmcud/gopherstack-f185i shape.
// Add to this list only when a new override is deliberate.
//
//nolint:gochecknoglobals // read-only allow-list, same pattern as iamErrorMappings
var intentionalDispatchOverrides = map[string]string{
	opListGroups:           "iamRefinement2ListTable/ListTable2 overrides with PathPrefix filtering",
	opListInstanceProfiles: "iamRefinement2ListTable/ListTable2 overrides with PathPrefix filtering",
	opListPolicies:         "iamRefinement2ListTable/ListTable2 overrides with PathPrefix filtering",
	opListRoles:            "iamRefinement2ListTable/ListTable2 overrides with PathPrefix filtering",
	opListUsers:            "iamRefinement2ListTable/ListTable2 overrides with PathPrefix filtering",
}

// TestBuildDispatchTable_NoUndocumentedDuplicates flattens every
// buildDispatchTable subtable with op constants resolved and asserts zero
// undocumented duplicate keys, so the gopherstack-qmcud/gopherstack-f185i
// shadowed-registration bug class (a stub-fill pass silently shadowed by a
// later real implementation, or vice versa, purely by maps.Copy merge order)
// cannot recur unnoticed.
func TestBuildDispatchTable_NoUndocumentedDuplicates(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend())

	registeredIn := make(map[string][]string)
	for _, st := range dispatchSubtablesInMergeOrder {
		for op := range st.fn(h) {
			registeredIn[op] = append(registeredIn[op], st.name)
		}
	}

	var undocumented []string

	for op, tables := range registeredIn {
		if len(tables) < 2 {
			continue
		}

		if _, ok := intentionalDispatchOverrides[op]; ok {
			continue
		}

		sort.Strings(tables)
		undocumented = append(undocumented, fmt.Sprintf("%s registered in %v", op, tables))
	}

	sort.Strings(undocumented)
	assert.Empty(t, undocumented,
		"accidental dispatch-table shadow(s): the last-merged table silently wins and the "+
			"other registration is dead code -- either delete the dead one (gopherstack-f185i) "+
			"or, if deliberate, add it to intentionalDispatchOverrides with a reason")

	for op, reason := range intentionalDispatchOverrides {
		require.GreaterOrEqualf(t, len(registeredIn[op]), 2, "%s is allow-listed (%s) but is no longer "+
			"duplicated -- update intentionalDispatchOverrides", op, reason)
	}
}
