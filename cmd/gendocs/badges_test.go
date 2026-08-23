package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTotalOpEntries proves totalOpEntries sums ops: block ENTRIES, not
// real operations -- a families-only doc (no ops: block) contributes 0 even
// though it has real operations, and a doc whose single entry names several
// grouped operations (e.g. "AddPermission/RemovePermission") still counts
// as one (gopherstack-mgna).
func TestTotalOpEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		docs []*ParityDoc
		want int
	}{
		{
			name: "no docs",
			docs: nil,
			want: 0,
		},
		{
			name: "single per-op doc sums its entries",
			docs: []*ParityDoc{
				{Ops: []OpStatus{{Name: "GetFoo"}, {Name: "GetBar"}}},
			},
			want: 2,
		},
		{
			name: "families-only doc contributes zero entries despite real operations",
			docs: []*ParityDoc{
				{Families: []FamilyStatus{{Name: "CRUD"}, {Name: "Tags"}}},
			},
			want: 0,
		},
		{
			name: "a grouped entry naming multiple real operations still counts as one",
			docs: []*ParityDoc{
				{Ops: []OpStatus{{Name: "AddPermission/RemovePermission"}}},
			},
			want: 1,
		},
		{
			name: "mixed corpus sums across docs",
			docs: []*ParityDoc{
				{Ops: []OpStatus{{Name: "GetFoo"}, {Name: "GetBar"}, {Name: "GetBaz"}}},
				{Families: []FamilyStatus{{Name: "CRUD"}}},
				{Ops: []OpStatus{{Name: "ListThings"}}},
			},
			want: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := totalOpEntries(tt.docs)

			assert.Equal(t, tt.want, got)
		})
	}
}

// TestBadgeSpecs_OperationsLabelIsHonest proves the operations.svg badge's
// label no longer claims to count "AWS operations" -- it counts PARITY.md
// ops: entries (a hand-grouped audit unit), which is a materially smaller
// and differently-shaped number than the real per-service operation total
// (gopherstack-mgna: lightsail's real GetSupportedOperations count is 161,
// but its PARITY.md has 0 ops: entries since it audits by families:).
//
// Against the unfixed code this fails with:
//
//	Error: Not equal:
//	 expected: "PARITY entries"
//	 actual  : "AWS operations"
func TestBadgeSpecs_OperationsLabelIsHonest(t *testing.T) {
	t.Parallel()

	docs := []*ParityDoc{
		{Ops: []OpStatus{{Name: "GetFoo"}}},
	}

	specs := badgeSpecs(1, docs, "1.26.5")

	var opSpec *badgeSpec
	for i := range specs {
		if specs[i].File == operationsBadgeFile {
			opSpec = &specs[i]
		}
	}
	require.NotNil(t, opSpec, "expected an operations.svg badge spec")
	assert.NotEqual(t, "AWS operations", opSpec.Label,
		"badge label still claims to count real AWS operations, not PARITY.md entries")
	assert.Equal(t, "PARITY entries", opSpec.Label)
}

// TestBadgeSpecs_OperationsValueTracksEntryCount proves the operations.svg
// badge's value is totalOpEntries's output, not any other figure, so the
// label and value change together.
func TestBadgeSpecs_OperationsValueTracksEntryCount(t *testing.T) {
	t.Parallel()

	docs := []*ParityDoc{
		{Ops: []OpStatus{{Name: "GetFoo"}, {Name: "GetBar"}}},
		{Families: []FamilyStatus{{Name: "CRUD"}}},
	}

	specs := badgeSpecs(2, docs, "1.26.5")

	var opSpec *badgeSpec
	for i := range specs {
		if specs[i].File == operationsBadgeFile {
			opSpec = &specs[i]
		}
	}
	require.NotNil(t, opSpec)
	assert.Equal(t, "2", opSpec.Value)
}
