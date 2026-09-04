package inspector2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestListFindingAggregations_NonAccountType_RealClient covers gopherstack-or9.
// ListFindingAggregations always emitted an "accountAggregation"-keyed entry
// regardless of the requested AggregationType. types.AggregationResponse
// (inspector2@v1.54.1 types/types.go) is a real Smithy union with 15 members
// (accountAggregation, amiAggregation, packageAggregation,
// findingTypeAggregation, ...); the real deserializer
// (deserializers.go's awsRestjson1_deserializeDocumentAggregationResponse)
// picks which union member to populate purely from which JSON key is
// present in the response object -- it does not consult the request's
// AggregationType at all. So a real client requesting, say,
// AggregationType=PACKAGE always received an AccountAggregation value back
// (wrong union member, its own requested PackageAggregation.PackageName/etc
// never populated), silently discarding the aggregation the caller actually
// asked for, for every AggregationType except ACCOUNT (14 of 15 real
// values). This backend's Finding model has no per-package/per-resource/
// per-repository/per-image detail to genuinely aggregate the other 14 types
// by, so the fix is to stop fabricating an accountAggregation-shaped entry
// for them and return an honestly empty responses list instead --
// AggregationType=ACCOUNT (the only type this backend has real data for)
// is unaffected.
func TestListFindingAggregations_NonAccountType_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		aggregationType string
	}{
		{name: "package", aggregationType: "PACKAGE"},
		{name: "finding_type", aggregationType: "FINDING_TYPE"},
		{name: "title", aggregationType: "TITLE"},
		{name: "repository", aggregationType: "REPOSITORY"},
		{name: "ami", aggregationType: "AMI"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := inspector2.NewInMemoryBackend("123456789012", "us-east-1")

			_, err := b.SeedFinding(inspector2.Finding{Type: "PACKAGE_VULNERABILITY"})
			require.NoError(t, err)

			got, err := b.ListFindingAggregations(tc.aggregationType, nil)
			require.NoError(t, err)

			assert.Equal(t, tc.aggregationType, got["aggregationType"],
				"aggregationType must always echo the request")
			assertEmptyAggregationResponses(t, got["responses"])
		})
	}
}

func assertEmptyAggregationResponses(t *testing.T, responses any) {
	t.Helper()

	switch r := responses.(type) {
	case []any:
		assert.Empty(t, r, "no accountAggregation-shaped entry may be returned for a non-ACCOUNT aggregationType")
	case []map[string]any:
		assert.Empty(t, r, "no accountAggregation-shaped entry may be returned for a non-ACCOUNT aggregationType")
	default:
		t.Fatalf("unexpected responses type %T", responses)
	}
}
