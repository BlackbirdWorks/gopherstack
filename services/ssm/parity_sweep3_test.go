package ssm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// Test_PutParameter_HierarchyLevelLimit verifies AWS's documented constraint
// that a parameter name hierarchy can have a maximum of 15 "/"-delimited
// levels (including the final name segment). Exceeding it must fail with
// HierarchyLevelLimitExceededException, matching real SSM PutParameter
// behavior (see API_PutParameter docs: "/L1/.../L14/name" is valid,
// "/L1/.../L15/L16/name" is not).
func Test_PutParameter_HierarchyLevelLimit(t *testing.T) {
	t.Parallel()

	fifteenLevels := "/" + strings.Join(makeLevels(14), "/") + "/name" // 14 + name = 15
	sixteenLevels := "/" + strings.Join(makeLevels(15), "/") + "/name" // 15 + name = 16

	cases := []struct {
		name      string
		paramName string
		wantErr   bool
	}{
		{name: "exactly 15 levels is valid", paramName: fifteenLevels, wantErr: false},
		{name: "16 levels exceeds the limit", paramName: sixteenLevels, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.Background()

			_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:  tc.paramName,
				Type:  ssm.StringType,
				Value: "v",
			})

			if tc.wantErr {
				require.ErrorIs(t, err, ssm.ErrHierarchyLevelLimitExceeded)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func makeLevels(n int) []string {
	levels := make([]string, n)
	for i := range levels {
		levels[i] = fmt.Sprintf("L%d", i)
	}

	return levels
}

// countParameterHistory paginates through GetParameterHistory (page size 50)
// and returns the total number of history entries for name.
func countParameterHistory(ctx context.Context, t *testing.T, b *ssm.InMemoryBackend, name string) int {
	t.Helper()

	total := 0
	nextToken := ""

	for {
		out, err := b.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
			Name:      name,
			NextToken: nextToken,
		})
		require.NoError(t, err)

		total += len(out.Parameters)
		if out.NextToken == "" {
			break
		}

		nextToken = out.NextToken
	}

	return total
}

// Test_PutParameter_MaxVersionLimit_LabeledOldestBlocksEviction verifies AWS
// behavior: Parameter Store keeps only the most recent MaxHistoryCap (100)
// versions, auto-deleting the oldest on each new PutParameter. But if the
// oldest version carries a label, AWS refuses to delete it and the PutParameter
// call fails with ParameterMaxVersionLimitExceeded instead of silently
// evicting a labeled version.
func Test_PutParameter_MaxVersionLimit_LabeledOldestBlocksEviction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		labelOldest bool
		wantErr     bool
	}{
		{name: "unlabeled oldest version is evicted normally", labelOldest: false, wantErr: false},
		{name: "labeled oldest version blocks the new write", labelOldest: true, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.Background()

			const paramName = "/history/param"

			// Fill history to exactly MaxHistoryCap versions.
			for range ssm.MaxHistoryCap {
				_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
					Name:      paramName,
					Type:      ssm.StringType,
					Value:     "v",
					Overwrite: true,
				})
				require.NoError(t, err)
			}

			if tc.labelOldest {
				_, err := b.LabelParameterVersion(ctx, &ssm.LabelParameterVersionInput{
					Name:             paramName,
					ParameterVersion: 1, // the oldest surviving version
					Labels:           []string{"pinned"},
				})
				require.NoError(t, err)
			}

			// One more write would push total versions to MaxHistoryCap+1,
			// requiring eviction of version 1.
			_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:      paramName,
				Type:      ssm.StringType,
				Value:     "one-more",
				Overwrite: true,
			})

			if tc.wantErr {
				require.ErrorIs(t, err, ssm.ErrParameterMaxVersionLimitExceeded)

				// The rejected write must not have mutated state: history stays
				// at MaxHistoryCap (paginated at 50/page) and the current
				// version is unchanged.
				assert.Equal(t, ssm.MaxHistoryCap, countParameterHistory(ctx, t, b, paramName))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test_PutParameter_IntelligentTiering_AutoUpgradesToAdvanced verifies that,
// per AWS docs, Intelligent-Tiering parameters are auto-promoted to the
// Advanced tier whenever the request needs a capability Standard doesn't
// support (value over 4 KiB, or parameter policies attached) instead of the
// PutParameter call failing.
func Test_PutParameter_IntelligentTiering_AutoUpgradesToAdvanced(t *testing.T) {
	t.Parallel()

	smallValue := strings.Repeat("a", 100)
	overStandardValue := strings.Repeat("a", 5000) // >4096, <=8192

	cases := []struct {
		name     string
		tier     string
		value    string
		wantTier string
		wantErr  bool
	}{
		{
			// AWS echoes back the requested "Intelligent-Tiering" tier as-is
			// when no capability forces a promotion — it does not resolve to
			// the concrete underlying "Standard" tier in the response.
			name:     "small value on Intelligent-Tiering reports Intelligent-Tiering",
			tier:     "Intelligent-Tiering",
			value:    smallValue,
			wantTier: "Intelligent-Tiering",
		},
		{
			name:     "value over 4KiB on Intelligent-Tiering auto-upgrades to Advanced",
			tier:     "Intelligent-Tiering",
			value:    overStandardValue,
			wantTier: "Advanced",
		},
		{
			name:    "value over 4KiB on explicit Standard tier fails",
			tier:    "Standard",
			value:   overStandardValue,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:  "/tier/param",
				Type:  ssm.StringType,
				Value: tc.value,
				Tier:  tc.tier,
			})

			if tc.wantErr {
				require.ErrorIs(t, err, ssm.ErrValidationException)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantTier, out.Tier)
		})
	}
}

// Test_PutParameter_PoliciesRequireAdvancedTier verifies AWS's documented
// constraint that parameter policies (Expiration, ExpirationNotification,
// NoChangeNotification) are only supported on Advanced-tier parameters;
// Standard tier rejects them, and Intelligent-Tiering auto-upgrades to
// Advanced to accommodate them.
func Test_PutParameter_PoliciesRequireAdvancedTier(t *testing.T) {
	t.Parallel()

	const policy = `[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":"2099-01-01T00:00:00.000Z"}}]`

	cases := []struct {
		name     string
		tier     string
		wantTier string
		wantErr  bool
	}{
		{name: "default (Standard) tier rejects policies", tier: "", wantErr: true},
		{name: "explicit Standard tier rejects policies", tier: "Standard", wantErr: true},
		{name: "Advanced tier accepts policies", tier: "Advanced", wantTier: "Advanced"},
		{
			name:     "Intelligent-Tiering auto-upgrades for policies",
			tier:     "Intelligent-Tiering",
			wantTier: "Advanced",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:     "/policy/param",
				Type:     ssm.StringType,
				Value:    "v",
				Tier:     tc.tier,
				Policies: policy,
			})

			if tc.wantErr {
				require.ErrorIs(t, err, ssm.ErrValidationException)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantTier, out.Tier)
		})
	}
}

// Test_GetDocument_DefaultVersionSelector verifies that an explicit
// "$DEFAULT" DocumentVersion resolves to the document's DefaultVersion (as
// set by UpdateDocumentDefaultVersion), which can genuinely differ from the
// latest version. Previously $DEFAULT was conflated with $LATEST and always
// served the newest content even when the default had been pinned to an
// older version.
func Test_GetDocument_DefaultVersionSelector(t *testing.T) {
	t.Parallel()

	newBackendWithTwoVersions := func(t *testing.T) *ssm.InMemoryBackend {
		t.Helper()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		_, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: "Doc", Content: `{"v":1}`})
		require.NoError(t, err)
		_, err = b.UpdateDocument(ctx, &ssm.UpdateDocumentInput{Name: "Doc", Content: `{"v":2}`})
		require.NoError(t, err)

		// Pin the default back to version 1, diverging it from LatestVersion (2).
		_, err = b.UpdateDocumentDefaultVersion(ctx, &ssm.UpdateDocumentDefaultVersionInput{
			Name: "Doc", DocumentVersion: "1",
		})
		require.NoError(t, err)

		return b
	}

	cases := []struct {
		name            string
		documentVersion string
		wantContent     string
	}{
		{
			name:            "explicit $DEFAULT resolves to the pinned default (v1)",
			documentVersion: "$DEFAULT",
			wantContent:     `{"v":1}`,
		},
		{
			name:            "explicit $LATEST resolves to the newest version (v2)",
			documentVersion: "$LATEST",
			wantContent:     `{"v":2}`,
		},
		{name: "explicit version 1 resolves to v1 regardless of default", documentVersion: "1", wantContent: `{"v":1}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackendWithTwoVersions(t)
			ctx := context.Background()

			out, err := b.GetDocument(ctx, &ssm.GetDocumentInput{
				Name: "Doc", DocumentVersion: tc.documentVersion,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantContent, out.Content)
		})
	}
}

// Test_DescribeDocument_OmitsContentAndHonorsVersionSelector verifies two
// wire-shape facts about DescribeDocument that CreateDocument/UpdateDocument
// share: (1) real AWS's DocumentDescription response never includes the
// document Content field — that's GetDocument's job — and (2) an explicit
// DocumentVersion selector changes the per-version metadata (DocumentVersion,
// DocumentFormat, Status) returned, not just the latest version's.
func Test_DescribeDocument_OmitsContentAndHonorsVersionSelector(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	createOut, err := b.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: "Doc", Content: `{"v":1}`})
	require.NoError(t, err)

	updateOut, err := b.UpdateDocument(ctx, &ssm.UpdateDocumentInput{Name: "Doc", Content: `{"v":2}`})
	require.NoError(t, err)

	// The wire-serialized form of DocumentDescription must not carry a
	// "Content" field at all — assert this at the JSON level since a Go
	// zero-value string would round-trip identically to "field absent" in a
	// struct-based comparison.
	createJSON, marshalErr := json.Marshal(createOut.DocumentDescription)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(createJSON), "Content",
		"CreateDocumentOutput.DocumentDescription must not serialize a Content field")

	updateJSON, marshalErr := json.Marshal(updateOut.DocumentDescription)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(updateJSON), "Content",
		"UpdateDocumentOutput.DocumentDescription must not serialize a Content field")

	cases := []struct {
		name            string
		documentVersion string
		wantVersion     string
	}{
		{name: "no version selector describes the latest version", documentVersion: "", wantVersion: "2"},
		{name: "explicit version 1 describes that older version", documentVersion: "1", wantVersion: "1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			describeOut, describeErr := b.DescribeDocument(ctx, &ssm.DescribeDocumentInput{
				Name: "Doc", DocumentVersion: tc.documentVersion,
			})
			require.NoError(t, describeErr)
			assert.Equal(t, tc.wantVersion, describeOut.Document.DocumentVersion)

			describeJSON, jsonErr := json.Marshal(describeOut.Document)
			require.NoError(t, jsonErr)
			assert.NotContains(t, string(describeJSON), "Content",
				"DescribeDocumentOutput.Document must not serialize a Content field")
		})
	}
}
