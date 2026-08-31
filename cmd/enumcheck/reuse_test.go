package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// guarddutyPreFixUsage is services/guardduty/usage.go as it stood at commit
// caf2a5f9f^ (git show caf2a5f9f~1:services/guardduty/usage.go):
// GetUsageStatistics.sumByDataSource reused the same detector-feature-name
// slice as sumByFeature, so an enabled S3_DATA_EVENTS/EKS_AUDIT_LOGS feature
// produced a "dataSource" entry with a value that is only ever a member of
// types.UsageFeature, never of the real six-member types.DataSource -- the
// bug commit caf2a5f9f fixed. This is the exact gopherstack-6flj class
// enumcheck exists to automate.
const guarddutyPreFixUsage = `package guardduty

func (b *InMemoryBackend) GetUsageStatistics(detectorID string, q UsageQuery) (map[string]any, error) {
	det, ok := b.detectors.Get(detectorID)
	if !ok {
		return nil, ErrDetectorNotFound
	}

	features := usageFeatureNames(det, q.Features)

	full := map[string]any{
		"sumByDataSource": usageByFeature(features, "dataSource", q.Unit),
		"sumByFeature":    usageByFeature(features, "feature", q.Unit),
	}

	return map[string]any{"usageStatistics": full}, nil
}

func usageByFeature(features []string, fieldName, unit string) []any {
	out := make([]any, 0, len(features))
	for _, f := range features {
		out = append(out, map[string]any{fieldName: f, keyTotal: zeroTotal(unit)})
	}

	return out
}
`

// guarddutyPostFixUsage is the same function post caf2a5f9f: sumByDataSource
// now derives its values from usageDataSourceNames(det), a distinct value
// source from sumByFeature's features -- the two calls no longer share a
// value-source text, so checkCrossEnumReuse's grouping key differs and no
// finding is produced.
const guarddutyPostFixUsage = `package guardduty

func (b *InMemoryBackend) GetUsageStatistics(detectorID string, q UsageQuery) (map[string]any, error) {
	det, ok := b.detectors.Get(detectorID)
	if !ok {
		return nil, ErrDetectorNotFound
	}

	features := usageFeatureNames(det, q.Features)

	full := map[string]any{
		"sumByDataSource": usageByFeature(usageDataSourceNames(det), "dataSource", q.Unit),
		"sumByFeature":    usageByFeature(features, "feature", q.Unit),
	}

	return map[string]any{"usageStatistics": full}, nil
}

func usageByFeature(features []string, fieldName, unit string) []any {
	out := make([]any, 0, len(features))
	for _, f := range features {
		out = append(out, map[string]any{fieldName: f, keyTotal: zeroTotal(unit)})
	}

	return out
}
`

// guarddutyReg mirrors the real guardduty@v1.85.4 facts this scan needs:
// types.DataSource's real six members (types/enums.go:320-330) and a
// deliberately different, non-overlapping subset of types.UsageFeature's
// real members -- the two enums must have different declared member sets
// for checkCrossEnumReuse to fire, exactly as the real SDK's do.
func guarddutyReg() *enumRegistry {
	return &enumRegistry{
		membersByType: map[string]map[string]bool{
			"DataSource": {
				"FLOW_LOGS": true, "CLOUD_TRAIL": true, "DNS_LOGS": true,
				"S3_LOGS": true, "KUBERNETES_AUDIT_LOGS": true, "EC2_MALWARE_SCAN": true,
			},
			"UsageFeature": {
				"S3_DATA_EVENTS": true, "EKS_AUDIT_LOGS": true, "EBS_MALWARE_PROTECTION": true,
			},
		},
		constByIdent: map[string]enumConst{},
	}
}

func guarddutyWireKeys() map[string]wireKeyFact {
	return map[string]wireKeyFact{
		"dataSource": {Enums: []string{"DataSource"}},
		"feature":    {Enums: []string{"UsageFeature"}},
	}
}

func TestCheckCrossEnumReuse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     string
		wantHit bool
	}{
		{name: "guardduty pre-fix flags reuse", src: guarddutyPreFixUsage, wantHit: true},
		{name: "guardduty post-fix is clean", src: guarddutyPostFixUsage, wantHit: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "usage.go"), []byte(tc.src), 0o600))

			findings, err := scanPackage(dir, guarddutyReg(), guarddutyWireKeys(), dir)
			require.NoError(t, err)

			var reuseHits []finding
			for _, f := range findings {
				if f.Kind == kindReuse {
					reuseHits = append(reuseHits, f)
				}
			}

			if !tc.wantHit {
				assert.Empty(t, reuseHits)

				return
			}

			require.Len(t, reuseHits, 1)
			got := reuseHits[0]
			assert.False(t, got.Confident, "cross-enum-reuse must never be confident")
			assert.ElementsMatch(t, []string{got.Key, got.OtherKey}, []string{"dataSource", "feature"})
			assert.ElementsMatch(t, []string{got.Enum, got.OtherEnum}, []string{"DataSource", "UsageFeature"})
		})
	}
}

func TestCheckCrossEnumReuse_SameMemberSetNeverFlags(t *testing.T) {
	t.Parallel()

	src := `package svc

func build(items []string, unit string) map[string]any {
	full := map[string]any{
		"a": tag(items, "alpha", unit),
		"b": tag(items, "beta", unit),
	}

	return full
}

func tag(items []string, fieldName, unit string) []any {
	out := make([]any, 0, len(items))
	for _, it := range items {
		out = append(out, map[string]any{fieldName: it, "unit": unit})
	}

	return out
}
`

	reg := &enumRegistry{
		membersByType: map[string]map[string]bool{
			"Alpha": {"X": true, "Y": true},
			"Beta":  {"X": true, "Y": true},
		},
		constByIdent: map[string]enumConst{},
	}
	wireKeys := map[string]wireKeyFact{
		"alpha": {Enums: []string{"Alpha"}},
		"beta":  {Enums: []string{"Beta"}},
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

	findings, err := scanPackage(dir, reg, wireKeys, dir)
	require.NoError(t, err)
	assert.Empty(t, findings, "Alpha and Beta declare identical member sets, so reuse is not suspicious")
}

// TestCheckCrossEnumReuse_SameMethodNameDifferentReceiverNeverFlags proves
// groupKey must not key on fd.Name.Name alone: (*TypeA).Report and
// (*TypeB).Report are two unrelated methods that happen to share a name and
// a value-source variable name ("items", coincidental, not real reuse).
// Each is internally consistent -- no bug exists inside either method -- so
// merging their call sites into one cross-function group and comparing them
// against each other would be a false cross-enum finding.
func TestCheckCrossEnumReuse_SameMethodNameDifferentReceiverNeverFlags(t *testing.T) {
	t.Parallel()

	src := `package svc

func (a *TypeA) Report(items []string, unit string) map[string]any {
	return map[string]any{"a": tag(items, "alpha", unit)}
}

func (b *TypeB) Report(items []string, unit string) map[string]any {
	return map[string]any{"b": tag(items, "beta", unit)}
}

func tag(items []string, fieldName, unit string) []any {
	out := make([]any, 0, len(items))
	for _, it := range items {
		out = append(out, map[string]any{fieldName: it, "unit": unit})
	}

	return out
}
`

	reg := &enumRegistry{
		membersByType: map[string]map[string]bool{
			"Alpha": {"X": true, "Y": true},
			"Beta":  {"P": true, "Q": true},
		},
		constByIdent: map[string]enumConst{},
	}
	wireKeys := map[string]wireKeyFact{
		"alpha": {Enums: []string{"Alpha"}},
		"beta":  {Enums: []string{"Beta"}},
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "svc.go"), []byte(src), 0o600))

	findings, err := scanPackage(dir, reg, wireKeys, dir)
	require.NoError(t, err)
	assert.Empty(
		t, findings,
		"(*TypeA).Report and (*TypeB).Report are unrelated methods sharing a name; must not be grouped together",
	)
}
