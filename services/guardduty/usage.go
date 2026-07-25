package guardduty

import (
	"slices"
	"sort"
)

// UsageQuery holds the optional criteria/type parameters for
// GetUsageStatistics, mirroring types.GetUsageStatisticsInput's
// UsageCriteria, UsageStatisticType, and Unit fields.
type UsageQuery struct {
	StatisticType string
	Unit          string
	AccountIDs    []string
	Features      []string
}

// keyTotal is the wire key for a types.Total{amount,unit} entry, shared by
// every UsageStatistics sub-list (sumByAccount, sumByFeature, ...).
const keyTotal = "total"

// zeroTotal builds a types.Total{Amount, Unit} entry. This backend does not
// meter real usage cost, so Amount is always "0.00" -- the point of this fix
// is the wire shape (a Total object, not a bare number), not fabricating a
// cost figure.
func zeroTotal(unit string) map[string]any {
	if unit == "" {
		unit = "USD"
	}

	return map[string]any{"amount": "0.00", "unit": unit}
}

// GetUsageStatistics returns usage statistics for a detector. Real
// GuardDuty accounting isn't modeled by this backend, so every Total is a
// deterministic zero amount; the fix here is emitting the real
// UsageStatistics wire shape (Total{amount,unit} objects, sumByFeature,
// topAccountsByFeature, topResources) instead of the old ad hoc field set,
// and honoring UsageStatisticType by nulling out every field except the one
// requested, per GetUsageStatisticsOutput's doc ("If a UsageStatisticType
// was provided, the objects representing other types will be null.").
func (b *InMemoryBackend) GetUsageStatistics(detectorID string, q UsageQuery) (map[string]any, error) {
	b.mu.RLock("GetUsageStatistics")
	defer b.mu.RUnlock()

	det, ok := b.detectors.Get(detectorID)
	if !ok {
		return nil, ErrDetectorNotFound
	}

	features := usageFeatureNames(det, q.Features)

	full := map[string]any{
		"sumByAccount":         []any{map[string]any{keyAccountIDField: b.accountID, keyTotal: zeroTotal(q.Unit)}},
		"sumByDataSource":      usageByFeature(features, "dataSource", q.Unit),
		"sumByFeature":         usageByFeature(features, "feature", q.Unit),
		"sumByResource":        []any{},
		"topAccountsByFeature": usageTopAccountsByFeature(b.accountID, features, q.Unit),
		"topResources":         []any{},
	}

	return map[string]any{"usageStatistics": selectUsageStatisticType(full, q.StatisticType)}, nil
}

// usageFeatureNames returns the enabled detector feature names to report
// usage for, filtered to requested (if non-empty).
func usageFeatureNames(det *Detector, requested []string) []string {
	var names []string

	for _, f := range det.Features {
		if f.Status != statusEnabled {
			continue
		}

		if len(requested) > 0 && !slices.Contains(requested, f.Name) {
			continue
		}

		names = append(names, f.Name)
	}

	sort.Strings(names)

	return names
}

func usageByFeature(features []string, fieldName, unit string) []any {
	out := make([]any, 0, len(features))
	for _, f := range features {
		out = append(out, map[string]any{fieldName: f, keyTotal: zeroTotal(unit)})
	}

	return out
}

func usageTopAccountsByFeature(accountID string, features []string, unit string) []any {
	out := make([]any, 0, len(features))
	for _, f := range features {
		out = append(out, map[string]any{
			"feature": f,
			"accounts": []any{
				map[string]any{keyAccountIDField: accountID, keyTotal: zeroTotal(unit)},
			},
		})
	}

	return out
}

// usageStatisticTypeFields maps the wire values of UsageStatisticType to the
// UsageStatistics field they select.
var usageStatisticTypeFields = map[string]string{ //nolint:gochecknoglobals // static lookup table, not mutable state
	"SUM_BY_ACCOUNT":          "sumByAccount",
	"SUM_BY_DATA_SOURCE":      "sumByDataSource",
	"SUM_BY_RESOURCE":         "sumByResource",
	"TOP_RESOURCES":           "topResources",
	"SUM_BY_FEATURES":         "sumByFeature",
	"TOP_ACCOUNTS_BY_FEATURE": "topAccountsByFeature",
}

// selectUsageStatisticType nulls out every UsageStatistics field except the
// one selected by statisticType, matching real behavior. An unrecognized or
// empty statisticType returns every field populated (best-effort default).
func selectUsageStatisticType(full map[string]any, statisticType string) map[string]any {
	field, ok := usageStatisticTypeFields[statisticType]
	if !ok {
		return full
	}

	return map[string]any{field: full[field]}
}

// GetRemainingFreeTrialDays returns remaining free trial days.
func (b *InMemoryBackend) GetRemainingFreeTrialDays(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetRemainingFreeTrialDays")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	return map[string]any{
		"accounts": []any{
			map[string]any{
				"accountId":              b.accountID, //nolint:goconst // existing issue.
				"dataSources":            map[string]any{},
				"features":               []any{}, //nolint:goconst // existing issue.
				"freeTrialDaysRemaining": 30,      //nolint:mnd // existing issue.
			},
		},
		"unprocessedAccounts": []any{}, //nolint:goconst // existing issue.
	}, nil
}
