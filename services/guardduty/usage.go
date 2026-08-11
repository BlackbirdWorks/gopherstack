package guardduty

import (
	"slices"
	"sort"
	"time"
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

// freeTrialDays is the length of GuardDuty's free trial period.
const freeTrialDays = 30

// freeTrialBaseFeatures are the always-on data sources every GuardDuty
// member account has from creation -- not toggleable via DetectorFeature, so
// they always appear in GetRemainingFreeTrialDaysOutput's features list.
//
//nolint:gochecknoglobals // static lookup table
var freeTrialBaseFeatures = []string{"FLOW_LOGS", "CLOUD_TRAIL", "DNS_LOGS"}

// GetRemainingFreeTrialDays returns remaining free-trial days for the
// requested member accounts. Real AccountFreeTrialInfo has no top-level
// freeTrialDaysRemaining member -- remainders live per feature, under
// features[]/dataSources. This backend tracks no per-feature enable
// timestamp, only Member.UpdatedAt (set when CreateMembers added the
// account), used as the best available honest proxy for when that account's
// trial started, in place of a hardcoded day count.
func (b *InMemoryBackend) GetRemainingFreeTrialDays(detectorID string, accountIDs []string) (map[string]any, error) {
	b.mu.RLock("GetRemainingFreeTrialDays")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	accounts := make([]any, 0, len(accountIDs))
	unprocessed := make([]any, 0)

	for _, id := range accountIDs {
		m, ok := b.members.Get(detectorKey(detectorID, id))
		if !ok {
			unprocessed = append(unprocessed, map[string]any{
				keyAccountIDField: id,
				"result":          errResourceNotFound, //nolint:goconst // existing issue.
			})

			continue
		}

		accounts = append(accounts, map[string]any{
			keyAccountIDField: id,
			"features":        freeTrialFeatures(m.UpdatedAt), //nolint:goconst // existing issue.
		})
	}

	return map[string]any{
		"accounts":            accounts,
		"unprocessedAccounts": unprocessed, //nolint:goconst // existing issue.
	}, nil
}

func freeTrialFeatures(since time.Time) []map[string]any {
	remaining := freeTrialDaysRemaining(since)
	out := make([]map[string]any, 0, len(freeTrialBaseFeatures))

	for _, name := range freeTrialBaseFeatures {
		out = append(out, map[string]any{"name": name, "freeTrialDaysRemaining": remaining})
	}

	return out
}

func freeTrialDaysRemaining(since time.Time) int32 {
	const hoursPerDay = 24

	elapsed := int32(time.Since(since).Hours() / hoursPerDay)
	remaining := freeTrialDays - elapsed

	if remaining < 0 {
		return 0
	}

	return remaining
}
