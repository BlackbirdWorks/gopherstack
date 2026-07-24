package inspector2

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// usageRatePerResource is a nominal per-covered-resource cost used to derive
// UsageTotal.EstimatedMonthlyCost from real backend state (enabled resource
// types and seeded coverage). gopherstack has no metering engine and Amazon
// Inspector's actual pricing is not something a mock can reproduce
// authoritatively, so this is a documented, deterministic placeholder rather
// than a fabricated/random value -- the response shape and field names are
// what matters for parity, not the dollar amount.
const usageRatePerResource = 0.01

// usageTypeForResourceType maps an Inspector2 resourceType to the real
// UsageType enum value ListUsageTotals reports it under.
func usageTypeForResourceType(rt string) string {
	switch rt {
	case resourceTypeEC2:
		return "EC2_INSTANCE_HOURS"
	case resourceTypeECR:
		return "ECR_INITIAL_SCAN"
	case resourceTypeLambda:
		return "LAMBDA_FUNCTION_HOURS"
	case resourceTypeLambdaCode:
		return "LAMBDA_FUNCTION_CODE_HOURS"
	default:
		return ""
	}
}

// ListUsageTotals returns real per-account usage, derived from which
// resource types are enabled and how many covered resources of each scan
// type have been seeded (see SeedCoverage) -- replacing the prior
// hardwired-empty-usage stub. accountIDs defaults to the backend's own
// account when empty, matching real AWS (an empty request means "my
// account").
func (b *InMemoryBackend) ListUsageTotals(accountIDs []string) ([]map[string]any, error) {
	b.mu.RLock("ListUsageTotals")
	defer b.mu.RUnlock()

	if len(accountIDs) == 0 {
		accountIDs = []string{b.accountID}
	}

	coverageCounts := make(map[string]int64)
	b.coverageEntries.Range(func(e *CoverageEntry) bool {
		coverageCounts[e.ScanType]++

		return true
	})

	totals := make([]map[string]any, 0, len(accountIDs))

	for _, id := range accountIDs {
		usage := make([]map[string]any, 0, len(knownResourceTypes()))

		for _, rt := range knownResourceTypes() {
			if !b.enabledTypes[rt] {
				continue
			}

			usageType := usageTypeForResourceType(rt)
			if usageType == "" {
				continue
			}

			total := float64(coverageCounts[rt])

			usage = append(usage, map[string]any{
				"currency":             "USD",
				"estimatedMonthlyCost": total * usageRatePerResource,
				"total":                total,
				keyType:                usageType,
			})
		}

		totals = append(totals, map[string]any{
			keyAccountID: id,
			"usage":      usage,
		})
	}

	return totals, nil
}

// BatchGetFreeTrialInfo returns free trial information for accounts.
func (b *InMemoryBackend) BatchGetFreeTrialInfo(accountIDs []string) (map[string]any, error) {
	accounts := make([]map[string]any, 0, len(accountIDs))

	now := time.Now().UTC()

	for _, id := range accountIDs {
		accounts = append(accounts, map[string]any{
			"accountId": id,
			"freeTrialInfo": []map[string]any{
				{
					// end/start are FreeTrialInfo's required DateTimeTimestamp
					// members: the restjson1 deserializer expects an
					// epoch-seconds JSON number, not an RFC3339 string.
					"end":     awstime.Epoch(now.AddDate(0, 0, 30)), //nolint:mnd // existing issue.
					"start":   awstime.Epoch(now),
					keyStatus: statusActive,
					keyType:   "EC2",
				},
			},
		})
	}

	return map[string]any{
		"accounts":       accounts,
		"failedAccounts": []any{},
	}, nil
}
