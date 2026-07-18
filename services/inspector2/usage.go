package inspector2

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// ListUsageTotals returns usage totals (stub).
func (b *InMemoryBackend) ListUsageTotals(_ []string) ([]map[string]any, error) {
	return []map[string]any{
		{
			keyAccountID: b.accountID,
			keyStatus:    statusActive,
			"usage":      []any{},
		},
	}, nil
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
