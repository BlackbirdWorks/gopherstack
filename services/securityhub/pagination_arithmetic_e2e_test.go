package securityhub_test

import (
	"fmt"
	"testing"

	securityhub "github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// maxWalkPages bounds every walkStrings call: 23 seeded items at page size 5
// need 5 pages, so this is a generous budget that still fails fast on an
// infinite loop (Class B) instead of hanging.
const maxWalkPages = 20

// walkStrings drains a (nextToken string, maxResults int) -> (page, next)
// paginator to completion, page-size 5, returning every id seen across every
// page. It fails the test if the walk does not terminate within
// maxWalkPages (guards against an infinite loop / Class B).
func walkStrings(t *testing.T, pageOf func(token string) ([]string, string)) []string {
	t.Helper()

	var (
		got   []string
		token string
	)

	for range maxWalkPages + 1 {
		page, next := pageOf(token)
		got = append(got, page...)

		if next == "" {
			return got
		}

		token = next
	}

	t.Fatalf("pagination did not terminate within %d pages", maxWalkPages)

	return nil
}

// TestListAggregatorsV2_BoundaryWalk proves ListAggregatorsV2 no longer
// drops/duplicates entries across a boundary walk now that it reads via
// store.Table.Snapshot() (sorted) instead of All() (unspecified map order).
func TestListAggregatorsV2_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	want := make([]string, 0, 23)
	for range 23 {
		agg, err := b.CreateAggregatorV2("ALL_REGIONS", nil)
		require.NoError(t, err)
		want = append(want, agg.AggregatorV2Arn)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.ListAggregatorsV2(token, 5)
		ids := make([]string, len(page))
		for i, a := range page {
			ids[i] = a.AggregatorV2Arn
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want), "no item dropped or duplicated across the boundary walk")
}

func TestListAutomationRulesV2_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	want := make([]string, 0, 23)
	for i := range 23 {
		rule, err := b.CreateAutomationRuleV2(
			fmt.Sprintf("rule-%02d", i), "ENABLED", "d", map[string]any{}, nil, float64(i), nil,
		)
		require.NoError(t, err)
		want = append(want, rule.RuleArn)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.ListAutomationRulesV2(token, 5)
		ids := make([]string, len(page))
		for i, r := range page {
			ids[i] = r.RuleArn
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}

func TestListFindingAggregators_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	want := make([]string, 0, 23)
	for range 23 {
		agg, err := b.CreateFindingAggregator("ALL_REGIONS", nil)
		require.NoError(t, err)
		want = append(want, agg.FindingAggregatorArn)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.ListFindingAggregators(token, 5)
		ids := make([]string, len(page))
		for i, a := range page {
			ids[i] = a.FindingAggregatorArn
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}

func TestListConfigurationPolicies_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	want := make([]string, 0, 23)
	for i := range 23 {
		p, err := b.CreateConfigurationPolicy(fmt.Sprintf("policy-%02d", i), "d", map[string]any{}, nil)
		require.NoError(t, err)
		want = append(want, p.Id)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.ListConfigurationPolicies(token, 5)
		ids := make([]string, len(page))
		for i, p := range page {
			ids[i] = p.Id
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}

func TestListMembers_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	accounts := make([]map[string]any, 0, 23)
	want := make([]string, 0, 23)

	for i := range 23 {
		id := fmt.Sprintf("%012d", i)
		accounts = append(accounts, map[string]any{"AccountId": id, "Email": "a@example.com"})
		want = append(want, id)
	}

	_, unprocessed := b.CreateMembers(accounts)
	require.Empty(t, unprocessed)

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.ListMembers(false, token, 5)
		ids := make([]string, len(page))
		for i, m := range page {
			ids[i] = m.AccountId
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}

func TestListOrganizationAdminAccounts_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	want := make([]string, 0, 23)
	for i := range 23 {
		id := fmt.Sprintf("%012d", i)
		require.NoError(t, b.EnableOrganizationAdminAccount(id))
		want = append(want, id)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.ListOrganizationAdminAccounts(token, 5)
		ids := make([]string, len(page))
		for i, a := range page {
			ids[i] = a.AccountId
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}

func TestListConnectorsV2_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	want := make([]string, 0, 23)
	for i := range 23 {
		c, err := b.CreateConnectorV2(fmt.Sprintf("conn-%02d", i), "d", map[string]any{}, nil)
		require.NoError(t, err)
		want = append(want, c.ConnectorArn)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.ListConnectorsV2(token, 5)
		ids := make([]string, len(page))
		for i, c := range page {
			ids[i] = c.ConnectorArn
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}

func TestListConnectors_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	want := make([]string, 0, 23)
	for i := range 23 {
		c, err := b.CreateConnector(fmt.Sprintf("conn-%02d", i), "d", map[string]any{"providerName": "JIRA"}, nil)
		require.NoError(t, err)
		want = append(want, c.ConnectorArn)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.ListConnectors("", "", "", token, 5)
		ids := make([]string, len(page))
		for i, c := range page {
			ids[i] = c.ConnectorArn
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}

func TestDescribeActionTargets_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.EnableHub(false, nil))

	want := make([]string, 0, 23)
	for i := range 23 {
		arn, err := b.CreateActionTarget(fmt.Sprintf("target-%02d", i), "d", fmt.Sprintf("target-%02d", i))
		require.NoError(t, err)
		want = append(want, arn)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.DescribeActionTargets(nil, token, 5)
		ids := make([]string, len(page))
		for i, a := range page {
			ids[i] = a.ActionTargetArn
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}

// minimalFinding builds the smallest ASFF finding validateASFFRequiredFields
// accepts.
func minimalFinding(id string) map[string]any {
	return map[string]any{
		"SchemaVersion": "2018-10-08",
		"Id":            id,
		"ProductArn":    "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default",
		"GeneratorId":   "test-generator",
		"AwsAccountId":  "000000000000",
		"Types":         []any{"Software and Configuration Checks"},
		"CreatedAt":     "2026-01-01T00:00:00Z",
		"UpdatedAt":     "2026-01-01T00:00:00Z",
		"Severity":      map[string]any{"Label": "LOW"},
		"Title":         "t",
		"Description":   "d",
		"Resources":     []any{map[string]any{"Type": "Other", "Id": "arn:aws:s3:::bucket/" + id}},
	}
}

// TestGetFindings_NoSortCriteria_BoundaryWalk proves GetFindings, called
// with no SortCriteria (the common shape), no longer drops/duplicates
// findings across a boundary walk now that sortFindings imposes a
// deterministic tiebreak even when the caller supplies zero criteria.
func TestGetFindings_NoSortCriteria_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	want := make([]string, 0, 23)

	for i := range 23 {
		id := fmt.Sprintf("finding-%02d", i)
		findings := []map[string]any{minimalFinding(id)}
		ok, failed, _ := b.ImportFindings(findings)
		require.Equal(t, 1, ok)
		require.Equal(t, 0, failed)
		want = append(want, id)
	}

	got := walkStrings(t, func(token string) ([]string, string) {
		page, next := b.GetFindings(nil, nil, token, 5)
		ids := make([]string, len(page))
		for i, f := range page {
			ids[i], _ = f["Id"].(string)
		}

		return ids, next
	})

	assert.ElementsMatch(t, want, got)
	assert.Len(t, got, len(want))
}
