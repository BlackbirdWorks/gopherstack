// Package dynamodb implements the AWS DynamoDB mock service.
// contributor_insights.go implements the Describe/List/UpdateContributorInsights family.
package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// --- DescribeContributorInsights ---

// DescribeContributorInsights returns contributor insights status for a table or GSI.
// Status is tracked per-table on the in-memory backend; GSI-level status mirrors the table.
func (db *InMemoryDB) DescribeContributorInsights(
	ctx context.Context,
	input *dynamodb.DescribeContributorInsightsInput,
) (*dynamodb.DescribeContributorInsightsOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	tableName := *input.TableName

	enabled := contributorInsightsEnabledRLocked(table)

	status := types.ContributorInsightsStatusDisabled
	if enabled {
		status = types.ContributorInsightsStatusEnabled
	}

	out := &dynamodb.DescribeContributorInsightsOutput{
		TableName:                   &tableName,
		ContributorInsightsStatus:   status,
		ContributorInsightsRuleList: []string{},
	}

	if input.IndexName != nil {
		out.IndexName = input.IndexName
	}

	return out, nil
}

// contributorInsightsEnabledRLocked returns table.ContributorInsightsEnabled
// under a defer-protected table.mu.RLock.
func contributorInsightsEnabledRLocked(table *Table) bool {
	table.mu.RLock("DescribeContributorInsights")
	defer table.mu.RUnlock()

	return table.ContributorInsightsEnabled
}

// --- ListContributorInsights ---

// ListContributorInsights returns the set of tables whose contributor insights are enabled,
// scoped to the request region.
func (db *InMemoryDB) ListContributorInsights(
	ctx context.Context,
	_ *dynamodb.ListContributorInsightsInput,
) (*dynamodb.ListContributorInsightsOutput, error) {
	region := getRegionFromContext(ctx, db)

	db.mu.RLock("ListContributorInsights")
	defer db.mu.RUnlock()

	var summaries []types.ContributorInsightsSummary

	for _, t := range db.tablesByRegion.Get(region) {
		enabled := contributorInsightsEnabledRLocked(t)
		if !enabled {
			continue
		}

		tableName := t.Name
		summaries = append(summaries, types.ContributorInsightsSummary{
			TableName:                 &tableName,
			ContributorInsightsStatus: types.ContributorInsightsStatusEnabled,
		})
	}

	return &dynamodb.ListContributorInsightsOutput{
		ContributorInsightsSummaries: summaries,
	}, nil
}

// --- UpdateContributorInsights ---

// UpdateContributorInsights toggles contributor insights for a table.
// The action is interpreted as ENABLE / DISABLE per AWS spec.
func (db *InMemoryDB) UpdateContributorInsights(
	ctx context.Context,
	input *dynamodb.UpdateContributorInsightsInput,
) (*dynamodb.UpdateContributorInsightsOutput, error) {
	if input.TableName == nil || *input.TableName == "" {
		return nil, NewValidationException("TableName is required")
	}

	table, err := db.getTable(ctx, *input.TableName)
	if err != nil {
		return nil, err
	}

	enable := input.ContributorInsightsAction == types.ContributorInsightsActionEnable

	setContributorInsightsLocked(table, enable)

	tableName := *input.TableName

	status := types.ContributorInsightsStatusDisabled
	if enable {
		status = types.ContributorInsightsStatusEnabled
	}

	out := &dynamodb.UpdateContributorInsightsOutput{
		TableName:                 &tableName,
		ContributorInsightsStatus: status,
	}

	if input.IndexName != nil {
		out.IndexName = input.IndexName
	}

	return out, nil
}

// setContributorInsightsLocked sets table.ContributorInsightsEnabled under a
// defer-protected table.mu.Lock.
func setContributorInsightsLocked(table *Table, enable bool) {
	table.mu.Lock("UpdateContributorInsights")
	defer table.mu.Unlock()

	table.ContributorInsightsEnabled = enable
}
