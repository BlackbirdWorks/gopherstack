// Package dynamodb implements the AWS DynamoDB mock service.
// contributor_insights.go implements the Describe/List/UpdateContributorInsights family.
package dynamodb

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
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

	enabled, mode := contributorInsightsStateRLocked(table)

	status := types.ContributorInsightsStatusDisabled
	if enabled {
		status = types.ContributorInsightsStatusEnabled
	}

	out := &dynamodb.DescribeContributorInsightsOutput{
		TableName:                   &tableName,
		ContributorInsightsStatus:   status,
		ContributorInsightsMode:     mode,
		ContributorInsightsRuleList: []string{},
	}

	if input.IndexName != nil {
		out.IndexName = input.IndexName
	}

	return out, nil
}

// contributorInsightsStateRLocked returns table.ContributorInsightsEnabled and
// table.ContributorInsightsMode under a defer-protected table.mu.RLock.
func contributorInsightsStateRLocked(table *Table) (bool, types.ContributorInsightsMode) {
	table.mu.RLock("DescribeContributorInsights")
	defer table.mu.RUnlock()

	return table.ContributorInsightsEnabled, types.ContributorInsightsMode(table.ContributorInsightsMode)
}

// --- ListContributorInsights ---

// defaultListContributorInsightsLimit caps a page when the caller omits MaxResults.
const defaultListContributorInsightsLimit = 100

// contributorInsightsSnapshot captures the table names and *Table pointers
// for region under a defer-protected db.mu.RLock, in one pass.
func contributorInsightsSnapshot(db *InMemoryDB, region string) ([]string, map[string]*Table) {
	db.mu.RLock("ListContributorInsights")
	defer db.mu.RUnlock()

	regionTables := db.tablesByRegion.Get(region)
	names := make([]string, 0, len(regionTables))
	byName := make(map[string]*Table, len(regionTables))

	for _, t := range regionTables {
		names = append(names, t.Name)
		byName[t.Name] = t
	}

	return names, byName
}

// ListContributorInsights returns ContributorInsightsSummary entries for
// tables in the request region, optionally filtered to a single table (or its
// ARN, per the wire contract) and paginated via MaxResults/NextToken.
func (db *InMemoryDB) ListContributorInsights(
	ctx context.Context,
	input *dynamodb.ListContributorInsightsInput,
) (*dynamodb.ListContributorInsightsOutput, error) {
	region := getRegionFromContext(ctx, db)
	tableFilter := tableNameFromARN(aws.ToString(input.TableName))

	if tableFilter != "" {
		if _, err := db.getTable(ctx, tableFilter); err != nil {
			return nil, err
		}
	}

	names, byName := contributorInsightsSnapshot(db, region)
	sort.Strings(names)

	pageSize := int(input.MaxResults)
	if pageSize <= 0 {
		pageSize = defaultListContributorInsightsLimit
	}

	summaries := collectContributorInsightsSummaries(names, byName, tableFilter, aws.ToString(input.NextToken))

	var nextToken *string
	if len(summaries) > pageSize {
		tok := aws.ToString(summaries[pageSize-1].TableName)
		nextToken = &tok
		summaries = summaries[:pageSize]
	}

	return &dynamodb.ListContributorInsightsOutput{
		ContributorInsightsSummaries: summaries,
		NextToken:                    nextToken,
	}, nil
}

// collectContributorInsightsSummaries walks names in sorted order, skipping
// forward past cursor (exclusive) and applying tableFilter, and returns one
// summary per table with contributor insights enabled.
func collectContributorInsightsSummaries(
	names []string, byName map[string]*Table, tableFilter, cursor string,
) []types.ContributorInsightsSummary {
	started := cursor == ""

	var summaries []types.ContributorInsightsSummary

	for _, name := range names {
		if tableFilter != "" && name != tableFilter {
			continue
		}

		if !started {
			if name == cursor {
				started = true
			}

			continue
		}

		enabled, mode := contributorInsightsStateRLocked(byName[name])
		if !enabled {
			continue
		}

		tableName := name
		summaries = append(summaries, types.ContributorInsightsSummary{
			TableName:                 &tableName,
			ContributorInsightsStatus: types.ContributorInsightsStatusEnabled,
			ContributorInsightsMode:   mode,
		})
	}

	return summaries
}

// --- UpdateContributorInsights ---

// UpdateContributorInsights toggles contributor insights for a table.
// The action is interpreted as ENABLE / DISABLE per AWS spec. When the
// caller supplies ContributorInsightsMode it is persisted and echoed back by
// this op and by Describe/ListContributorInsights; when omitted, whatever
// mode (if any) was previously configured is left unchanged and echoed.
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

	mode := setContributorInsightsLocked(table, enable, input.ContributorInsightsMode)

	tableName := *input.TableName

	status := types.ContributorInsightsStatusDisabled
	if enable {
		status = types.ContributorInsightsStatusEnabled
	}

	out := &dynamodb.UpdateContributorInsightsOutput{
		TableName:                 &tableName,
		ContributorInsightsStatus: status,
		ContributorInsightsMode:   mode,
	}

	if input.IndexName != nil {
		out.IndexName = input.IndexName
	}

	return out, nil
}

// setContributorInsightsLocked sets table.ContributorInsightsEnabled and,
// when mode is non-empty, table.ContributorInsightsMode, under a
// defer-protected table.mu.Lock. Returns the table's mode after the update.
func setContributorInsightsLocked(
	table *Table, enable bool, mode types.ContributorInsightsMode,
) types.ContributorInsightsMode {
	table.mu.Lock("UpdateContributorInsights")
	defer table.mu.Unlock()

	table.ContributorInsightsEnabled = enable
	if mode != "" {
		table.ContributorInsightsMode = string(mode)
	}

	return types.ContributorInsightsMode(table.ContributorInsightsMode)
}
