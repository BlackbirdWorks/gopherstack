// legacy_query_scan.go translates Query/Scan's legacy pre-2013 parameters
// (KeyConditions, QueryFilter, ScanFilter) into their modern expression
// equivalents (KeyConditionExpression, FilterExpression, plus synthesized
// ExpressionAttributeNames/Values) so they run through the exact same
// evaluation paths the modern expression API already uses -- no second
// evaluator. Reuses legacy_conditions.go's placeholder machinery and
// renderComparison (the ComparisonOperator -> expression-fragment mapping is
// shared verbatim between Condition and ExpectedAttributeValue; see
// types/types.go:672-770 vs :1279-1391).

package dynamodb

import (
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// maxKeyConditionsEntries is the most attributes KeyConditions may name: the
// partition key, plus optionally the sort key. AWS's KeyConditions guide
// (linked, not inlined, from api_op_Query.go:281-284's KeyConditions doc)
// documents this restriction; it is not stated in the SDK struct itself, so
// this is our own transcription of that guide rather than an SDK-cited fact.
const maxKeyConditionsEntries = 2

// keyConditionsAllowedOps is the ComparisonOperator subset AWS accepts for a
// KeyConditions sort-key entry: EQ, LE, LT, GE, GT, BEGINS_WITH, BETWEEN.
// Same disclosure as maxKeyConditionsEntries -- documented in the linked
// guide, not the SDK struct.
//
//nolint:gochecknoglobals,exhaustive // fixed lookup table, see comment above
var keyConditionsAllowedOps = map[types.ComparisonOperator]bool{
	types.ComparisonOperatorEq:         true,
	types.ComparisonOperatorLe:         true,
	types.ComparisonOperatorLt:         true,
	types.ComparisonOperatorGe:         true,
	types.ComparisonOperatorGt:         true,
	types.ComparisonOperatorBeginsWith: true,
	types.ComparisonOperatorBetween:    true,
}

// rejectMixedLegacyKeyConditions enforces the KeyConditions/KeyConditionExpression
// half of AWS's legacy-vs-expression mutual exclusion rule (see
// rejectMixedLegacyAndExpressionParams in legacy_conditions.go for the
// disclosure this message wording has no SDK-validated line to cite).
func rejectMixedLegacyKeyConditions(hasKeyConditions, hasKeyConditionExpr bool) error {
	if hasKeyConditions && hasKeyConditionExpr {
		return NewValidationException(
			"Cannot use both the legacy KeyConditions parameter and " +
				"KeyConditionExpression in the same request",
		)
	}

	return nil
}

// rejectMixedLegacyFilter enforces the QueryFilter/ScanFilter vs
// FilterExpression half of the same mutual exclusion rule. filterParamName
// names the legacy parameter in the message ("QueryFilter" or "ScanFilter").
func rejectMixedLegacyFilter(hasLegacyFilter, hasFilterExpr bool, filterParamName string) error {
	if hasLegacyFilter && hasFilterExpr {
		return NewValidationException(fmt.Sprintf(
			"Cannot use both the legacy %s parameter (or ConditionalOperator) "+
				"and FilterExpression in the same request",
			filterParamName,
		))
	}

	return nil
}

// requireConditionalOperatorNeedsFilter rejects a ConditionalOperator set
// without any entries in the legacy filter map it's meant to combine --
// mirrors requireConditionalOperatorNeedsExpected in legacy_conditions.go.
// ConditionalOperator's own SDK doc ("Use FilterExpression instead", see
// api_op_Query.go:92-98 / api_op_Scan.go:89-95) ties it to QueryFilter/
// ScanFilter, not KeyConditions -- KeyConditions is always ANDed.
func requireConditionalOperatorNeedsFilter(
	condOp types.ConditionalOperator, filterLen int, filterParamName string,
) error {
	if condOp != "" && filterLen == 0 {
		return NewValidationException(fmt.Sprintf(
			"ConditionalOperator can only be used in conjunction with %s", filterParamName,
		))
	}

	return nil
}

// translateLegacyFilterConditions translates a legacy QueryFilter/ScanFilter
// map (combined per ConditionalOperator) into a FilterExpression fragment
// plus the ExpressionAttributeNames/Values it references. Structurally
// identical to translateExpectedToConditionExpression, but Condition (unlike
// ExpectedAttributeValue) only has the ComparisonOperator+AttributeValueList
// style -- no Value/Exists shorthand to normalize.
func translateLegacyFilterConditions(
	conditions map[string]types.Condition,
	condOp types.ConditionalOperator,
	prefix string,
	existingEAN map[string]string,
	existingEAV map[string]types.AttributeValue,
) (string, map[string]string, map[string]types.AttributeValue, error) {
	joiner, err := legacyConditionalJoiner(condOp)
	if err != nil {
		return "", nil, nil, err
	}

	keys := make([]string, 0, len(conditions))
	for k := range conditions {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ph := newLegacyPlaceholders(prefix, existingEAN, existingEAV)
	fragments := make([]string, 0, len(keys))

	for _, k := range keys {
		cond := conditions[k]
		alias := ph.nameFor(k)

		frag, renderErr := renderComparison(alias, cond.ComparisonOperator, cond.AttributeValueList, ph)
		if renderErr != nil {
			return "", nil, nil, renderErr
		}

		fragments = append(fragments, frag)
	}

	return strings.Join(fragments, joiner), ph.ean, ph.eav, nil
}

// translateKeyConditionsToKeyConditionExpression translates a legacy
// KeyConditions map into a KeyConditionExpression string plus the
// ExpressionAttributeNames/Values it references. Unlike QueryFilter's
// translation, order is NOT the map's (nonexistent) iteration order or a
// sorted-keys order -- it is explicitly reconstructed as [partition key,
// sort key] against keySchema, because item_ops_query.go's
// filterCandidatesForKeyCondition/preParseQueryPKValue assume the first
// AND-clause of the resulting expression is the partition-key equality
// condition for their indexed-lookup fast path. A caller listing the sort
// key first in the KeyConditions map (which Go's unordered map allows) must
// still produce the partition-key clause first here.
func translateKeyConditionsToKeyConditionExpression(
	keyConditions map[string]types.Condition,
	keySchema []models.KeySchemaElement,
	existingEAN map[string]string,
	existingEAV map[string]types.AttributeValue,
) (string, map[string]string, map[string]types.AttributeValue, error) {
	if len(keyConditions) > maxKeyConditionsEntries {
		return "", nil, nil, NewValidationException(
			"KeyConditions can specify conditions for at most the partition key and sort key",
		)
	}

	pkDef, skDef := getPKAndSK(keySchema)

	pkCond, hasPK := keyConditions[pkDef.AttributeName]
	if !hasPK {
		return "", nil, nil, NewValidationException(fmt.Sprintf(
			"KeyConditions must include an equality condition on the partition key %q",
			pkDef.AttributeName,
		))
	}
	if pkCond.ComparisonOperator != types.ComparisonOperatorEq {
		return "", nil, nil, NewValidationException(fmt.Sprintf(
			"KeyConditions: the partition key %q only supports the EQ operator, got %s",
			pkDef.AttributeName, pkCond.ComparisonOperator,
		))
	}

	skCond, hasSK, skErr := extractKeyConditionsSortKeyEntry(keyConditions, pkDef, skDef)
	if skErr != nil {
		return "", nil, nil, skErr
	}

	ph := newLegacyPlaceholders("keycond", existingEAN, existingEAV)

	pkAlias := ph.nameFor(pkDef.AttributeName)

	pkFrag, err := renderComparison(pkAlias, pkCond.ComparisonOperator, pkCond.AttributeValueList, ph)
	if err != nil {
		return "", nil, nil, err
	}

	fragments := []string{pkFrag}

	if hasSK {
		if !keyConditionsAllowedOps[skCond.ComparisonOperator] {
			return "", nil, nil, NewValidationException(fmt.Sprintf(
				"KeyConditions: unsupported ComparisonOperator %s for sort key %q",
				skCond.ComparisonOperator, skDef.AttributeName,
			))
		}

		skAlias := ph.nameFor(skDef.AttributeName)

		skFrag, renderErr := renderComparison(skAlias, skCond.ComparisonOperator, skCond.AttributeValueList, ph)
		if renderErr != nil {
			return "", nil, nil, renderErr
		}

		fragments = append(fragments, skFrag)
	}

	return strings.Join(fragments, " AND "), ph.ean, ph.eav, nil
}

// extractKeyConditionsSortKeyEntry returns the KeyConditions entry for the
// sort key (ok=false if none was given) after rejecting any entry that names
// neither the partition key nor the sort key -- the map may only ever
// contain those two attributes (enforced above by maxKeyConditionsEntries,
// this catches e.g. two non-key or wrong-key entries).
func extractKeyConditionsSortKeyEntry(
	keyConditions map[string]types.Condition,
	pkDef, skDef models.KeySchemaElement,
) (types.Condition, bool, error) {
	for attr, c := range keyConditions {
		if attr == pkDef.AttributeName {
			continue
		}
		if skDef.AttributeName == "" || attr != skDef.AttributeName {
			return types.Condition{}, false, NewValidationException(fmt.Sprintf(
				"KeyConditions: %q is not a key attribute for this table or index", attr,
			))
		}

		return c, true, nil
	}

	return types.Condition{}, false, nil
}

// legacyKeySchemaForQuery resolves the KeySchema that a legacy KeyConditions
// translation must reorder against: the base table's when idxName is "",
// otherwise the named GSI/LSI's. Takes a short RLock to copy just the schema
// slices (not items) -- this runs before snapshotTableForQuery's own lock
// cycle, which needs KeyConditionExpression already resolved (via
// preParseQueryPKValue) to know what to snapshot.
func (db *InMemoryDB) legacyKeySchemaForQuery(
	table *Table, idxName string, consistentRead bool,
) ([]models.KeySchemaElement, error) {
	table.mu.RLock("Query.legacyKeyConditions")
	keySchema := make([]models.KeySchemaElement, len(table.KeySchema))
	copy(keySchema, table.KeySchema)
	gsiList := make([]models.GlobalSecondaryIndex, len(table.GlobalSecondaryIndexes))
	copy(gsiList, table.GlobalSecondaryIndexes)
	lsiList := make([]models.LocalSecondaryIndex, len(table.LocalSecondaryIndexes))
	copy(lsiList, table.LocalSecondaryIndexes)
	table.mu.RUnlock()

	schemaTable := &Table{
		KeySchema:              keySchema,
		GlobalSecondaryIndexes: gsiList,
		LocalSecondaryIndexes:  lsiList,
	}

	ks, _, err := db.extractKeySchema(schemaTable, idxName, consistentRead)

	return ks, err
}

// applyLegacyQueryParams rewrites input in place: translating KeyConditions
// into KeyConditionExpression, and QueryFilter (+ConditionalOperator) into
// FilterExpression, after rejecting any mix with their modern expression
// equivalents. Both may apply to the same request (legal -- the all-legacy
// equivalent of setting both KeyConditionExpression and FilterExpression).
func applyLegacyQueryParams(
	db *InMemoryDB, table *Table, idxName string, consistentRead bool, input *dynamodb.QueryInput,
) error {
	hasKeyConditions := len(input.KeyConditions) > 0
	hasKeyCondExpr := aws.ToString(input.KeyConditionExpression) != ""

	if err := rejectMixedLegacyKeyConditions(hasKeyConditions, hasKeyCondExpr); err != nil {
		return err
	}

	hasQueryFilter := len(input.QueryFilter) > 0
	hasFilterExpr := aws.ToString(input.FilterExpression) != ""

	if err := rejectMixedLegacyFilter(
		hasQueryFilter || input.ConditionalOperator != "", hasFilterExpr, "QueryFilter",
	); err != nil {
		return err
	}
	if err := requireConditionalOperatorNeedsFilter(
		input.ConditionalOperator, len(input.QueryFilter), "QueryFilter",
	); err != nil {
		return err
	}

	if hasKeyConditions {
		keySchema, schemaErr := db.legacyKeySchemaForQuery(table, idxName, consistentRead)
		if schemaErr != nil {
			return schemaErr
		}

		exprStr, ean, eav, err := translateKeyConditionsToKeyConditionExpression(
			input.KeyConditions, keySchema, input.ExpressionAttributeNames, input.ExpressionAttributeValues,
		)
		if err != nil {
			return err
		}

		input.KeyConditionExpression = aws.String(exprStr)
		input.ExpressionAttributeNames = mergeEAN(input.ExpressionAttributeNames, ean)
		input.ExpressionAttributeValues = mergeEAV(input.ExpressionAttributeValues, eav)
	}

	if hasQueryFilter {
		exprStr, ean, eav, err := translateLegacyFilterConditions(
			input.QueryFilter, input.ConditionalOperator, "queryfilter",
			input.ExpressionAttributeNames, input.ExpressionAttributeValues,
		)
		if err != nil {
			return err
		}

		input.FilterExpression = aws.String(exprStr)
		input.ExpressionAttributeNames = mergeEAN(input.ExpressionAttributeNames, ean)
		input.ExpressionAttributeValues = mergeEAV(input.ExpressionAttributeValues, eav)
	}

	return nil
}

// applyLegacyScanParams is applyLegacyQueryParams's ScanFilter counterpart --
// Scan has no KeyConditions equivalent (a Scan has no key condition at all).
func applyLegacyScanParams(input *dynamodb.ScanInput) error {
	hasScanFilter := len(input.ScanFilter) > 0
	hasFilterExpr := aws.ToString(input.FilterExpression) != ""

	if err := rejectMixedLegacyFilter(
		hasScanFilter || input.ConditionalOperator != "", hasFilterExpr, "ScanFilter",
	); err != nil {
		return err
	}
	if err := requireConditionalOperatorNeedsFilter(
		input.ConditionalOperator, len(input.ScanFilter), "ScanFilter",
	); err != nil {
		return err
	}

	if !hasScanFilter {
		return nil
	}

	exprStr, ean, eav, err := translateLegacyFilterConditions(
		input.ScanFilter, input.ConditionalOperator, "scanfilter",
		input.ExpressionAttributeNames, input.ExpressionAttributeValues,
	)
	if err != nil {
		return err
	}

	input.FilterExpression = aws.String(exprStr)
	input.ExpressionAttributeNames = mergeEAN(input.ExpressionAttributeNames, ean)
	input.ExpressionAttributeValues = mergeEAV(input.ExpressionAttributeValues, eav)

	return nil
}
