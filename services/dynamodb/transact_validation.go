// Package dynamodb implements the AWS DynamoDB mock service.
// transact_validation.go validates TransactWriteItems / TransactGetItems input:
// duplicate-key detection, the 4 MB total size limit, key-modification guards,
// and the 100-item count limit.
package dynamodb

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// maxTransactWriteSizeBytes is the maximum total size for TransactWriteItems (4 MB).
const maxTransactWriteSizeBytes = 4 * 1024 * 1024

// maxTransactItems is the maximum item count for TransactWriteItems/TransactGetItems.
const maxTransactItems = 100

// transactWriteKey is the canonical key used for duplicate detection.
type transactWriteKey struct {
	TableName string
	KeyJSON   string
}

// validateTransactWriteItems checks for duplicate keys and total size limit.
// Returns TransactionCanceledException on duplicate keys, ValidationException on size.
func validateTransactWriteItems(
	items []types.TransactWriteItem,
	tables map[string]*Table,
) error {
	seen := make(map[transactWriteKey]bool, len(items))
	totalBytes := 0

	for i, ti := range items {
		if err := validateTransactUnusedExpressionAttrs(ti); err != nil {
			return err
		}

		if err := validateTransactUpdateKeys(ti, tables); err != nil {
			return err
		}

		if err := checkTransactWriteItemSizeAndDupe(i, ti, items, tables, seen, &totalBytes); err != nil {
			return err
		}
	}

	return nil
}

// checkTransactWriteItemSizeAndDupe accumulates the running total-size estimate
// for one TransactWriteItem and checks it against the same-key duplicate-write
// set, returning ValidationException on the 4 MB limit or
// TransactionCanceledException on a duplicate key. Split out of
// validateTransactWriteItems to keep that function's cognitive complexity low.
func checkTransactWriteItemSizeAndDupe(
	i int,
	ti types.TransactWriteItem,
	items []types.TransactWriteItem,
	tables map[string]*Table,
	seen map[transactWriteKey]bool,
	totalBytes *int,
) error {
	tableName, keyItem, itemForSize := extractTransactWriteKeyAndItem(ti)
	if tableName == "" {
		return nil
	}

	// Accumulate size estimate.
	if itemForSize != nil {
		sz, _ := CalculateItemSize(models.FromSDKItem(itemForSize))
		*totalBytes += sz
	}

	if *totalBytes > maxTransactWriteSizeBytes {
		return NewValidationException(
			"Transaction size exceeded: maximum allowed is 4 MB",
		)
	}

	if keyItem == nil {
		return nil
	}

	wireKey := models.FromSDKItem(keyItem)

	// Resolve the table to extract only the key attributes.
	if table, ok := tables[tableName]; ok {
		pkDef, skDef := getPKAndSK(table.KeySchema)
		keyOnly := map[string]any{pkDef.AttributeName: wireKey[pkDef.AttributeName]}
		if skDef.AttributeName != "" {
			keyOnly[skDef.AttributeName] = wireKey[skDef.AttributeName]
		}
		wireKey = keyOnly
	}

	// A marshal failure only affects duplicate-key detection (not a real
	// validation error), so it's not surfaced — the item is simply not
	// tracked in seen and duplicate detection silently skips it, matching
	// the pre-refactor loop's "continue on marshal error" behavior.
	keyBytes, marshalErr := json.Marshal(wireKey)
	if marshalErr == nil {
		twk := transactWriteKey{TableName: tableName, KeyJSON: string(keyBytes)}
		if seen[twk] {
			reasons := makeDuplicateKeyReasons(items, i)

			return NewTransactionCanceledException(
				txCancelPrefix, reasons,
			)
		}

		seen[twk] = true
	}

	return nil
}

// validateTransactUpdateKeys rejects a TransactWriteItem Update action whose
// UpdateExpression touches a key attribute — the same restriction plain
// UpdateItem enforces. Without this check a transactional update can rewrite
// an item's key in place while leaving the OLD key's index entry dangling
// (updateIndexes only ever adds/overwrites the new key's index slot, it never
// removes a stale one), corrupting pkIndex/pkskIndex lookups. Non-Update
// actions are always allowed through (nil).
func validateTransactUpdateKeys(ti types.TransactWriteItem, tables map[string]*Table) error {
	if ti.Update == nil {
		return nil
	}

	table, ok := tables[aws.ToString(ti.Update.TableName)]
	if !ok {
		return nil
	}

	return validateUpdateDoesNotModifyKeys(
		aws.ToString(ti.Update.UpdateExpression),
		ti.Update.ExpressionAttributeNames,
		table.KeySchema,
	)
}

// validateTransactUnusedExpressionAttrs rejects a TransactWriteItem whose
// ExpressionAttributeNames or ExpressionAttributeValues declare a placeholder
// that no expression on that item actually references (ConditionExpression for
// Put/Delete/ConditionCheck; UpdateExpression + ConditionExpression for
// Update) -- the same requirement plain PutItem/UpdateItem/DeleteItem enforce
// via checkUnusedExpressionAttributeNames/Values (item_ops_crud.go).
func validateTransactUnusedExpressionAttrs(ti types.TransactWriteItem) error {
	switch {
	case ti.Put != nil:
		return checkUnusedExpressionAttrs(
			ti.Put.ExpressionAttributeNames,
			ti.Put.ExpressionAttributeValues,
			aws.ToString(ti.Put.ConditionExpression),
		)
	case ti.Delete != nil:
		return checkUnusedExpressionAttrs(
			ti.Delete.ExpressionAttributeNames,
			ti.Delete.ExpressionAttributeValues,
			aws.ToString(ti.Delete.ConditionExpression),
		)
	case ti.Update != nil:
		return checkUnusedExpressionAttrs(
			ti.Update.ExpressionAttributeNames,
			ti.Update.ExpressionAttributeValues,
			aws.ToString(ti.Update.UpdateExpression),
			aws.ToString(ti.Update.ConditionExpression),
		)
	case ti.ConditionCheck != nil:
		return checkUnusedExpressionAttrs(
			ti.ConditionCheck.ExpressionAttributeNames,
			ti.ConditionCheck.ExpressionAttributeValues,
			aws.ToString(ti.ConditionCheck.ConditionExpression),
		)
	}

	return nil
}

// checkUnusedExpressionAttrs runs both the EAN and EAV unused-placeholder
// checks (checkUnusedExpressionAttributeNames/Values in expressions.go)
// against the combined expression text.
func checkUnusedExpressionAttrs(
	ean map[string]string,
	eav map[string]types.AttributeValue,
	exprs ...string,
) error {
	if err := checkUnusedExpressionAttributeNames(ean, exprs...); err != nil {
		return err
	}

	return checkUnusedExpressionAttributeValues(models.FromSDKItem(eav), exprs...)
}

// extractTransactWriteKeyAndItem returns the table name, key map, and item map
// from a single TransactWriteItem (for duplicate detection and size accounting).
// ConditionCheck is excluded from duplicate key detection because AWS DynamoDB
// allows one ConditionCheck and one write operation on the same key.
func extractTransactWriteKeyAndItem(
	ti types.TransactWriteItem,
) (string, map[string]types.AttributeValue, map[string]types.AttributeValue) {
	switch {
	case ti.Put != nil:
		return aws.ToString(ti.Put.TableName), ti.Put.Item, ti.Put.Item
	case ti.Delete != nil:
		return aws.ToString(ti.Delete.TableName), ti.Delete.Key, nil
	case ti.Update != nil:
		return aws.ToString(ti.Update.TableName), ti.Update.Key, nil
	case ti.ConditionCheck != nil:
		// ConditionCheck is not a write; exclude from duplicate key tracking.
		// Size is also not counted for ConditionCheck-only operations.
		return "", nil, nil
	}

	return "", nil, nil
}

// makeDuplicateKeyReasons builds a cancellation reasons slice with a
// DuplicateItem code at position idx (all others are "None").
func makeDuplicateKeyReasons(items []types.TransactWriteItem, idx int) []CancellationReason {
	reasons := make([]CancellationReason, len(items))
	for i := range reasons {
		reasons[i] = CancellationReason{Code: "None"}
	}

	if idx >= 0 && idx < len(reasons) {
		reasons[idx] = CancellationReason{
			Code:    "DuplicateItem",
			Message: "Transaction contains more than one action for the same item",
		}
	}

	return reasons
}

// validateTransactItemCount returns a ValidationException when the item count
// exceeds maxTransactItems (100). AWS enforces this limit on both Transact ops.
func validateTransactItemCount(n int, opName string) error {
	if n > maxTransactItems {
		return NewValidationException(
			fmt.Sprintf(
				"Member must have length less than or equal to %d",
				maxTransactItems,
			),
		)
	}

	_ = opName // reserved for future context-specific messages

	return nil
}
