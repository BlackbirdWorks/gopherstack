// Package dynamodb implements the AWS DynamoDB mock service.
// execute_transaction.go implements ExecuteTransaction: a set of PartiQL DML
// statements executed atomically via snapshot-based rollback.
package dynamodb

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// --- ExecuteTransaction ---

// ExecuteTransaction executes a set of PartiQL DML statements atomically.
// Atomicity is provided via snapshot-based rollback: pre-transaction snapshots
// of all affected tables are captured, statements are executed sequentially,
// and all tables are restored from their snapshots if any statement fails.
// This matches the observable contract of real AWS ExecuteTransaction for
// single-process in-memory usage.
func (db *InMemoryDB) ExecuteTransaction(
	ctx context.Context,
	input *dynamodb.ExecuteTransactionInput,
) (*dynamodb.ExecuteTransactionOutput, error) {
	if len(input.TransactStatements) == 0 {
		return nil, NewValidationException("TransactStatements must contain at least one statement")
	}

	const maxTransactStatements = 100

	if len(input.TransactStatements) > maxTransactStatements {
		return nil, NewValidationException(
			fmt.Sprintf("Too many statements in transaction: maximum is %d", maxTransactStatements),
		)
	}

	// Collect unique table names so we can snapshot them before execution.
	tableNames := executeTransactionTableNames(input.TransactStatements)

	// Capture pre-transaction snapshots of all affected tables.
	snapshots := db.captureExecTxnSnapshots(ctx, tableNames)

	runner := &partiQLRunner{backend: db}
	responses := make([]types.ItemResponse, len(input.TransactStatements))
	tableRCU := make(map[string]float64)
	tableWCU := make(map[string]float64)
	returnCC := input.ReturnConsumedCapacity != "" &&
		input.ReturnConsumedCapacity != types.ReturnConsumedCapacityNone

	for i, stmt := range input.TransactStatements {
		resp, stmtStr, execErr := executeTransactionStatement(ctx, runner, stmt)
		if execErr != nil {
			// Roll back all tables to their pre-transaction state.
			db.restoreExecTxnSnapshots(ctx, tableNames, snapshots)

			return nil, execErr
		}
		responses[i] = resp

		if returnCC {
			trackTransactCC(stmtStr, tableRCU, tableWCU)
		}
	}

	return &dynamodb.ExecuteTransactionOutput{
		Responses:        responses,
		ConsumedCapacity: buildTransactionConsumedCapacity(tableRCU, tableWCU, returnCC),
	}, nil
}

// executeTransactionTableNames extracts sorted unique table names from transaction statements.
func executeTransactionTableNames(stmts []types.ParameterizedStatement) []string {
	seen := make(map[string]struct{})
	for _, stmt := range stmts {
		if stmt.Statement == nil {
			continue
		}
		name := partiqlStmtTableName(*stmt.Statement)
		if name != "" {
			seen[name] = struct{}{}
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// captureExecTxnSnapshots takes read-locked snapshots of the named tables.
// Tables that don't exist yet are skipped (the statement execution will produce
// the appropriate error).
func (db *InMemoryDB) captureExecTxnSnapshots(
	ctx context.Context,
	tableNames []string,
) map[string]tableStateSnapshot {
	region := getRegionFromContext(ctx, db)
	snapshots := make(map[string]tableStateSnapshot, len(tableNames))

	tables := db.resolveTablesByNameRLocked(region, tableNames, "ExecuteTransaction.snapshot")

	for _, name := range tableNames {
		t, ok := tables[name]
		if !ok {
			continue
		}

		snapshots[name] = snapshotTxnTableStateRLocked(t)
	}

	return snapshots
}

// resolveTablesByNameRLocked resolves tableNames to their *Table pointers in
// region under a defer-protected db.mu.RLock, using op as the lock's metrics
// label. Names with no matching table are omitted from the result.
func (db *InMemoryDB) resolveTablesByNameRLocked(
	region string,
	tableNames []string,
	op string,
) map[string]*Table {
	db.mu.RLock(op)
	defer db.mu.RUnlock()

	tables := make(map[string]*Table, len(tableNames))
	for _, name := range tableNames {
		if t, ok := db.tables.Get(tableKey(region, name)); ok {
			tables[name] = t
		}
	}

	return tables
}

// snapshotTxnTableStateRLocked copies the item slice and PK/SK indexes needed
// to restore a table after a failed transaction, under a defer-protected
// table.mu.RLock.
func snapshotTxnTableStateRLocked(t *Table) tableStateSnapshot {
	t.mu.RLock("ExecuteTransaction.snapshot")
	defer t.mu.RUnlock()

	itemsCopy := make([]map[string]any, len(t.Items))
	copy(itemsCopy, t.Items)
	// Snapshot itemSizes alongside Items so rollback restores the
	// len(itemSizes) == len(Items) invariant (and accurate size accounting).
	itemSizesCopy := make([]int, len(t.itemSizes))
	copy(itemSizesCopy, t.itemSizes)
	pkIdxCopy := make(map[string]int, len(t.pkIndex))
	maps.Copy(pkIdxCopy, t.pkIndex)
	pkskIdxCopy := make(map[string]map[string]int, len(t.pkskIndex))
	for pk, skMap := range t.pkskIndex {
		skMapCopy := make(map[string]int, len(skMap))
		maps.Copy(skMapCopy, skMap)
		pkskIdxCopy[pk] = skMapCopy
	}

	return tableStateSnapshot{
		items:              itemsCopy,
		itemSizes:          itemSizesCopy,
		totalItemSizeBytes: t.totalItemSizeBytes,
		pkIndex:            pkIdxCopy,
		pkskIndex:          pkskIdxCopy,
	}
}

// restoreExecTxnSnapshots restores all snapshotted tables to their pre-transaction
// state. Each table is write-locked individually during its restore.
func (db *InMemoryDB) restoreExecTxnSnapshots(
	ctx context.Context,
	tableNames []string,
	snapshots map[string]tableStateSnapshot,
) {
	region := getRegionFromContext(ctx, db)

	tables := db.resolveTablesByNameRLocked(region, tableNames, "ExecuteTransaction.restore")

	for _, name := range tableNames {
		snap, ok := snapshots[name]
		if !ok {
			continue
		}

		t, tableOK := tables[name]
		if !tableOK {
			continue
		}

		restoreTxnTableStateLocked(t, snap)
	}
}

// restoreTxnTableStateLocked restores a table's items and PK/SK indexes from
// snap under a defer-protected table.mu.Lock.
func restoreTxnTableStateLocked(t *Table, snap tableStateSnapshot) {
	t.mu.Lock("ExecuteTransaction.restore")
	defer t.mu.Unlock()

	t.Items = snap.items
	t.itemSizes = snap.itemSizes
	t.totalItemSizeBytes = snap.totalItemSizeBytes
	t.pkIndex = snap.pkIndex
	t.pkskIndex = snap.pkskIndex
}

// executeTransactionStatement converts one ParameterizedStatement to wire format,
// runs it, and returns the ItemResponse plus the statement string for CC tracking.
func executeTransactionStatement(
	ctx context.Context,
	runner *partiQLRunner,
	stmt types.ParameterizedStatement,
) (types.ItemResponse, string, error) {
	stmtStr := ""
	if stmt.Statement != nil {
		stmtStr = *stmt.Statement
	}

	wireParams := make([]map[string]any, 0, len(stmt.Parameters))
	for _, p := range stmt.Parameters {
		wire, ok := models.FromSDKAttributeValue(p).(map[string]any)
		if !ok {
			return types.ItemResponse{}, "", NewValidationException(
				"invalid parameter type in TransactStatement",
			)
		}

		wireParams = append(wireParams, wire)
	}

	out, err := runner.executeStatement(ctx, executeStatementRequest{
		Statement:  stmtStr,
		Parameters: wireParams,
	})
	if err != nil {
		return types.ItemResponse{}, "", err
	}

	resp := types.ItemResponse{}
	if len(out.Items) > 0 {
		if sdkItem, convErr := models.ToSDKItem(out.Items[0]); convErr == nil {
			resp.Item = sdkItem
		}
	}

	return resp, stmtStr, nil
}

// trackTransactCC updates per-table RCU/WCU counters for a single statement.
func trackTransactCC(stmtStr string, tableRCU, tableWCU map[string]float64) {
	tbl := partiqlStmtTableName(stmtStr)
	if tbl == "" {
		return
	}

	if isWriteStmt(stmtStr) {
		tableWCU[tbl]++
	} else {
		tableRCU[tbl]++
	}
}

// buildTransactionConsumedCapacity assembles the ConsumedCapacity slice from
// per-table RCU/WCU accumulators. Returns nil when returnCC is false.
func buildTransactionConsumedCapacity(
	tableRCU, tableWCU map[string]float64,
	returnCC bool,
) []types.ConsumedCapacity {
	if !returnCC {
		return nil
	}

	result := make([]types.ConsumedCapacity, 0, len(tableRCU)+len(tableWCU))
	seen := make(map[string]bool, len(tableRCU))

	for tbl, rcu := range tableRCU {
		seen[tbl] = true
		result = append(result, types.ConsumedCapacity{
			TableName:          aws.String(tbl),
			ReadCapacityUnits:  aws.Float64(rcu),
			WriteCapacityUnits: aws.Float64(tableWCU[tbl]),
		})
	}

	for tbl, wcu := range tableWCU {
		if seen[tbl] {
			continue
		}
		result = append(result, types.ConsumedCapacity{
			TableName:          aws.String(tbl),
			ReadCapacityUnits:  aws.Float64(0),
			WriteCapacityUnits: aws.Float64(wcu),
		})
	}

	return result
}

// isWriteStmt reports whether a PartiQL statement is a write (INSERT/UPDATE/DELETE).
func isWriteStmt(stmt string) bool {
	upper := strings.ToUpper(strings.TrimSpace(stmt))

	return strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "UPDATE") ||
		strings.HasPrefix(upper, "DELETE")
}

// partiqlStmtTableName extracts the table name from a PartiQL statement string.
// Returns empty string when the table name cannot be determined.
func partiqlStmtTableName(stmt string) string {
	return extractPartiQLTableName(strings.TrimSpace(stmt))
}
