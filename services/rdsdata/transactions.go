package rdsdata

import (
	"context"
	"fmt"
)

// BeginTransaction starts a new transaction and returns its ID.
func (b *InMemoryBackend) BeginTransaction(ctx context.Context, resourceARN string) (string, error) {
	b.mu.Lock("BeginTransaction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	b.txCounter[region]++
	id := fmt.Sprintf("txn-%06d", b.txCounter[region])

	b.transactionsStore(region).Put(&Transaction{
		TransactionID: id,
		Status:        transactionStatusActive,
	})

	// Open a matching engine-side transaction so statements tagged with this ID
	// share atomic visibility. A failure here is non-fatal: such statements
	// fall back to autocommit execution.
	_ = b.engine.beginTx(ctx, region, resourceARN, id)

	return id, nil
}

// CommitTransaction commits a transaction by ID.
func (b *InMemoryBackend) CommitTransaction(
	ctx context.Context,
	transactionID string,
) (string, error) {
	b.mu.Lock("CommitTransaction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.transactionsStore(region)

	if !tbl.Has(transactionID) {
		return "", fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, transactionID)
	}

	tbl.Delete(transactionID)
	b.engine.finalizeTx(transactionID, true)

	return transactionStatusCommitted, nil
}

// RollbackTransaction rolls back a transaction by ID.
func (b *InMemoryBackend) RollbackTransaction(
	ctx context.Context,
	transactionID string,
) (string, error) {
	b.mu.Lock("RollbackTransaction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.transactionsStore(region)

	if !tbl.Has(transactionID) {
		return "", fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, transactionID)
	}

	tbl.Delete(transactionID)
	b.engine.finalizeTx(transactionID, false)

	return transactionStatusRolledBack, nil
}

// ListTransactions returns a deep copy of all active transactions for the request's region.
func (b *InMemoryBackend) ListTransactions(ctx context.Context) map[string]Transaction {
	b.mu.RLock("ListTransactions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.transactionRegion(region)

	if tbl == nil {
		return map[string]Transaction{}
	}

	items := tbl.All()
	result := make(map[string]Transaction, len(items))

	for _, v := range items {
		result[v.TransactionID] = *v
	}

	return result
}

// AddTransactionInternal directly inserts a transaction into the backend's default region.
// This is intended only for seeding test data.
func (b *InMemoryBackend) AddTransactionInternal(txID string) {
	b.mu.Lock("AddTransactionInternal")
	defer b.mu.Unlock()

	b.transactionsStore(b.defaultRegion).Put(&Transaction{
		TransactionID: txID,
		Status:        transactionStatusActive,
	})
}
