package rdsdata

import (
	"context"
	"fmt"
)

// WithRealEngine routes Data API calls for docker-backed Aurora clusters to their real database;
// other clusters keep the SQLite engine.
func (b *InMemoryBackend) WithRealEngine(r ClusterResolver, s SecretReader) *InMemoryBackend {
	re := newRealEngine(r, s)

	b.mu.Lock("WithRealEngine")
	b.real = re
	b.mu.Unlock()

	return b
}

func (b *InMemoryBackend) realEngine() *realEngine {
	b.mu.RLock("realEngine")
	defer b.mu.RUnlock()

	return b.real
}

func errTxNotFound(id string) error {
	return fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, id)
}

// touchRealTx refreshes the idle clock of a live transaction record; false when it has expired.
func (b *InMemoryBackend) touchRealTx(region, id string) bool {
	b.mu.Lock("touchRealTx")
	defer b.mu.Unlock()

	if !b.transactionsStore(region).Has(id) {
		return false
	}

	b.touchTransactionLocked(region, id)

	return true
}

// realCall is a routed Data API call: the engine, login and held transaction key serving it.
type realCall struct {
	re    *realEngine
	txKey string
	lg    realLogin
}

// realRoute reports whether the real engine serves the call; false falls back to SQLite.
func (b *InMemoryBackend) realRoute(
	ctx context.Context, region, resourceARN, txID string,
) (realCall, bool, error) {
	re := b.realEngine()
	if re == nil {
		return realCall{}, false, nil
	}

	if txID != "" {
		key := realTxKey(region, txID)
		if !re.hasTx(key) {
			return realCall{}, false, nil
		}

		if !b.touchRealTx(region, txID) {
			return realCall{}, true, errTxNotFound(txID)
		}

		return realCall{re: re, txKey: key}, true, nil
	}

	lg, ok, err := re.loginFor(ctx, resourceARN)

	return realCall{re: re, lg: lg}, ok, err
}

func (b *InMemoryBackend) executeReal(
	ctx context.Context, resourceARN, stmt, txID string, params []SQLParameter,
) (realResult, bool, error) {
	rc, ok, err := b.realRoute(ctx, getRegion(ctx, b.defaultRegion), resourceARN, txID)
	if !ok || err != nil {
		return realResult{}, ok || err != nil, err
	}

	res, err := rc.re.execute(ctx, rc.lg, rc.txKey, stmt, params)

	return res, true, realTxErr(err, txID)
}

func (b *InMemoryBackend) batchReal(
	ctx context.Context, resourceARN, stmt, txID string, sets [][]SQLParameter,
) ([]UpdateResult, bool, error) {
	rc, ok, err := b.realRoute(ctx, getRegion(ctx, b.defaultRegion), resourceARN, txID)
	if !ok || err != nil {
		return nil, ok || err != nil, err
	}

	if len(sets) == 0 {
		sets = [][]SQLParameter{nil}
	}

	results, err := rc.re.executeBatch(ctx, rc.lg, rc.txKey, stmt, sets)

	return results, true, realTxErr(err, txID)
}

// realTxErr reports a vanished transaction as TransactionNotFoundException.
func realTxErr(err error, txID string) error {
	if err != nil && txID != "" && isDeadTransactionError(err) {
		return errTxNotFound(txID)
	}

	return err
}

// beginReal opens a held transaction on the real engine; handled is false when SQLite serves the cluster.
func (b *InMemoryBackend) beginReal(ctx context.Context, resourceARN string) (string, bool, error) {
	re := b.realEngine()
	if re == nil {
		return "", false, nil
	}

	lg, ok, err := re.loginFor(ctx, resourceARN)
	if !ok || err != nil {
		return "", ok || err != nil, err
	}

	region := getRegion(ctx, b.defaultRegion)
	id := b.newTransactionRecord(region)

	if err = re.begin(lg, realTxKey(region, id)); err != nil {
		b.mu.Lock("BeginTransactionAbort")
		b.transactionsStore(region).Delete(id)
		b.mu.Unlock()

		return "", true, err
	}

	return id, true, nil
}

func (b *InMemoryBackend) newTransactionRecord(region string) string {
	b.mu.Lock("BeginTransaction")
	defer b.mu.Unlock()

	b.txCounter[region]++
	id := fmt.Sprintf("txn-%06d", b.txCounter[region])
	now := b.nowFunc()

	b.transactionsStore(region).Put(&Transaction{
		TransactionID: id, Status: transactionStatusActive, CreatedAt: now, LastActivityAt: now,
	})

	return id
}

// finalizeReal ends a real-engine transaction; handled is false when txID is not held there.
func (b *InMemoryBackend) finalizeReal(ctx context.Context, txID string, commit bool) (bool, error) {
	re := b.realEngine()
	if re == nil {
		return false, nil
	}

	region := getRegion(ctx, b.defaultRegion)
	key := realTxKey(region, txID)

	if !re.hasTx(key) {
		return false, nil
	}

	b.mu.Lock("finalizeReal")
	b.transactionsStore(region).Delete(txID)
	b.mu.Unlock()

	held, err := re.finalize(key, commit)
	if !held {
		return true, errTxNotFound(txID)
	}

	return true, err
}
