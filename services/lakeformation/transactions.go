package lakeformation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	transactionStatusActive    = "ACTIVE"
	transactionStatusCommitted = "COMMITTED"
	transactionStatusAborted   = "ABORTED"

	transactionTypeReadOnly  = "READ_ONLY"
	transactionTypeReadWrite = "READ_AND_WRITE"
)

// transactionInfo holds transaction metadata.
type transactionInfo struct {
	// ID is the transaction ID this value is keyed by in the transactions
	// Table (see store_setup.go). It is tagged json:"-" because the
	// transactions Table is a "dirty" table -- persistence.go instead
	// round-trips it through a dedicated transactionSnapshot DTO that carries
	// the ID as a real JSON field, so it survives the round trip despite
	// being excluded here. It must never change after the transaction is
	// created (store.Table's keyFn purity requirement).
	ID           string `json:"-"`
	Status       string `json:"Status"`
	Type         string `json:"Type,omitempty"`
	StartTime    string `json:"StartTime,omitempty"`
	EndTime      string `json:"EndTime,omitempty"`
	LastExtended string `json:"LastExtended,omitempty"`
}

// CancelTransaction cancels an in-flight transaction.
// Returns an error if the transaction is already committed.
func (b *InMemoryBackend) CancelTransaction(transactionID string) error {
	b.mu.Lock("CancelTransaction")
	defer b.mu.Unlock()

	if info, ok := b.transactions.Get(transactionID); ok && info.Status == transactionStatusCommitted {
		return awserr.New(
			fmt.Sprintf("transaction %s is already committed", transactionID),
			errTransactionCommitted,
		)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if info, ok := b.transactions.Get(transactionID); ok {
		info.Status = transactionStatusAborted
		info.EndTime = now
	} else {
		b.transactions.Put(&transactionInfo{
			ID:        transactionID,
			Status:    transactionStatusAborted,
			StartTime: now,
			EndTime:   now,
		})
	}

	return nil
}

// CommitTransaction commits an in-flight transaction.
// Returns an error if the transaction is already aborted.
func (b *InMemoryBackend) CommitTransaction(transactionID string) (string, error) {
	b.mu.Lock("CommitTransaction")
	defer b.mu.Unlock()

	if info, ok := b.transactions.Get(transactionID); ok && info.Status == transactionStatusAborted {
		return "", awserr.New(
			fmt.Sprintf("transaction %s has been cancelled", transactionID),
			awserr.ErrConflict,
		)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if info, ok := b.transactions.Get(transactionID); ok {
		info.Status = transactionStatusCommitted
		info.EndTime = now
	} else {
		b.transactions.Put(&transactionInfo{
			ID:        transactionID,
			Status:    transactionStatusCommitted,
			StartTime: now,
			EndTime:   now,
		})
	}

	return transactionStatusCommitted, nil
}

// transactionIDBytesLen is the number of random bytes used for transaction IDs.
const transactionIDBytesLen = 16

// newTransactionID generates a random hex transaction ID.
func newTransactionID() string {
	b := make([]byte, transactionIDBytesLen)

	if _, err := rand.Read(b); err != nil {
		// Fallback: use time-based ID (practically unreachable).
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	return hex.EncodeToString(b)
}

// StartTransaction begins a new in-flight transaction and returns its ID.
func (b *InMemoryBackend) StartTransaction(transactionType string) string {
	id := newTransactionID()

	if transactionType == "" {
		transactionType = transactionTypeReadWrite
	}

	b.mu.Lock("StartTransaction")
	defer b.mu.Unlock()

	b.transactions.Put(&transactionInfo{
		ID:        id,
		Status:    transactionStatusActive,
		Type:      transactionType,
		StartTime: time.Now().UTC().Format(time.RFC3339),
	})

	return id
}

// StartJanitor starts a background goroutine to clean up stale transactions.
const janitorInterval = 5 * time.Minute
const janitorTimeout = time.Hour

// StartJanitor starts a background goroutine to clean up stale transactions.
func (b *InMemoryBackend) StartJanitor(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		l := logger.Load(ctx).With("worker", "lakeformation-janitor")
		ticker := time.NewTicker(janitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.cleanupStaleTransactions(ctx, l)
			}
		}
	}()
}

func (b *InMemoryBackend) cleanupStaleTransactions(ctx context.Context, l *slog.Logger) {
	b.mu.Lock("JanitorCleanup")
	now := time.Now()
	staleCount := 0
	for _, info := range b.transactions.All() {
		refTimeStr := info.LastExtended
		if refTimeStr == "" {
			refTimeStr = info.StartTime
		}
		t, err := time.Parse(time.RFC3339, refTimeStr)
		if err == nil && now.Sub(t) > janitorTimeout {
			b.transactions.Delete(info.ID)
			staleCount++
		}
	}
	b.mu.Unlock()
	if staleCount > 0 {
		l.InfoContext(ctx, "cleaned up stale lakeformation transactions", "count", staleCount)
	}
}

// DescribeTransaction returns the status of a specific transaction.
func (b *InMemoryBackend) DescribeTransaction(transactionID string) (*Transaction, error) {
	if strings.TrimSpace(transactionID) == "" {
		return nil, fmt.Errorf("TransactionId is required: %w", ErrValidation)
	}

	b.mu.RLock("DescribeTransaction")
	defer b.mu.RUnlock()

	info, ok := b.transactions.Get(transactionID)
	if !ok {
		return nil, awserr.New(
			"transaction not found: "+transactionID,
			awserr.ErrNotFound,
		)
	}

	return &Transaction{
		TransactionID:        transactionID,
		TransactionStatus:    info.Status,
		TransactionStartTime: info.StartTime,
		TransactionEndTime:   info.EndTime,
	}, nil
}

// ListTransactions returns a paginated list of transactions, optionally filtered by status.
func (b *InMemoryBackend) ListTransactions(
	statusFilter string, maxResults int, nextToken string,
) ([]*Transaction, string) {
	b.mu.RLock("ListTransactions")
	defer b.mu.RUnlock()

	all := make([]*Transaction, 0, b.transactions.Len())

	for _, info := range b.transactions.All() {
		if statusFilter != "" && statusFilter != "ALL" {
			if statusFilter == "COMPLETED" {
				// COMPLETED means both COMMITTED and ABORTED.
				if info.Status != transactionStatusCommitted && info.Status != transactionStatusAborted {
					continue
				}
			} else if info.Status != statusFilter {
				continue
			}
		}

		all = append(all, &Transaction{
			TransactionID:        info.ID,
			TransactionStatus:    info.Status,
			TransactionStartTime: info.StartTime,
			TransactionEndTime:   info.EndTime,
		})
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].TransactionID < all[j].TransactionID
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// ExtendTransaction validates that a transaction is active and records the extension.
func (b *InMemoryBackend) ExtendTransaction(transactionID string) error {
	if strings.TrimSpace(transactionID) == "" {
		return fmt.Errorf("TransactionId is required: %w", ErrValidation)
	}
	b.mu.Lock("ExtendTransaction")
	defer b.mu.Unlock()
	info, ok := b.transactions.Get(transactionID)
	if !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}
	if info.Status == transactionStatusCommitted {
		return awserr.New(fmt.Sprintf("transaction %s is already committed", transactionID), errTransactionCommitted)
	}
	if info.Status != transactionStatusActive {
		return awserr.New(fmt.Sprintf("transaction %s is not active", transactionID), awserr.ErrConflict)
	}
	info.LastExtended = time.Now().UTC().Format(time.RFC3339)

	return nil
}

// DeleteObjectsOnCancel removes governed table objects written during a cancelled transaction.
// AWS requires the transaction to be in ABORTED state before objects can be deleted.
func (b *InMemoryBackend) DeleteObjectsOnCancel(transactionID string) error {
	if strings.TrimSpace(transactionID) == "" {
		return fmt.Errorf("TransactionId is required: %w", ErrValidation)
	}
	b.mu.RLock("DeleteObjectsOnCancel")
	defer b.mu.RUnlock()
	info, ok := b.transactions.Get(transactionID)
	if !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}
	if info.Status == transactionStatusCommitted {
		return awserr.New(fmt.Sprintf("transaction %s is already committed", transactionID), errTransactionCommitted)
	}
	if info.Status != transactionStatusAborted {
		return awserr.New(
			fmt.Sprintf("transaction %s must be in ABORTED state (current: %s)", transactionID, info.Status),
			awserr.ErrConflict,
		)
	}

	return nil
}
