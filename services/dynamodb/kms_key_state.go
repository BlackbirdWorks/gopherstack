package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// KMSKeyStateChecker reports whether a customer-managed KMS key can no longer be used
// (disabled or pending deletion). Unknown keys are reported as usable.
type KMSKeyStateChecker interface {
	KMSKeyInaccessible(ctx context.Context, keyARN string) bool
}

// SetKMSKeyStateChecker wires the KMS key-state lookup that drives
// INACCESSIBLE_ENCRYPTION_CREDENTIALS and SSEDescription.InaccessibleEncryptionDateTime.
func (db *InMemoryDB) SetKMSKeyStateChecker(c KMSKeyStateChecker) {
	db.mu.Lock("SetKMSKeyStateChecker")
	defer db.mu.Unlock()

	db.kmsKeys = c
}

// applyKMSKeyState marks td inaccessible while its SSE key is disabled or pending
// deletion, stamping the time the condition was first observed and clearing it once
// the key is usable again.
func (db *InMemoryDB) applyKMSKeyState(ctx context.Context, td *types.TableDescription) {
	if td.SSEDescription == nil || td.SSEDescription.KMSMasterKeyArn == nil || td.TableArn == nil {
		return
	}

	db.mu.RLock("applyKMSKeyState")
	checker := db.kmsKeys
	db.mu.RUnlock()

	if checker == nil {
		return
	}

	tableARN := aws.ToString(td.TableArn)
	inaccessible := checker.KMSKeyInaccessible(ctx, *td.SSEDescription.KMSMasterKeyArn)

	db.mu.Lock("applyKMSKeyState")
	defer db.mu.Unlock()

	if !inaccessible {
		delete(db.keyInaccessibleSince, tableARN)

		return
	}

	since, ok := db.keyInaccessibleSince[tableARN]
	if !ok {
		since = time.Now().UTC()
		db.keyInaccessibleSince[tableARN] = since
	}

	td.TableStatus = types.TableStatusInaccessibleEncryptionCredentials
	td.SSEDescription.InaccessibleEncryptionDateTime = &since
}
