package backup_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestListRecoveryPointsByLegalHold_NoSelection covers the AWS semantics
// that a legal hold created with no RecoveryPointSelection (or an all-empty
// one) covers every recovery point -- there is no additional constraint.
func TestListRecoveryPointsByLegalHold_NoSelection(t *testing.T) {
	t.Parallel()

	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	mustVault(t, b, "v1")
	mustRP(t, b, "v1", "arn:aws:backup:::rp/1", "arn:aws:ec2:::instance/i-1", "EC2")
	mustRP(t, b, "v1", "arn:aws:backup:::rp/2", "arn:aws:ec2:::instance/i-2", "EC2")

	lh, err := b.CreateLegalHold("blanket hold", "no selection", nil)
	require.NoError(t, err)

	rps := b.ListRecoveryPointsByLegalHold(lh.LegalHoldID)
	assert.Len(t, rps, 2)
}

// TestListRecoveryPointsByLegalHold_VaultNames covers filtering a legal
// hold's coverage down to specific backup vaults.
func TestListRecoveryPointsByLegalHold_VaultNames(t *testing.T) {
	t.Parallel()

	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	mustVault(t, b, "vault-a")
	mustVault(t, b, "vault-b")
	mustRP(t, b, "vault-a", "arn:aws:backup:::rp/a1", "arn:aws:ec2:::instance/i-a1", "EC2")
	mustRP(t, b, "vault-b", "arn:aws:backup:::rp/b1", "arn:aws:ec2:::instance/i-b1", "EC2")

	lh, err := b.CreateLegalHold("vault-scoped hold", "desc", &backup.RecoveryPointSelection{
		VaultNames: []string{"vault-a"},
	})
	require.NoError(t, err)

	rps := b.ListRecoveryPointsByLegalHold(lh.LegalHoldID)
	require.Len(t, rps, 1)
	assert.Equal(t, "vault-a", rps[0].BackupVaultName)
}

// TestListRecoveryPointsByLegalHold_ResourceIdentifiers covers filtering by
// specific resource ARNs.
func TestListRecoveryPointsByLegalHold_ResourceIdentifiers(t *testing.T) {
	t.Parallel()

	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	mustVault(t, b, "v1")
	mustRP(t, b, "v1", "arn:aws:backup:::rp/1", "arn:aws:ec2:::instance/i-1", "EC2")
	mustRP(t, b, "v1", "arn:aws:backup:::rp/2", "arn:aws:rds:::db/i-2", "RDS")

	lh, err := b.CreateLegalHold("resource-scoped hold", "desc", &backup.RecoveryPointSelection{
		ResourceIdentifiers: []string{"arn:aws:rds:::db/i-2"},
	})
	require.NoError(t, err)

	rps := b.ListRecoveryPointsByLegalHold(lh.LegalHoldID)
	require.Len(t, rps, 1)
	assert.Equal(t, "arn:aws:rds:::db/i-2", rps[0].ResourceArn)
}

// TestListRecoveryPointsByLegalHold_DateRange covers filtering by an
// inclusive CreationDate window.
func TestListRecoveryPointsByLegalHold_DateRange(t *testing.T) {
	t.Parallel()

	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	mustVault(t, b, "v1")
	mustRP(t, b, "v1", "arn:aws:backup:::rp/1", "arn:aws:ec2:::instance/i-1", "EC2")

	// mustRP stamps CreationDate as time.Now().UTC(); use a window that
	// excludes it to prove the filter actually narrows results.
	future := time.Now().UTC().Add(24 * time.Hour)
	farFuture := future.Add(24 * time.Hour)
	lh, err := b.CreateLegalHold("future-only hold", "desc", &backup.RecoveryPointSelection{
		DateRange: &backup.DateRange{FromDate: &future, ToDate: &farFuture},
	})
	require.NoError(t, err)

	rps := b.ListRecoveryPointsByLegalHold(lh.LegalHoldID)
	assert.Empty(t, rps)
}

// TestListRecoveryPointsByLegalHold_UnknownID covers AWS's real behavior:
// ListRecoveryPointsByLegalHold's error list has no ResourceNotFoundException,
// so an unknown legal hold ID is not an error -- it simply matches nothing.
func TestListRecoveryPointsByLegalHold_UnknownID(t *testing.T) {
	t.Parallel()

	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	mustVault(t, b, "v1")
	mustRP(t, b, "v1", "arn:aws:backup:::rp/1", "arn:aws:ec2:::instance/i-1", "EC2")

	rps := b.ListRecoveryPointsByLegalHold("nonexistent-hold-id")
	assert.Empty(t, rps)
}
