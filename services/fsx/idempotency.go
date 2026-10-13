package fsx

import (
	"encoding/json"
	"fmt"
	"time"
)

const clientRequestTokenField = "ClientRequestToken"

// tokenFingerprint canonicalises input minus its ClientRequestToken so a retry
// with the same parameters hashes equal and a changed one does not.
func tokenFingerprint(input any) (string, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("canonicalize request: %w", err)
	}

	var fields map[string]any
	if err = json.Unmarshal(raw, &fields); err != nil {
		return "", fmt.Errorf("canonicalize request: %w", err)
	}

	delete(fields, clientRequestTokenField)

	out, err := json.Marshal(fields)
	if err != nil {
		return "", fmt.Errorf("canonicalize request: %w", err)
	}

	return string(out), nil
}

// idempotencyKey scopes a token to its operation: tokens are per-API in FSx.
func idempotencyKey(op, token string) string {
	return op + "/" + token
}

// replayTokenLocked reports the resource ID recorded for a previously seen
// ClientRequestToken. A token reused with different parameters yields
// IncompatibleParameterError. Caller must hold b.mu.
func (b *InMemoryBackend) replayTokenLocked(op, token string, input any) (string, string, error) {
	if token == "" {
		return "", "", nil
	}

	fp, err := tokenFingerprint(input)
	if err != nil {
		return "", "", fmt.Errorf("%w: %w", ErrValidation, err)
	}

	entry, ok := b.createFileSystemTokens[idempotencyKey(op, token)]
	if !ok || time.Since(entry.CreatedAt) >= createFileSystemTokenTTL {
		return fp, "", nil
	}

	if entry.Fingerprint != fp {
		return fp, "", fmt.Errorf(
			"%w: ClientRequestToken %q was already used by a %s request with different parameters",
			ErrIncompatibleParameter, token, op,
		)
	}

	return fp, entry.FileSystemID, nil
}

// recordTokenLocked remembers token -> resourceID for replay. Caller must hold b.mu.
func (b *InMemoryBackend) recordTokenLocked(op, token, fingerprint, resourceID string) {
	if token == "" {
		return
	}

	now := time.Now().UTC()
	b.sweepCreateFileSystemTokensLocked(now)

	b.createFileSystemTokens[idempotencyKey(op, token)] = fsCreateTokenEntry{
		Fingerprint:  fingerprint,
		FileSystemID: resourceID,
		CreatedAt:    now,
	}
}
