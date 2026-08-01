package grafana

import "time"

// CreateWorkspaceServiceAccountToken creates a token for a workspace service
// account, returning the plaintext key exactly once (matching
// ServiceAccountTokenSummaryWithKey's doc comment: "it is not retrievable at
// a later time").
func (b *InMemoryBackend) CreateWorkspaceServiceAccountToken(
	workspaceID, serviceAccountID, name string, secondsToLive int32,
) (*ServiceAccountToken, error) {
	if secondsToLive <= 0 || secondsToLive > maxKeyTTLSeconds {
		return nil, validationError("secondsToLive must be between 1 and 2592000 (30 days)")
	}

	b.mu.Lock("CreateWorkspaceServiceAccountToken")
	defer b.mu.Unlock()

	saKey := serviceAccountKeyFn(&ServiceAccount{WorkspaceID: workspaceID, ID: serviceAccountID})
	if !b.serviceAccounts.Has(saKey) {
		return nil, notFoundError("serviceAccount", serviceAccountID)
	}

	for _, t := range b.tokensByServiceAccount.Get(workspaceID + "::" + serviceAccountID) {
		if t.Name == name {
			return nil, conflictError("serviceAccountToken", name,
				"a token named "+name+" already exists for this service account")
		}
	}

	now := time.Now().UTC()
	tok := &ServiceAccountToken{
		WorkspaceID:      workspaceID,
		ServiceAccountID: serviceAccountID,
		ID:               b.nextTokenIDLocked(),
		Name:             name,
		Key:              "gst-" + randomHexID() + randomHexID(),
		CreatedAt:        now,
		ExpiresAt:        now.Add(time.Duration(secondsToLive) * time.Second),
	}

	b.tokens.Put(tok)

	cp := *tok

	return &cp, nil
}

// DeleteWorkspaceServiceAccountToken deletes a token for a workspace service
// account.
func (b *InMemoryBackend) DeleteWorkspaceServiceAccountToken(workspaceID, serviceAccountID, tokenID string) error {
	b.mu.Lock("DeleteWorkspaceServiceAccountToken")
	defer b.mu.Unlock()

	key := tokenKeyFn(&ServiceAccountToken{WorkspaceID: workspaceID, ServiceAccountID: serviceAccountID, ID: tokenID})
	if !b.tokens.Delete(key) {
		return notFoundError("serviceAccountToken", tokenID)
	}

	return nil
}

// ListWorkspaceServiceAccountTokens returns every token for a workspace
// service account (never including the plaintext key -- see
// types.ServiceAccountTokenSummary's doc comment on this file's models.go).
func (b *InMemoryBackend) ListWorkspaceServiceAccountTokens(
	workspaceID, serviceAccountID string,
) ([]*ServiceAccountToken, error) {
	b.mu.RLock("ListWorkspaceServiceAccountTokens")
	defer b.mu.RUnlock()

	saKey := serviceAccountKeyFn(&ServiceAccount{WorkspaceID: workspaceID, ID: serviceAccountID})
	if !b.serviceAccounts.Has(saKey) {
		return nil, notFoundError("serviceAccount", serviceAccountID)
	}

	items := b.tokensByServiceAccount.Get(workspaceID + "::" + serviceAccountID)
	out := make([]*ServiceAccountToken, len(items))

	for i, t := range items {
		cp := *t
		out[i] = &cp
	}

	return out, nil
}
