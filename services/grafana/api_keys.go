package grafana

import "time"

// maxKeyTTLSeconds is the maximum SecondsToLive for both legacy API keys and
// service account tokens ("Keys can be valid for up to 30 days" /
// "You can set the time up to 30 days in the future").
const maxKeyTTLSeconds = 30 * 24 * 60 * 60

//nolint:gochecknoglobals // static enum table, never mutated after init
var validAPIKeyRoles = map[string]bool{RoleAdmin: true, RoleEditor: true, RoleViewer: true}

// CreateWorkspaceAPIKey creates a Grafana API key for a workspace
// (implements the CreateWorkspaceApiKey SDK operation).
func (b *InMemoryBackend) CreateWorkspaceAPIKey(
	workspaceID, keyName, keyRole string, secondsToLive int32,
) (*APIKey, error) {
	if !validAPIKeyRoles[keyRole] {
		return nil, validationError("invalid keyRole: " + keyRole)
	}

	if secondsToLive <= 0 || secondsToLive > maxKeyTTLSeconds {
		return nil, validationError("secondsToLive must be between 1 and 2592000 (30 days)")
	}

	b.mu.Lock("CreateWorkspaceApiKey")
	defer b.mu.Unlock()

	if _, ok := b.workspaces.Get(workspaceID); !ok {
		return nil, notFoundError(resourceTypeWorkspace, workspaceID)
	}

	key := &APIKey{
		WorkspaceID: workspaceID,
		KeyName:     keyName,
		KeyRole:     keyRole,
		Key:         "gk-" + randomHexID() + randomHexID(),
		CreatedAt:   time.Now().UTC(),
		ExpiresAt:   time.Now().UTC().Add(time.Duration(secondsToLive) * time.Second),
	}

	if _, exists := b.apiKeys.Get(apiKeyKeyFn(key)); exists {
		return nil, conflictError("apiKey", keyName, "an API key named "+keyName+" already exists in this workspace")
	}

	b.apiKeys.Put(key)

	cp := *key

	return &cp, nil
}

// DeleteWorkspaceAPIKey deletes a Grafana API key from a workspace
// (implements the DeleteWorkspaceApiKey SDK operation).
func (b *InMemoryBackend) DeleteWorkspaceAPIKey(workspaceID, keyName string) error {
	b.mu.Lock("DeleteWorkspaceApiKey")
	defer b.mu.Unlock()

	key := &APIKey{WorkspaceID: workspaceID, KeyName: keyName}
	if !b.apiKeys.Delete(apiKeyKeyFn(key)) {
		return notFoundError("apiKey", keyName)
	}

	return nil
}
