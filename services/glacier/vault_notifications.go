package glacier

import "fmt"

// isValidNotificationEvent reports whether ev is an allowed vault notification event.
func isValidNotificationEvent(ev string) bool {
	return ev == "ArchiveRetrievalCompleted" || ev == "InventoryRetrievalCompleted"
}

// SetVaultNotifications sets the notification configuration for a vault.
func (b *InMemoryBackend) SetVaultNotifications(accountID, region, vaultName, snsTopic string, events []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return ErrVaultNotFound
	}

	for _, ev := range events {
		if !isValidNotificationEvent(ev) {
			return fmt.Errorf(
				"%w: event %q must be ArchiveRetrievalCompleted or InventoryRetrievalCompleted",
				ErrValidation, ev,
			)
		}
	}

	v.NotificationSNSTopic = snsTopic
	v.NotificationEvents = append([]string(nil), events...)

	return nil
}

// GetVaultNotifications returns the notification configuration for a vault.
func (b *InMemoryBackend) GetVaultNotifications(accountID, region, vaultName string) (string, []string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return "", nil, ErrVaultNotFound
	}

	return v.NotificationSNSTopic, v.NotificationEvents, nil
}

// DeleteVaultNotifications deletes the notification configuration for a vault.
func (b *InMemoryBackend) DeleteVaultNotifications(accountID, region, vaultName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return ErrVaultNotFound
	}

	v.NotificationSNSTopic = ""
	v.NotificationEvents = nil

	return nil
}
