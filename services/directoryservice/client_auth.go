package directoryservice

import (
	"context"
	"sort"
	"time"
)

// EnableClientAuthentication enables client authentication.
func (b *InMemoryBackend) EnableClientAuthentication(ctx context.Context, directoryID, authType string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableClientAuthentication")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFoundDDNE
	}

	now := time.Now().UTC()
	if existing, ok := b.clientAuthSettingGet(region, directoryID, authType); ok {
		existing.Status = "Enabled" //nolint:goconst // existing issue.
		existing.LastUpdatedDateTime = now
	} else {
		b.clientAuthSettingPut(&storedClientAuthSetting{
			region:              region,
			DirectoryID:         directoryID,
			AuthType:            authType,
			Status:              "Enabled",
			LastUpdatedDateTime: now,
		})
	}

	return nil
}

// DisableClientAuthentication disables client authentication.
func (b *InMemoryBackend) DisableClientAuthentication(ctx context.Context, directoryID, authType string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisableClientAuthentication")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFoundDDNE
	}

	now := time.Now().UTC()
	if existing, ok := b.clientAuthSettingGet(region, directoryID, authType); ok {
		existing.Status = "Disabled" //nolint:goconst // existing issue.
		existing.LastUpdatedDateTime = now
	} else {
		b.clientAuthSettingPut(&storedClientAuthSetting{
			region:              region,
			DirectoryID:         directoryID,
			AuthType:            authType,
			Status:              "Disabled",
			LastUpdatedDateTime: now,
		})
	}

	return nil
}

// DescribeClientAuthenticationSettings returns client auth settings.
func (b *InMemoryBackend) DescribeClientAuthenticationSettings(
	ctx context.Context,
	directoryID, authType string,
	limit int32, //nolint:revive // existing issue.
	nextToken string, //nolint:revive // existing issue.
) ([]ClientAuthInfo, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeClientAuthenticationSettings")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFoundDDNE
	}

	var result []ClientAuthInfo
	for _, s := range b.clientAuthSettingsInRegion(region) {
		if s.DirectoryID != directoryID {
			continue
		}
		if authType != "" && s.AuthType != authType {
			continue
		}
		result = append(result, ClientAuthInfo{
			DirectoryID:         s.DirectoryID,
			AuthType:            s.AuthType,
			Status:              s.Status,
			LastUpdatedDateTime: s.LastUpdatedDateTime,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AuthType < result[j].AuthType })

	return result, "", nil
}
