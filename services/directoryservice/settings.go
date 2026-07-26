package directoryservice

import (
	"context"
	"sort"
	"time"
)

// EnableDirectoryDataAccess enables directory data access.
func (b *InMemoryBackend) EnableDirectoryDataAccess(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableDirectoryDataAccess")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	b.dirDataAccessStore(region)[directoryID] = true

	return nil
}

// DisableDirectoryDataAccess disables directory data access.
func (b *InMemoryBackend) DisableDirectoryDataAccess(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisableDirectoryDataAccess")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	b.dirDataAccessStore(region)[directoryID] = false

	return nil
}

// DescribeDirectoryDataAccess returns data access status for a directory.
func (b *InMemoryBackend) DescribeDirectoryDataAccess(
	ctx context.Context,
	directoryID string,
) (*DirectoryDataAccessStatus, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeDirectoryDataAccess")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, ErrDirectoryNotFound
	}

	enabled := b.dirDataAccessStoreRO(region)[directoryID]

	return &DirectoryDataAccessStatus{DirectoryID: directoryID, Enabled: enabled}, nil
}

// UpdateSettings updates directory settings.
func (b *InMemoryBackend) UpdateSettings(
	ctx context.Context,
	directoryID string,
	settings []DirectorySetting,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateSettings")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return "", ErrDirectoryNotFound
	}

	dirSettings := b.dirSettingsStore(region)
	now := time.Now().UTC()
	existing := make(map[string]*storedDirectorySetting)
	for _, s := range dirSettings[directoryID] {
		existing[s.Name] = s
	}

	for _, s := range settings {
		if e, ok := existing[s.Name]; ok {
			e.RequestedValue = s.Value
			e.Status = "Requested"
			e.LastUpdatedDateTime = now
		} else {
			ns := &storedDirectorySetting{
				DirectoryID:         directoryID,
				Name:                s.Name,
				AllowedValues:       s.AllowedValues,
				RequestedValue:      s.Value,
				AppliedValue:        s.Value,
				Status:              "Updated", //nolint:goconst // existing issue.
				LastUpdatedDateTime: now,
			}
			dirSettings[directoryID] = append(dirSettings[directoryID], ns)
		}
	}

	return directoryID, nil
}

// DescribeSettings returns directory settings.
func (b *InMemoryBackend) DescribeSettings(
	ctx context.Context,
	directoryID, status, nextToken string, //nolint:revive // existing issue.
) ([]SettingEntry, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeSettings")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFound
	}

	settings := b.dirSettingsStoreRO(region)[directoryID]
	var filtered []storedDirectorySetting
	for _, s := range settings {
		if status != "" && s.Status != status {
			continue
		}
		filtered = append(filtered, *s)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Name < filtered[j].Name })

	result := make([]SettingEntry, 0, len(filtered))
	for _, s := range filtered {
		result = append(result, SettingEntry(s))
	}

	return result, "", nil
}

// UpdateDirectorySetup initiates a directory setup update.
func (b *InMemoryBackend) UpdateDirectorySetup(ctx context.Context, directoryID, updateType string, _ bool) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateDirectorySetup")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	now := time.Now().UTC()
	entries := b.updateInfoEntriesStore(region)
	entries[directoryID] = append(entries[directoryID], &storedUpdateInfo{
		DirectoryID:         directoryID,
		UpdateType:          updateType,
		Status:              "Updated",
		StartTime:           now,
		LastUpdatedDateTime: now,
		Region:              region,
		InitiatedBy:         b.accountID,
	})

	return nil
}

// DescribeUpdateDirectory returns update info entries for a directory.
func (b *InMemoryBackend) DescribeUpdateDirectory(
	ctx context.Context,
	directoryID, updateType, nextToken string, //nolint:revive // existing issue.
) ([]UpdateInfoEntry, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeUpdateDirectory")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFound
	}

	var result []UpdateInfoEntry
	for _, u := range b.updateInfoEntriesStoreRO(region)[directoryID] {
		if updateType != "" && u.UpdateType != updateType {
			continue
		}
		result = append(result, UpdateInfoEntry{
			DirectoryID:         u.DirectoryID,
			UpdateType:          u.UpdateType,
			Status:              u.Status,
			NewValue:            u.NewValue,
			PreviousValue:       u.PreviousValue,
			InitiatedBy:         u.InitiatedBy,
			Region:              u.Region,
			StartTime:           u.StartTime,
			LastUpdatedDateTime: u.LastUpdatedDateTime,
		})
	}

	return result, "", nil
}
