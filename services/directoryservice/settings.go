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
		return ErrDirectoryNotFoundDDNE
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
		return ErrDirectoryNotFoundDDNE
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
		return nil, ErrDirectoryNotFoundDDNE
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
		return "", ErrDirectoryNotFoundDDNE
	}

	dirSettings := b.dirSettingsStore(region)
	now := time.Now().UTC()
	existing := make(map[string]*storedDirectorySetting)
	for _, s := range dirSettings[directoryID] {
		existing[s.Name] = s
	}

	for _, s := range settings {
		e, ok := existing[s.Name]
		if !ok {
			e = &storedDirectorySetting{
				DirectoryID:   directoryID,
				Name:          s.Name,
				AllowedValues: s.AllowedValues,
			}
			dirSettings[directoryID] = append(dirSettings[directoryID], e)
			existing[s.Name] = e
		}

		e.RequestedValue = s.Value
		e.Status = string(settingStatusRequested)
		e.LastUpdatedDateTime = now
		e.LastRequestedTime = now
		b.applySetting(region, directoryID, s.Name)
	}

	return directoryID, nil
}

const (
	settingStatusRequested = "Requested"
	settingStatusUpdating  = "Updating"
	settingStatusUpdated   = "Updated"
)

// applySetting walks a requested setting through Updating to Updated, copying the requested value
// into AppliedValue. A newer request for the same setting supersedes the pending walk. Caller holds b.mu.
func (b *InMemoryBackend) applySetting(region, directoryID, name string) {
	find := func() *storedDirectorySetting {
		for _, e := range b.dirSettingsStoreRO(region)[directoryID] {
			if e.Name == name {
				return e
			}
		}

		return nil
	}

	b.settleLater("Setting:updating", func() {
		if e := find(); e != nil && e.Status == settingStatusRequested {
			e.Status = settingStatusUpdating
			e.LastUpdatedDateTime = time.Now().UTC()
		}

		b.settleLater("Setting:updated", func() {
			if e := find(); e != nil && e.Status == settingStatusUpdating {
				e.Status = settingStatusUpdated
				e.AppliedValue = e.RequestedValue
				e.LastUpdatedDateTime = time.Now().UTC()
			}
		})
	})
}

// settleLater runs fn under b.mu on the backend worker once statusTransitionDelay elapses.
func (b *InMemoryBackend) settleLater(name string, fn func()) {
	b.work.After(name, statusTransitionDelay, func() {
		b.mu.Lock(name)
		defer b.mu.Unlock()
		fn()
	})
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
		return nil, "", ErrDirectoryNotFoundDDNE
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

	regions := []string{region}
	for _, r := range b.dsRegionsInRegion(region) {
		if r.DirectoryID == directoryID {
			regions = append(regions, r.RegionName)
		}
	}

	result := make([]SettingEntry, 0, len(filtered))
	for _, s := range filtered {
		entry := SettingEntry{
			LastUpdatedDateTime: s.LastUpdatedDateTime,
			LastRequestedTime:   s.LastRequestedTime,
			DirectoryID:         s.DirectoryID,
			Name:                s.Name,
			AllowedValues:       s.AllowedValues,
			AppliedValue:        s.AppliedValue,
			RequestedValue:      s.RequestedValue,
			Status:              s.Status,
			RegionStatuses:      make(map[string]string, len(regions)),
		}
		for _, r := range regions {
			entry.RegionStatuses[r] = s.Status
		}
		result = append(result, entry)
	}

	return result, "", nil
}

// UpdateDirectorySetup applies an OS, network or size update and records the update activity.
func (b *InMemoryBackend) UpdateDirectorySetup(
	ctx context.Context,
	directoryID string,
	update DirectorySetupUpdate,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateDirectorySetup")
	defer b.mu.Unlock()

	dir, ok := b.directoryGet(region, directoryID)
	if !ok {
		return ErrDirectoryNotFoundDDNE
	}

	if update.CreateSnapshotBeforeUpdate {
		b.newAutoSnapshot(region, directoryID, "Directory setup update snapshot")
	}

	previous := dir.OSVersion
	applyDirectorySetupUpdate(dir, update)

	entry := &storedUpdateInfo{
		DirectoryID: directoryID,
		UpdateType:  update.UpdateType,
		Status:      "Updated",
		Region:      region,
		InitiatedBy: b.accountID,
	}

	if update.UpdateType == string(UpdateTypeOS) && update.OSVersion != "" {
		entry.PreviousValue = previous
		entry.NewValue = update.OSVersion
	}

	now := time.Now().UTC()
	entry.StartTime = now
	entry.LastUpdatedDateTime = now

	entries := b.updateInfoEntriesStore(region)
	entries[directoryID] = append(entries[directoryID], entry)

	return nil
}

func applyDirectorySetupUpdate(dir *storedDirectory, update DirectorySetupUpdate) {
	switch update.UpdateType {
	case string(UpdateTypeOS):
		if update.OSVersion != "" {
			dir.OSVersion = update.OSVersion
		}
	case string(UpdateTypeSize):
		if update.DirectorySize != "" {
			dir.Size = update.DirectorySize
		}
	case string(UpdateTypeNetwork):
		if update.NetworkType != "" {
			dir.NetworkType = update.NetworkType

			nt := NetworkType(update.NetworkType)
			if (nt == NetworkTypeDualStack || nt == NetworkTypeIPv6Only) && len(dir.DNSIPv6Addrs) == 0 {
				dir.DNSIPv6Addrs = synthesizeDNSIPv6Addrs(dir.DirectoryID)
			}
		}

		if len(update.CustomerDNSIPsV6) > 0 && dir.ConnectSettings != nil {
			dir.ConnectSettings.CustomerDNSIPsV6 = append([]string(nil), update.CustomerDNSIPsV6...)
			dir.DNSIPv6Addrs = append([]string(nil), update.CustomerDNSIPsV6...)
		}
	}
}

// DescribeUpdateDirectory returns update info entries for a directory.
func (b *InMemoryBackend) DescribeUpdateDirectory(
	ctx context.Context,
	directoryID, updateType, regionName, nextToken string, //nolint:revive // existing issue.
) ([]UpdateInfoEntry, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeUpdateDirectory")
	defer b.mu.RUnlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return nil, "", ErrDirectoryNotFoundDDNE
	}

	var result []UpdateInfoEntry
	for _, u := range b.updateInfoEntriesStoreRO(region)[directoryID] {
		if updateType != "" && u.UpdateType != updateType {
			continue
		}
		if regionName != "" && u.Region != regionName {
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
