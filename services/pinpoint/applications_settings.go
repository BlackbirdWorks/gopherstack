package pinpoint

// storedAppSettings holds the persisted application-level settings.
type storedAppSettings struct {
	CampaignHook        map[string]any `json:"CampaignHook"`
	Limits              map[string]any `json:"Limits"`
	QuietTime           map[string]any `json:"QuietTime"`
	LastModifiedDate    string         `json:"LastModifiedDate"`
	CloudWatchMetrics   bool           `json:"CloudWatchMetrics"`
	EventTaggingEnabled bool           `json:"EventTaggingEnabled"`
}

// GetApplicationDateRangeKpi returns stub KPI data for an application.
func (b *InMemoryBackend) GetApplicationDateRangeKpi(appID, kpiName string) (*kpiResult, error) {
	b.mu.RLock("GetApplicationDateRangeKpi")
	defer b.mu.RUnlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	return &kpiResult{
		ApplicationID: appID,
		KpiName:       kpiName,
		KpiResult:     kpiRows{Rows: []kpiRow{}},
	}, nil
}

// GetApplicationSettings retrieves the settings for a Pinpoint application.
func (b *InMemoryBackend) GetApplicationSettings(appID string) (*storedAppSettings, error) {
	b.mu.RLock("GetApplicationSettings")
	defer b.mu.RUnlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	settings, ok := b.appSettings[appID]
	if !ok {
		// Return defaults when no settings have been stored yet.
		return &storedAppSettings{
			CampaignHook: map[string]any{},
			Limits:       map[string]any{},
			QuietTime:    map[string]any{},
		}, nil
	}

	cp := *settings
	cp.CampaignHook = cloneAnyMap(settings.CampaignHook)
	cp.Limits = cloneAnyMap(settings.Limits)
	cp.QuietTime = cloneAnyMap(settings.QuietTime)

	return &cp, nil
}

// UpdateApplicationSettings updates the settings for a Pinpoint application.
func (b *InMemoryBackend) UpdateApplicationSettings(
	appID string,
	settings *storedAppSettings,
) (*storedAppSettings, error) {
	b.mu.Lock("UpdateApplicationSettings")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	stored := &storedAppSettings{
		CampaignHook:        cloneAnyMap(settings.CampaignHook),
		Limits:              cloneAnyMap(settings.Limits),
		QuietTime:           cloneAnyMap(settings.QuietTime),
		CloudWatchMetrics:   settings.CloudWatchMetrics,
		EventTaggingEnabled: settings.EventTaggingEnabled,
		LastModifiedDate:    nowRFC3339(),
	}

	b.appSettings[appID] = stored

	cp := *stored
	cp.CampaignHook = cloneAnyMap(stored.CampaignHook)
	cp.Limits = cloneAnyMap(stored.Limits)
	cp.QuietTime = cloneAnyMap(stored.QuietTime)

	return &cp, nil
}
