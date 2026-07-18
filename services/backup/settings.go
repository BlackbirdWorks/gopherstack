package backup

import (
	"maps"
	"time"
)

// Settings holds service-level configuration for the Backup backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command.
type Settings struct {
	JanitorInterval time.Duration `json:"janitor_interval" env:"BACKUP_JANITOR_INTERVAL" default:"1m"  help:"Janitor tick interval."`                                 //nolint:lll // Kong struct tag makes this line long
	JobTTL          time.Duration `json:"job_ttl"          env:"BACKUP_JOB_TTL"          default:"24h" help:"TTL for completed backup jobs before they are evicted."` //nolint:lll // Kong struct tag makes this line long
}

// DescribeGlobalSettings returns the account-level global backup settings.
func (b *InMemoryBackend) DescribeGlobalSettings() (map[string]string, time.Time) {
	b.mu.RLock("DescribeGlobalSettings")
	defer b.mu.RUnlock()

	cp := make(map[string]string, len(b.globalSettings))
	maps.Copy(cp, b.globalSettings)

	return cp, b.globalSettingsLastUpdate
}

// UpdateGlobalSettings updates the account-level global backup settings.
func (b *InMemoryBackend) UpdateGlobalSettings(settings map[string]string) {
	b.mu.Lock("UpdateGlobalSettings")
	defer b.mu.Unlock()

	maps.Copy(b.globalSettings, settings)
	b.globalSettingsLastUpdate = time.Now().UTC()
}

// DescribeRegionSettings returns the regional backup preferences.
func (b *InMemoryBackend) DescribeRegionSettings() *RegionSettings {
	b.mu.RLock("DescribeRegionSettings")
	defer b.mu.RUnlock()

	if b.regionSettings == nil {
		return &RegionSettings{
			ResourceTypeManagementPreference: map[string]bool{},
			ResourceTypeOptInPreference:      map[string]bool{},
		}
	}

	return b.regionSettings
}

// UpdateRegionSettings updates the regional backup preferences.
func (b *InMemoryBackend) UpdateRegionSettings(
	mgmtPref map[string]bool,
	optInPref map[string]bool,
) {
	b.mu.Lock("UpdateRegionSettings")
	defer b.mu.Unlock()

	if b.regionSettings == nil {
		b.regionSettings = &RegionSettings{
			ResourceTypeManagementPreference: make(map[string]bool),
			ResourceTypeOptInPreference:      make(map[string]bool),
		}
	}
	maps.Copy(b.regionSettings.ResourceTypeManagementPreference, mgmtPref)
	maps.Copy(b.regionSettings.ResourceTypeOptInPreference, optInPref)
}
