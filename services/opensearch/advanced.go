package opensearch

import (
	"fmt"
	"strings"
	"time"
)

// UpgradeHistory records a single domain upgrade event.
type UpgradeHistory struct {
	UpgradeName    string            `json:"UpgradeName"`
	UpgradeStatus  string            `json:"UpgradeStatus"`
	StepsList      []UpgradeStepItem `json:"StepsList"`
	StartTimestamp float64           `json:"StartTimestamp"`
}

// UpgradeStepItem describes one step in an upgrade.
type UpgradeStepItem struct {
	UpgradeStep       string   `json:"UpgradeStep"`
	UpgradeStepStatus string   `json:"UpgradeStepStatus"`
	Issues            []string `json:"Issues,omitempty"`
	ProgressPercent   float64  `json:"ProgressPercent"`
}

// AutoTuneConfig stores auto-tune configuration for a domain.
type AutoTuneConfig struct {
	DesiredState         string                        `json:"DesiredState"`
	DomainName           string                        `json:"-"`
	MaintenanceSchedules []AutoTuneMaintenanceSchedule `json:"MaintenanceSchedules,omitempty"`
}

// AutoTuneMaintenanceSchedule represents a maintenance window.
type AutoTuneMaintenanceSchedule struct {
	CronExpression string           `json:"CronExpressionForRecurrence,omitempty"`
	Duration       AutoTuneDuration `json:"Duration"`
	StartAt        float64          `json:"StartAt,omitempty"`
}

// AutoTuneDuration represents a duration for a maintenance window.
type AutoTuneDuration struct {
	Unit  string `json:"Unit"`
	Value int    `json:"Value"`
}

// AutoTune is the response shape for one auto-tune item.
type AutoTune struct {
	AutoTuneType    string          `json:"AutoTuneType"`
	AutoTuneDetails AutoTuneDetails `json:"AutoTuneDetails"`
}

// AutoTuneDetails holds the state details of an auto-tune entry.
type AutoTuneDetails struct {
	ScheduledAutoTuneDetails ScheduledAutoTuneDetails `json:"ScheduledAutoTuneDetails"`
}

// ScheduledAutoTuneDetails holds action details for a scheduled auto-tune.
type ScheduledAutoTuneDetails struct {
	ActionType string  `json:"ActionType,omitempty"`
	Action     string  `json:"Action,omitempty"`
	Severity   string  `json:"Severity,omitempty"`
	Date       float64 `json:"Date,omitempty"`
}

// InstanceTypeLimits holds limits for a given OpenSearch instance type.
type InstanceTypeLimits struct {
	InstanceType     string            `json:"InstanceType"`
	InstanceLimits   map[string]any    `json:"InstanceLimits"`
	StorageTypes     []StorageType     `json:"StorageTypes,omitempty"`
	AdditionalLimits []AdditionalLimit `json:"AdditionalLimits,omitempty"`
}

// StorageType describes a storage configuration available for an instance type.
type StorageType struct {
	StorageTypeName    string             `json:"StorageTypeName"`
	StorageSubTypeName string             `json:"StorageSubTypeName"`
	StorageTypeLimits  []StorageTypeLimit `json:"StorageTypeLimits"`
}

// StorageTypeLimit holds a named limit value.
type StorageTypeLimit struct {
	LimitName   string   `json:"LimitName"`
	LimitValues []string `json:"LimitValues"`
}

// AdditionalLimit is an additional named limit for an instance type.
type AdditionalLimit struct {
	LimitName   string   `json:"LimitName"`
	LimitValues []string `json:"LimitValues"`
}

const (
	upgradeStatusSucceeded  = "SUCCEEDED"
	upgradeStepUpgrade      = "UPGRADE"
	upgradeStepPreCheck     = "PRE_UPGRADE_CHECK"
	upgradeStepSnapshot     = "SNAPSHOT"
	instanceCountLimitsKey  = "InstanceCountLimits"
	minInstanceCountKey     = "MinimumInstanceCount"
	maxInstanceCountKey     = "MaximumInstanceCount"
	storageTypeEBSOnly      = "ebsOnly"
	storageLimitMinVolume   = "MinimumVolumeSize"
	storageLimitMaxVolume   = "MaximumVolumeSize"
	engineTypeOpenSearch    = "OpenSearch"
	engineTypeElasticsearch = "Elasticsearch"
	instanceTypeT3Small     = "t3.small.search"

	// upgradeProgressComplete is the 100% progress value for a completed upgrade step.
	upgradeProgressComplete = float64(100)
	// autoTuneScheduleLookahead is the look-ahead duration for the next auto-tune window.
	autoTuneScheduleLookahead = 24 * time.Hour
	// maxUpgradeHistoryPerDomain caps the number of upgrade history entries kept
	// per domain to prevent unbounded memory growth in long-running backends.
	maxUpgradeHistoryPerDomain = 100
	// maxMaintenancesPerDomain caps the number of maintenance records kept per domain.
	maxMaintenancesPerDomain = 200
	// maxDataNodesR6gLarge is the max data-node count for r6g.large.search.
	maxDataNodesR6gLarge = 40
	// maxDataNodesR6gXLarge is the max data-node count for r6g.xlarge.search.
	maxDataNodesR6gXLarge = 40
	// maxDataNodesM6gLarge is the max data-node count for m6g.large.search.
	maxDataNodesM6gLarge = 80
	// maxDataNodesT3Small is the max data-node count for t3.small.search.
	maxDataNodesT3Small = 10
	// maxDataNodesOR1Medium is the max data-node count for or1.medium.search.
	maxDataNodesOR1Medium = 40
	// maxDataNodesWithoutMasterStr is the string max for data nodes without a dedicated master.
	maxDataNodesWithoutMasterStr = "10"

	// String representations for storage limit values.
	storageLimitMinVolumeGP3  = "20"
	storageLimitMaxVolumeEBS  = "16384"
	storageLimitMaxIOPS       = "16000"
	storageLimitMaxThroughput = "1000"
	storageLimitMinVolumeT3   = "10"
	storageLimitMaxVolumeT3   = "100"

	// String representations for instance count limits.
	maxNodesStr40 = "40"
	maxNodesStr80 = "80"
	maxNodesStr10 = "10"

	// minInstanceCountValue is MinimumInstanceCount's value. Unlike
	// AdditionalLimit.LimitValues (a real []string), InstanceCountLimits'
	// MinimumInstanceCount/MaximumInstanceCount deserialize as a JSON
	// number (json.Number, aws-sdk-go-v2/service/opensearch@v1.75.4's
	// deserializers.go case "MinimumInstanceCount") -- gopherstack's
	// MaximumInstanceCount values are already int constants, but this one
	// was a string, which would fail DescribeInstanceTypeLimits' decode.
	minInstanceCountValue = 1
)

// upgradeHistoryKey returns the map key for a domain's upgrade history list.
func upgradeHistoryKey(domainName string) string {
	return "upgrade:" + domainName
}

// autoTuneKey returns the map key for a domain's auto-tune config.
func autoTuneKey(domainName string) string {
	return "autotune:" + domainName
}

// UpgradeDomain records an upgrade in the domain's history.
// Called when an upgrade is triggered.
func (b *InMemoryBackend) UpgradeDomain(domainName, upgradeName string) error {
	b.mu.Lock("UpgradeDomain")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(domainName)
	if !ok || deleteWindowElapsed(d, b.clock()) {
		return fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainName)
	}

	b.beginProcessing(d, dpsUpgrading)

	uh := &UpgradeHistory{
		UpgradeName:    upgradeName,
		StartTimestamp: float64(time.Now().Unix()),
		UpgradeStatus:  upgradeStatusSucceeded,
		StepsList: []UpgradeStepItem{
			{
				UpgradeStep:       upgradeStepPreCheck,
				UpgradeStepStatus: upgradeStatusSucceeded,
				ProgressPercent:   upgradeProgressComplete,
			},
			{
				UpgradeStep:       upgradeStepSnapshot,
				UpgradeStepStatus: upgradeStatusSucceeded,
				ProgressPercent:   upgradeProgressComplete,
			},
			{
				UpgradeStep:       upgradeStepUpgrade,
				UpgradeStepStatus: upgradeStatusSucceeded,
				ProgressPercent:   upgradeProgressComplete,
			},
		},
	}

	key := upgradeHistoryKey(domainName)
	b.upgradeHistory[key] = append(b.upgradeHistory[key], uh)
	// Trim to the cap, keeping the most recent entries.
	if len(b.upgradeHistory[key]) > maxUpgradeHistoryPerDomain {
		b.upgradeHistory[key] = b.upgradeHistory[key][len(b.upgradeHistory[key])-maxUpgradeHistoryPerDomain:]
	}

	return nil
}

// GetUpgradeHistory returns the upgrade history for a domain, newest first.
func (b *InMemoryBackend) GetUpgradeHistory(domainName string) ([]*UpgradeHistory, error) {
	b.mu.RLock("GetUpgradeHistory")
	defer b.mu.RUnlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainName)
	}

	src := b.upgradeHistory[upgradeHistoryKey(domainName)]
	out := make([]*UpgradeHistory, len(src))

	for i, uh := range src {
		cp := *uh
		out[len(src)-1-i] = &cp
	}

	return out, nil
}

// GetUpgradeStatus returns the most recent upgrade status for a domain.
func (b *InMemoryBackend) GetUpgradeStatus(domainName string) (string, string, string, error) {
	b.mu.RLock("GetUpgradeStatus")
	defer b.mu.RUnlock()

	if !b.domains.Has(domainName) {
		return "", "", "", fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainName)
	}

	history := b.upgradeHistory[upgradeHistoryKey(domainName)]
	if len(history) == 0 {
		return "INITIAL", upgradeStatusSucceeded, upgradeStepUpgrade, nil
	}

	latest := history[len(history)-1]

	return latest.UpgradeName, latest.UpgradeStatus, upgradeStepUpgrade, nil
}

// SetAutoTune stores auto-tune configuration for a domain.
func (b *InMemoryBackend) SetAutoTune(
	domainName, desiredState string,
	schedules []AutoTuneMaintenanceSchedule,
) error {
	b.mu.Lock("SetAutoTune")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainName)
	}

	b.autoTunes.Put(&AutoTuneConfig{
		DesiredState:         desiredState,
		MaintenanceSchedules: schedules,
		DomainName:           domainName,
	})

	return nil
}

// GetAutoTune returns auto-tune details for a domain.
func (b *InMemoryBackend) GetAutoTune(domainName string) ([]*AutoTune, error) {
	b.mu.RLock("GetAutoTune")
	defer b.mu.RUnlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainName)
	}

	cfg, ok := b.autoTunes.Get(autoTuneKey(domainName))
	if !ok || cfg == nil {
		return []*AutoTune{}, nil
	}

	out := []*AutoTune{
		{
			AutoTuneType: "SCHEDULED",
			AutoTuneDetails: AutoTuneDetails{
				ScheduledAutoTuneDetails: ScheduledAutoTuneDetails{
					Date:       float64(time.Now().Add(autoTuneScheduleLookahead).Unix()),
					ActionType: "JVM_HEAP_SIZE_TUNING",
					Action:     "Increase JVM heap size to improve performance",
					Severity:   "LOW",
				},
			},
		},
	}

	return out, nil
}

// DescribeInstanceTypeLimits returns instance type limits for a given instance type and engine version.
// This is a static lookup table for common types.
func (b *InMemoryBackend) DescribeInstanceTypeLimits(
	instanceType, _ string,
) (*InstanceTypeLimits, error) {
	limits := instanceTypeLimitsTable(instanceType)
	if limits == nil {
		// Return a generic stub for unknown types.
		limits = &InstanceTypeLimits{
			InstanceType: instanceType,
			InstanceLimits: map[string]any{
				instanceCountLimitsKey: map[string]any{
					minInstanceCountKey: minInstanceCountValue,
					maxInstanceCountKey: maxDataNodesR6gLarge,
				},
			},
			StorageTypes: defaultStorageTypes(),
		}
	}

	return limits, nil
}

// instanceTypeLimitsTable returns known limits for common OpenSearch instance types.
func instanceTypeLimitsTable(instanceType string) *InstanceTypeLimits {
	table := map[string]*InstanceTypeLimits{
		instanceTypeR6gLarge: {
			InstanceType: instanceTypeR6gLarge,
			InstanceLimits: map[string]any{
				instanceCountLimitsKey: map[string]any{
					minInstanceCountKey: minInstanceCountValue,
					maxInstanceCountKey: maxDataNodesR6gLarge,
				},
			},
			StorageTypes: defaultStorageTypes(),
			AdditionalLimits: []AdditionalLimit{
				{
					LimitName:   "MaximumNumberOfDataNodesSupported",
					LimitValues: []string{maxNodesStr40},
				},
				{
					LimitName:   "MaximumNumberOfDataNodesWithoutMasterNode",
					LimitValues: []string{maxDataNodesWithoutMasterStr},
				},
			},
		},
		instanceTypeR6gXLarge: {
			InstanceType: instanceTypeR6gXLarge,
			InstanceLimits: map[string]any{
				instanceCountLimitsKey: map[string]any{
					minInstanceCountKey: minInstanceCountValue,
					maxInstanceCountKey: maxDataNodesR6gXLarge,
				},
			},
			StorageTypes: defaultStorageTypes(),
			AdditionalLimits: []AdditionalLimit{
				{
					LimitName:   "MaximumNumberOfDataNodesSupported",
					LimitValues: []string{maxNodesStr40},
				},
			},
		},
		instanceTypeM6gLarge: {
			InstanceType: instanceTypeM6gLarge,
			InstanceLimits: map[string]any{
				instanceCountLimitsKey: map[string]any{
					minInstanceCountKey: minInstanceCountValue,
					maxInstanceCountKey: maxDataNodesM6gLarge,
				},
			},
			StorageTypes: defaultStorageTypes(),
		},
		instanceTypeT3Small: {
			InstanceType: instanceTypeT3Small,
			InstanceLimits: map[string]any{
				instanceCountLimitsKey: map[string]any{
					minInstanceCountKey: minInstanceCountValue,
					maxInstanceCountKey: maxDataNodesT3Small,
				},
			},
			StorageTypes: []StorageType{
				{
					StorageTypeName:    storageTypeEBSOnly,
					StorageSubTypeName: "standard",
					StorageTypeLimits: []StorageTypeLimit{
						{LimitName: storageLimitMinVolume, LimitValues: []string{storageLimitMinVolumeT3}},
						{LimitName: storageLimitMaxVolume, LimitValues: []string{storageLimitMaxVolumeT3}},
					},
				},
			},
		},
		instanceTypeOR1Medium: {
			InstanceType: instanceTypeOR1Medium,
			InstanceLimits: map[string]any{
				instanceCountLimitsKey: map[string]any{
					minInstanceCountKey: minInstanceCountValue,
					maxInstanceCountKey: maxDataNodesOR1Medium,
				},
			},
			StorageTypes: defaultStorageTypes(),
		},
	}

	return table[instanceType]
}

// defaultStorageTypes returns the standard EBS storage types for most instance types.
func defaultStorageTypes() []StorageType {
	return []StorageType{
		{
			StorageTypeName:    storageTypeEBSOnly,
			StorageSubTypeName: "gp3",
			StorageTypeLimits: []StorageTypeLimit{
				{LimitName: storageLimitMinVolume, LimitValues: []string{storageLimitMinVolumeGP3}},
				{LimitName: storageLimitMaxVolume, LimitValues: []string{storageLimitMaxVolumeEBS}},
				{LimitName: "MaximumIops", LimitValues: []string{storageLimitMaxIOPS}},
				{LimitName: "MaximumThroughput", LimitValues: []string{storageLimitMaxThroughput}},
			},
		},
		{
			StorageTypeName:    storageTypeEBSOnly,
			StorageSubTypeName: "gp2",
			StorageTypeLimits: []StorageTypeLimit{
				{LimitName: storageLimitMinVolume, LimitValues: []string{storageLimitMinVolumeT3}},
				{LimitName: storageLimitMaxVolume, LimitValues: []string{storageLimitMaxVolumeEBS}},
			},
		},
	}
}

// ListDomainNamesByEngine returns domain names filtered by engine type.
func (b *InMemoryBackend) ListDomainNamesByEngine(engineType string) []string {
	b.mu.RLock("ListDomainNamesByEngine")
	defer b.mu.RUnlock()

	now := b.clock()
	out := make([]string, 0, b.domains.Len())

	for _, d := range b.domains.All() {
		if deleteWindowElapsed(d, now) {
			continue
		}

		if engineType == "" {
			out = append(out, d.Name)

			continue
		}

		// OpenSearch domains have EngineVersion starting with "OpenSearch_"
		// Elasticsearch domains have EngineVersion starting with a number.
		switch engineType {
		case engineTypeOpenSearch:
			if isOpenSearchEngine(d.EngineVersion) {
				out = append(out, d.Name)
			}
		case engineTypeElasticsearch:
			if !isOpenSearchEngine(d.EngineVersion) {
				out = append(out, d.Name)
			}
		}
	}

	return out
}

// isOpenSearchEngine returns true if the engine version is an OpenSearch (not Elasticsearch) engine.
func isOpenSearchEngine(engineVersion string) bool {
	if len(engineVersion) < 11 { //nolint:mnd // len("OpenSearch_") == 11
		return false
	}

	return engineVersion[:11] == "OpenSearch_"
}

// DomainEntry holds the name and engine version of a domain, used for list responses.
type DomainEntry struct {
	Name          string
	EngineVersion string
}

// ListDomainEntriesFiltered returns name+engine version for all domains matching engineType,
// under a single read lock. Pass an empty string to return all domains.
func (b *InMemoryBackend) ListDomainEntriesFiltered(engineType string) []DomainEntry {
	b.mu.RLock("ListDomainEntriesFiltered")
	defer b.mu.RUnlock()

	now := b.clock()
	out := make([]DomainEntry, 0, b.domains.Len())

	for _, d := range b.domains.All() {
		if deleteWindowElapsed(d, now) {
			continue
		}

		if engineType == "" {
			out = append(out, DomainEntry{Name: d.Name, EngineVersion: d.EngineVersion})

			continue
		}

		switch engineType {
		case engineTypeOpenSearch:
			if isOpenSearchEngine(d.EngineVersion) {
				out = append(out, DomainEntry{Name: d.Name, EngineVersion: d.EngineVersion})
			}
		case engineTypeElasticsearch:
			if !isOpenSearchEngine(d.EngineVersion) {
				out = append(out, DomainEntry{Name: d.Name, EngineVersion: d.EngineVersion})
			}
		default:
			if strings.HasPrefix(d.EngineVersion, engineType+"_") {
				out = append(out, DomainEntry{Name: d.Name, EngineVersion: d.EngineVersion})
			}
		}
	}

	return out
}

// ListInstanceTypeDetails returns a static list of common OpenSearch instance type details.
func (b *InMemoryBackend) ListInstanceTypeDetails(_, _ string) []map[string]any {
	dataRole := []string{nodeRoleData}
	warmRole := []string{nodeRoleData, "UltraWarm"}

	return []map[string]any{
		{
			jsonKeyInstanceType:            instanceTypeT3Small,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          false,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             false,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            dataRole,
		},
		{
			jsonKeyInstanceType:            instanceTypeR6gLarge,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          true,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             true,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            warmRole,
		},
		{
			jsonKeyInstanceType:            instanceTypeM6gLarge,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          true,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             true,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            warmRole,
		},
		{
			jsonKeyInstanceType:            instanceTypeR6gXLarge,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          true,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             true,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            warmRole,
		},
		{
			jsonKeyInstanceType:            instanceTypeOR1Medium,
			jsonKeyAppLogEnabled:           true,
			jsonKeyCognitoEnabled:          false,
			jsonKeyEncryptEnabled:          true,
			jsonKeyWarmEnabled:             false,
			jsonKeyAdvancedSecurityEnabled: true,
			jsonKeyInstanceRole:            dataRole,
		},
	}
}

// GetCompatibleVersions returns static compatible version pairs.
// If domainName is non-empty, target versions are filtered to those
// reachable from the domain's current EngineVersion.
func (b *InMemoryBackend) GetCompatibleVersions(domainName string) []map[string]any {
	static := []map[string]any{
		{
			jsonKeySourceVersion:  engineVersionOpenSearch29,
			jsonKeyTargetVersions: []string{engineVersionOpenSearch211},
		},
		{
			jsonKeySourceVersion:  engineVersionOpenSearch27,
			jsonKeyTargetVersions: []string{engineVersionOpenSearch29, engineVersionOpenSearch211},
		},
		{
			jsonKeySourceVersion:  engineVersionOpenSearch13,
			jsonKeyTargetVersions: []string{engineVersionOpenSearch27},
		},
		{
			jsonKeySourceVersion:  engineVersionOpenSearch211,
			jsonKeyTargetVersions: []string{},
		},
	}

	if domainName == "" {
		return static
	}

	b.mu.RLock("GetCompatibleVersions")
	d, exists := b.domains.Get(domainName)
	b.mu.RUnlock()

	if !exists {
		return static
	}

	for _, entry := range static {
		if entry["SourceVersion"] == d.EngineVersion {
			return []map[string]any{entry}
		}
	}

	return []map[string]any{
		{jsonKeySourceVersion: d.EngineVersion, jsonKeyTargetVersions: []string{}},
	}
}
