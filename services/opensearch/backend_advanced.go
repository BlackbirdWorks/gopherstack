package opensearch

import (
	"fmt"
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
	minNodesStr1  = "1"
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

	if _, ok := b.domains[domainName]; !ok {
		return fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainName)
	}

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

	return nil
}

// GetUpgradeHistory returns the upgrade history for a domain, newest first.
func (b *InMemoryBackend) GetUpgradeHistory(domainName string) ([]*UpgradeHistory, error) {
	b.mu.RLock("GetUpgradeHistory")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
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

	if _, ok := b.domains[domainName]; !ok {
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

	if _, ok := b.domains[domainName]; !ok {
		return fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainName)
	}

	b.autoTunes[autoTuneKey(domainName)] = &AutoTuneConfig{
		DesiredState:         desiredState,
		MaintenanceSchedules: schedules,
	}

	return nil
}

// GetAutoTune returns auto-tune details for a domain.
func (b *InMemoryBackend) GetAutoTune(domainName string) ([]*AutoTune, error) {
	b.mu.RLock("GetAutoTune")
	defer b.mu.RUnlock()

	if _, ok := b.domains[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain %q not found", ErrDomainNotFound, domainName)
	}

	cfg, ok := b.autoTunes[autoTuneKey(domainName)]
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
					minInstanceCountKey: minNodesStr1,
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
					minInstanceCountKey: minNodesStr1,
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
					minInstanceCountKey: minNodesStr1,
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
					minInstanceCountKey: minNodesStr1,
					maxInstanceCountKey: maxDataNodesM6gLarge,
				},
			},
			StorageTypes: defaultStorageTypes(),
		},
		instanceTypeT3Small: {
			InstanceType: instanceTypeT3Small,
			InstanceLimits: map[string]any{
				instanceCountLimitsKey: map[string]any{
					minInstanceCountKey: minNodesStr1,
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
					minInstanceCountKey: minNodesStr1,
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

	out := make([]string, 0, len(b.domains))

	for name, d := range b.domains {
		if engineType == "" {
			out = append(out, name)

			continue
		}

		// OpenSearch domains have EngineVersion starting with "OpenSearch_"
		// Elasticsearch domains have EngineVersion starting with a number.
		switch engineType {
		case "OpenSearch":
			if isOpenSearchEngine(d.EngineVersion) {
				out = append(out, name)
			}
		case "Elasticsearch":
			if !isOpenSearchEngine(d.EngineVersion) {
				out = append(out, name)
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
