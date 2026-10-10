package fsx

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
)

const (
	lustreDeploymentTypePersistent1 = "PERSISTENT_1"
	lustreDeploymentTypePersistent2 = "PERSISTENT_2"
	lustreDeploymentTypeScratch2    = "SCRATCH_2"
	storageTypeHDD                  = "HDD"
	maxAutomaticBackupRetentionDays = 90
	storageTypeSSD                  = "SSD"
	compressionNone                 = "NONE"
	lustreStepHDD12                 = 6000
	lustreStepHDD40                 = 1800
	lustreMinSSDCapacity            = 1200
	lustreSSDStep                   = 2400
)

var weeklyMaintenancePattern = regexp.MustCompile(`^[1-7]:([01]\d|2[0-3]):?([0-5]\d)$`)

// createLustreConfiguration mirrors CreateFileSystemLustreConfiguration.
type createLustreConfiguration struct {
	DriveCacheType                string          `json:"DriveCacheType,omitempty"`
	ImportPath                    string          `json:"ImportPath,omitempty"`
	WeeklyMaintenanceStartTime    string          `json:"WeeklyMaintenanceStartTime,omitempty"`
	ExportPath                    string          `json:"ExportPath,omitempty"`
	DeploymentType                string          `json:"DeploymentType,omitempty"`
	AutoImportPolicy              string          `json:"AutoImportPolicy,omitempty"`
	DailyAutomaticBackupStartTime string          `json:"DailyAutomaticBackupStartTime,omitempty"`
	DataCompressionType           string          `json:"DataCompressionType,omitempty"`
	DataReadCacheConfiguration    json.RawMessage `json:"DataReadCacheConfiguration,omitempty"`
	RootSquashConfiguration       json.RawMessage `json:"RootSquashConfiguration,omitempty"`
	LogConfiguration              json.RawMessage `json:"LogConfiguration,omitempty"`
	MetadataConfiguration         json.RawMessage `json:"MetadataConfiguration,omitempty"`
	AutomaticBackupRetentionDays  int32           `json:"AutomaticBackupRetentionDays,omitempty"`
	ImportedFileChunkSize         int32           `json:"ImportedFileChunkSize,omitempty"`
	PerUnitStorageThroughput      int32           `json:"PerUnitStorageThroughput,omitempty"`
	ThroughputCapacity            int32           `json:"ThroughputCapacity,omitempty"`
	CopyTagsToBackups             bool            `json:"CopyTagsToBackups,omitempty"`
	EfaEnabled                    bool            `json:"EfaEnabled,omitempty"`
}

// storedLustreExtras keeps the Lustre members that have no counterpart on the shared
// storedFileSystem fields.
type storedLustreExtras struct {
	AutoImportPolicy           string          `json:"autoImportPolicy,omitempty"`
	DataCompressionType        string          `json:"dataCompression,omitempty"`
	DriveCacheType             string          `json:"driveCache,omitempty"`
	ExportPath                 string          `json:"exportPath,omitempty"`
	ImportPath                 string          `json:"importPath,omitempty"`
	RootSquashConfiguration    json.RawMessage `json:"rootSquash,omitempty"`
	DataReadCacheConfiguration json.RawMessage `json:"dataReadCache,omitempty"`
	MetadataConfiguration      json.RawMessage `json:"metadata,omitempty"`
	LogConfiguration           json.RawMessage `json:"log,omitempty"`
	ImportedFileChunkSize      int32           `json:"importedChunk,omitempty"`
	PerUnitStorageThroughput   int32           `json:"perUnitThroughput,omitempty"`
	EfaEnabled                 bool            `json:"efa,omitempty"`
}

func validLustreThroughputs(deployment, storageType string) []int32 {
	switch {
	case deployment == lustreDeploymentTypePersistent2:
		return []int32{125, 250, 500, 1000}
	case deployment == lustreDeploymentTypePersistent1 && storageType == storageTypeHDD:
		return []int32{12, 40}
	case deployment == lustreDeploymentTypePersistent1:
		return []int32{50, 100, 200}
	default:
		return nil
	}
}

func validateLustreDeployment(deployment string) error {
	switch deployment {
	case lustreDeploymentTypeScratch1, lustreDeploymentTypeScratch2,
		lustreDeploymentTypePersistent1, lustreDeploymentTypePersistent2:
		return nil
	default:
		return fmt.Errorf("%w: LustreConfiguration.DeploymentType %q is not valid", ErrValidation, deployment)
	}
}

func validateLustreStorageCapacity(deployment, storageType string, throughput, capacity int32) error {
	if storageType == storageTypeIntelligentTiered {
		return nil
	}

	var valid bool

	switch {
	case storageType == storageTypeHDD && throughput == 12:
		valid = capacity%lustreStepHDD12 == 0
	case storageType == storageTypeHDD && throughput == 40:
		valid = capacity%lustreStepHDD40 == 0
	case deployment == lustreDeploymentTypeScratch1:
		valid = capacity == 1200 || capacity == 2400 || capacity%3600 == 0
	default:
		valid = capacity == lustreMinSSDCapacity || capacity%lustreSSDStep == 0
	}

	if !valid {
		return fmt.Errorf(
			"%w: StorageCapacity %d GiB is not a valid size for %s %s storage",
			ErrValidation, capacity, deployment, storageType,
		)
	}

	return nil
}

func validateLustreEnums(cfg *createLustreConfiguration) error {
	checks := []struct {
		field string
		value string
		valid []string
	}{
		{
			"AutoImportPolicy",
			cfg.AutoImportPolicy,
			[]string{compressionNone, "NEW", "NEW_CHANGED", "NEW_CHANGED_DELETED"},
		},
		{"DataCompressionType", cfg.DataCompressionType, []string{compressionNone, "LZ4"}},
		{"DriveCacheType", cfg.DriveCacheType, []string{compressionNone, "READ"}},
	}

	for _, c := range checks {
		if c.value != "" && !slices.Contains(c.valid, c.value) {
			return fmt.Errorf("%w: LustreConfiguration.%s %q is not valid", ErrValidation, c.field, c.value)
		}
	}

	if cfg.WeeklyMaintenanceStartTime != "" && !weeklyMaintenancePattern.MatchString(cfg.WeeklyMaintenanceStartTime) {
		return fmt.Errorf("%w: LustreConfiguration.WeeklyMaintenanceStartTime must be d:HH:MM", ErrValidation)
	}

	if cfg.AutomaticBackupRetentionDays < 0 || cfg.AutomaticBackupRetentionDays > maxAutomaticBackupRetentionDays {
		return fmt.Errorf(
			"%w: LustreConfiguration.AutomaticBackupRetentionDays must be 0-%d",
			ErrValidation, maxAutomaticBackupRetentionDays,
		)
	}

	return nil
}

func validateLustreConfig(cfg *createLustreConfiguration, storageType string, capacity int32) error {
	deployment := lustreDeploymentTypeScratch1
	if cfg != nil && cfg.DeploymentType != "" {
		deployment = cfg.DeploymentType
	} else {
		cfg = &createLustreConfiguration{}
	}

	if err := validateLustreDeployment(deployment); err != nil {
		return err
	}

	if err := validateLustreEnums(cfg); err != nil {
		return err
	}

	if storageType == "" {
		storageType = storageTypeSSD
	}

	allowed := validLustreThroughputs(deployment, storageType)
	if allowed == nil && cfg.PerUnitStorageThroughput != 0 && storageType != storageTypeIntelligentTiered {
		return fmt.Errorf(
			"%w: PerUnitStorageThroughput is only valid for PERSISTENT deployment types", ErrValidation,
		)
	}

	if allowed != nil && storageType != storageTypeIntelligentTiered &&
		!slices.Contains(allowed, cfg.PerUnitStorageThroughput) {
		return fmt.Errorf(
			"%w: PerUnitStorageThroughput %d is not valid for %s %s storage (valid: %v)",
			ErrValidation, cfg.PerUnitStorageThroughput, deployment, storageType, allowed,
		)
	}

	return validateLustreStorageCapacity(deployment, storageType, cfg.PerUnitStorageThroughput, capacity)
}

// applyLustreConfig sets the Lustre-specific fields on fs. LustreConfiguration is optional on the
// real input; an absent block defaults DeploymentType to SCRATCH_1.
func applyLustreConfig(fs *storedFileSystem, cfg *createLustreConfiguration) error {
	if err := validateLustreConfig(cfg, fs.StorageType, fs.StorageCapacityGiB); err != nil {
		return err
	}

	fs.MountName = generateLustreMountName()
	fs.DeploymentType = lustreDeploymentTypeScratch1

	if cfg != nil {
		if cfg.DeploymentType != "" {
			fs.DeploymentType = cfg.DeploymentType
		}

		fs.WeeklyMaintenanceStartTime = cfg.WeeklyMaintenanceStartTime
		fs.DailyAutomaticBackupStartTime = cfg.DailyAutomaticBackupStartTime
		fs.AutomaticBackupRetentionDays = cfg.AutomaticBackupRetentionDays
		fs.CopyTagsToBackups = cfg.CopyTagsToBackups
		fs.ThroughputCapacity = cfg.ThroughputCapacity
		fs.Lustre = &storedLustreExtras{
			RootSquashConfiguration:    cfg.RootSquashConfiguration,
			DataReadCacheConfiguration: cfg.DataReadCacheConfiguration,
			MetadataConfiguration:      cfg.MetadataConfiguration,
			LogConfiguration:           cfg.LogConfiguration,
			AutoImportPolicy:           cfg.AutoImportPolicy,
			DataCompressionType:        cfg.DataCompressionType,
			DriveCacheType:             cfg.DriveCacheType,
			ExportPath:                 cfg.ExportPath,
			ImportPath:                 cfg.ImportPath,
			ImportedFileChunkSize:      cfg.ImportedFileChunkSize,
			PerUnitStorageThroughput:   cfg.PerUnitStorageThroughput,
			EfaEnabled:                 cfg.EfaEnabled,
		}
	}

	if fs.FileSystemTypeVersion == "" {
		fs.FileSystemTypeVersion = defaultLustreVersion(fs.DeploymentType, cfg)
	}

	return nil
}

// metadataMode extracts MetadataConfiguration.Mode for defaultLustreVersion.
func metadataMode(raw json.RawMessage) string {
	var m struct {
		Mode string `json:"Mode"`
	}

	if len(raw) == 0 || json.Unmarshal(raw, &m) != nil {
		return ""
	}

	return m.Mode
}

// updateLustreConfiguration mirrors UpdateFileSystemLustreConfiguration.
type updateLustreConfiguration struct {
	AutoImportPolicy              string          `json:"AutoImportPolicy,omitempty"`
	DailyAutomaticBackupStartTime string          `json:"DailyAutomaticBackupStartTime,omitempty"`
	DataCompressionType           string          `json:"DataCompressionType,omitempty"`
	WeeklyMaintenanceStartTime    string          `json:"WeeklyMaintenanceStartTime,omitempty"`
	RootSquashConfiguration       json.RawMessage `json:"RootSquashConfiguration,omitempty"`
	DataReadCacheConfiguration    json.RawMessage `json:"DataReadCacheConfiguration,omitempty"`
	MetadataConfiguration         json.RawMessage `json:"MetadataConfiguration,omitempty"`
	LogConfiguration              json.RawMessage `json:"LogConfiguration,omitempty"`
	AutomaticBackupRetentionDays  int32           `json:"AutomaticBackupRetentionDays,omitempty"`
	PerUnitStorageThroughput      int32           `json:"PerUnitStorageThroughput,omitempty"`
	ThroughputCapacity            int32           `json:"ThroughputCapacity,omitempty"`
}

func applyLustreUpdate(fs *storedFileSystem, cfg *updateLustreConfiguration) error {
	if cfg == nil {
		return nil
	}

	if err := validateLustreEnums(&createLustreConfiguration{
		AutoImportPolicy:             cfg.AutoImportPolicy,
		DataCompressionType:          cfg.DataCompressionType,
		WeeklyMaintenanceStartTime:   cfg.WeeklyMaintenanceStartTime,
		AutomaticBackupRetentionDays: cfg.AutomaticBackupRetentionDays,
	}); err != nil {
		return err
	}

	if cfg.PerUnitStorageThroughput != 0 {
		storageType := fs.StorageType
		if storageType == "" {
			storageType = storageTypeSSD
		}

		allowed := validLustreThroughputs(fs.DeploymentType, storageType)
		if !slices.Contains(allowed, cfg.PerUnitStorageThroughput) {
			return fmt.Errorf(
				"%w: PerUnitStorageThroughput %d is not valid for %s",
				ErrValidation, cfg.PerUnitStorageThroughput, fs.DeploymentType,
			)
		}
	}

	if fs.Lustre == nil {
		fs.Lustre = &storedLustreExtras{}
	}

	setIf(&fs.WeeklyMaintenanceStartTime, cfg.WeeklyMaintenanceStartTime)
	setIf(&fs.DailyAutomaticBackupStartTime, cfg.DailyAutomaticBackupStartTime)
	setIf(&fs.Lustre.AutoImportPolicy, cfg.AutoImportPolicy)
	setIf(&fs.Lustre.DataCompressionType, cfg.DataCompressionType)

	if cfg.AutomaticBackupRetentionDays != 0 {
		fs.AutomaticBackupRetentionDays = cfg.AutomaticBackupRetentionDays
	}

	if cfg.PerUnitStorageThroughput != 0 {
		fs.Lustre.PerUnitStorageThroughput = cfg.PerUnitStorageThroughput
	}

	if cfg.ThroughputCapacity != 0 {
		fs.ThroughputCapacity = cfg.ThroughputCapacity
	}

	setRaw(&fs.Lustre.RootSquashConfiguration, cfg.RootSquashConfiguration)
	setRaw(&fs.Lustre.DataReadCacheConfiguration, cfg.DataReadCacheConfiguration)
	setRaw(&fs.Lustre.MetadataConfiguration, cfg.MetadataConfiguration)
	setRaw(&fs.Lustre.LogConfiguration, cfg.LogConfiguration)

	return nil
}

func setIf(dst *string, v string) {
	if v != "" {
		*dst = v
	}
}

func setRaw(dst *json.RawMessage, v json.RawMessage) {
	if len(v) > 0 {
		*dst = v
	}
}

// validateStorageCapacityUpdate applies the UpdateFileSystem.StorageCapacity rules: only ONTAP can
// shrink, increases need at least 10 percent (Windows/OpenZFS/ONTAP) and SCRATCH_1 cannot grow.
func validateStorageCapacityUpdate(fs *storedFileSystem, target int32) error {
	current := fs.StorageCapacityGiB
	if target == current {
		return nil
	}

	if target < current {
		if fs.FileSystemType != fileSystemTypeONTAP {
			return fmt.Errorf("%w: StorageCapacity cannot be decreased for %s", ErrValidation, fs.FileSystemType)
		}

		return nil
	}

	switch fs.FileSystemType {
	case fileSystemTypeLustre:
		if fs.DeploymentType == lustreDeploymentTypeScratch1 {
			return fmt.Errorf("%w: SCRATCH_1 file systems cannot increase storage capacity", ErrValidation)
		}

		var throughput int32
		if fs.Lustre != nil {
			throughput = fs.Lustre.PerUnitStorageThroughput
		}

		return validateLustreStorageCapacity(fs.DeploymentType, fs.StorageType, throughput, target)
	case fileSystemTypeWindows, fileSystemTypeOpenZFS, fileSystemTypeONTAP:
		if int64(target)*10 < int64(current)*11 {
			return fmt.Errorf(
				"%w: StorageCapacity must be at least 10 percent greater than the current %d GiB",
				ErrValidation,
				current,
			)
		}
	}

	return nil
}
