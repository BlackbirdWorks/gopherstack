package fsx

import "fmt"

const (
	maxStorageCapacityWindows = 65536
	minStorageCapacityWinHDD  = 2000
	maxStorageCapacityOpenZFS = 524288
	maxStorageCapacityPerHA   = 524288
	minStorageCapacityPerHA   = 1024
	maxONTAPHAPairs           = 12
	ontapDeploymentSingleAZ2  = "SINGLE_AZ_2"
)

// validateStorageBounds enforces the per-type StorageCapacity limits documented on
// CreateFileSystemInput.StorageCapacity.
func validateStorageBounds(input *createFileSystemInput) error {
	capacity := input.StorageCapacityGiB

	var minCap, maxCap int32

	switch input.FileSystemType {
	case fileSystemTypeWindows:
		minCap, maxCap = minStorageCapacityWindows, maxStorageCapacityWindows
		if input.StorageType == storageTypeHDD {
			minCap = minStorageCapacityWinHDD
		}
	case fileSystemTypeOpenZFS:
		minCap, maxCap = minStorageCapacityOpenZFS, maxStorageCapacityOpenZFS
	case fileSystemTypeONTAP:
		pairs := int32(defaultHAPairs)
		if cfg := input.OntapConfiguration; cfg != nil && cfg.HAPairs > 0 {
			pairs = cfg.HAPairs
		}

		minCap, maxCap = minStorageCapacityPerHA*pairs, maxStorageCapacityPerHA*pairs
	default:
		return nil
	}

	if capacity < minCap || capacity > maxCap {
		return fmt.Errorf(
			"%w: StorageCapacity %d GiB must be between %d and %d GiB for %s",
			ErrValidation, capacity, minCap, maxCap, input.FileSystemType,
		)
	}

	return nil
}

// validateOntapHA enforces the HAPairs/ThroughputCapacity rules listed on
// CreateFileSystemOntapConfiguration.
func validateOntapHA(cfg *createOntapConfiguration) error {
	if cfg.HAPairs < 0 || cfg.HAPairs > maxONTAPHAPairs {
		return fmt.Errorf("%w: OntapConfiguration.HAPairs must be between 1 and %d", ErrValidation, maxONTAPHAPairs)
	}

	if cfg.HAPairs > 1 && cfg.DeploymentType != ontapDeploymentSingleAZ2 {
		return fmt.Errorf(
			"%w: OntapConfiguration.HAPairs greater than 1 requires DeploymentType SINGLE_AZ_2", ErrValidation,
		)
	}

	pairs := max(cfg.HAPairs, 1)
	if cfg.ThroughputCapacity > 0 && cfg.ThroughputCapacityPerHAPair > 0 &&
		cfg.ThroughputCapacity != cfg.ThroughputCapacityPerHAPair*pairs {
		return fmt.Errorf(
			"%w: OntapConfiguration.ThroughputCapacity must equal ThroughputCapacityPerHAPair times HAPairs",
			ErrValidation,
		)
	}

	return nil
}
