package mgn

import (
	"fmt"
	"slices"
)

func validateEnum(field, value string, allowed ...string) error {
	if value == "" || slices.Contains(allowed, value) {
		return nil
	}

	return validationError(fmt.Sprintf("%s: %q is not a valid value", field, value))
}

func strVal(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

func validateLaunchEnums(bootMode, disposition, rightSizing string) error {
	if err := validateEnum("bootMode", bootMode, "LEGACY_BIOS", "UEFI", "USE_SOURCE"); err != nil {
		return err
	}

	if err := validateEnum("launchDisposition", disposition, "STOPPED", "STARTED"); err != nil {
		return err
	}

	return validateEnum("targetInstanceTypeRightSizingMethod", rightSizing, "NONE", "BASIC")
}

func validateReplicationEnums(routing, diskType, ebsEncryption, internetProtocol string, bandwidth int64) error {
	if err := validateEnum("dataPlaneRouting", routing, "PRIVATE_IP", "PUBLIC_IP"); err != nil {
		return err
	}

	if err := validateEnum("defaultLargeStagingDiskType", diskType, "GP2", "ST1", "GP3"); err != nil {
		return err
	}

	if err := validateEnum("ebsEncryption", ebsEncryption, "DEFAULT", "CUSTOM"); err != nil {
		return err
	}

	if err := validateEnum("internetProtocol", internetProtocol, "IPV4", "IPV6"); err != nil {
		return err
	}

	if bandwidth < 0 {
		return validationError("bandwidthThrottling must not be negative")
	}

	return nil
}

func validateReplicatedDisks(disks []ReplicationConfigurationReplicatedDisk) error {
	for _, d := range disks {
		err := validateEnum(
			"replicatedDisks.stagingDiskType", d.StagingDiskType,
			"AUTO", "GP2", "IO1", "SC1", "ST1", "STANDARD", "GP3", "IO2", "FSX_ONTAP",
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func int64Val(p *int64) int64 {
	if p == nil {
		return 0
	}

	return *p
}
