package elasticsearch

import (
	"fmt"
	"regexp"
	"slices"
)

const (
	maxInstanceCount = 80
	minEBSVolumeSize = 10
	maxEBSVolumeSize = 16384
)

//nolint:gochecknoglobals // immutable lookup tables
var (
	instanceTypeRe = regexp.MustCompile(`^[a-z][a-z0-9]*\.[a-z0-9]+\.elasticsearch$`)
	ebsVolumeTypes = []string{"standard", "gp2", "gp3", "io1"}
)

func validateDedicatedMaster(cc ClusterConfig) error {
	if cc.DedicatedMasterEnabled && cc.DedicatedMasterCount != 0 &&
		cc.DedicatedMasterCount != 3 && cc.DedicatedMasterCount != 5 {
		return fmt.Errorf("%w: DedicatedMasterCount must be 3 or 5", ErrValidation)
	}

	if cc.DedicatedMasterType != "" && !instanceTypeRe.MatchString(cc.DedicatedMasterType) {
		return fmt.Errorf("%w: invalid DedicatedMasterType %q", ErrValidation, cc.DedicatedMasterType)
	}

	return nil
}

// validateClusterAndEBS applies the documented ClusterConfig and EBSOptions
// limits (instance type shape, instance count 1-80, dedicated master count
// 3 or 5, EBS volume type and 10-16384 GiB size).
func validateClusterAndEBS(cc ClusterConfig, ebs EBSOptions) error {
	if cc.InstanceType != "" && !instanceTypeRe.MatchString(cc.InstanceType) {
		return fmt.Errorf("%w: invalid InstanceType %q", ErrValidation, cc.InstanceType)
	}

	if cc.InstanceCount < 0 || cc.InstanceCount > maxInstanceCount {
		return fmt.Errorf("%w: InstanceCount must be between 1 and %d", ErrValidation, maxInstanceCount)
	}

	if err := validateDedicatedMaster(cc); err != nil {
		return err
	}

	if !ebs.EBSEnabled {
		return nil
	}

	if ebs.VolumeType != "" && !slices.Contains(ebsVolumeTypes, ebs.VolumeType) {
		return fmt.Errorf("%w: invalid EBS VolumeType %q", ErrValidation, ebs.VolumeType)
	}

	if ebs.VolumeSize != 0 && (ebs.VolumeSize < minEBSVolumeSize || ebs.VolumeSize > maxEBSVolumeSize) {
		return fmt.Errorf(
			"%w: EBS VolumeSize must be between %d and %d GiB",
			ErrValidation,
			minEBSVolumeSize,
			maxEBSVolumeSize,
		)
	}

	return nil
}

func validateConfigUpdate(d *Domain, cfg UpdateConfig) error {
	cc, ebs := d.ClusterConfig, d.EBSOptions

	if cfg.ClusterConfig != nil {
		cc = *cfg.ClusterConfig
	}

	if cfg.EBSOptions != nil {
		ebs = *cfg.EBSOptions
	}

	if cfg.ClusterConfigPatch != nil {
		cfg.ClusterConfigPatch(&cc)
	}

	if cfg.EBSOptionsPatch != nil {
		cfg.EBSOptionsPatch(&ebs)
	}

	return validateClusterAndEBS(cc, ebs)
}

func versionParts(v string) (int, int, bool) {
	var major, minor int
	if n, err := fmt.Sscanf(v, "%d.%d", &major, &minor); err != nil || n != 2 {
		return 0, 0, false
	}

	return major, minor, true
}

// validateUpgradeTarget requires a known Elasticsearch version newer than the
// current one; downgrades are not supported by the service.
func validateUpgradeTarget(current, target string) error {
	if target == "" {
		return fmt.Errorf("%w: TargetVersion is required", ErrValidation)
	}

	if !validElasticsearchVersions[target] {
		return fmt.Errorf("%w: invalid TargetVersion %q", ErrValidation, target)
	}

	cMaj, cMin, cOK := versionParts(current)
	tMaj, tMin, tOK := versionParts(target)

	if cOK && tOK && (tMaj < cMaj || (tMaj == cMaj && tMin <= cMin)) {
		return fmt.Errorf(
			"%w: cannot upgrade from %s to %s; TargetVersion must be newer",
			ErrValidation,
			current,
			target,
		)
	}

	return nil
}
