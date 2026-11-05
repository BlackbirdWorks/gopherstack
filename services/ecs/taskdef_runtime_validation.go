package ecs

import (
	"fmt"
	"slices"
)

// validateContainerRuntime rejects the container wiring real ECS rejects at registration: dependsOn and
// volumesFrom targets that do not exist or form a cycle, unknown conditions, HEALTHY on a container without a
// health check, mount points for undefined volumes, autoprovision outside a shared Docker volume, and a
// healthCheck command that does not start with CMD, CMD-SHELL or NONE.
func validateContainerRuntime(defs []ContainerDefinition, volumes []Volume) error {
	if _, err := orderContainers(defs); err != nil {
		return fmt.Errorf("%w: %w", ErrClient, err)
	}

	byName := make(map[string]ContainerDefinition, len(defs))
	for _, cd := range defs {
		byName[cd.Name] = cd
	}

	volumeNames := make(map[string]bool, len(volumes))
	for _, v := range volumes {
		volumeNames[v.Name] = true

		if err := validateDockerVolume(v); err != nil {
			return err
		}
	}

	for _, cd := range defs {
		if err := validateContainerRefs(cd, byName, volumeNames); err != nil {
			return err
		}
	}

	return nil
}

func validateContainerRefs(
	cd ContainerDefinition,
	byName map[string]ContainerDefinition,
	volumes map[string]bool,
) error {
	for _, d := range cd.DependsOn {
		if !slices.Contains([]string{dependsStart, dependsComplete, dependsSuccess, dependsHealthy}, d.Condition) {
			return fmt.Errorf("%w: container %q dependsOn %q has invalid condition %q",
				ErrClient, cd.Name, d.ContainerName, d.Condition)
		}

		if d.Condition == dependsHealthy && byName[d.ContainerName].HealthCheck == nil {
			return fmt.Errorf("%w: container %q depends on %q being HEALTHY but %q has no healthCheck",
				ErrClient, cd.Name, d.ContainerName, d.ContainerName)
		}
	}

	for _, mp := range cd.MountPoints {
		if !volumes[mp.SourceVolume] {
			return fmt.Errorf("%w: container %q mounts undefined volume %q", ErrClient, cd.Name, mp.SourceVolume)
		}
	}

	if err := validateRestartPolicy(cd); err != nil {
		return err
	}

	if err := validateLogMode(cd); err != nil {
		return err
	}

	if hc := cd.HealthCheck; hc != nil {
		if len(hc.Command) == 0 || !slices.Contains([]string{"CMD", "CMD-SHELL", healthCheckNone}, hc.Command[0]) {
			return fmt.Errorf("%w: container %q healthCheck command must start with CMD, CMD-SHELL or NONE",
				ErrClient, cd.Name)
		}
	}

	return nil
}

func validateDockerVolume(v Volume) error {
	cfg := v.DockerVolumeConfig
	if cfg == nil {
		return nil
	}

	if cfg.Scope != "" && cfg.Scope != volumeScopeTask && cfg.Scope != volumeScopeShared {
		return fmt.Errorf("%w: volume %q has invalid scope %q", ErrClient, v.Name, cfg.Scope)
	}

	if cfg.Autoprovision != nil && *cfg.Autoprovision && cfg.Scope != volumeScopeShared {
		return fmt.Errorf("%w: volume %q sets autoprovision but its scope is not shared", ErrClient, v.Name)
	}

	return nil
}

const (
	maxIgnoredExitCodes = 50
	maxRestartPeriod    = 1800
)

// validateRestartPolicy enforces ContainerRestartPolicy's documented limits: at most 50 ignoredExitCodes and a
// restartAttemptPeriod between 60 and 1800 seconds.
func validateRestartPolicy(cd ContainerDefinition) error {
	rp := cd.RestartPolicy
	if rp == nil {
		return nil
	}

	if len(rp.IgnoredExitCodes) > maxIgnoredExitCodes {
		return fmt.Errorf("%w: container %q restartPolicy.ignoredExitCodes has more than %d entries",
			ErrClient, cd.Name, maxIgnoredExitCodes)
	}

	if p := rp.RestartAttemptPeriod; p != 0 && (p < int(minRestartAttemptPeriod.Seconds()) || p > maxRestartPeriod) {
		return fmt.Errorf("%w: container %q restartPolicy.restartAttemptPeriod %d must be between %d and %d seconds",
			ErrClient, cd.Name, p, int(minRestartAttemptPeriod.Seconds()), maxRestartPeriod)
	}

	return nil
}

// validateLogMode rejects a logConfiguration mode other than blocking or non-blocking.
func validateLogMode(cd ContainerDefinition) error {
	if cd.LogConfiguration == nil {
		return nil
	}

	switch cd.LogConfiguration.Options[logOptMode] {
	case "", logModeBlocking, logModeNonBlocking:
		return nil
	default:
		return fmt.Errorf("%w: container %q logConfiguration option %q must be %s or %s",
			ErrClient, cd.Name, logOptMode, logModeBlocking, logModeNonBlocking)
	}
}
