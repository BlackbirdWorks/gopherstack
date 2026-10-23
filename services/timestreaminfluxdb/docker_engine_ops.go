package timestreaminfluxdb

import (
	"context"
	"fmt"
	"os"
)

// Restart stops and starts the same container: data, published port and container ID are kept.
func (d *DockerEngine) Restart(ctx context.Context, h EngineHandle) (EngineHandle, error) {
	if err := d.Runtime.StopContainer(ctx, h.ContainerID); err != nil {
		return EngineHandle{}, fmt.Errorf("restart influxdb: %w", err)
	}

	if err := d.Runtime.StartContainer(ctx, h.ContainerID); err != nil {
		return EngineHandle{}, fmt.Errorf("restart influxdb: %w", err)
	}

	if err := d.waitHealthy(ctx, h); err != nil {
		return EngineHandle{}, err
	}

	d.followLogs(h.ContainerID)

	return h, nil
}

// Recreate replaces the container with one built from spec, on the same host port and data directory.
func (d *DockerEngine) Recreate(ctx context.Context, h EngineHandle, spec EngineSpec) (EngineHandle, error) {
	pending := d.takeLogs(h.ContainerID)

	if err := d.Runtime.StopAndRemove(ctx, h.ContainerID); err != nil {
		return EngineHandle{}, fmt.Errorf("recreate influxdb: %w", err)
	}

	if spec.RestoreFrom != "" && spec.DataDir != "" {
		if err := wipeDir(spec.DataDir); err != nil {
			return EngineHandle{}, err
		}
	}

	spec.HostPort = h.Port

	nh, err := d.launch(ctx, spec)
	if err != nil {
		return EngineHandle{}, err
	}

	d.restoreLogs(nh.ContainerID, pending)

	return nh, nil
}

func wipeDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("clear data directory: %w", err)
	}

	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	return nil
}

// Backup runs influx backup inside the container; the backup lands in the mounted backups directory.
func (d *DockerEngine) Backup(ctx context.Context, h EngineHandle, token, backupID string) error {
	res, err := d.Runtime.Exec(ctx, h.ContainerID,
		[]string{"influx", "backup", "--host", localURL, "--token", token, backupsMount + "/" + backupID})
	if err != nil {
		return fmt.Errorf("influx backup: %w", err)
	}

	if res.ExitCode != 0 {
		return fmt.Errorf("%w: influx backup exited %d: %s", ErrExecFailed, res.ExitCode, res.Stderr)
	}

	return nil
}
