package timestreaminfluxdb

import "context"

// EngineSpec describes the InfluxDB container to run for one instance.
type EngineSpec struct {
	Name         string
	Username     string
	Password     string
	Organization string
	Bucket       string
	Token        string
	// RestoreFrom is a backup ID: the server starts uninitialised and is restored from it.
	RestoreFrom string
	// DataDir and BackupsDir are host directories mounted into the container.
	DataDir    string
	BackupsDir string
	// Env holds InfluxDB server settings (INFLUXD_*) derived from the parameter group.
	Env []string
	// HostPort reuses an already held host port; zero allocates one.
	HostPort int32
}

// EngineHandle identifies a running InfluxDB container and where it is reachable.
type EngineHandle struct {
	ContainerID string
	Host        string
	Port        int32
}

// Engine runs real InfluxDB servers for instances; nil keeps instances metadata-only.
type Engine interface {
	// Start launches a server initialised from spec and returns once it accepts connections.
	Start(ctx context.Context, spec EngineSpec) (EngineHandle, error)
	// Stop removes the server; the data directories stay.
	Stop(ctx context.Context, h EngineHandle) error
	// Restart restarts the server in place, keeping its data and port.
	Restart(ctx context.Context, h EngineHandle) (EngineHandle, error)
	// Recreate replaces the container with one built from spec on the same port and data directory.
	Recreate(ctx context.Context, h EngineHandle, spec EngineSpec) (EngineHandle, error)
	// Backup runs influx backup inside the server into BackupsDir/backupID.
	Backup(ctx context.Context, h EngineHandle, token, backupID string) error
	// DrainLogs returns and clears the server output captured since the last call.
	DrainLogs(h EngineHandle) []byte
}
