package timestreaminfluxdb

import "context"

// EngineSpec describes the InfluxDB container to run for one instance.
type EngineSpec struct {
	Name         string
	Username     string
	Password     string
	Organization string
	Bucket       string
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
	// Stop removes the server.
	Stop(ctx context.Context, h EngineHandle) error
}
