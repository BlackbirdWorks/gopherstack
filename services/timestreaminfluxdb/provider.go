// Package timestreaminfluxdb emulates Amazon Timestream for InfluxDB: DB instances, clusters, parameter groups
// and backups, with an opt-in docker engine that runs a real InfluxDB 2 server per instance.
package timestreaminfluxdb

import (
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// EngineEnv selects the engine mode: "stub" (metadata only, default) or "docker".
	EngineEnv = "TIMESTREAM_INFLUXDB_ENGINE"
	// HostEnv is the address engine-backed instances are reached on; default 127.0.0.1.
	HostEnv = "TIMESTREAM_INFLUXDB_HOST"
	// EngineDockerMode runs a real influxdb:2 container per instance.
	EngineDockerMode = "docker"
)

// DataDirConfig locates the directory instance data and backups persist in; empty keeps them in a temporary one.
type DataDirConfig interface {
	GetTimestreamInfluxDBDataDir() string
}

// Provider implements service.Provider for Amazon Timestream for InfluxDB.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "TimestreamInfluxDB" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)
	backend := NewInMemoryBackend(accountID, region)

	if dc, ok := ctx.Config.(DataDirConfig); ok {
		backend.SetDataDir(dc.GetTimestreamInfluxDBDataDir())
	}

	if os.Getenv(EngineEnv) == EngineDockerMode {
		rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
		if err != nil {
			ctx.Logger.Warn(
				"TimestreamInfluxDB: container runtime unavailable; instances stay metadata-only",
				"error",
				err,
			)
		} else if cr, ok := rt.(ContainerRuntime); ok {
			backend.SetEngine(&DockerEngine{Runtime: cr, Ports: ctx.PortAlloc, Host: os.Getenv(HostEnv)})
		} else {
			ctx.Logger.Warn(
				"TimestreamInfluxDB: container runtime cannot restart containers; instances stay metadata-only",
			)
		}
	}

	backend.StartScheduler(0)

	return NewHandler(backend), nil
}
