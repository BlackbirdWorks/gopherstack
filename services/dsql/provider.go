package dsql

import (
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/pgengine"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Aurora DSQL service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "DSQL" }

// Init initializes the Aurora DSQL service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend()

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		if st := cp.GetDSQLSettings(); st.Engine == EngineDocker {
			enableDockerEngine(ctx, backend, st)
		}
	}

	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}

func enableDockerEngine(ctx *service.AppContext, backend *InMemoryBackend, st Settings) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("DSQL: container runtime unavailable; clusters stay control-plane only", "error", err)

		return
	}

	er, ok := rt.(pgengine.Runtime)
	if !ok {
		ctx.Logger.Warn("DSQL: container runtime cannot stop/start containers; clusters stay control-plane only")

		return
	}

	var caDir string
	if cc, isCA := ctx.Config.(CADirConfig); isCA {
		caDir = cc.GetDSQLCADir()
	}

	err = backend.EnableDockerEngine(DockerConfig{
		Runtime: er, Ports: ctx.PortAlloc, Logger: ctx.Logger, Image: st.EngineImage, CADir: caDir,
	})
	if err != nil {
		ctx.Logger.Warn("DSQL: docker engine unavailable; clusters stay control-plane only", "error", err)
	}
}
