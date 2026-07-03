package cloudwatchlogs

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("AppContext is required")

// ConfigProvider is a private interface to extract CloudWatch Logs configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetCloudWatchLogsSettings() Settings
}

// s3HandlerProvider is the subset of the server's backends provider needed to
// obtain the S3 handler so export tasks can write to the emulated S3 service.
type s3HandlerProvider interface {
	GetS3Handler() service.Registerable
}

// Provider implements service.Provider for the CloudWatch Logs service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "CloudWatchLogs" }

// Init initializes the CloudWatch Logs service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w", ErrNilAppContext)
	}

	var backend *InMemoryBackend

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, cfg.GetAccountID(), cfg.GetRegion())
	} else {
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, config.DefaultAccountID, config.DefaultRegion)
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetCloudWatchLogsSettings()
	}

	wireExportSink(ctx, backend)

	handler := NewHandler(backend).WithJanitor(settings.JanitorInterval, ctx.JanitorTimeout)

	return handler, nil
}

// wireExportSink connects the backend to the emulated S3 service when the app
// context exposes an S3 handler, enabling real CreateExportTask writes.
func wireExportSink(ctx *service.AppContext, backend *InMemoryBackend) {
	hp, ok := ctx.Config.(s3HandlerProvider)
	if !ok {
		return
	}

	sh, ok := hp.GetS3Handler().(*s3.S3Handler)
	if !ok || sh == nil || sh.Backend == nil {
		return
	}

	backend.SetExportSink(newS3ExportSink(sh.Backend))
}
