package s3

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract S3 configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetS3Settings() Settings
	GetS3Endpoint() string
}

// Provider implements service.Provider for the S3 service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "S3"
}

// Init initializes the S3 service backend, compressor, janitor, and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	settings := DefaultSettings()
	var endpoint string

	// Try to extract configuration if the config implements the extractor interface
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetS3Settings()
		endpoint = cp.GetS3Endpoint()
	}

	backend := NewInMemoryBackend(&GzipCompressor{}, ctx.Logger).
		WithCompressionMinBytes(settings.CompressionMinBytes)
	handler := NewHandler(backend, ctx.Logger).WithJanitor(settings)
	handler.Endpoint = endpoint

	return handler, nil
}
