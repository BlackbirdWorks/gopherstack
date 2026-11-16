package ocifunctions

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

const (
	platformAMD64 = "linux/amd64"
	platformARM64 = "linux/arm64"
)

// shapePlatform maps an application shape to the container platform; GENERIC_X86_ARM runs on either.
func shapePlatform(shape string) string {
	switch shape {
	case "GENERIC_ARM":
		return platformARM64
	case "GENERIC_X86":
		return platformAMD64
	}

	return ""
}

// checkRunnableImage must be called with the lock held.
func (b *InMemoryBackend) checkRunnableImage(app *Application, f *Function) *APIError {
	if b.links.images.Resolve == nil {
		return nil
	}

	img, found, _ := b.links.images.Resolve(f.Image)

	return b.checkSignature(app, img, found)
}

func (b *InMemoryBackend) logTrace(ctx context.Context, p execPlan) {
	if p.trace == nil || !ptrconv.Bool(p.trace.IsEnabled) {
		return
	}

	logger.Load(ctx).
		InfoContext(ctx, "ocifunctions: tracing is enabled; no APM service is emulated, spans not exported",
			"function", p.fnID, "apmDomain", ptrconv.String(p.trace.DomainID))
}
