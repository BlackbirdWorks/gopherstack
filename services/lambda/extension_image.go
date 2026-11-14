package lambda

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	imageExtensionListScript = `for f in /opt/extensions/*; do
  if [ -f "$f" ] && [ -x "$f" ]; then echo "${f##*/}"; fi
done`
	imageExtensionStartScript = `exec </dev/null
(
` + extensionStartLoop + `) >>/proc/1/fd/1 2>&1 &
`
)

// startImageExtensions launches the executables found under /opt/extensions in a container-image
// function (image and layers merged), then tells the registry which names are external extensions.
// Images without /bin/sh cannot be probed; the registry then treats every extension as unclassified.
func (b *InMemoryBackend) startImageExtensions(ctx context.Context, srv *runtimeServer, containerID string) {
	res, err := b.docker.Exec(ctx, containerID, []string{shellPath, "-c", imageExtensionListScript})
	if err != nil || res.ExitCode != 0 {
		logger.Load(ctx).
			WarnContext(ctx, "lambda: cannot list /opt/extensions in the image; extensions are not started",
				"error", err, "exitCode", res.ExitCode)
		srv.exts.releaseInit()

		return
	}

	names := strings.Fields(res.Stdout)
	srv.exts.setExternal(names)

	if len(names) == 0 {
		return
	}

	start, startErr := b.docker.Exec(ctx, containerID, []string{shellPath, "-c", imageExtensionStartScript})
	if startErr != nil || start.ExitCode != 0 {
		logger.Load(ctx).WarnContext(ctx, "lambda: starting image extensions failed",
			"error", startErr, "exitCode", start.ExitCode, "stderr", start.Stderr)
	}
}
