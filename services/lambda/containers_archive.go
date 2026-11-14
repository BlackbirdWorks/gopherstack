package lambda

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	codeArchiveAt = lambdaTaskRoot
	optArchiveAt  = "/opt"
)

// deliversArchives reports whether the runtime has no host bind mounts and takes code as archives (Kubernetes).
func (b *InMemoryBackend) deliversArchives() bool {
	d, ok := b.docker.(container.ArchiveDeliverer)

	return ok && d.DeliversArchives()
}

// startArchiveContainer starts the function through a runtime that unpacks code and layers itself.
// Hot reload, EFS mounts, extensions and LAMBDA_DOCKER_FLAGS need host paths and are refused or skipped.
func (b *InMemoryBackend) startArchiveContainer(
	ctx context.Context,
	fn *FunctionConfiguration,
	env []string,
) (string, error) {
	if len(fn.FileSystemConfigs) > 0 {
		return "", fmt.Errorf(
			"%w: EFS file systems need host mounts, unavailable on this container runtime",
			ErrLambdaUnavailable,
		)
	}

	if b.isHotReloadFunction(fn) {
		return "", fmt.Errorf(
			"%w: hot reload needs host mounts, unavailable on this container runtime",
			ErrLambdaUnavailable,
		)
	}

	spec := container.Spec{
		Name:        fmt.Sprintf("gopherstack-lambda-%s-%s", fn.FunctionName, uuid.New().String()[:8]),
		Env:         env,
		MemoryBytes: int64(fn.MemorySize) * bytesPerMiB,
	}

	if err := b.fillCode(ctx, fn, &spec); err != nil {
		return "", err
	}

	for _, l := range b.collectLayerEntries(fn) {
		spec.Archives = append(spec.Archives, container.Archive{Dest: optArchiveAt, Zip: l.zipData})
	}

	spec.OCIRuntime = b.ociRuntime

	return b.docker.CreateAndStart(ctx, spec)
}

// fillCode sets the image and code archive for fn's package type.
func (b *InMemoryBackend) fillCode(ctx context.Context, fn *FunctionConfiguration, spec *container.Spec) error {
	if fn.PackageType != PackageTypeZip {
		spec.Image = fn.ImageURI

		return nil
	}

	baseImage := baseImageForRuntime(fn.Runtime)
	if baseImage == "" {
		return fmt.Errorf("%w: unsupported runtime %q", ErrLambdaUnavailable, fn.Runtime)
	}

	zipData, err := b.resolveZipData(ctx, fn)
	if err != nil {
		return err
	}

	spec.Image = baseImage
	spec.Archives = append(spec.Archives, container.Archive{Dest: codeArchiveAt, Zip: zipData})

	if strings.HasPrefix(fn.Runtime, "provided") {
		spec.Entrypoint = []string{"/var/task/bootstrap"}
	} else if fn.Handler != "" {
		spec.Cmd = []string{fn.Handler}
	}

	return nil
}
