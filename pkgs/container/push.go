package container

import (
	"context"
	"errors"
	"fmt"
)

// ErrPushUnsupported is returned by TagAndPush when the underlying client cannot tag and push images.
var ErrPushUnsupported = errors.New("image push is not supported by this runtime client")

// ImagePusher is implemented by runtimes that can publish a local image to a registry.
type ImagePusher interface {
	// TagAndPush tags the local image source as target, pushes target, and returns the pushed digest when known.
	TagAndPush(ctx context.Context, source, target string) (string, error)
}

// PushAPIClient is implemented by clients that can tag and push images.
type PushAPIClient interface {
	ImageTag(ctx context.Context, source, target string) error
	ImagePush(ctx context.Context, ref string) (string, error)
}

// TagAndPush tags the local image source as target and pushes it.
func (r *DockerRuntime) TagAndPush(ctx context.Context, source, target string) (string, error) {
	api, ok := r.docker.(PushAPIClient)
	if !ok {
		return "", ErrPushUnsupported
	}

	if err := api.ImageTag(ctx, source, target); err != nil {
		return "", fmt.Errorf("tag %q as %q: %w", source, target, err)
	}

	digest, err := api.ImagePush(ctx, target)
	if err != nil {
		return "", fmt.Errorf("push %q: %w", target, err)
	}

	return digest, nil
}
