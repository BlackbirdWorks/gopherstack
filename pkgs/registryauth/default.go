package registryauth

import (
	"context"
	"sync/atomic"
)

//nolint:gochecknoglobals // process-wide resolver shared by every pull site
var defaultResolver atomic.Pointer[Resolver]

// Configure installs the process-wide resolver built from s.
func Configure(s Settings) error {
	r, err := New(s)
	if err != nil {
		return err
	}

	defaultResolver.Store(r)

	return nil
}

// Default returns the process-wide resolver, creating a credentials-from-docker-config
// only resolver when Configure was never called.
func Default() *Resolver {
	if r := defaultResolver.Load(); r != nil {
		return r
	}

	r, _ := New(Settings{})
	if defaultResolver.CompareAndSwap(nil, r) {
		return r
	}

	return defaultResolver.Load()
}

// Pull pulls ref through the default resolver.
func Pull(ctx context.Context, ref string, pull PullFunc, tag TagFunc) error {
	return Default().Pull(ctx, ref, pull, tag)
}

// ImageTagger is implemented by Docker clients able to alias an image.
type ImageTagger interface {
	ImageTag(ctx context.Context, source, target string) error
}

// TaggerFrom returns a TagFunc backed by client, or nil when it cannot tag.
func TaggerFrom(client any) TagFunc {
	t, ok := client.(ImageTagger)
	if !ok {
		return nil
	}

	return t.ImageTag
}
