package azurefunctions

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// SwapContent exchanges the deployed site content of two apps (or an app and its slot) and restarts both.
func (b *InMemoryBackend) SwapContent(first, second string) error {
	b.mu.Lock("SwapContent")

	x, okX := b.apps[strs.Fold(first)]
	y, okY := b.apps[strs.Fold(second)]

	if !okX || !okY {
		b.mu.Unlock()

		return ErrAppNotFound
	}

	x.Zip, y.Zip = y.Zip, x.Zip
	x.startErr, y.startErr = "", ""
	oldX, oldY := b.takeBoxLocked(x), b.takeBoxLocked(y)
	b.mu.Unlock()

	b.teardown(oldX)
	b.teardown(oldY)

	return nil
}

// WarmUp starts the app's host and waits until it answers, as a slot swap does before routing traffic. Apps that
// cannot be served here (stopped, no content, no container runtime) are skipped.
func (b *InMemoryBackend) WarmUp(ctx context.Context, name string) error {
	b.mu.RLock("WarmUp")
	a, err := b.appLocked(name)

	if err != nil {
		b.mu.RUnlock()

		return err
	}

	skip := a.Stopped || !a.servable() || b.runtime == nil
	b.mu.RUnlock()

	if skip {
		return nil
	}

	if _, err = b.ensureStarted(ctx, name); err != nil && !errors.Is(err, errNoRuntime) &&
		!errors.Is(err, errUnsupportedRT) {
		return err
	}

	return nil
}
