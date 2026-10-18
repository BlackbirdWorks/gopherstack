package globalaccelerator

import (
	"time"

	"github.com/labstack/echo/v5"
)

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// AcceleratorCount returns the stored accelerator count.
func (b *InMemoryBackend) AcceleratorCount() int {
	b.mu.RLock("AcceleratorCount")
	defer b.mu.RUnlock()

	return b.accelerators.Len()
}

// WriteErrorForTest renders err through the handler's error path.
func WriteErrorForTest(c *echo.Context, err error) error {
	return (&Handler{}).writeError(c, err)
}
