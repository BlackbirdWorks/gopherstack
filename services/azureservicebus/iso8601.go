package azureservicebus

import (
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
)

// ErrInvalidISO8601Duration is returned by parseISO8601Duration for a string
// that doesn't match the supported PnDTnHnMnS/PTnHnMnS subset.
var ErrInvalidISO8601Duration = errors.New("azureservicebus: invalid ISO 8601 duration")

// zeroISO8601Duration is the shortest valid ISO 8601 representation of "no
// duration", used for a non-positive input to formatISO8601Duration.
const zeroISO8601Duration = iso8601.Zero

// parseISO8601Duration and formatISO8601Duration wrap pkgs/iso8601 (moved
// there when services/azurearm (M9) became a second caller needing the
// exact same LockDuration/DefaultMessageTimeToLive parsing, mirroring
// pkgs/odatatable's earlier extraction for the same reason) -- see that
// package for the parsing/formatting logic itself. This wrapper only
// exists to preserve this package's own ErrInvalidISO8601Duration sentinel
// for existing call sites and tests.
func parseISO8601Duration(s string) (time.Duration, error) {
	d, err := iso8601.Parse(s)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidISO8601Duration, err)
	}

	return d, nil
}

func formatISO8601Duration(d time.Duration) string {
	return iso8601.Format(d)
}
