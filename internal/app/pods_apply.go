package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/pods"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var errPodRolledBack = errors.New("restore failed; previous state was restored")

// podInvalidError marks a pod rejected before any state was touched.
type podInvalidError struct{ err error }

func (e podInvalidError) Error() string { return "invalid pod: " + e.err.Error() }
func (e podInvalidError) Unwrap() error { return e.err }

func podApplyHTTPError(err error) error {
	if _, ok := errors.AsType[podInvalidError](err); ok {
		return echo.NewHTTPError(http.StatusUnprocessableEntity, err.Error())
	}

	return echo.NewHTTPError(http.StatusInternalServerError, "restore failed: "+err.Error())
}

// validateSnaps rejects any section that is not well-formed JSON.
func validateSnaps(snaps map[string][]byte) error {
	for n, d := range snaps {
		if !json.Valid(d) {
			return podInvalidError{fmt.Errorf("%w: service %q section is not valid JSON", pods.ErrCorrupt, n)}
		}
	}

	return nil
}

// mergeSnaps rewrites snaps so each service carries current state plus the pod's
// account/region scopes. Services without account/region records are replaced.
func mergeSnaps(current, snaps map[string][]byte) (map[string][]byte, error) {
	out := make(map[string][]byte, len(snaps))

	for n, d := range snaps {
		merged, _, err := pods.MergeScoped(current[n], d)
		if err != nil {
			return nil, podInvalidError{fmt.Errorf("service %q: %w", n, err)}
		}

		out[n] = merged
	}

	return out, nil
}

func (a *podAPI) resetAll() {
	for _, svc := range a.services {
		if r, ok := svc.(service.Resettable); ok {
			r.Reset()
		}
	}
}

// apply restores snaps atomically: everything is validated and merged first, and a
// failed restore rolls every service back to its pre-load state.
func (a *podAPI) apply(ctx context.Context, strategy string, merge bool, snaps map[string][]byte) error {
	if err := validateSnaps(snaps); err != nil {
		return err
	}

	backup := a.manager.ExportAll()

	if merge {
		var err error
		if snaps, err = mergeSnaps(backup, snaps); err != nil {
			return err
		}
	}

	if strategy == podStrategyOverwrite && !merge {
		a.resetAll()
	}

	err := a.manager.ImportAll(ctx, snaps)
	if err == nil {
		return nil
	}

	a.resetAll()

	if rbErr := a.manager.ImportAll(ctx, backup); rbErr != nil {
		return fmt.Errorf("%w; rollback also failed: %w", err, rbErr)
	}

	return fmt.Errorf("%w: %w", errPodRolledBack, err)
}
