// Package buildcheck holds pre-flight checks shared by the test suites
// under test/ that run the built bin/gopherstack-linux binary in a
// container (test/terraform and test/integration).
package buildcheck

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// repoRoot mirrors the relative path used by both test/terraform and
// test/integration to reach the repo root: both packages live one
// directory below test/, so `go test`'s package-directory CWD puts the
// repo root at the same relative depth for either caller.
const repoRoot = "../.."

// ErrStaleBinary means bin/gopherstack-linux predates a .go file that builds
// into it. Dockerfile.test serves that gitignored binary as-is (gopherstack-ydop):
// a stale one makes container-backed tests silently validate old code and
// report a false result -- a real fix looks like "still broken" and a real
// revert looks like "no change".
var ErrStaleBinary = errors.New("bin/gopherstack-linux is stale")

// CheckFreshness fails loudly when any .go file under services/, pkgs/, or
// the root package is newer than the already-stat'd bin/gopherstack-linux.
//
// Skipped in CI: the build job for these suites compiles the binary fresh
// from the exact commit under test, moments before this check would run.
// That guarantee comes from the job graph (ci.yml's `needs: [build]`), not
// from filesystem mtimes -- and mtimes are not a reliable signal across an
// artifact upload/download round-trip, so comparing them here could turn
// into exactly the permanent CI failure this check must not become.
func CheckFreshness(logger *slog.Logger, binInfo fs.FileInfo) error {
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		logger.Info("skipping bin/gopherstack-linux freshness check under CI; " +
			"the build job compiles it fresh from this exact checkout")

		return nil
	}

	newestPath, newestMod, err := newestGoFile()
	if err != nil {
		return fmt.Errorf("checking bin/gopherstack-linux freshness: %w", err)
	}

	if newestPath == "" || !newestMod.After(binInfo.ModTime()) {
		return nil
	}

	return fmt.Errorf(
		"%w: %s (modified %s) is newer than bin/gopherstack-linux (built %s). "+
			"Tests would silently run against old code. Rebuild with:\n\n"+
			"    make build-linux",
		ErrStaleBinary,
		newestPath,
		newestMod.Format(time.RFC3339),
		binInfo.ModTime().Format(time.RFC3339),
	)
}

// newestGoFile returns the path and mtime of the most recently modified .go
// file under services/, pkgs/, and the root package -- the source that
// actually builds into bin/gopherstack-linux.
func newestGoFile() (string, time.Time, error) {
	var (
		newestPath string
		newestMod  time.Time
	)

	consider := func(path string, info fs.FileInfo) {
		if info.IsDir() || filepath.Ext(info.Name()) != ".go" {
			return
		}

		if info.ModTime().After(newestMod) {
			newestMod = info.ModTime()
			newestPath = path
		}
	}

	for _, dir := range []string{filepath.Join(repoRoot, "services"), filepath.Join(repoRoot, "pkgs")} {
		walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			info, infoErr := d.Info()
			if infoErr != nil {
				return infoErr
			}

			consider(path, info)

			return nil
		})
		if walkErr != nil {
			return "", time.Time{}, fmt.Errorf("walking %s: %w", dir, walkErr)
		}
	}

	entries, err := os.ReadDir(repoRoot)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("reading %s: %w", repoRoot, err)
	}

	for _, e := range entries {
		info, infoErr := e.Info()
		if infoErr != nil {
			return "", time.Time{}, fmt.Errorf("reading %s: %w", filepath.Join(repoRoot, e.Name()), infoErr)
		}

		consider(filepath.Join(repoRoot, e.Name()), info)
	}

	return newestPath, newestMod, nil
}
