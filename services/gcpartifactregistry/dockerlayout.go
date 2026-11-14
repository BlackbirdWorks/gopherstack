package gcpartifactregistry

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

// snapshotLayout 2 names registry repositories project/location/repository; earlier snapshots omit it and
// used project/repository, which made Docker repository ids unique per project.
const snapshotLayout = 2

const migrateDir = ".migrating"

// legacyDockerLocations maps project/repository to the location of the Docker repository that owned the
// legacy registry path (the first by name if several somehow exist).
func legacyDockerLocations(repos map[string]*repoRec) map[string]repoRef {
	out := map[string]repoRef{}

	for _, name := range collections.SortedKeys(repos) {
		if repos[name].pb.GetFormat() != artifactregistrypb.Repository_DOCKER {
			continue
		}

		ref, err := parseRepo(name)
		if err != nil {
			continue
		}

		if _, dup := out[ref.clientPath()]; !dup {
			out[ref.clientPath()] = ref
		}
	}

	return out
}

// migrateLedger rewrites legacy project/repository/image ledger keys; keys of unknown repositories stay.
func migrateLedger(l ociregistry.Ledger, owners map[string]repoRef) ociregistry.Ledger {
	out := make(ociregistry.Ledger, len(l))

	for key, entry := range l {
		parts := strings.SplitN(key, "/", minImageSegs)
		if len(parts) == minImageSegs {
			if ref, ok := owners[parts[0]+"/"+parts[1]]; ok {
				key = ref.dockerKey() + "/" + parts[2]
			}
		}

		out[key] = entry
	}

	return out
}

// migrateImageDirs moves <root>/docker/registry/v2/repositories/<project>/<repo> under the repository's
// location. Every legacy directory is parked first so a repository id equal to a location name cannot collide.
func migrateImageDirs(imagesDir string, owners map[string]repoRef) error {
	root := filepath.Join(imagesDir, "docker", "registry", "v2", "repositories")
	park := filepath.Join(root, migrateDir)

	type move struct{ parked, final string }

	var moves []move

	for _, key := range collections.SortedKeys(owners) {
		ref := owners[key]
		old := filepath.Join(root, filepath.FromSlash(key))

		if _, err := os.Stat(old); errors.Is(err, fs.ErrNotExist) {
			continue
		}

		if err := os.MkdirAll(park, 0o750); err != nil {
			return fmt.Errorf("gcpartifactregistry: migrate images: %w", err)
		}

		parked := filepath.Join(park, strconv.Itoa(len(moves)))
		if err := os.Rename(old, parked); err != nil {
			return fmt.Errorf("gcpartifactregistry: migrate images: %w", err)
		}

		moves = append(moves, move{parked: parked, final: filepath.Join(root, filepath.FromSlash(ref.dockerKey()))})
	}

	for _, m := range moves {
		if err := os.MkdirAll(filepath.Dir(m.final), 0o750); err != nil {
			return fmt.Errorf("gcpartifactregistry: migrate images: %w", err)
		}

		if err := os.Rename(m.parked, m.final); err != nil {
			return fmt.Errorf("gcpartifactregistry: migrate images: %w", err)
		}
	}

	if len(moves) > 0 {
		_ = os.Remove(park)
	}

	return nil
}
