package codebuild

import (
	"fmt"
	"os"
	"path"
)

type secondarySource struct {
	src          ProjectSource
	id           string
	hostDir      string
	containerDir string
	version      string
}

// prepareSecondarySources creates one workspace per secondary source and returns its container bind mount.
func (r *buildRunner) prepareSecondarySources() ([]string, error) {
	var mounts []string

	for _, src := range r.build.SecondarySources {
		if src.SourceIdentifier == "" {
			continue
		}

		dir, err := os.MkdirTemp(r.cfg.WorkDir, "codebuild-src-")
		if err != nil {
			return nil, fmt.Errorf("create workspace for %s: %w", src.SourceIdentifier, err)
		}

		s := secondarySource{
			src: src, id: src.SourceIdentifier, hostDir: dir,
			containerDir: path.Join(path.Dir(r.srcDir), src.SourceIdentifier),
			version:      r.secondaryVersion(src.SourceIdentifier),
		}
		r.secondary = append(r.secondary, s)
		mounts = append(mounts, dir+":"+s.containerDir)
	}

	return mounts, nil
}

func (r *buildRunner) secondaryVersion(id string) string {
	for _, v := range r.build.SecondarySourceVersions {
		if v.SourceIdentifier == id {
			return v.SourceVersion
		}
	}

	return ""
}

func (r *buildRunner) fetchSecondarySources() error {
	for i := range r.secondary {
		s := &r.secondary[i]
		if err := r.fetchSourceInto(r.ctx, &s.src, s.version, s.hostDir, false); err != nil {
			return fmt.Errorf("secondary source %s: %w", s.id, err)
		}
	}

	return nil
}
