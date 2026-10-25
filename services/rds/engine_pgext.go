package rds

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

//go:embed pgext
var pgExtFS embed.FS

const (
	pgExtMount       = "/gs-ext"
	pgExtAWSMount    = pgExtMount + "/aws"
	pgExtVectorMount = pgExtMount + "/vector"
	pgExtDoneMarker  = "done"
	pgExtPollEvery   = 250 * time.Millisecond
	pgExtExtractWait = 5 * time.Minute
	pgExtDirMode     = 0o755
	pgExtFileMode    = 0o644

	pgPostGISImage = "postgis/postgis"
	pgPostGISTag   = "-3.5"
	pgVectorImage  = "pgvector/pgvector"
	pgVectorTag    = "0.8.6"

	envCalloutAddr = "GS_CALLOUT_ADDR"
	envCalloutKey  = "GS_CALLOUT_TOKEN"
	envAWSHelper   = "GS_AWS_HELPER"

	pgExtPrelude = `set -e
rm -f /docker-entrypoint-initdb.d/10_postgis.sh
v=/usr/share/postgresql/$PG_MAJOR/extension
cp ` + pgExtAWSMount + `/*.sql ` + pgExtAWSMount + `/*.control "$v"/
if [ -d ` + pgExtVectorMount + ` ]; then
  cp ` + pgExtVectorMount + `/vector.so /usr/lib/postgresql/$PG_MAJOR/lib/
  cp ` + pgExtVectorMount + `/vector*.sql ` + pgExtVectorMount + `/vector.control "$v"/
fi
`
	pgExtScript = pgExtPrelude + `if [ $# -eq 0 ]; then set -- postgres; fi
exec docker-entrypoint.sh "$@"`

	pgVectorExtractScript = `set -e
cp /usr/lib/postgresql/$PG_MAJOR/lib/vector.so /out/
cp /usr/share/postgresql/$PG_MAJOR/extension/vector* /out/
chmod -R a+rwX /out
touch /out/` + pgExtDoneMarker
)

var errPgExtExtract = errors.New("pgvector extraction did not complete")

// pgExt prepares the host directories mounted into extension-enabled postgres containers.
type pgExt struct {
	dirs map[string]string
	mu   sync.Mutex
}

func newPgExt() *pgExt { return &pgExt{dirs: make(map[string]string)} }

func pgExtImage(major string) string { return pgPostGISImage + ":" + major + pgPostGISTag }

func pgVectorSourceImage(major string) string {
	return pgVectorImage + ":" + pgVectorTag + "-pg" + major + "-bookworm"
}

func pgExtRoot() (string, error) {
	h := sha256.New()

	entries, err := pgExtFS.ReadDir("pgext")
	if err != nil {
		return "", fmt.Errorf("read embedded extensions: %w", err)
	}

	for _, ent := range entries {
		data, rerr := pgExtFS.ReadFile("pgext/" + ent.Name())
		if rerr != nil {
			return "", fmt.Errorf("read embedded %s: %w", ent.Name(), rerr)
		}

		h.Write([]byte(ent.Name()))
		h.Write(data)
	}

	return filepath.Join(os.TempDir(), "gopherstack-rds-pgext-"+hex.EncodeToString(h.Sum(nil))[:12]), nil
}

func writePgExtAWS(dir string) error {
	if err := os.MkdirAll(dir, pgExtDirMode); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}

	entries, err := pgExtFS.ReadDir("pgext")
	if err != nil {
		return fmt.Errorf("read embedded extensions: %w", err)
	}

	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), ".pl") {
			continue
		}

		data, rerr := pgExtFS.ReadFile("pgext/" + ent.Name())
		if rerr != nil {
			return fmt.Errorf("read embedded %s: %w", ent.Name(), rerr)
		}

		if werr := os.WriteFile(filepath.Join(dir, ent.Name()), data, pgExtFileMode); werr != nil {
			return fmt.Errorf("write %s: %w", ent.Name(), werr)
		}
	}

	return nil
}

// prepare returns the host directory holding the aws_* scripts and the pgvector files for major.
func (p *pgExt) prepare(ctx context.Context, rt EngineRuntime, major string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if d, ok := p.dirs[major]; ok {
		return d, nil
	}

	root, err := pgExtRoot()
	if err != nil {
		return "", err
	}

	if err = writePgExtAWS(filepath.Join(root, "aws")); err != nil {
		return "", err
	}

	vec := filepath.Join(root, "vector-"+major)
	if _, serr := os.Stat(filepath.Join(vec, pgExtDoneMarker)); serr != nil {
		if err = extractPgVector(ctx, rt, major, vec); err != nil {
			return "", err
		}
	}

	p.dirs[major] = root

	return root, nil
}

func extractPgVector(ctx context.Context, rt EngineRuntime, major, dst string) error {
	tmp, err := os.MkdirTemp(filepath.Dir(dst), "extract-"+major+"-")
	if err != nil {
		return fmt.Errorf("create extract dir: %w", err)
	}

	id, err := rt.CreateAndStart(ctx, container.Spec{
		Image:      pgVectorSourceImage(major),
		Entrypoint: []string{"sh", "-c", pgVectorExtractScript},
		Mounts:     []string{tmp + ":/out"},
	})
	if err != nil {
		_ = os.RemoveAll(tmp)

		return fmt.Errorf("start pgvector extractor: %w", err)
	}

	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), engineStopTimeout)
		defer cancel()

		_ = rt.StopAndRemove(cctx, id)
	}()

	if err = waitForFile(ctx, filepath.Join(tmp, pgExtDoneMarker)); err != nil {
		_ = os.RemoveAll(tmp)

		return err
	}

	if err = os.Rename(tmp, dst); err != nil {
		if _, serr := os.Stat(filepath.Join(dst, pgExtDoneMarker)); serr == nil {
			_ = os.RemoveAll(tmp)

			return nil
		}

		return fmt.Errorf("install pgvector files: %w", err)
	}

	return nil
}

func waitForFile(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, pgExtExtractWait)
	defer cancel()

	tick := time.NewTicker(pgExtPollEvery)
	defer tick.Stop()

	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return errPgExtExtract
		case <-tick.C:
		}
	}
}

func pgMajor(version string) string {
	return pickVersion(version, defaultPostgresMajor, versionSegmentsMajor, "13", "14", "15", "16", "17")
}

func (e *dbEngine) pgExtEnabled(lb *liveDB) bool {
	return e.cfg.PostgresExtensions && lb.kind == enginePostgres
}

// preparePgExt readies the extension mounts for lb before its container is created.
func (e *dbEngine) preparePgExt(ctx context.Context, lb *liveDB) error {
	if !e.pgExtEnabled(lb) {
		return nil
	}

	dir, err := e.ext.prepare(ctx, e.cfg.Runtime, pgMajor(lb.version))
	if err != nil {
		return err
	}

	lb.extDir.Store(&dir)

	return e.startCallout()
}

// applyPgExt rewrites a postgres spec to the extension image and wires the aws_* callout into it.
func (e *dbEngine) applyPgExt(s *container.Spec, lb *liveDB) {
	dir := lb.extDir.Load()
	if !e.pgExtEnabled(lb) || dir == nil {
		return
	}

	major := pgMajor(lb.version)
	s.Image = pgExtImage(major)
	s.Mounts = append(s.Mounts,
		filepath.Join(*dir, "aws")+":"+pgExtAWSMount+":ro",
		filepath.Join(*dir, "vector-"+major)+":"+pgExtVectorMount+":ro")

	if tok := e.calloutToken(lb); tok != "" {
		helper, _ := pgExtFS.ReadFile("pgext/gsaws.pl")
		s.Env = append(s.Env,
			envAWSHelper+"="+string(helper),
			envCalloutAddr+"="+e.calloutAddr(),
			envCalloutKey+"="+tok)
	}

	if len(s.Entrypoint) == 0 {
		s.Entrypoint = append([]string{"sh", "-c", pgExtScript, "pg-ext"}, s.Cmd...)
		s.Cmd = nil

		return
	}

	// replica entrypoint: sh -c <script> pg-replica ...; splice the prelude in front of the script
	s.Entrypoint[2] = pgExtPrelude + s.Entrypoint[2]
}
