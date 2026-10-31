package mysqlengine

import (
	"context"
	"maps"
	"os"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const adoptReadyWait = 30 * time.Second

func (e *Engine) dumpDirLabel() string { return "gopherstack." + e.cfg.Service + ".dumpdir" }
func (e *Engine) shapeLabel() string   { return "gopherstack." + e.cfg.Service + ".shape" }

func (s *Server) shape() string {
	return s.spec.Image + "|" + strconv.FormatInt(s.spec.MemoryBytes, 10) + "|" + strconv.FormatInt(s.spec.NanoCPUs, 10)
}

func (s *Server) ownerLabels() map[string]string {
	l := map[string]string{}

	if s.spec.Resource != "" {
		l = container.ResourceLabels(s.e.cfg.Service, s.spec.Resource)
		l[s.e.dumpDirLabel()] = s.dir
		l[s.e.shapeLabel()] = s.shape()
	}

	maps.Copy(l, s.spec.Labels)

	return l
}

func (s *Server) adopt(ctx context.Context) bool {
	return s.adoptFrom(ctx, container.ClaimOrphan)
}

// adoptFrom takes over the running container a crashed gopherstack left for this server; false means launch fresh.
func (s *Server) adoptFrom(
	ctx context.Context,
	claim func(service, resource string) (container.ManagedContainer, bool),
) bool {
	if !s.spec.Adopt || s.spec.Resource == "" || s.spec.Fresh {
		return false
	}

	oc, ok := claim(s.e.cfg.Service, s.spec.Resource)
	if !ok {
		return false
	}

	dir, port := oc.Labels[s.e.dumpDirLabel()], oc.Ports[mysqlPort]
	if !s.adoptable(oc, dir, port) {
		s.discard(ctx, oc.ID)

		return false
	}

	if pool := s.e.cfg.Ports; pool != nil && pool.Reserve(port, s.e.cfg.Service) != nil {
		s.discard(ctx, oc.ID)

		return false
	}

	old := s.dir

	s.mu.Lock()
	s.dir, s.port, s.ctrNm, s.ctrID = dir, port, oc.Name, oc.ID
	s.mu.Unlock()

	wctx, cancel := context.WithTimeout(ctx, adoptReadyWait)
	defer cancel()

	if err := s.waitReady(wctx); err != nil {
		s.e.cfg.Logger.WarnContext(ctx, "mysqlengine: orphaned container unusable, launching fresh",
			"server", s.spec.ID, "container", oc.ID, "error", err)
		s.discard(ctx, oc.ID)

		s.mu.Lock()
		s.dir, s.ctrID, s.port = old, "", 0
		s.mu.Unlock()

		portalloc.ReleaseTo(s.e.cfg.Ports, port)

		return false
	}

	_ = os.RemoveAll(old)

	s.e.cfg.Logger.InfoContext(ctx, "mysqlengine: adopted container from an earlier run",
		"server", s.spec.ID, "container", oc.ID)

	return true
}

func (s *Server) adoptable(oc container.ManagedContainer, dir string, port int) bool {
	if oc.Labels[s.e.shapeLabel()] != s.shape() || port == 0 || dir == "" {
		return false
	}

	st, err := os.Stat(dir)

	return err == nil && st.IsDir()
}

func (s *Server) discard(ctx context.Context, id string) {
	if err := s.e.cfg.Runtime.StopAndRemove(ctx, id); err != nil {
		s.e.cfg.Logger.WarnContext(ctx, "mysqlengine: orphan removal failed", "container", id, "error", err)
	}
}
