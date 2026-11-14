package ocifilestorage

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	handleCache    = 4096
	privilegedPort = 1024
	loopback       = "127.0.0.1"
)

var errHandleCache = errors.New("ocifilestorage: file handles are resolved by the caching handler")

//nolint:gochecknoglobals // go-nfs logs through a package-level logger
var quietNFS sync.Once

// NFSConfig configures the NFS data plane.
type NFSConfig struct {
	Ports *portalloc.Allocator
	// Dir holds file system data; empty creates a temporary directory removed on Close.
	Dir string
}

// NFSPlane keeps one directory per file system and serves each mount target's exports over NFSv3 on a loopback port.
type NFSPlane struct {
	mu       *lockmetrics.RWMutex
	ports    *portalloc.Allocator
	servers  map[string]*mountServer
	views    map[string]*vfs
	root     string
	ownsRoot bool
}

type mountServer struct {
	ln   *trackingListener
	done chan struct{}
	port int
}

// NewNFSPlane prepares the data directory.
func NewNFSPlane(cfg NFSConfig) (*NFSPlane, error) {
	quietNFS.Do(func() { nfs.Log.SetLevel(nfs.PanicLevel) })

	p := &NFSPlane{
		mu: lockmetrics.New("ocifilestorage.nfs"), ports: cfg.Ports, servers: map[string]*mountServer{},
		views: map[string]*vfs{}, root: cfg.Dir,
	}

	if p.root == "" {
		dir, err := os.MkdirTemp("", "gopherstack-ocifilestorage-")
		if err != nil {
			return nil, fmt.Errorf("ocifilestorage: create data directory: %w", err)
		}

		p.root, p.ownsRoot = dir, true
	}

	if err := os.MkdirAll(p.root, dirPerm); err != nil {
		return nil, fmt.Errorf("ocifilestorage: create data directory: %w", err)
	}

	return p, nil
}

func (p *NFSPlane) fsDir(id string) string      { return filepath.Join(p.root, "fs", id) }
func (p *NFSPlane) snapRoot(id string) string   { return filepath.Join(p.root, "snap", id) }
func (p *NFSPlane) snapDir(id, n string) string { return filepath.Join(p.snapRoot(id), n) }

// CreateFS creates the empty directories of a file system; existing data is kept.
func (p *NFSPlane) CreateFS(id string) error {
	for _, d := range []string{p.fsDir(id), p.snapRoot(id)} {
		if err := os.MkdirAll(d, dirPerm); err != nil {
			return err
		}
	}

	return nil
}

// RemoveFS deletes a file system's data and snapshots.
func (p *NFSPlane) RemoveFS(id string) {
	p.dropViews(id)
	_ = os.RemoveAll(p.fsDir(id))
	_ = os.RemoveAll(p.snapRoot(id))
}

// Usage returns the bytes held by a file system and its snapshots.
func (p *NFSPlane) Usage(id string) int64 {
	return treeSize(p.fsDir(id)) + treeSize(p.snapRoot(id))
}

// TakeSnapshot copies the file system tree into its snapshot area.
func (p *NFSPlane) TakeSnapshot(fsID, name string) error {
	return copyTree(p.fsDir(fsID), p.snapDir(fsID, name))
}

// DropSnapshot removes a snapshot copy.
func (p *NFSPlane) DropSnapshot(fsID, name string) { _ = os.RemoveAll(p.snapDir(fsID, name)) }

// CloneFrom populates dstFS from a snapshot of srcFS.
func (p *NFSPlane) CloneFrom(srcFS, snapshot, dstFS string) error {
	if err := p.CreateFS(dstFS); err != nil {
		return err
	}

	return copyTree(p.snapDir(srcFS, snapshot), p.fsDir(dstFS))
}

// ReplicateSnapshot replaces the contents of dstFS with a snapshot of srcFS.
func (p *NFSPlane) ReplicateSnapshot(srcFS, snapshot, dstFS string) error {
	if err := p.CreateFS(dstFS); err != nil {
		return err
	}

	if err := emptyDir(p.fsDir(dstFS)); err != nil {
		return err
	}

	return copyTree(p.snapDir(srcFS, snapshot), p.fsDir(dstFS))
}

// Endpoints maps mount target ids to their listen addresses.
func (p *NFSPlane) Endpoints() map[string]string {
	p.mu.RLock("Endpoints")
	defer p.mu.RUnlock()

	out := make(map[string]string, len(p.servers))
	for id, s := range p.servers {
		out[id] = net.JoinHostPort(loopback, strconv.Itoa(s.port))
	}

	return out
}

// StartMount starts the NFS server of a mount target; a running one is left alone.
func (p *NFSPlane) StartMount(mtID string, exports func(string) []ExportMount) error {
	p.mu.Lock("StartMount")
	defer p.mu.Unlock()

	if _, ok := p.servers[mtID]; ok {
		return nil
	}

	port, err := portalloc.AcquireFrom(p.ports, "ocifilestorage-nfs")
	if err != nil {
		return fmt.Errorf("allocate NFS port: %w", err)
	}

	var lc net.ListenConfig

	raw, err := lc.Listen(context.Background(), "tcp", net.JoinHostPort(loopback, strconv.Itoa(port)))
	if err != nil {
		portalloc.ReleaseTo(p.ports, port)

		return fmt.Errorf("listen on NFS port %d: %w", port, err)
	}

	ln := &trackingListener{Listener: raw, conns: map[net.Conn]struct{}{}}
	h := &nfsHandler{plane: p, mtID: mtID, exports: exports}
	srv := &nfs.Server{Handler: helpers.NewCachingHandler(h, handleCache)}
	ms := &mountServer{ln: ln, port: port, done: make(chan struct{})}

	go func() {
		defer close(ms.done)

		_ = srv.Serve(ln)
	}()

	p.servers[mtID] = ms

	return nil
}

// StopMount stops a mount target's NFS server.
func (p *NFSPlane) StopMount(mtID string) {
	p.mu.Lock("StopMount")
	ms := p.servers[mtID]
	delete(p.servers, mtID)
	p.mu.Unlock()

	p.stop(ms)
}

func (p *NFSPlane) stop(ms *mountServer) {
	if ms == nil {
		return
	}

	_ = ms.ln.Close()
	<-ms.done
	portalloc.ReleaseTo(p.ports, ms.port)
}

func (p *NFSPlane) stopAll() {
	p.mu.Lock("stopAll")
	all := p.servers
	p.servers = map[string]*mountServer{}
	views := p.views
	p.views = map[string]*vfs{}
	p.mu.Unlock()

	for _, ms := range all {
		p.stop(ms)
	}

	for _, v := range views {
		v.close()
	}
}

// StopMounts stops every NFS server but keeps the data.
func (p *NFSPlane) StopMounts() { p.stopAll() }

// Reset stops every server and deletes all data.
func (p *NFSPlane) Reset() {
	p.stopAll()
	_ = emptyDir(p.root)
}

// Close stops every server; a temporary data directory is removed.
func (p *NFSPlane) Close() {
	p.stopAll()

	if p.ownsRoot {
		_ = os.RemoveAll(p.root)
	}
}

func (p *NFSPlane) dropViews(fsID string) {
	p.mu.Lock("dropViews")
	defer p.mu.Unlock()

	for k, v := range p.views {
		if v.fsID == fsID {
			v.close()
			delete(p.views, k)
		}
	}
}

// view returns the shared billy file system for an export and access mode.
func (p *NFSPlane) view(ex ExportMount, ro bool) (*vfs, error) {
	key := ex.ExportID + "|" + strconv.FormatBool(ro)

	p.mu.Lock("view")
	defer p.mu.Unlock()

	if v, ok := p.views[key]; ok {
		return v, nil
	}

	root, err := os.OpenRoot(p.fsDir(ex.FileSystemID))
	if err != nil {
		return nil, err
	}

	if err = os.MkdirAll(p.snapRoot(ex.FileSystemID), dirPerm); err != nil {
		_ = root.Close()

		return nil, err
	}

	v := &vfs{
		fsID: ex.FileSystemID, ro: ro, root: root,
		live:  osfs.New(p.fsDir(ex.FileSystemID), osfs.WithBoundOS()),
		snaps: osfs.New(p.snapRoot(ex.FileSystemID), osfs.WithBoundOS()),
		stat:  ex,
	}
	p.views[key] = v

	return v, nil
}

// trackingListener remembers accepted connections so Close can end them.
type trackingListener struct {
	net.Listener
	conns map[net.Conn]struct{}
	mu    sync.Mutex
	shut  bool
}

type trackedConn struct {
	net.Conn
	ln *trackingListener
}

func (c *trackedConn) Close() error {
	c.ln.mu.Lock()
	delete(c.ln.conns, c.Conn)
	c.ln.mu.Unlock()

	return c.Conn.Close()
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.shut {
		_ = c.Close()

		return nil, net.ErrClosed
	}

	l.conns[c] = struct{}{}

	return &trackedConn{Conn: c, ln: l}, nil
}

func (l *trackingListener) Close() error {
	err := l.Listener.Close()

	l.mu.Lock()
	l.shut = true

	for c := range l.conns {
		_ = c.Close()
	}

	l.conns = map[net.Conn]struct{}{}
	l.mu.Unlock()

	return err
}

// nfsHandler authorises mounts against the live exports of one mount target.
type nfsHandler struct {
	plane   *NFSPlane
	exports func(string) []ExportMount
	mtID    string
}

func clientAddr(conn net.Conn) (netip.Addr, int) {
	ap, err := netip.ParseAddrPort(conn.RemoteAddr().String())
	if err != nil {
		return netip.Addr{}, 0
	}

	return ap.Addr().Unmap(), int(ap.Port())
}

// matchOption returns the first export option whose source contains the client.
func matchOption(opts []clientOptions, ip netip.Addr) (clientOptions, bool) {
	for _, o := range opts {
		if p, err := netip.ParsePrefix(o.Source); err == nil && p.Contains(ip) {
			return o, true
		}

		if a, err := netip.ParseAddr(o.Source); err == nil && a.Unmap() == ip {
			return o, true
		}
	}

	return clientOptions{}, false
}

// refused is handed back with an error status: go-nfs derives a root handle from it before checking the status.
//
//nolint:gochecknoglobals // stateless placeholder
var refused billy.Filesystem = &vfs{}

func (h *nfsHandler) Mount(
	_ context.Context,
	conn net.Conn,
	req nfs.MountRequest,
) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	want := path.Clean("/" + string(req.Dirpath))
	ip, port := clientAddr(conn)

	for _, ex := range h.exports(h.mtID) {
		if ex.Path != want {
			continue
		}

		opt, ok := matchOption(ex.Options, ip)
		if !ok {
			return nfs.MountStatusErrAcces, refused, nil
		}

		if deref(opt.RequirePrivilegedSourcePort, false) && port >= privilegedPort {
			return nfs.MountStatusErrAcces, refused, nil
		}

		v, err := h.plane.view(ex, ex.ForceRO || opt.Access == accessRO)
		if err != nil {
			return nfs.MountStatusErrServerFault, refused, nil
		}

		return nfs.MountStatusOk, v, []nfs.AuthFlavor{nfs.AuthFlavorNull, nfs.AuthFlavorUnix}
	}

	return nfs.MountStatusErrNoEnt, refused, nil
}

func (h *nfsHandler) Change(f billy.Filesystem) billy.Change {
	if c, ok := f.(*vfs); ok && !c.ro {
		return c
	}

	return nil
}

func (h *nfsHandler) FSStat(_ context.Context, f billy.Filesystem, s *nfs.FSStat) error {
	v, ok := f.(*vfs)
	if !ok {
		return nil
	}

	if v.stat.MaxFsStatBytes > 0 {
		used := uint64(max(0, h.plane.Usage(v.fsID)))
		s.TotalSize = uint64(v.stat.MaxFsStatBytes)
		s.FreeSize = s.TotalSize - min(s.TotalSize, used)

		if !v.ro {
			s.AvailableSize = s.FreeSize
		}
	}

	if v.stat.MaxFsStatFiles > 0 {
		s.TotalFiles = uint64(v.stat.MaxFsStatFiles)
		s.FreeFiles = s.TotalFiles

		if !v.ro {
			s.AvailableFiles = s.FreeFiles
		}
	}

	return nil
}

func (h *nfsHandler) ToHandle(billy.Filesystem, []string) []byte { return nil }

func (h *nfsHandler) FromHandle([]byte) (billy.Filesystem, []string, error) {
	return nil, nil, errHandleCache
}

func (h *nfsHandler) InvalidateHandle(billy.Filesystem, []byte) error { return nil }

func (h *nfsHandler) HandleLimit() int { return handleCache }
