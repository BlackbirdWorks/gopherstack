// Package redisdockertest provides an in-memory container runtime and control connection for exercising
// redisdocker groups without Docker.
package redisdockertest

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/redisdocker"
)

// Cmd is one control command a group sent to a node.
type Cmd struct {
	Addr string
	Args []any
}

// Fake is a runtime and control plane that records everything and answers like a healthy cluster.
type Fake struct {
	tlsSeen map[string]bool
	specs   []container.Spec
	removed []string
	cmds    []Cmd
	mu      sync.Mutex
}

// New returns a Fake and an Engine wired to it.
func New(t *testing.T, base portRange) (*Fake, *redisdocker.Engine) {
	t.Helper()

	alloc, err := portalloc.New(base.Start, base.End)
	require.NoError(t, err)

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	f := &Fake{tlsSeen: map[string]bool{}}
	eng := redisdocker.New(redisdocker.Config{
		Runtime: f, Ports: alloc, CA: ca, Host: "127.0.0.1", Label: "fake", TempDir: t.TempDir(),
		Dial: func(tg redisdocker.Target) redisdocker.Admin { return &admin{target: tg, f: f} },
		FetchRDB: func(_ context.Context, tg redisdocker.Target) ([]byte, error) {
			return []byte("REDIS0011" + tg.Addr), nil
		},
	})

	return f, eng
}

type portRange struct{ Start, End int }

// Ports returns a port range for the fake's allocator.
func Ports(start, end int) portRange { return portRange{Start: start, End: end} }

// CreateAndStart implements redisdocker.Runtime.
func (f *Fake) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.specs = append(f.specs, spec)

	if f.tlsSeen == nil {
		f.tlsSeen = map[string]bool{}
	}

	for _, m := range spec.Mounts {
		dir, _, _ := strings.Cut(m, ":")
		dir = filepath.Clean(dir)

		if !strings.HasPrefix(dir, filepath.Clean(os.TempDir())+string(filepath.Separator)) {
			continue
		}

		if _, err := os.Stat(filepath.Join(dir, "tls", "tls.key")); err == nil {
			f.tlsSeen[spec.Name] = true
		}
	}

	return "c" + strconv.Itoa(len(f.specs)), nil
}

// StopAndRemove implements redisdocker.Runtime.
func (f *Fake) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

// Exec implements redisdocker.Runtime; it answers `hostname -i`.
func (f *Fake) Exec(_ context.Context, id string, _ []string) (container.ExecResult, error) {
	return container.ExecResult{Stdout: "10.0.0." + strings.TrimPrefix(id, "c") + " \n"}, nil
}

// Specs returns the containers started so far.
func (f *Fake) Specs() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...)
}

// Removed returns the IDs of removed containers.
func (f *Fake) Removed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.removed...)
}

// CertsPresentAtStart reports whether the named container's TLS files existed when it started.
func (f *Fake) CertsPresentAtStart(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.tlsSeen[name]
}

// Cmds returns the control commands whose leading arguments equal prefix.
func (f *Fake) Cmds(prefix ...string) []Cmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []Cmd

	for _, c := range f.cmds {
		if len(c.Args) < len(prefix) {
			continue
		}

		match := true

		for i, p := range prefix {
			if fmt.Sprint(c.Args[i]) != p {
				match = false
			}
		}

		if match {
			out = append(out, c)
		}
	}

	return out
}

// indexOf maps a published host:port to the 1-based container index.
func (f *Fake) indexOf(addr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, port, _ := net.SplitHostPort(addr)

	for i, sp := range f.specs {
		if len(sp.Ports) == 1 && strings.HasSuffix(sp.Ports[0], ":"+port+":"+port) {
			return i + 1
		}
	}

	return 0
}

// components groups container indexes joined by CLUSTER MEET into gossip components.
func (f *Fake) components() func(int) int {
	parent := map[int]int{}

	var find func(int) int

	find = func(x int) int {
		if p, ok := parent[x]; ok && p != x {
			parent[x] = find(p)

			return parent[x]
		}

		parent[x] = x

		return x
	}

	for _, m := range f.Cmds("CLUSTER", "MEET") {
		ip := fmt.Sprint(m.Args[2])
		oct, _ := strconv.Atoi(ip[strings.LastIndex(ip, ".")+1:])
		parent[find(f.indexOf(m.Addr))] = find(oct)
	}

	return find
}

// knownBy counts the nodes in addr's gossip component.
func (f *Fake) knownBy(addr string) int {
	find := f.components()
	me := find(f.indexOf(addr))
	n := 0

	for i := 1; i <= len(f.Specs()); i++ {
		if find(i) == me {
			n++
		}
	}

	return n
}

// replicasIn counts the CLUSTER REPLICATE commands issued inside addr's gossip component.
func (f *Fake) replicasIn(addr string) int {
	find := f.components()
	me := find(f.indexOf(addr))
	n := 0

	for _, r := range f.Cmds("CLUSTER", "REPLICATE") {
		if find(f.indexOf(r.Addr)) == me {
			n++
		}
	}

	return n
}

func (f *Fake) record(c Cmd) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cmds = append(f.cmds, c)
}

type admin struct {
	f      *Fake
	target redisdocker.Target
}

func (a *admin) Do(_ context.Context, args ...any) (any, error) {
	a.f.record(Cmd{Addr: a.target.Addr, Args: args})

	switch strings.ToUpper(fmt.Sprint(args[0])) {
	case "PING":
		return "PONG", nil
	case "INFO":
		return "role:slave\r\nmaster_link_status:up\r\n", nil
	case "CLUSTER":
		return a.cluster(args), nil
	}

	return "OK", nil
}

func (a *admin) cluster(args []any) any {
	switch fmt.Sprint(args[1]) {
	case "INFO":
		return fmt.Sprintf("cluster_state:ok\r\ncluster_known_nodes:%d\r\n", a.f.knownBy(a.target.Addr))
	case "NODES":
		return strings.Repeat("n 1.2.3.4:1 myself,slave p 0 0 1 connected\n", a.f.replicasIn(a.target.Addr))
	case "MYID":
		return "id-" + a.target.Addr
	}

	return "OK"
}

func (a *admin) Close() error { return nil }
