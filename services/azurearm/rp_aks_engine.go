package azurearm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	aksEngineFallback = "1.36"
	aksJoinTokenBytes = 16
	aksCodeStarting   = "ClusterStarting"
	aksCodeStartFail  = "ClusterStartFailed"
)

// AKSEngineConfig configures docker-backed clusters: each running cluster is a single-node k3s container.
type AKSEngineConfig struct {
	Runtime      k3sengine.Runtime
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Probe        func(ctx context.Context, addr, token string) ([]byte, error)
	Host         string
	Token        string
	Image        string
	StartTimeout time.Duration
}

type aksLive struct {
	inst  *k3sengine.Instance
	addr  string
	ca    []byte
	ready bool
}

type aksEngine struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mu      *lockmetrics.RWMutex
	core    *k3sengine.Engine
	factory func() (*k3sengine.Engine, error)
	live    map[string]*aksLive
	image   string
}

func newAKSEngine() *aksEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &aksEngine{ctx: ctx, cancel: cancel, mu: lockmetrics.New("azurearm.containerservice.engine"),
		live: map[string]*aksLive{}}
}

// EnableEngine switches the provider to docker mode with an engine built from cfg.
func (p *ContainerServiceProvider) EnableEngine(cfg AKSEngineConfig) error {
	core, err := k3sengine.New(k3sengine.Config{
		Runtime: cfg.Runtime, Ports: cfg.Ports, Logger: cfg.Logger, Probe: cfg.Probe, Prefix: "aks",
		Host: cfg.Host, Token: cfg.Token, StartTimeout: cfg.StartTimeout,
	})
	if err != nil {
		return err
	}

	p.eng.mu.Lock("EnableEngine")
	defer p.eng.mu.Unlock()

	p.eng.core, p.eng.image = core, cfg.Image

	return nil
}

// SetEngineFactory enables docker mode with an engine built on first use, so the runtime is only dialed when a
// cluster is created.
func (p *ContainerServiceProvider) SetEngineFactory(fn func() (*k3sengine.Engine, error), image string) {
	p.eng.mu.Lock("SetEngineFactory")
	defer p.eng.mu.Unlock()

	p.eng.factory, p.eng.image = fn, image
}

func (e *aksEngine) enabled() bool {
	e.mu.RLock("enabled")
	defer e.mu.RUnlock()

	return e.core != nil || e.factory != nil
}

func (e *aksEngine) engine() (*k3sengine.Engine, error) {
	e.mu.Lock("engine")
	defer e.mu.Unlock()

	if e.core != nil || e.factory == nil {
		return e.core, nil
	}

	core, err := e.factory()
	if err != nil {
		return nil, err
	}

	e.core = core

	return core, nil
}

func (e *aksEngine) imageFor(version string) string {
	e.mu.RLock("imageFor")
	defer e.mu.RUnlock()

	if e.image != "" {
		return e.image
	}

	img, _ := k3sengine.Image(version, aksEngineFallback)

	return img
}

func aksJoinToken() string {
	buf := make([]byte, aksJoinTokenBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// start launches the cluster's node and waits for its API server; exists reports whether the cluster is still wanted.
func (e *aksEngine) start(ctx context.Context, key, name, version string, exists func() bool) error {
	core, err := e.engine()
	if err != nil {
		return aksStartErr(err)
	}

	if core == nil {
		return nil
	}

	e.mu.RLock("start")
	_, running := e.live[key]
	e.mu.RUnlock()

	if running {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()

	lv := &aksLive{}

	inst, err := core.Launch(ctx, k3sengine.Node{Name: name, Image: e.imageFor(version), JoinToken: aksJoinToken()},
		func(i *k3sengine.Instance) bool { return e.record(key, lv, i, exists) })
	if err != nil {
		if errors.Is(err, k3sengine.ErrSuperseded) {
			return nil
		}

		return aksStartErr(err)
	}

	addr := core.Addr(inst.Port)

	ca, err := core.AwaitReady(ctx, addr, inst.CA.CertPEM)
	if err != nil {
		e.removeKeys([]string{key})

		return aksStartErr(err)
	}

	e.mu.Lock("start")
	defer e.mu.Unlock()

	if e.live[key] == lv {
		lv.addr, lv.ca, lv.ready = addr, ca, true
	}

	return nil
}

func (e *aksEngine) record(key string, lv *aksLive, i *k3sengine.Instance, exists func() bool) bool {
	if !exists() {
		return false
	}

	e.mu.Lock("record")
	defer e.mu.Unlock()

	if _, taken := e.live[key]; taken {
		return false
	}

	lv.inst = i
	e.live[key] = lv

	return true
}

func aksStartErr(err error) error {
	return netErr(http.StatusInternalServerError, aksCodeStartFail, "The cluster's API server failed to start: %v", err)
}

func (e *aksEngine) removeKeys(keys []string) {
	e.mu.Lock("removeKeys")

	var dead []*aksLive

	for _, k := range keys {
		if lv, ok := e.live[k]; ok {
			dead = append(dead, lv)

			delete(e.live, k)
		}
	}

	core := e.core
	e.mu.Unlock()

	for _, lv := range dead {
		if core != nil && lv.inst != nil {
			core.Remove(lv.inst.ContainerID, append([]int{lv.inst.Port}, lv.inst.LBHost...)...)
		}
	}
}

func (e *aksEngine) removeAll() {
	e.mu.RLock("removeAll")
	keys := make([]string, 0, len(e.live))

	for k := range e.live {
		keys = append(keys, k)
	}

	e.mu.RUnlock()

	e.removeKeys(keys)
}

func (e *aksEngine) shutdown() {
	e.cancel()
	e.removeAll()

	e.mu.Lock("shutdown")
	core := e.core
	e.core = nil
	e.mu.Unlock()

	if core != nil {
		core.Close()
	}
}

// access returns the live API server of a cluster: its address, serving CA and admin token.
func (e *aksEngine) access(key string) (aksServing, bool) {
	e.mu.RLock("access")
	defer e.mu.RUnlock()

	lv, ok := e.live[key]
	if !ok || !lv.ready || e.core == nil {
		return aksServing{}, false
	}

	return aksServing{server: "https://" + lv.addr, ca: lv.ca, token: e.core.Token()}, true
}

func (p *ContainerServiceProvider) ensureNode(ctx context.Context, c *storedAKSCluster) error {
	key := aksKey(c.ResourceGroup, c.Name)

	return p.eng.start(ctx, key, c.Name, c.Current, func() bool {
		_, ok := p.lookup(c.ResourceGroup, c.Name)

		return ok
	})
}

// relaunchAll starts a fresh node for every restored cluster that should be running.
func (p *ContainerServiceProvider) relaunchAll() {
	if !p.eng.enabled() {
		return
	}

	p.mu.RLock("relaunchAll")
	defer p.mu.RUnlock()

	for _, c := range p.clusters {
		if c.Stopped {
			continue
		}

		p.wg.Go(func() {
			err := p.ensureNode(p.eng.ctx, c)
			if err != nil {
				slog.Default().WarnContext(p.eng.ctx, "azurearm: restored AKS cluster failed to start",
					"cluster", strings.ToLower(c.Name), "error", err)
			}
		})
	}
}
