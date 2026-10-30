package azurearm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	aksEngineFallback  = "1.36"
	aksJoinTokenHexLen = 32
	aksVolumeHashLen   = 16
	aksVolumeCleanup   = 30 * time.Second
	aksCodeStarting    = "ClusterStarting"
	aksCodeStartFail   = "ClusterStartFailed"
	aksCodeStopFail    = "ClusterStopFailed"
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
	// stopped marks a node whose container is kept but not running.
	stopped bool
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

// aksNodeSpec is what identifies a cluster's node across launches: the same volume and join token let a fresh
// container pick the cluster state up again.
type aksNodeSpec struct {
	key, name, version, secret string
	// fresh discards any data volume left under the cluster's name: a cluster being created has no state to resume.
	fresh bool
}

func (n aksNodeSpec) volume() string { return aksHashed("gopherstack-aks-", n.key, aksVolumeHashLen) }

func (n aksNodeSpec) nodeName() string { return aksHashed("aks-", n.key, aksHashLen) + "-vmss000000" }

func (n aksNodeSpec) joinToken() string {
	return aksHashed("", "aks-join|"+n.secret, aksJoinTokenHexLen)
}

func aksHashed(prefix, in string, n int) string {
	sum := sha256.Sum256([]byte(in))

	return prefix + hex.EncodeToString(sum[:])[:n]
}

// adopt takes over the running node a crashed gopherstack left for this cluster; false means launch fresh.
func (e *aksEngine) adopt(ctx context.Context, core *k3sengine.Engine, n aksNodeSpec, lv *aksLive,
	exists func() bool,
) bool {
	oc, ok := container.ClaimOrphan("k3s-aks", n.key)
	if !ok {
		return false
	}

	inst, err := core.Adopt(oc)
	if err != nil {
		core.Remove(oc.ID)

		return false
	}

	ca, err := core.ServingCA(ctx, oc.ID)
	if err == nil {
		addr := core.Addr(inst.Port)
		ca, err = core.AwaitReady(ctx, addr, ca)
		inst.CA = &k3sengine.CA{CertPEM: ca}
	}

	if err != nil || !e.record(n.key, lv, inst, exists) {
		core.Remove(oc.ID, inst.Port)

		return false
	}

	e.mu.Lock("adopt")
	defer e.mu.Unlock()

	if e.live[n.key] == lv {
		lv.addr, lv.ca, lv.ready = core.Addr(inst.Port), inst.CA.CertPEM, true
	}

	return true
}

// start launches the cluster's node and waits for its API server; exists reports whether the cluster is still wanted.
func (e *aksEngine) start(ctx context.Context, n aksNodeSpec, exists func() bool) error {
	key, name, version := n.key, n.name, n.version
	core, err := e.engine()
	if err != nil {
		return aksStartErr(err)
	}

	if core == nil {
		return nil
	}

	e.mu.RLock("start")
	cur, running := e.live[key]
	kept := running && cur.stopped
	e.mu.RUnlock()

	if kept {
		return e.resume(ctx, core, key, cur)
	}

	if running {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()

	lv := &aksLive{}

	if n.fresh {
		if err = core.RemoveVolume(ctx, n.volume()); err != nil {
			return aksStartErr(err)
		}
	} else if e.adopt(ctx, core, n, lv, exists) {
		return nil
	}

	inst, err := core.Launch(ctx, k3sengine.Node{
		Name: name, Image: e.imageFor(version), JoinToken: n.joinToken(), Resource: key, Volume: n.volume(),
		NodeName: n.nodeName(),
	}, func(i *k3sengine.Instance) bool { return e.record(key, lv, i, exists) })
	if err != nil {
		if errors.Is(err, k3sengine.ErrSuperseded) {
			return nil
		}

		return aksStartErr(err)
	}

	addr := core.Addr(inst.Port)

	caPEM := inst.CA.CertPEM
	if got, caErr := core.ServingCA(ctx, inst.ContainerID); caErr == nil {
		caPEM = got
	}

	ca, err := core.AwaitReady(ctx, addr, caPEM)
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

// resume restarts the kept container of a stopped node, keeping its cluster state.
func (e *aksEngine) resume(ctx context.Context, core *k3sengine.Engine, key string, lv *aksLive) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()

	ca, err := core.Start(ctx, lv.inst.ContainerID, lv.addr, lv.ca)
	if err != nil {
		return aksStartErr(err)
	}

	e.mu.Lock("resume")
	defer e.mu.Unlock()

	if e.live[key] == lv {
		lv.ca, lv.ready, lv.stopped = ca, true, false
	}

	return nil
}

// pause stops the node's container but keeps it, so start brings the same cluster state back. Runtimes that cannot
// keep a stopped container fall back to removing it.
func (e *aksEngine) pause(ctx context.Context, key string) error {
	e.mu.Lock("pause")
	lv, ok := e.live[key]
	core := e.core

	if !ok || lv.inst == nil || core == nil {
		e.mu.Unlock()

		e.removeKeys([]string{key})

		return nil
	}

	lv.ready = false
	e.mu.Unlock()

	err := core.Stop(ctx, lv.inst.ContainerID)
	if errors.Is(err, k3sengine.ErrStopUnsupported) {
		e.removeKeys([]string{key})

		return nil
	}

	if err != nil {
		return netErr(http.StatusInternalServerError, aksCodeStopFail, "The cluster failed to stop: %v", err)
	}

	e.mu.Lock("pause")
	lv.stopped = true
	e.mu.Unlock()

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

// purge removes the nodes of keys together with their data volumes: the clusters are gone for good.
func (e *aksEngine) purge(keys []string) {
	e.removeKeys(keys)

	e.mu.RLock("purge")
	core := e.core
	e.mu.RUnlock()

	if core == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), aksVolumeCleanup)
	defer cancel()

	for _, k := range keys {
		if err := core.RemoveVolume(ctx, aksNodeSpec{key: k}.volume()); err != nil {
			slog.Default().WarnContext(ctx, "azurearm: AKS data volume removal failed", "cluster", k, "error", err)
		}
	}
}

// sweepVolumes removes data volumes no container uses and no known cluster owns, leftovers of clusters deleted
// while gopherstack was down.
func (e *aksEngine) sweepVolumes(known []string) {
	e.mu.RLock("sweepVolumes")
	core := e.core
	e.mu.RUnlock()

	if core == nil {
		return
	}

	keep := make(map[string]bool, len(known))
	for _, k := range known {
		keep[aksNodeSpec{key: k}.volume()] = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), aksVolumeCleanup)
	defer cancel()

	names, err := core.UnusedVolumes(ctx)
	if err != nil {
		return
	}

	for _, n := range names {
		if !keep[n] {
			_ = core.RemoveVolume(ctx, n)
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

// target returns the node of a ready cluster for running commands in.
func (e *aksEngine) target(key string) (aksExecutor, string, bool) {
	e.mu.RLock("target")
	defer e.mu.RUnlock()

	lv, ok := e.live[key]
	if !ok || !lv.ready || lv.inst == nil || e.core == nil {
		return nil, "", false
	}

	return e.core, lv.inst.ContainerID, true
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

func (p *ContainerServiceProvider) ensureNode(ctx context.Context, c *storedAKSCluster, fresh bool) error {
	key := aksKey(c.ResourceGroup, c.Name)
	spec := aksNodeSpec{key: key, name: c.Name, version: c.Current, secret: c.Token, fresh: fresh}

	return p.eng.start(ctx, spec, func() bool {
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
			err := p.ensureNode(p.eng.ctx, c, false)
			if err != nil {
				slog.Default().WarnContext(p.eng.ctx, "azurearm: restored AKS cluster failed to start",
					"cluster", strings.ToLower(c.Name), "error", err)
			}
		})
	}
}
