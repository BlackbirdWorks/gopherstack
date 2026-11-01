package gcpcontainer

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	joinTokenBytes  = 16
	engineFallback  = "1.34"
	engineStartCode = "CLUSTER_START_FAILED"
)

// EngineConfig configures docker-backed clusters: each CreateCluster starts a single-node k3s container.
type EngineConfig struct {
	Runtime      k3sengine.Runtime
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Probe        func(ctx context.Context, addr, token string) ([]byte, error)
	Host         string
	Token        string
	Image        string
	StartTimeout time.Duration
}

type liveCluster struct {
	cancel      context.CancelFunc
	containerID string
	lbHost      []int
	port        int
}

type clusterEngine struct {
	core  *k3sengine.Engine
	live  map[string]*liveCluster
	image string
	wg    sync.WaitGroup
}

// EnableEngine switches the backend to docker mode.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) error {
	core, err := k3sengine.New(k3sengine.Config{
		Runtime: cfg.Runtime, Ports: cfg.Ports, Logger: cfg.Logger, Probe: cfg.Probe, Prefix: "gke",
		Host: cfg.Host, Token: cfg.Token, StartTimeout: cfg.StartTimeout,
	})
	if err != nil {
		return err
	}

	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	b.engine = &clusterEngine{core: core, live: map[string]*liveCluster{}, image: cfg.Image}

	return nil
}

// EngineToken is the cluster-admin bearer token of docker-engine clusters; empty without the engine.
func (b *InMemoryBackend) EngineToken() string {
	b.mu.RLock("EngineToken")
	defer b.mu.RUnlock()

	if b.engine == nil {
		return ""
	}

	return b.engine.core.Token()
}

func (e *clusterEngine) imageFor(version string) string {
	if e.image != "" {
		return e.image
	}

	img, _ := k3sengine.Image(version, engineFallback)

	return img
}

func (e *clusterEngine) reap(lcs []*liveCluster) {
	for _, lc := range lcs {
		lc.cancel()

		if lc.containerID != "" {
			e.wg.Go(func() { e.core.Remove(lc.containerID, append([]int{lc.port}, lc.lbHost...)...) })
		}
	}
}

func (b *InMemoryBackend) detachAllLocked() []*liveCluster {
	if b.engine == nil {
		return nil
	}

	out := make([]*liveCluster, 0, len(b.engine.live))
	for k, lc := range b.engine.live {
		out = append(out, lc)
		delete(b.engine.live, k)
	}

	return out
}

// reapLocked releases the engine cluster of key; container removal runs in the background.
func (b *InMemoryBackend) reapLocked(key string) {
	if b.engine == nil {
		return
	}

	if lc, ok := b.engine.live[key]; ok {
		delete(b.engine.live, key)
		b.engine.reap([]*liveCluster{lc})
	}
}

func (b *InMemoryBackend) closeEngine() {
	b.mu.Lock("closeEngine")
	e := b.engine
	lcs := b.detachAllLocked()
	b.mu.Unlock()

	if e == nil {
		return
	}

	e.reap(lcs)
	e.wg.Wait()
	e.core.Close()
}

func joinToken() string {
	buf := make([]byte, joinTokenBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// launchLocked registers the cluster with the engine and starts its container in the background.
func (b *InMemoryBackend) launchLocked(r ref, rec *clusterRec, o *opRec) {
	e := b.engine
	key := r.clusterKey()
	ctx, cancel := context.WithCancel(b.runCtx)
	lc := &liveCluster{cancel: cancel}
	e.live[key] = lc
	image := e.imageFor(rec.Cluster.GetCurrentMasterVersion())
	opName := o.Op.GetName()

	b.wg.Go(func() { b.runEngine(ctx, r, lc, image, opName) })
}

func (b *InMemoryBackend) runEngine(ctx context.Context, r ref, lc *liveCluster, image, opName string) {
	e := b.engine
	key := r.clusterKey()

	inst, err := e.core.Launch(ctx, k3sengine.Node{Name: r.cluster, Image: image, JoinToken: joinToken()},
		func(i *k3sengine.Instance) bool { return b.recordLive(key, lc, i) })
	if err != nil {
		if !errors.Is(err, k3sengine.ErrSuperseded) {
			b.failEngine(ctx, r, lc, opName, err)
		}

		return
	}

	addr := e.core.Addr(inst.Port)

	ca, err := e.core.AwaitReady(ctx, addr, inst.CA.CertPEM)
	if err != nil {
		b.failEngine(ctx, r, lc, opName, err)

		return
	}

	b.readyEngine(r, lc, addr, ca, opName)
}

func (b *InMemoryBackend) recordLive(key string, lc *liveCluster, i *k3sengine.Instance) bool {
	b.mu.Lock("recordLive")
	defer b.mu.Unlock()

	if b.engine.live[key] != lc {
		return false
	}

	lc.containerID, lc.port, lc.lbHost = i.ContainerID, i.Port, i.LBHost

	return true
}

func (b *InMemoryBackend) readyEngine(r ref, lc *liveCluster, addr string, ca []byte, opName string) {
	b.mu.Lock("readyEngine")
	defer b.mu.Unlock()

	c, ok := b.clusters[r.clusterKey()]
	if !ok || b.engine.live[r.clusterKey()] != lc {
		return
	}

	c.Cluster.Endpoint = addr
	c.Cluster.MasterAuth.ClusterCaCertificate = base64.StdEncoding.EncodeToString(ca)

	if o, found := b.ops[opKey(r.project, r.location, opName)]; found {
		b.finishOpLocked(o, "")
	}
}

// failEngine marks a still-wanted cluster ERROR and releases its container.
func (b *InMemoryBackend) failEngine(ctx context.Context, r ref, lc *liveCluster, opName string, cause error) {
	b.mu.Lock("failEngine")

	key := r.clusterKey()
	c, ok := b.clusters[key]

	if !ok || b.engine.live[key] != lc {
		b.mu.Unlock()

		return
	}

	delete(b.engine.live, key)

	msg := "Cluster failed to start: " + cause.Error()
	c.Failure = msg

	if o, found := b.ops[opKey(r.project, r.location, opName)]; found {
		b.finishOpLocked(o, msg)
	}

	e := b.engine
	b.mu.Unlock()

	e.core.Log(ctx).WarnContext(ctx, "gcpcontainer: cluster failed to start",
		"code", engineStartCode, "cluster", r.clusterName(), "error", cause)
	e.reap([]*liveCluster{lc})
}

// relaunchLocked starts a fresh engine cluster for every restored cluster.
func (b *InMemoryBackend) relaunchLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.clusters {
		if c.DeleteOp != "" {
			continue
		}

		r := ref{project: c.Project, location: c.Cluster.GetLocation(), cluster: c.Cluster.GetName()}
		o := b.newOpLocked(r, containerpb.Operation_REPAIR_CLUSTER, kindCreate, c.Cluster.GetSelfLink(), 0, true)
		c.Failure = ""
		b.launchLocked(r, c, o)
	}
}
