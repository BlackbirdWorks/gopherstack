package ocicontainerengine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	joinTokenBytes = 16
	engineFallback = "1.34"
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

// EnableEngine switches the backend to docker mode. Call it before serving.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) error {
	core, err := k3sengine.New(k3sengine.Config{
		Runtime: cfg.Runtime, Ports: cfg.Ports, Logger: cfg.Logger, Probe: cfg.Probe, Prefix: "oke",
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

	img, _ := k3sengine.Image(engineImageVersion(version), engineFallback)

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

// reapLocked releases the engine cluster of id; container removal runs in the background.
func (b *InMemoryBackend) reapLocked(id string) {
	if b.engine == nil {
		return
	}

	if lc, ok := b.engine.live[id]; ok {
		delete(b.engine.live, id)
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
func (b *InMemoryBackend) launchLocked(c *Cluster, wrID string) {
	e := b.engine
	ctx, cancel := context.WithCancel(b.runCtx)
	lc := &liveCluster{cancel: cancel}
	e.live[c.ID] = lc
	id, image := c.ID, e.imageFor(c.Version)

	b.wg.Go(func() { b.runEngine(ctx, id, lc, image, wrID) })
}

func (b *InMemoryBackend) runEngine(ctx context.Context, id string, lc *liveCluster, image, wrID string) {
	e := b.engine

	inst, err := e.core.Launch(ctx, k3sengine.Node{Name: ocidTail(id, longTail), Image: image, JoinToken: joinToken()},
		func(i *k3sengine.Instance) bool { return b.recordLive(id, lc, i) })
	if err != nil {
		if !errors.Is(err, k3sengine.ErrSuperseded) {
			b.failEngine(ctx, id, lc, wrID, err)
		}

		return
	}

	addr := e.core.Addr(inst.Port)

	ca, err := e.core.AwaitReady(ctx, addr, inst.CA.CertPEM)
	if err != nil {
		b.failEngine(ctx, id, lc, wrID, err)

		return
	}

	b.readyEngine(id, lc, addr, ca, wrID)
}

func (b *InMemoryBackend) recordLive(id string, lc *liveCluster, i *k3sengine.Instance) bool {
	b.mu.Lock("recordLive")
	defer b.mu.Unlock()

	if b.engine.live[id] != lc {
		return false
	}

	lc.containerID, lc.port, lc.lbHost = i.ContainerID, i.Port, i.LBHost

	return true
}

// engineEndpoints points the cluster endpoints at the engine's API server.
func engineEndpoints(c *Cluster, addr string) {
	if c.EndpointConfig == nil || c.EndpointConfig.SubnetID == "" {
		c.Endpoints = Endpoints{Kubernetes: addr}

		return
	}

	c.Endpoints.PrivateEndpoint, c.Endpoints.PublicEndpoint = addr, ""
	if c.EndpointConfig.IsPublicIPEnabled != nil && *c.EndpointConfig.IsPublicIPEnabled {
		c.Endpoints.PublicEndpoint = addr
	}
}

func (b *InMemoryBackend) readyEngine(id string, lc *liveCluster, addr string, ca []byte, wrID string) {
	b.mu.Lock("readyEngine")
	defer b.mu.Unlock()

	c, ok := b.clusters[id]
	if !ok || b.engine.live[id] != lc {
		return
	}

	now := b.nowUTC()
	engineEndpoints(c, addr)
	c.CAPEM, c.Token = string(ca), b.engine.core.Token()
	c.State, c.TransitionAt, c.Details = stateActive, nil, ""

	if wr, found := b.workReqs[wrID]; found {
		wr.FinishAt = &now
	}
}

// failEngine marks a still-wanted cluster FAILED and releases its container.
func (b *InMemoryBackend) failEngine(ctx context.Context, id string, lc *liveCluster, wrID string, cause error) {
	b.mu.Lock("failEngine")

	c, ok := b.clusters[id]
	if !ok || b.engine.live[id] != lc {
		b.mu.Unlock()

		return
	}

	delete(b.engine.live, id)

	msg := "Cluster failed to start: " + cause.Error()
	now := b.nowUTC()
	c.State, c.Details, c.TransitionAt = stateFailed, msg, nil

	if wr, found := b.workReqs[wrID]; found {
		wr.FinishAt, wr.Failure = &now, msg
	}

	e := b.engine
	b.mu.Unlock()

	e.core.Log(ctx).WarnContext(ctx, "ocicontainerengine: cluster failed to start",
		"code", "CLUSTER_START_FAILED", "cluster", id, "error", cause)
	e.reap([]*liveCluster{lc})
}

// relaunchLocked starts a fresh engine cluster for every restored cluster that still exists.
func (b *InMemoryBackend) relaunchLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.clusters {
		if c.State == stateDeleted || c.State == stateDeleting || c.State == stateFailed {
			continue
		}

		c.State, c.TransitionAt = stateUpdating, nil
		c.UpdatedByWR = b.newWR(wrClusterUpdate, c.CompartmentID, entityCluster, c.ID, actionUpdated, nil)
		b.launchLocked(c, c.UpdatedByWR)
	}
}
