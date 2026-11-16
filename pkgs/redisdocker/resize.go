package redisdocker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// minFailoverNodes is a primary and one replica.
const minFailoverNodes = 2

var (
	// ErrShardRange is returned for a shard index the group does not have.
	ErrShardRange = errors.New("shard index out of range")
	// ErrNoReplica is returned when a failover has no replica to promote.
	ErrNoReplica = errors.New("shard has no replica to promote")
	// ErrGroupClosed is returned for operations on a closed group.
	ErrGroupClosed = errors.New("redis group is closed")
)

// beginOp serialises topology changes (failover, resize); the returned func ends the operation.
func (g *Group) beginOp(ctx context.Context) (func(), error) {
	if g.ops == nil {
		return func() {}, nil
	}

	select {
	case g.ops <- struct{}{}:
		return func() { <-g.ops }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for topology change: %w", ctx.Err())
	}
}

// shardOrdered returns the shard's nodes, primary first.
func (g *Group) shardOrdered(shard int) ([]*node, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.closed {
		return nil, ErrGroupClosed
	}

	if shard < 0 || shard >= len(g.spec.Shards) {
		return nil, fmt.Errorf("%w: %d", ErrShardRange, shard)
	}

	var out []*node

	for _, n := range g.nodes {
		if n.shard == shard {
			out = append(out, n)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].replica < out[j].replica })

	return out, nil
}

// Primary returns the endpoint currently serving as the shard's primary.
func (g *Group) Primary(shard int) (Endpoint, error) {
	nodes, err := g.shardOrdered(shard)
	if err != nil {
		return Endpoint{}, err
	}

	return Endpoint{Host: g.engine.cfg.Host, Port: nodes[0].port}, nil
}

// Failover promotes the shard's first replica to primary and makes the old primary follow it.
func (g *Group) Failover(ctx context.Context, shard int) error {
	end, err := g.beginOp(ctx)
	if err != nil {
		return err
	}

	defer end()

	nodes, err := g.shardOrdered(shard)
	if err != nil {
		return err
	}

	if len(nodes) < minFailoverNodes {
		return ErrNoReplica
	}

	lctx, cancel := context.WithTimeout(ctx, g.engine.cfg.StartTimeout)
	defer cancel()

	old, target := nodes[0], nodes[1]

	if g.spec.ClusterMode {
		err = g.promoteInCluster(lctx, target, old)
	} else {
		err = g.promoteStandalone(lctx, target, nodes)
	}

	if err != nil {
		return err
	}

	g.mu.Lock()
	old.replica, target.replica = target.replica, old.replica
	g.mu.Unlock()

	return nil
}

func (g *Group) role(ctx context.Context, n *node) (string, error) {
	var role string

	_, err := g.onNode(n, func(a Admin) (bool, error) {
		res, derr := a.Do(ctx, "INFO", "replication")
		role = infoField(asString(res), "role")

		return derr == nil, derr
	})

	return role, err
}

func (g *Group) awaitRole(ctx context.Context, n *node, want string) error {
	return g.poll(ctx, "role "+want, func(ctx context.Context) (bool, error) {
		role, err := g.role(ctx, n)

		return err == nil && role == want, err
	})
}

func (g *Group) promoteInCluster(ctx context.Context, target, old *node) error {
	_, err := g.onNode(target, func(a Admin) (bool, error) {
		_, derr := a.Do(ctx, "CLUSTER", "FAILOVER")

		return derr == nil, derr
	})
	if err != nil {
		return fmt.Errorf("cluster failover: %w", err)
	}

	if err = g.awaitRole(ctx, target, "master"); err != nil {
		return err
	}

	return g.awaitRole(ctx, old, "slave")
}

func (g *Group) promoteStandalone(ctx context.Context, target *node, nodes []*node) error {
	_, err := g.onNode(target, func(a Admin) (bool, error) {
		_, derr := a.Do(ctx, "REPLICAOF", "NO", "ONE")

		return derr == nil, derr
	})
	if err != nil {
		return fmt.Errorf("promote replica: %w", err)
	}

	for _, n := range nodes {
		if n == target {
			continue
		}

		if err = g.follow(ctx, n, target); err != nil {
			return err
		}
	}

	return nil
}

// SetReplicas grows or shrinks one shard to the given replica count; extra replicas are the highest numbered.
func (g *Group) SetReplicas(ctx context.Context, shard, replicas int) error {
	if replicas < 0 || replicas > maxReplicas {
		return fmt.Errorf("%w: replicas must be 0-%d", ErrSpec, maxReplicas)
	}

	end, err := g.beginOp(ctx)
	if err != nil {
		return err
	}

	defer end()

	nodes, err := g.shardOrdered(shard)
	if err != nil {
		return err
	}

	lctx, cancel := context.WithTimeout(ctx, g.engine.cfg.StartTimeout)
	defer cancel()

	have := len(nodes) - 1

	switch {
	case replicas > have:
		err = g.addReplicas(lctx, nodes, replicas-have)
	case replicas < have:
		err = g.removeReplicas(ctx, nodes[1+replicas:])
	}

	if err != nil {
		return err
	}

	g.mu.Lock()
	g.spec.Shards = slices.Clone(g.spec.Shards)
	g.spec.Shards[shard].Replicas = replicas
	g.mu.Unlock()

	return nil
}

func (g *Group) addReplicas(ctx context.Context, nodes []*node, count int) error {
	var fresh []*node

	err := g.growReplicas(ctx, nodes, count, &fresh)
	if err != nil {
		g.rollbackReplicas(ctx, fresh)
	}

	return err
}

func (g *Group) growReplicas(ctx context.Context, nodes []*node, count int, started *[]*node) error {
	primary := nodes[0]
	next := nodes[len(nodes)-1].replica + 1

	for i := range count {
		n := &node{shard: primary.shard, replica: next + i}
		if err := g.startNode(ctx, n); err != nil {
			return err
		}

		*started = append(*started, n)
	}

	fresh := *started

	for _, n := range fresh {
		if err := g.awaitNode(ctx, n); err != nil {
			return err
		}

		if err := g.resolveIP(ctx, n); err != nil {
			return err
		}
	}

	if !g.spec.ClusterMode {
		for _, n := range fresh {
			if err := g.follow(ctx, n, primary); err != nil {
				return err
			}
		}

		return nil
	}

	return g.joinReplicas(ctx, primary, fresh)
}

func (g *Group) joinReplicas(ctx context.Context, primary *node, fresh []*node) error {
	_, err := g.onNode(primary, func(a Admin) (bool, error) {
		for _, n := range fresh {
			if _, derr := a.Do(ctx, "CLUSTER", "MEET", n.ip, strconv.Itoa(n.port), strconv.Itoa(busPort)); derr != nil {
				return false, derr
			}
		}

		return true, nil
	})
	if err != nil {
		return err
	}

	if err = g.awaitKnown(ctx); err != nil {
		return err
	}

	id, err := g.nodeID(ctx, primary)
	if err != nil {
		return err
	}

	for _, n := range fresh {
		err = g.poll(ctx, "cluster replicate", func(ctx context.Context) (bool, error) {
			return g.onNode(n, func(a Admin) (bool, error) {
				_, derr := a.Do(ctx, "CLUSTER", "REPLICATE", id)

				return derr == nil, derr
			})
		})
		if err != nil {
			return err
		}
	}

	return g.awaitClusterOK(ctx)
}

func (g *Group) removeReplicas(ctx context.Context, drop []*node) error {
	for _, n := range drop {
		if g.isClosed() {
			return ErrGroupClosed
		}

		if g.spec.ClusterMode {
			if err := g.forget(ctx, n); err != nil {
				return err
			}
		}

		g.removeNode(ctx, n)
	}

	return nil
}

func (g *Group) isClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.closed
}

// removeNode stops n and frees its port, but only when it still belongs to the group: Close takes ownership
// of every node it finds, so a node it already claimed is never released twice.
func (g *Group) removeNode(ctx context.Context, n *node) {
	g.mu.Lock()
	before := len(g.nodes)
	g.nodes = slices.DeleteFunc(g.nodes, func(x *node) bool { return x == n })
	owned := len(g.nodes) != before
	g.mu.Unlock()

	if !owned {
		return
	}

	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	defer cancel()

	if err := g.engine.cfg.Runtime.StopAndRemove(sctx, n.containerID); err != nil {
		logger.Load(sctx).WarnContext(sctx, "redis group: container removal failed",
			"container", n.containerID, "error", err)
	}

	portalloc.ReleaseTo(g.engine.cfg.Ports, n.port)
}

// rollbackReplicas discards replicas of a failed grow so the group matches its spec.
func (g *Group) rollbackReplicas(ctx context.Context, fresh []*node) {
	for _, n := range fresh {
		if g.spec.ClusterMode && n.ip != "" {
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
			_ = g.forget(fctx, n)

			cancel()
		}

		g.removeNode(ctx, n)
	}
}

// forget makes every remaining node drop n from the cluster view.
func (g *Group) forget(ctx context.Context, n *node) error {
	id, err := g.nodeID(ctx, n)
	if err != nil {
		return err
	}

	g.mu.Lock()
	others := slices.Clone(g.nodes)
	g.mu.Unlock()

	for _, o := range others {
		if o == n {
			continue
		}

		_, err = g.onNode(o, func(a Admin) (bool, error) {
			_, derr := a.Do(ctx, "CLUSTER", "FORGET", id)

			return derr == nil, derr
		})
		if err != nil {
			return fmt.Errorf("forget node: %w", err)
		}
	}

	return nil
}
