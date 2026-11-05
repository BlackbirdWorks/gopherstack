package redisdocker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// ErrNotConverged is returned when a group does not reach its target state before the start timeout.
var ErrNotConverged = errors.New("redis group did not converge")

const (
	nodeFlagsField = 2
	nodeSlotsField = 8
)

var errNoIP = errors.New("container reported no address")

// poll runs cond every PollInterval until it reports done, fails, or ctx ends; then the last error is returned.
func (g *Group) poll(ctx context.Context, what string, cond func(ctx context.Context) (bool, error)) error {
	t := time.NewTicker(g.engine.cfg.PollInterval)
	defer t.Stop()

	var last error

	for {
		done, err := cond(ctx)
		if done {
			return nil
		}

		if err != nil {
			last = err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s: %w", ErrNotConverged, what, errors.Join(ctx.Err(), last))
		case <-t.C:
		}
	}
}

// onNode runs fn with a connection to n.
func (g *Group) onNode(n *node, fn func(Admin) (bool, error)) (bool, error) {
	adm := g.dial(n)
	defer func() { _ = adm.Close() }()

	return fn(adm)
}

func (g *Group) awaitNodes(ctx context.Context) error {
	for _, n := range g.nodes {
		err := g.poll(ctx, "node "+strconv.Itoa(n.port)+" accepting commands", func(ctx context.Context) (bool, error) {
			return g.onNode(n, func(a Admin) (bool, error) {
				res, err := a.Do(ctx, "PING")

				return err == nil && asString(res) == "PONG", err
			})
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *Group) shardNodes(shard int) []*node {
	var out []*node

	for _, n := range g.nodes {
		if n.shard == shard {
			out = append(out, n)
		}
	}

	return out
}

func (g *Group) resolveIPs(ctx context.Context) error {
	for _, n := range g.nodes {
		res, err := g.engine.cfg.Runtime.Exec(ctx, n.containerID, []string{"hostname", "-i"})
		if err != nil {
			return fmt.Errorf("resolve node address: %w", err)
		}

		for f := range strings.FieldsSeq(res.Stdout) {
			if ip := net.ParseIP(f); ip != nil && ip.To4() != nil {
				n.ip = f

				break
			}
		}

		if n.ip == "" {
			return errNoIP
		}
	}

	return nil
}

func (g *Group) wire(ctx context.Context) error {
	if err := g.resolveIPs(ctx); err != nil {
		return err
	}

	if g.spec.ClusterMode {
		return g.formCluster(ctx)
	}

	return g.replicate(ctx)
}

// replicate points each replica at the primary and waits for the link.
func (g *Group) replicate(ctx context.Context) error {
	nodes := g.shardNodes(0)
	primary := nodes[0]

	for _, r := range nodes[1:] {
		if err := g.follow(ctx, r, primary); err != nil {
			return err
		}
	}

	return nil
}

func (g *Group) follow(ctx context.Context, replica, primary *node) error {
	return g.poll(ctx, "replica link", func(ctx context.Context) (bool, error) {
		return g.onNode(replica, func(a Admin) (bool, error) {
			if _, err := a.Do(ctx, "REPLICAOF", primary.ip, strconv.Itoa(primary.port)); err != nil {
				return false, err
			}

			res, err := a.Do(ctx, "INFO", "replication")
			if err != nil {
				return false, err
			}

			return infoField(asString(res), "master_link_status") == "up", nil
		})
	})
}

func (g *Group) formCluster(ctx context.Context) error {
	first := g.nodes[0]

	if err := g.meetAll(ctx, first); err != nil {
		return err
	}

	if err := g.assignSlots(ctx); err != nil {
		return err
	}

	if err := g.awaitKnown(ctx); err != nil {
		return err
	}

	if err := g.attachReplicas(ctx); err != nil {
		return err
	}

	return g.awaitClusterOK(ctx)
}

func (g *Group) meetAll(ctx context.Context, first *node) error {
	_, err := g.onNode(first, func(a Admin) (bool, error) {
		for _, n := range g.nodes[1:] {
			if _, err := a.Do(ctx, "CLUSTER", "MEET", n.ip, strconv.Itoa(n.port), strconv.Itoa(busPort)); err != nil {
				return false, err
			}
		}

		return true, nil
	})

	return err
}

func (g *Group) assignSlots(ctx context.Context) error {
	for i, sh := range g.spec.Shards {
		primary := g.shardNodes(i)[0]

		_, err := g.onNode(primary, func(a Admin) (bool, error) {
			missing := sh.Slots

			if len(sh.Seed) > 0 {
				// a node that loaded keys already claimed the slots they hash to
				var perr error
				if missing, perr = g.unclaimed(ctx, a, sh.Slots); perr != nil {
					return false, perr
				}
			}

			if len(missing) == 0 {
				return true, nil
			}

			args := []any{"CLUSTER", "ADDSLOTSRANGE"}
			for _, r := range missing {
				args = append(args, strconv.Itoa(r.Start), strconv.Itoa(r.End))
			}

			_, derr := a.Do(ctx, args...)

			return derr == nil, derr
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *Group) unclaimed(ctx context.Context, a Admin, want []SlotRange) ([]SlotRange, error) {
	res, err := a.Do(ctx, "CLUSTER", "NODES")
	if err != nil {
		return nil, err
	}

	return subtractSlots(want, ownedSlots(asString(res)))
}

// ownedSlots lists the slots the "myself" line of CLUSTER NODES output serves; migrating markers are skipped.
func ownedSlots(nodes string) []SlotRange {
	var out []SlotRange

	for line := range strings.SplitSeq(nodes, "\n") {
		fields := strings.Fields(line)
		if len(fields) <= nodeSlotsField || !strings.Contains(fields[nodeFlagsField], "myself") {
			continue
		}

		for _, tok := range fields[nodeSlotsField:] {
			if strings.HasPrefix(tok, "[") {
				continue
			}

			if r, err := ParseSlots(tok); err == nil {
				out = append(out, r...)
			}
		}
	}

	return out
}

// subtractSlots returns the slots of want not in owned; owned slots outside want are an error.
func subtractSlots(want, owned []SlotRange) ([]SlotRange, error) {
	var set [TotalSlots]bool

	for _, r := range want {
		for s := r.Start; s <= r.End; s++ {
			set[s] = true
		}
	}

	for _, r := range owned {
		for s := r.Start; s <= r.End; s++ {
			if !set[s] {
				return nil, fmt.Errorf("%w: snapshot holds keys for slot %d outside the shard's slots", ErrSlots, s)
			}

			set[s] = false
		}
	}

	var out []SlotRange

	for s := range TotalSlots {
		switch {
		case !set[s]:
		case len(out) > 0 && out[len(out)-1].End == s-1:
			out[len(out)-1].End = s
		default:
			out = append(out, SlotRange{Start: s, End: s})
		}
	}

	return out, nil
}

// awaitKnown waits until every node has met every other node.
func (g *Group) awaitKnown(ctx context.Context) error {
	want := strconv.Itoa(len(g.nodes))

	for _, n := range g.nodes {
		err := g.poll(ctx, "cluster gossip", func(ctx context.Context) (bool, error) {
			return g.onNode(n, func(a Admin) (bool, error) {
				res, err := a.Do(ctx, "CLUSTER", "INFO")

				return err == nil && infoField(asString(res), "cluster_known_nodes") == want, err
			})
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *Group) attachReplicas(ctx context.Context) error {
	for i := range g.spec.Shards {
		nodes := g.shardNodes(i)

		id, err := g.nodeID(ctx, nodes[0])
		if err != nil {
			return err
		}

		for _, r := range nodes[1:] {
			err = g.poll(ctx, "cluster replicate", func(ctx context.Context) (bool, error) {
				return g.onNode(r, func(a Admin) (bool, error) {
					_, derr := a.Do(ctx, "CLUSTER", "REPLICATE", id)

					return derr == nil, derr
				})
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *Group) nodeID(ctx context.Context, n *node) (string, error) {
	var id string

	_, err := g.onNode(n, func(a Admin) (bool, error) {
		res, derr := a.Do(ctx, "CLUSTER", "MYID")
		id = asString(res)

		return derr == nil, derr
	})

	return id, err
}

func (g *Group) awaitClusterOK(ctx context.Context) error {
	for _, n := range g.nodes {
		err := g.poll(ctx, "cluster state", func(ctx context.Context) (bool, error) {
			return g.onNode(n, func(a Admin) (bool, error) {
				res, err := a.Do(ctx, "CLUSTER", "INFO")
				if err != nil || infoField(asString(res), "cluster_state") != "ok" {
					return false, err
				}

				if ok, nerr := g.replicasVisible(ctx, a); !ok {
					return false, nerr
				}

				if n.primary() {
					return true, nil
				}

				repl, rerr := a.Do(ctx, "INFO", "replication")

				return rerr == nil && infoField(asString(repl), "master_link_status") == "up", rerr
			})
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// replicasVisible reports whether the node's view lists every replica as a healthy slave.
func (g *Group) replicasVisible(ctx context.Context, a Admin) (bool, error) {
	res, err := a.Do(ctx, "CLUSTER", "NODES")
	if err != nil {
		return false, err
	}

	want, slaves := len(g.nodes)-len(g.spec.Shards), 0

	for line := range strings.SplitSeq(asString(res), "\n") {
		fields := strings.Fields(line)
		if len(fields) < nodeFlagsField+1 {
			continue
		}

		flags := fields[nodeFlagsField]
		if strings.Contains(flags, "fail") || strings.Contains(flags, "handshake") {
			return false, nil
		}

		if strings.Contains(flags, "slave") {
			slaves++
		}
	}

	return slaves == want, nil
}
