package ocimysql

import (
	"context"
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
)

var errChannelPassword = errors.New(
	"ocimysql: the source password is not kept across a restart; supply it with UpdateChannel",
)

var errChannelSource = errors.New("ocimysql: the source DB system has no running server")

// ChannelServer is implemented by servers that run inbound replication channels.
type ChannelServer interface {
	JoinNetwork(ctx context.Context, src mysqlengine.ReplSource) error
	AddChannel(ctx context.Context, c mysqlengine.ChannelSpec, start bool) error
	SetChannel(ctx context.Context, name string, on bool) error
	RemoveChannel(ctx context.Context, name string) error
	ChannelStatus(ctx context.Context, name string) (mysqlengine.ChannelState, error)
}

// spawnChannel runs work off the lock and then settles the channel: done on success, NEEDS_ATTENTION otherwise.
func (b *InMemoryBackend) spawnChannel(id, wrID string, work func(ctx context.Context) error, done func(c *Channel)) {
	ctx := b.runCtx

	b.wg.Go(func() { b.settleChannelJob(ctx, id, wrID, work(ctx), done) })
}

func (b *InMemoryBackend) settleChannelJob(ctx context.Context, id, wrID string, jobErr error, done func(*Channel)) {
	b.mu.Lock("settleChannelJob")
	defer b.mu.Unlock()

	c, ok := b.channels[id]
	if !ok || ctx.Err() != nil {
		return
	}

	c.TimeUpdated, c.Etag, c.TransitionAt, c.After = b.nowUTC(), newEtag(), nil, ""
	c.Details = ""

	if jobErr == nil {
		done(c)
		b.finishWR(wrID, "")

		return
	}

	c.State, c.Details = stateNeedsAttention, "Operation failed: "+jobErr.Error()
	b.finishWR(wrID, c.Details)
}

// channelJob configures the channel on the target server; it does nothing without the engine.
func (b *InMemoryBackend) channelJob(c *Channel, wrID string, work func(ctx context.Context) error) {
	if b.eng == nil {
		return
	}

	b.spawnChannel(c.ID, wrID, work, func(c *Channel) { c.State = enabledState(c.Enabled) })
}

func (b *InMemoryBackend) deleteChannelJob(c *Channel, wrID string) {
	if b.eng == nil {
		return
	}

	id, target, name := c.ID, c.Target.DBSystemID, c.Target.ChannelName

	b.spawnChannel(id, wrID, func(ctx context.Context) error {
		cs, ok := b.instance(target).(ChannelServer)
		if !ok {
			return nil
		}

		return cs.RemoveChannel(ctx, name) //nolint:wrapcheck // engine errors carry the cause
	}, func(c *Channel) { c.State = stateDeleted })
}

// interruptChannels settles the channels whose engine job an emulator restart cut short.
func (b *InMemoryBackend) interruptChannels() {
	for _, c := range b.channels {
		if c.TransitionAt != nil {
			continue
		}

		switch c.State {
		case stateCreating, stateUpdating:
			c.State, c.Details = stateNeedsAttention, "Operation interrupted by an emulator restart"
		case stateDeleting:
			c.State = stateDeleted
		}
	}
}

// emulatedSource finds the DB system a channel source address names. It must be called with the lock held.
func (b *InMemoryBackend) emulatedSource(host string, port int, except string) (string, bool) {
	if strings.EqualFold(host, "localhost") {
		host = "127.0.0.1"
	}

	for id, s := range b.systems {
		if id == except || s.State == stateDeleted {
			continue
		}

		eh, ep := b.endpointFor(s)

		if (eh == host && ep == port) || (s.IP == host && s.Port == port) ||
			(s.Hostname != "" && strings.EqualFold(s.Hostname, host) && s.Port == port) {
			return id, true
		}
	}

	return "", false
}

type channelPlan struct {
	srcID  string
	target string
	spec   mysqlengine.ChannelSpec
	start  bool
}

func (b *InMemoryBackend) planChannel(id string) (channelPlan, bool) {
	b.mu.RLock("planChannel")
	defer b.mu.RUnlock()

	c, ok := b.channels[id]
	if !ok {
		return channelPlan{}, false
	}

	p := channelPlan{
		target: c.Target.DBSystemID, start: c.Enabled,
		spec: mysqlengine.ChannelSpec{
			Name: c.Target.ChannelName, Host: c.Source.Hostname, Port: c.Source.Port, User: c.Source.Username,
			Password: c.Source.Password, SSL: !strings.EqualFold(c.Source.SslMode, "DISABLED"),
			Delay: c.Target.DelayInSeconds,
		},
	}
	p.srcID, _ = b.emulatedSource(c.Source.Hostname, c.Source.Port, p.target)

	return p, true
}

// channelConfigure points the target server at the channel's source and starts it when the channel is enabled. A
// source that is another emulated DB system is reached over its replication network.
func (b *InMemoryBackend) channelConfigure(id string, reset bool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		p, ok := b.planChannel(id)
		if !ok {
			return nil
		}

		cs, ok := b.instance(p.target).(ChannelServer)
		if !ok {
			return errNoReplication
		}

		if p.spec.Password == "" {
			return resumeChannel(ctx, cs, p, reset)
		}

		if err := b.linkSource(ctx, cs, &p); err != nil {
			return err
		}

		if reset {
			if err := cs.RemoveChannel(ctx, p.spec.Name); err != nil {
				return err //nolint:wrapcheck // engine errors carry the cause
			}
		}

		return cs.AddChannel(ctx, p.spec, p.start) //nolint:wrapcheck // engine errors carry the cause
	}
}

// resumeChannel restarts or stops a channel already configured on the server; its password is no longer known.
func resumeChannel(ctx context.Context, cs ChannelServer, p channelPlan, reset bool) error {
	if reset {
		return errChannelPassword
	}

	return cs.SetChannel(ctx, p.spec.Name, p.start) //nolint:wrapcheck // engine errors carry the cause
}

// linkSource points the plan at the replication network of an emulated source DB system.
func (b *InMemoryBackend) linkSource(ctx context.Context, cs ChannelServer, p *channelPlan) error {
	if p.srcID == "" {
		return nil
	}

	rep, ok := b.instance(p.srcID).(Replicable)
	if !ok {
		return errChannelSource
	}

	link, err := rep.ReplicationSource(ctx)
	if err != nil {
		return err //nolint:wrapcheck // engine errors carry the cause
	}

	if err = cs.JoinNetwork(ctx, link); err != nil {
		return err //nolint:wrapcheck // engine errors carry the cause
	}

	p.spec.Host, p.spec.Port = link.Host, link.Port

	return nil
}

// refreshChannelLive reads the replication health of a channel from its target server.
func (b *InMemoryBackend) refreshChannelLive(ctx context.Context, id string) {
	b.mu.RLock("refreshChannelLive")
	c, ok := b.channels[id]

	var target, name string
	if ok && b.eng != nil {
		target, name = c.Target.DBSystemID, c.Target.ChannelName
	}
	b.mu.RUnlock()

	if target == "" {
		return
	}

	cs, ok := b.instance(target).(ChannelServer)
	if !ok {
		return
	}

	st, err := cs.ChannelStatus(ctx, name)

	live := &ChannelLive{Healthy: err == nil && st.Running}
	if err != nil {
		live.Errors = []string{err.Error()}
	}

	for _, e := range []string{st.IOError, st.SQLError} {
		if e != "" {
			live.Errors = append(live.Errors, e)
		}
	}

	b.mu.Lock("refreshChannelLive")
	if cur, found := b.channels[id]; found {
		cur.Live = live
	}
	b.mu.Unlock()
}
