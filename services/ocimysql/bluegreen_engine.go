package ocimysql

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tcpfront"
)

const gtidPoll = 100 * time.Millisecond

var (
	errNotSeedable = errors.New("ocimysql: the green server cannot take a seed")
	errNoGTIDView  = errors.New("ocimysql: the servers do not answer SQL")
	errNotCaughtUp = errors.New("ocimysql: the green DB system did not apply the blue GTID set in time")
)

// Seeder is implemented by servers that can reset their binary log state and load a ReplicaSeed dump.
type Seeder interface {
	Seed(ctx context.Context, seed []byte) error
}

// spawnBlueGreen runs work off the lock and then settles the deployment: done on success, FAILED otherwise. Without
// the engine the clock settles it.
func (b *InMemoryBackend) spawnBlueGreen(
	id, wrID string,
	work func(ctx context.Context) error,
	done func(g *BlueGreen),
) {
	if b.eng == nil {
		return
	}

	ctx := b.runCtx

	b.wg.Go(func() { b.settleBlueGreenJob(ctx, id, wrID, work(ctx), done) })
}

func (b *InMemoryBackend) settleBlueGreenJob(
	ctx context.Context,
	id, wrID string,
	jobErr error,
	done func(*BlueGreen),
) {
	b.mu.Lock("settleBlueGreenJob")
	defer b.mu.Unlock()

	g, ok := b.blueGreens[id]
	if !ok || ctx.Err() != nil {
		return
	}

	g.TimeUpdated, g.Etag, g.TransitionAt, g.After, g.Details = b.nowUTC(), newEtag(), nil, "", ""

	if jobErr == nil {
		done(g)
		b.finishWR(wrID, "")

		return
	}

	g.State, g.Details = stateFailed, "Operation failed: "+jobErr.Error()
	if g.Switchover == bgInProgress {
		g.Switchover = bgFailed
	}

	b.finishWR(wrID, g.Details)
}

type bgPlan struct {
	srcID, greenID, channelID string
	wait                      time.Duration
}

func (b *InMemoryBackend) planBlueGreen(id string) (bgPlan, bool) {
	b.mu.RLock("planBlueGreen")
	defer b.mu.RUnlock()

	g, ok := b.blueGreens[id]
	if !ok {
		return bgPlan{}, false
	}

	return bgPlan{
		srcID:     g.SourceID,
		greenID:   g.TargetID,
		channelID: g.ChannelID,
		wait:      defaultSwitchWait * time.Second,
	}, true
}

// blueGreenCreateWork seeds the green server from the blue one and starts the channel between them.
func (b *InMemoryBackend) blueGreenCreateWork(id, _ string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		p, ok := b.planBlueGreen(id)
		if !ok {
			return nil
		}

		greenInst, err := b.waitSource(ctx, p.greenID)
		if err != nil {
			return err
		}

		seeder, ok := greenInst.(Seeder)
		if !ok {
			return errNotSeedable
		}

		src, ok := b.instance(p.srcID).(Replicable)
		if !ok {
			return errNoReplication
		}

		seed, err := replicaSeed(ctx, src)
		if err != nil {
			return err
		}

		if err = seeder.Seed(ctx, seed); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}

		b.setBlueGreenStatus(id, bgReplicate)

		if err = b.channelConfigure(p.channelID, false)(ctx); err != nil {
			return err
		}

		b.mu.Lock("blueGreenCreateWork")
		defer b.mu.Unlock()

		if c, found := b.channels[p.channelID]; found {
			c.State, c.After, c.TransitionAt, c.Etag = stateActive, "", nil, newEtag()
		}

		return nil
	}
}

func (b *InMemoryBackend) setBlueGreenStatus(id, status string) {
	b.mu.Lock("setBlueGreenStatus")
	defer b.mu.Unlock()

	if g, ok := b.blueGreens[id]; ok {
		g.Switchover = status
	}
}

// SwitchoverBlueGreen waits for the green DB system to apply the blue GTID set and then swaps their endpoints.
func (b *InMemoryBackend) SwitchoverBlueGreen(
	id, ifMatch, token string,
	waitSeconds *int,
) (Doc, string, string, error) {
	b.mu.Lock("SwitchoverBlueGreen")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("SwitchoverBlueGreen/"+id, token, b.nowUTC()); ok {
		g := b.blueGreens[id]

		return b.blueGreenDoc(g), g.Etag, v, nil
	}

	g, aerr := b.blueGreenFor(id, ifMatch)
	if aerr != nil {
		return nil, "", "", aerr
	}

	if g.State != stateActive || (g.Switchover != bgReady && g.Switchover != bgCompleted) {
		return nil, "", "", errState(
			"blue/green deployment " + id + " is in state " + g.State + " with switchover status " +
				g.Switchover + "; switchover requires ACTIVE and " + bgReady + " or " + bgCompleted,
		)
	}

	wait := defaultSwitchWait * time.Second

	if waitSeconds != nil {
		if *waitSeconds < 0 {
			return nil, "", "", errParam("waitTimeInSeconds must not be negative")
		}

		wait = time.Duration(*waitSeconds) * time.Second
	}

	g.State, g.Switchover, g.TimeUpdated, g.Etag = stateUpdating, bgInProgress, b.nowUTC(), newEtag()

	if b.eng == nil {
		b.swapRecordEndpoints(g)

		g.State, g.TransitionAt = stateUpdating, b.transitionAt()
		g.Switchover = bgCompleted
	}

	wr := b.newWR(wrSwitchoverBG, g.CompartmentID, entityBlueGreen, g.ID, actionUpdated, g.TransitionAt)
	b.retry.Remember("SwitchoverBlueGreen/"+id, token, wr, b.nowUTC())
	b.spawnBlueGreen(g.ID, wr, b.switchoverWork(g.ID, wait), func(g *BlueGreen) {
		g.State, g.Switchover = stateActive, bgCompleted
	})

	return b.blueGreenDoc(g), g.Etag, wr, nil
}

// swapRecordEndpoints exchanges the endpoint fields of the two DB systems without the engine and flips the active
// side. It must be called with the lock held.
func (b *InMemoryBackend) swapRecordEndpoints(g *BlueGreen) {
	a, c := b.systems[g.SourceID], b.systems[g.TargetID]
	if a == nil || c == nil {
		return
	}

	a.IP, c.IP = c.IP, a.IP
	a.Hostname, c.Hostname = c.Hostname, a.Hostname
	a.Port, c.Port = c.Port, a.Port
	a.PortX, c.PortX = c.PortX, a.PortX

	g.ActiveID = other(g, g.ActiveID)
}

func other(g *BlueGreen, id string) string {
	if id == g.SourceID {
		return g.TargetID
	}

	return g.SourceID
}

func (b *InMemoryBackend) switchoverWork(id string, wait time.Duration) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		p, ok := b.planBlueGreen(id)
		if !ok {
			return nil
		}

		blue, green := b.instance(p.srcID), b.instance(p.greenID)
		bq, qok := blue.(Querier)
		gq, gok := green.(Querier)
		cs, cok := green.(ChannelServer)

		if !qok || !gok || !cok {
			return errNoGTIDView
		}

		if err := b.waitCaughtUp(ctx, bq, gq, wait); err != nil {
			_ = b.syncSettings(ctx, p.srcID, true)

			return err
		}

		if err := b.stopBlueGreenChannel(ctx, p.channelID, cs); err != nil {
			return err
		}

		if err := b.syncSettings(ctx, p.greenID, true); err != nil {
			return err
		}

		return b.swapEngineEndpoints(ctx, id)
	}
}

// waitCaughtUp makes the blue server read-only, then waits until the green server has applied every blue transaction.
func (b *InMemoryBackend) waitCaughtUp(ctx context.Context, blue, green Querier, wait time.Duration) error {
	for _, stmt := range []string{"SET GLOBAL read_only = ON", "SET GLOBAL super_read_only = ON"} {
		if _, err := blue.Query(ctx, "", stmt); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	rows, err := blue.Query(ctx, "", "SELECT @@GLOBAL.gtid_executed")
	if err != nil || len(rows) < 2 {
		return errNoGTIDView
	}

	executed := strings.NewReplacer("\n", "", "'", "").Replace(rows[1][0])

	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	tick := time.NewTicker(gtidPoll)
	defer tick.Stop()

	for {
		got, qerr := green.Query(ctx, "", "SELECT GTID_SUBSET('"+executed+"', @@GLOBAL.gtid_executed)")
		if qerr == nil && len(got) > 1 && got[1][0] == "1" {
			return nil
		}

		select {
		case <-ctx.Done():
			return errNotCaughtUp
		case <-tick.C:
		}
	}
}

func (b *InMemoryBackend) stopBlueGreenChannel(ctx context.Context, channelID string, cs ChannelServer) error {
	b.mu.Lock("stopBlueGreenChannel")
	c, ok := b.channels[channelID]

	var name string

	if ok {
		name = c.Target.ChannelName
		c.Enabled, c.State, c.After, c.TransitionAt, c.Etag = false, stateInactive, "", nil, newEtag()
	}
	b.mu.Unlock()

	if !ok {
		return nil
	}

	return cs.RemoveChannel(ctx, name) //nolint:wrapcheck // engine errors carry the cause
}

// swapEngineEndpoints makes each DB system's stable endpoint reach the other's server.
func (b *InMemoryBackend) swapEngineEndpoints(ctx context.Context, id string) error {
	p, _ := b.planBlueGreen(id)

	b.mu.Lock("swapEngineEndpoints")
	defer b.mu.Unlock()

	g, ok := b.blueGreens[id]
	if !ok {
		return nil
	}

	for _, sys := range []string{p.srcID, p.greenID} {
		if err := b.ensureFront(ctx, sys); err != nil {
			return err
		}
	}

	b.eng.route[p.srcID], b.eng.route[p.greenID] = b.servingFor(p.greenID), b.servingFor(p.srcID)
	g.ActiveID = other(g, g.ActiveID)

	return nil
}

func (b *InMemoryBackend) servingFor(id string) string {
	if r, ok := b.eng.route[id]; ok {
		return r
	}

	return id
}

// ensureFront gives a DB system a stable client endpoint that follows the server currently serving it. It must be
// called with the lock held.
func (b *InMemoryBackend) ensureFront(ctx context.Context, id string) error {
	if _, ok := b.eng.fronts[id]; ok {
		return nil
	}

	f, err := tcpfront.New(context.WithoutCancel(ctx), func() (string, int) { return b.servingEndpoint(id) })
	if err != nil {
		return err //nolint:wrapcheck // listen errors carry the cause
	}

	b.eng.fronts[id] = f

	if s, ok := b.systems[id]; ok {
		s.EngineHost, s.EnginePort = f.Addr()
	}

	return nil
}

func (b *InMemoryBackend) servingEndpoint(id string) (string, int) {
	b.mu.RLock("servingEndpoint")
	inst := b.eng.live[b.servingFor(id)]
	b.mu.RUnlock()

	if inst == nil {
		return "", 0
	}

	return inst.Endpoint()
}

// dropFront closes the stable endpoint of a DB system that no longer exists.
func (b *InMemoryBackend) dropFront(id string) {
	b.mu.Lock("dropFront")
	fs := []*tcpfront.Front{b.eng.fronts[id], b.eng.fronts[readFrontPrefix+id]}
	delete(b.eng.fronts, id)
	delete(b.eng.fronts, readFrontPrefix+id)
	delete(b.eng.route, id)
	b.mu.Unlock()

	for _, f := range fs {
		if f != nil {
			f.Close()
		}
	}
}

// closeFronts closes the listeners taken by takeFrontsLocked; call it without the lock.
func closeFronts(fs []*tcpfront.Front) {
	for _, f := range fs {
		f.Close()
	}
}

// takeFrontsLocked detaches every front and its routing. It must be called with the lock held.
func (b *InMemoryBackend) takeFrontsLocked() []*tcpfront.Front {
	if b.eng == nil {
		return nil
	}

	out := make([]*tcpfront.Front, 0, len(b.eng.fronts))
	for _, f := range b.eng.fronts {
		out = append(out, f)
	}

	b.eng.fronts, b.eng.route = map[string]*tcpfront.Front{}, map[string]string{}

	return out
}

func (h *Handler) switchoverBlueGreen(q *req) error {
	var in struct {
		Wait *int `json:"waitTimeInSeconds"`
	}

	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	d, etag, wr, err := h.Backend.SwitchoverBlueGreen(q.param("blueGreenDeploymentId"), q.ifMatch(),
		q.r.Header.Get(headerRetry), in.Wait)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}
