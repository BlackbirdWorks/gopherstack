package rds

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

const (
	autoPauseDefaultSeconds = 300
	autoPauseMinSeconds     = 300
	autoPauseMaxSeconds     = 86400
	serverlessMinACU        = 0.5
	serverlessMaxACU        = 256
	serverlessACUStep       = 0.5
	serverlessInstanceClass = "db.serverless"
	autoPauseSweepInterval  = time.Second
	autoPauseMetricInterval = time.Minute

	// Event texts for RDS-EVENT-0370, 0372, 0373 and 0374 (Aurora events table, DB instance events).
	eventPauseInitiated  = "Initiated pause for the DB instance."
	eventPauseDone       = "Successfully paused the DB instance."
	eventResumeInitiated = "Initiated resume for the DB instance."
	eventResumeDone      = "Successfully resumed the DB instance."

	envAutoPauseDisabled    = "RDS_AUTO_PAUSE_DISABLED"
	envAutoPauseIdleScale   = "RDS_AUTO_PAUSE_IDLE_SCALE"
	envAutoPauseResumeDelay = "RDS_AUTO_PAUSE_RESUME_DELAY"
)

// AutoPauseConfig tunes Aurora Serverless v2 auto-pause; the zero value is real-AWS timing.
type AutoPauseConfig struct {
	// ResumeDelay is extra time a resume takes before the cluster accepts connections.
	ResumeDelay time.Duration
	// IdleScale multiplies SecondsUntilAutoPause (0 means 1); e.g. 0.01 turns 300s into 3s.
	IdleScale float64
	// Disabled turns auto-pause off for every cluster.
	Disabled bool
}

// AutoPauseConfigFromEnv reads RDS_AUTO_PAUSE_DISABLED, RDS_AUTO_PAUSE_IDLE_SCALE and RDS_AUTO_PAUSE_RESUME_DELAY.
func AutoPauseConfigFromEnv() AutoPauseConfig {
	var cfg AutoPauseConfig

	cfg.Disabled, _ = strconv.ParseBool(os.Getenv(envAutoPauseDisabled))

	if v, err := strconv.ParseFloat(os.Getenv(envAutoPauseIdleScale), 64); err == nil && v > 0 {
		cfg.IdleScale = v
	}

	if d, err := time.ParseDuration(os.Getenv(envAutoPauseResumeDelay)); err == nil && d > 0 {
		cfg.ResumeDelay = d
	}

	return cfg
}

type pauseTracker struct {
	lastActive time.Time
	lastMetric time.Time
	resumeDone chan struct{}
	stopDone   chan struct{}
	conns      int
	paused     bool
}

// SetAutoPause replaces the auto-pause tuning.
func (b *InMemoryBackend) SetAutoPause(cfg AutoPauseConfig) {
	b.mu.Lock("SetAutoPause")
	defer b.mu.Unlock()

	b.autoPause = cfg
}

// AutoPause returns the current auto-pause tuning.
func (b *InMemoryBackend) AutoPause() AutoPauseConfig {
	b.mu.RLock("AutoPause")
	defer b.mu.RUnlock()

	return b.autoPause
}

// SetMetricEmitter on the handler sets the emitter on every regional backend, present and future.
func (h *Handler) SetMetricEmitter(e cwmetric.Emitter) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.Backend.SetMetricEmitter(e)

	for _, bk := range h.backends {
		bk.SetMetricEmitter(e)
	}
}

// SetMetricEmitter sets the emitter that publishes zeroed capacity metrics for paused instances.
func (b *InMemoryBackend) SetMetricEmitter(e cwmetric.Emitter) { b.metrics.Set(e) }

// IsClusterAutoPaused reports whether the cluster's Aurora Serverless v2 instances are currently auto-paused.
func (b *InMemoryBackend) IsClusterAutoPaused(clusterID string) bool {
	b.mu.RLock("IsClusterAutoPaused")
	defer b.mu.RUnlock()

	t := b.pause[normalizeID(clusterID)]

	return t != nil && t.paused
}

// ClusterAutoPaused reports a cluster's pause state from a copy returned by the describe operations.
func ClusterAutoPaused(c *DBCluster) bool { return c != nil && c.AutoPaused }

func autoPauseConfigured(c *DBCluster) bool {
	cfg := c.ServerlessV2ScalingConfig

	return cfg != nil && cfg.MinCapacity == 0 && strings.HasPrefix(c.Engine, "aurora")
}

func (b *InMemoryBackend) autoPauseWindowLocked(c *DBCluster) time.Duration {
	secs := c.ServerlessV2ScalingConfig.SecondsUntilAutoPause
	if secs <= 0 {
		secs = autoPauseDefaultSeconds
	}

	d := time.Duration(secs) * time.Second
	if s := b.autoPause.IdleScale; s > 0 {
		d = time.Duration(float64(d) * s)
	}

	return d
}

func (b *InMemoryBackend) trackerLocked(id string) *pauseTracker {
	key := normalizeID(id)

	t, ok := b.pause[key]
	if !ok {
		t = &pauseTracker{lastActive: time.Now()}
		b.pause[key] = t
	}

	return t
}

func (b *InMemoryBackend) clusterMembersLocked(c *DBCluster) []*DBInstance {
	var out []*DBInstance

	for _, inst := range b.instances.All() {
		if idEqual(inst.DBClusterIdentifier, c.DBClusterIdentifier) {
			out = append(out, inst)
		}
	}

	return out
}

// autoPauseBlockedLocked reports why a configured cluster cannot pause right now.
func (b *InMemoryBackend) autoPauseBlockedLocked(c *DBCluster) bool {
	if b.autoPause.Disabled || c.Status != instanceStatusAvailable {
		return true
	}

	members := b.clusterMembersLocked(c)
	for _, inst := range members {
		if inst.DBInstanceClass != serverlessInstanceClass || inst.DBInstanceStatus != instanceStatusAvailable {
			return true
		}
	}

	if b.inGlobalClusterLocked(c) || b.hasProxyTargetLocked(c, members) {
		return true
	}

	lb := b.clusterUnitLocked(c.DBClusterIdentifier)

	return lb != nil && (!lb.ready || !lb.observable())
}

func (b *InMemoryBackend) inGlobalClusterLocked(c *DBCluster) bool {
	for _, g := range b.globalClusters.All() {
		for _, m := range g.GlobalClusterMembers {
			if m.DBClusterArn == c.DBClusterArn {
				return true
			}
		}
	}

	return false
}

func (b *InMemoryBackend) hasProxyTargetLocked(c *DBCluster, members []*DBInstance) bool {
	for _, targets := range b.proxyTargets {
		for _, t := range targets {
			if idEqual(t.TrackedClusterID, c.DBClusterIdentifier) || t.RdsResourceID == c.DBClusterResourceID {
				return true
			}

			for _, inst := range members {
				if idEqual(t.TrackedClusterID, inst.DBInstanceIdentifier) {
					return true
				}
			}
		}
	}

	return false
}

func (b *InMemoryBackend) publishMemberEventsLocked(c *DBCluster, msg string) {
	for _, inst := range b.clusterMembersLocked(c) {
		b.publishInstanceEventLocked(inst.DBInstanceIdentifier, msg)
	}
}

func (b *InMemoryBackend) scheduleAutoPauseLocked() {
	if b.pauseRunning || b.closed {
		return
	}

	b.pauseRunning = true
	b.pauseWG.Add(1)
	stopCh := b.stopCh

	go func() {
		defer b.pauseWG.Done()

		ticker := time.NewTicker(autoPauseSweepInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
			}

			b.mu.Lock("autoPauseSweep")

			if !b.sweepAutoPauseLocked() {
				b.pauseRunning = false
				b.mu.Unlock()

				return
			}

			b.mu.Unlock()
		}
	}()
}

// sweepAutoPauseLocked pauses idle clusters; false means no cluster uses auto-pause and the sweeper can exit.
func (b *InMemoryBackend) sweepAutoPauseLocked() bool {
	now := time.Now()
	active := false

	for _, c := range b.clusters.All() {
		if !autoPauseConfigured(c) {
			continue
		}

		active = true
		t := b.trackerLocked(c.DBClusterIdentifier)

		switch {
		case t.paused:
			b.emitPausedMetricsLocked(c, t, now)
		case b.autoPauseBlockedLocked(c) || t.conns > 0:
			t.lastActive = now
		case now.Sub(t.lastActive) >= b.autoPauseWindowLocked(c):
			b.pauseClusterLocked(c, t)
		}
	}

	return active
}

func (b *InMemoryBackend) pauseClusterLocked(c *DBCluster, t *pauseTracker) {
	t.paused = true
	t.stopDone = make(chan struct{})
	c.AutoPaused = true
	b.publishMemberEventsLocked(c, eventPauseInitiated)

	lb := b.clusterUnitLocked(c.DBClusterIdentifier)
	if lb == nil {
		close(t.stopDone)
		b.publishMemberEventsLocked(c, eventPauseDone)

		return
	}

	lb.paused = true
	id, cid := lb.containerID, c.DBClusterIdentifier

	b.pauseWG.Go(func() { b.execPause(lb, t, id, cid) })
}

func (b *InMemoryBackend) execPause(lb *liveDB, t *pauseTracker, containerID, clusterID string) {
	ctx, cancel := lb.opContext(0)
	defer cancel()

	sctx, scancel := context.WithTimeout(ctx, engineStopTimeout)
	err := b.engine.cfg.Runtime.StopContainer(sctx, containerID)

	scancel()

	if err != nil {
		b.failUnit(ctx, lb, "pause stop container", err)
	}

	b.mu.Lock("autoPauseStopped")
	defer b.mu.Unlock()

	if err == nil && t.paused {
		if c, ok := b.clusters.Get(normalizeID(clusterID)); ok {
			b.publishMemberEventsLocked(c, eventPauseDone)
		}
	}

	close(t.stopDone)
}

// wakeClusterLocked starts resuming a paused cluster; the channel closes when done (nil if not paused).
func (b *InMemoryBackend) wakeClusterLocked(clusterID string) <-chan struct{} {
	t := b.pause[normalizeID(clusterID)]
	if t == nil || !t.paused {
		return nil
	}

	if t.resumeDone != nil {
		return t.resumeDone
	}

	if b.closed {
		return nil
	}

	c, ok := b.clusters.Get(normalizeID(clusterID))
	if !ok {
		t.paused = false

		return nil
	}

	t.resumeDone = make(chan struct{})
	b.publishMemberEventsLocked(c, eventResumeInitiated)

	lb := b.clusterUnitLocked(clusterID)
	done := t.resumeDone
	delay := b.autoPause.ResumeDelay

	b.pauseWG.Go(func() { b.execResume(lb, t, c.DBClusterIdentifier, delay) })

	return done
}

func (b *InMemoryBackend) execResume(lb *liveDB, t *pauseTracker, clusterID string, delay time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	if lb != nil {
		ctx, cancel = lb.opContext(0)
	}

	defer cancel()

	ok := b.resumeSteps(ctx, lb, t, delay)

	b.mu.Lock("autoPauseResumed")
	defer b.mu.Unlock()

	t.paused = false
	t.lastActive = time.Now()

	if lb != nil {
		lb.paused = false
	}

	if t.resumeDone != nil {
		close(t.resumeDone)
		t.resumeDone = nil
	}

	c, found := b.clusters.Get(normalizeID(clusterID))
	if !found {
		return
	}

	c.AutoPaused = false

	if ok {
		b.publishMemberEventsLocked(c, eventResumeDone)
	}
}

func (b *InMemoryBackend) resumeSteps(ctx context.Context, lb *liveDB, t *pauseTracker, delay time.Duration) bool {
	select {
	case <-t.stopDone:
	case <-ctx.Done():
		return false
	case <-b.stopCh:
		return false
	}

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-ctx.Done():
			return false
		case <-b.stopCh:
			return false
		}
	}

	if lb == nil {
		return true
	}

	b.mu.RLock("autoPauseResumeID")
	id := lb.containerID
	b.mu.RUnlock()

	sctx, cancel := context.WithTimeout(ctx, engineOpTimeout)
	err := b.engine.cfg.Runtime.StartContainer(sctx, id)

	cancel()

	if err != nil {
		b.failUnit(ctx, lb, "resume start container", err)

		return false
	}

	b.mu.RLock("autoPauseResumeLogin")
	login := b.engine.login(lb)
	b.mu.RUnlock()

	rctx, rcancel := context.WithTimeout(ctx, b.engine.cfg.StartTimeout)
	defer rcancel()

	if err = b.engine.awaitReady(rctx, login); err != nil {
		b.failUnit(ctx, lb, "resume readiness", err)

		return false
	}

	return true
}

// cancelPauseLocked drops a unit's pause state before an explicit container operation takes over.
func (b *InMemoryBackend) cancelPauseLocked(lb *liveDB) {
	lb.paused = false

	cid, ok := strings.CutPrefix(lb.key, unitClusterPrefix)
	if !ok {
		return
	}

	if t := b.pause[cid]; t != nil {
		t.paused = false
	}

	if c, found := b.clusters.Get(cid); found {
		c.AutoPaused = false
	}
}

// beginConn counts a client connection against the cluster behind lb, resuming it first when paused.
func (b *InMemoryBackend) beginConn(lb *liveDB) (func(), bool) {
	b.mu.Lock("rdsConnBegin")

	cid, isCluster := strings.CutPrefix(lb.key, unitClusterPrefix)
	if !isCluster || !b.attachedLocked(lb) {
		b.mu.Unlock()

		return func() {}, true
	}

	t := b.trackerLocked(cid)
	t.conns++
	t.lastActive = time.Now()
	wait := b.wakeClusterLocked(cid)
	b.mu.Unlock()

	var released bool

	release := func() {
		b.mu.Lock("rdsConnEnd")
		defer b.mu.Unlock()

		if released {
			return
		}

		released = true
		t.conns--
		t.lastActive = time.Now()
	}

	if wait != nil {
		select {
		case <-wait:
		case <-lb.gone:
			release()

			return nil, false
		}
	}

	return release, true
}

// wakeUnit resumes a paused cluster unit and waits for it, e.g. before an in-engine administrative change.
func (b *InMemoryBackend) wakeUnit(lb *liveDB) {
	b.mu.Lock("rdsWakeUnit")

	cid, isCluster := strings.CutPrefix(lb.key, unitClusterPrefix)
	if !isCluster {
		b.mu.Unlock()

		return
	}

	wait := b.wakeClusterLocked(cid)
	b.mu.Unlock()

	if wait != nil {
		select {
		case <-wait:
		case <-lb.gone:
		}
	}
}

// resumeClusterAsyncLocked resumes a paused cluster without waiting (administrative changes wake it).
func (b *InMemoryBackend) resumeClusterAsyncLocked(clusterID string) {
	_ = b.wakeClusterLocked(clusterID)
}

func (b *InMemoryBackend) emitPausedMetricsLocked(c *DBCluster, t *pauseTracker, now time.Time) {
	e := b.metrics.Emitter()
	if e == nil || now.Sub(t.lastMetric) < autoPauseMetricInterval {
		return
	}

	t.lastMetric = now

	for _, inst := range b.clusterMembersLocked(c) {
		dims := []cwmetric.Dimension{{Name: "DBInstanceIdentifier", Value: inst.DBInstanceIdentifier}}

		for _, m := range []struct{ name, unit string }{
			{"ServerlessDatabaseCapacity", "Count"},
			{"ACUUtilization", "Percent"},
			{"CPUUtilization", "Percent"},
		} {
			_ = e.EmitMetric(cwmetric.Point{
				Region: b.region, Namespace: "AWS/RDS", Name: m.name, Unit: m.unit, Dimensions: dims, Value: 0,
			})
		}
	}
}

// validateServerlessV2Scaling checks a ServerlessV2ScalingConfiguration against the documented limits.
func validateServerlessV2Scaling(cfg *ServerlessV2ScalingConfiguration) error {
	if cfg == nil {
		return nil
	}

	if err := validateACU("MinCapacity", cfg.MinCapacity, 0); err != nil {
		return err
	}

	if err := validateACU("MaxCapacity", cfg.MaxCapacity, serverlessMinACU); err != nil {
		return err
	}

	if cfg.MinCapacity > cfg.MaxCapacity {
		return fmt.Errorf("%w: ServerlessV2ScalingConfiguration.MinCapacity %g must not exceed MaxCapacity %g",
			ErrInvalidParameter, cfg.MinCapacity, cfg.MaxCapacity)
	}

	if s := cfg.SecondsUntilAutoPause; s != 0 && (s < autoPauseMinSeconds || s > autoPauseMaxSeconds) {
		return fmt.Errorf("%w: ServerlessV2ScalingConfiguration.SecondsUntilAutoPause must be between %d and %d",
			ErrInvalidParameter, autoPauseMinSeconds, autoPauseMaxSeconds)
	}

	return nil
}

func validateACU(field string, v, lo float64) error {
	steps := v / serverlessACUStep
	if v < lo || v > serverlessMaxACU || steps != float64(int64(steps)) {
		return fmt.Errorf("%w: ServerlessV2ScalingConfiguration.%s %g must be between %g and %d in %g increments",
			ErrInvalidParameter, field, v, lo, serverlessMaxACU, serverlessACUStep)
	}

	return nil
}

// normalizeServerlessV2Scaling applies the documented SecondsUntilAutoPause defaulting: only shown (and
// defaulted to 300) when MinCapacity is 0, removed otherwise.
func normalizeServerlessV2Scaling(cfg *ServerlessV2ScalingConfiguration) {
	switch {
	case cfg == nil:
	case cfg.MinCapacity != 0:
		cfg.SecondsUntilAutoPause = 0
	case cfg.SecondsUntilAutoPause == 0:
		cfg.SecondsUntilAutoPause = autoPauseDefaultSeconds
	}
}

// mergeServerlessV2Update validates a ModifyDBCluster scaling update merged over the cluster's configuration;
// a nil update leaves the cluster unchanged.
func mergeServerlessV2Update(
	c *DBCluster,
	upd *ServerlessV2ScalingConfiguration,
) (*ServerlessV2ScalingConfiguration, error) {
	if upd == nil {
		return c.ServerlessV2ScalingConfig, nil
	}

	merged := ServerlessV2ScalingConfiguration{}
	if cur := c.ServerlessV2ScalingConfig; cur != nil {
		merged = *cur
	}

	if !upd.MinCapacityOmitted {
		merged.MinCapacity = upd.MinCapacity
	}

	if upd.MaxCapacity != 0 {
		merged.MaxCapacity = upd.MaxCapacity
	}

	if upd.SecondsUntilAutoPause != 0 {
		merged.SecondsUntilAutoPause = upd.SecondsUntilAutoPause
	}

	if err := validateServerlessV2Scaling(&merged); err != nil {
		return nil, err
	}

	normalizeServerlessV2Scaling(&merged)

	return &merged, nil
}

// applyServerlessV2Update stores a merged scaling configuration; changing the scaling range wakes a paused cluster.
func (b *InMemoryBackend) applyServerlessV2Update(c *DBCluster, merged *ServerlessV2ScalingConfiguration) {
	if merged == nil || merged == c.ServerlessV2ScalingConfig {
		return
	}

	c.ServerlessV2ScalingConfig = merged

	b.resumeClusterAsyncLocked(c.DBClusterIdentifier)

	if autoPauseConfigured(c) {
		b.trackerLocked(c.DBClusterIdentifier).lastActive = time.Now()
		b.scheduleAutoPauseLocked()
	}
}
