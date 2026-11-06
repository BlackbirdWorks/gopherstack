package azurearm

import (
	"context"
	"time"
)

const (
	acaSettleInterval    = time.Second
	acaDefaultAppPolling = 30
	acaScaleUpMinStep    = 4
	acaScaleUpFactor     = 2
	acaSchedulerStopped  = "scheduler stopped"
)

// SetQueueMeters wires the queue depths that KEDA queue scale rules poll.
func (p *ContainerAppsProvider) SetQueueMeters(m ACAQueueMeters) {
	p.mu.Lock("SetQueueMeters")
	p.meters = m
	p.mu.Unlock()

	p.armScheduler()
}

func (rt *acaRuntime) arm(d time.Duration, fn func()) {
	rt.mu.Lock("arm")
	defer rt.mu.Unlock()

	if rt.timer != nil {
		rt.timer.Stop()
		rt.timer = nil
	}

	rt.gen++

	if rt.stopped {
		return
	}

	gen := rt.gen

	rt.timer = rt.clock.AfterFunc(d, func() {
		rt.mu.RLock("fire")
		current := rt.gen == gen
		rt.mu.RUnlock()

		if current {
			fn()
		}
	})
}

func (rt *acaRuntime) disarm() {
	rt.mu.Lock("disarm")
	defer rt.mu.Unlock()

	if rt.timer != nil {
		rt.timer.Stop()
		rt.timer = nil
	}

	rt.gen++
}

func (rt *acaRuntime) stop() {
	rt.mu.Lock("stop")
	rt.stopped = true
	rt.mu.Unlock()

	rt.disarm()
}

func (rt *acaRuntime) live(key string) int {
	rt.mu.RLock("live")
	ap := rt.pools[key]
	rt.mu.RUnlock()

	if ap == nil {
		return 0
	}

	return ap.pool.Live()
}

func (rt *acaRuntime) scaleTo(ctx context.Context, key string, n int) error {
	rt.mu.RLock("scaleTo")
	ap := rt.pools[key]
	rt.mu.RUnlock()

	if ap == nil {
		return nil
	}

	return ap.pool.Ensure(ctx, n)
}

func acaJobPolling(j *storedACAJob) time.Duration {
	sc := mapOf(mapOf(j.Config["eventTriggerConfig"])[fieldScale])

	return time.Duration(acaIntOf(sc, "pollingInterval", acaDefaultJobPolling)) * time.Second
}

func acaAppPolling(r *storedACARevision) time.Duration {
	sc := mapOf(r.Template[fieldScale])

	return time.Duration(acaIntOf(sc, "pollingInterval", acaDefaultAppPolling)) * time.Second
}

func (p *ContainerAppsProvider) appQueueRules(a *storedACAApp, r *storedACARevision) []acaQueueRule {
	if a.Stopped || !r.Active {
		return nil
	}

	return acaAppQueueRules(acaList(mapOf(r.Template[fieldScale])["rules"]))
}

// nextWake is the delay until the scheduler next has work: a cron fire, a queue poll or an execution to settle.
// The caller holds mu.
func (p *ContainerAppsProvider) nextWake(now time.Time) (time.Duration, bool) {
	best, found := time.Duration(0), false

	consider := func(d time.Duration) {
		d = max(d, 0)
		if !found || d < best {
			best, found = d, true
		}
	}

	for k, j := range p.jobs {
		switch strOf(j.Config, "triggerType") {
		case acaJobTriggerSchedule:
			c, err := parseACACron(strOf(mapOf(j.Config["scheduleTriggerConfig"]), "cronExpression"))
			if err != nil {
				continue
			}

			if n := c.next(j.LastFire); !n.IsZero() {
				consider(n.Sub(now))
			}
		case acaJobTriggerEvent:
			if p.meters != nil && len(acaJobQueueRules(acaList(mapOf(mapOf(j.Config["eventTriggerConfig"])[fieldScale])["rules"]))) > 0 {
				consider(acaJobPolling(j) - now.Sub(p.lastPoll[k]))
			}
		}

		for _, e := range j.Executions {
			if !e.Done && !e.Stopped {
				consider(acaSettleInterval)

				break
			}
		}
	}

	if p.meters != nil {
		for _, a := range p.apps {
			for _, r := range a.Revisions {
				if len(p.appQueueRules(a, r)) > 0 {
					consider(acaAppPolling(r) - now.Sub(p.lastPoll[acaRevKey(a, r.Name)]))
				}
			}
		}
	}

	return best, found
}

// armScheduler (re)arms the clock timer for the next piece of work, or disarms it when there is none.
func (p *ContainerAppsProvider) armScheduler() {
	p.mu.RLock("armScheduler")
	d, ok := p.nextWake(p.now().UTC())
	p.mu.RUnlock()

	if !ok {
		p.rt.disarm()

		return
	}

	p.rt.arm(d, p.tick)
}

func (p *ContainerAppsProvider) tick() {
	if !p.rt.busy.CompareAndSwap(false, true) {
		return
	}

	defer p.rt.busy.Store(false)

	now := p.now().UTC()

	p.settleExecutions(now)
	p.fireSchedules(now)
	p.pollEventJobs(now)
	p.scaleApps(now)
	p.armScheduler()
}

type acaJobRef struct{ rg, name string }

func (p *ContainerAppsProvider) fireSchedules(now time.Time) {
	var due []acaJobRef

	p.mu.Lock("fireSchedules")

	for _, j := range p.jobs {
		if strOf(j.Config, "triggerType") != acaJobTriggerSchedule {
			continue
		}

		c, err := parseACACron(strOf(mapOf(j.Config["scheduleTriggerConfig"]), "cronExpression"))
		if err != nil {
			continue
		}

		if n := c.next(j.LastFire); !n.IsZero() && !n.After(now) {
			j.LastFire = now
			due = append(due, acaJobRef{j.ResourceGroup, j.Name})
		}
	}
	p.mu.Unlock()

	for _, ref := range due {
		_, _ = p.startExecution(ref.rg, ref.name, nil, acaJobTriggerSchedule)
	}
}

type acaEventPoll struct {
	acaJobRef
	rules    []acaQueueRule
	min, max int
	running  int
	meters   ACAQueueMeters
}

func (p *ContainerAppsProvider) pollEventJobs(now time.Time) {
	var polls []acaEventPoll

	p.mu.Lock("pollEventJobs")

	for k, j := range p.jobs {
		if strOf(j.Config, "triggerType") != acaJobTriggerEvent || p.meters == nil {
			continue
		}

		sc := mapOf(mapOf(j.Config["eventTriggerConfig"])[fieldScale])

		rules := acaJobQueueRules(acaList(sc["rules"]))
		if len(rules) == 0 || now.Sub(p.lastPoll[k]) < acaJobPolling(j) {
			continue
		}

		p.lastPoll[k] = now

		running := 0

		for _, e := range j.Executions {
			if !e.Done && !e.Stopped {
				running++
			}
		}

		polls = append(polls, acaEventPoll{
			acaJobRef: acaJobRef{j.ResourceGroup, j.Name}, rules: rules, running: running, meters: p.meters,
			min: acaIntOf(sc, "minExecutions", 0), max: acaIntOf(sc, "maxExecutions", acaDefaultMaxExecutions),
		})
	}
	p.mu.Unlock()

	for _, poll := range polls {
		want, _ := acaDesiredFromQueues(poll.meters, poll.rules)
		want = max(min(want, poll.max), poll.min)

		for range want - poll.running {
			if _, err := p.startExecution(poll.rg, poll.name, nil, acaJobTriggerEvent); err != nil {
				break
			}
		}
	}
}

type acaScaleTask struct {
	key   string
	rules []acaQueueRule
}

func (p *ContainerAppsProvider) scaleApps(now time.Time) {
	p.mu.Lock("scaleApps")

	meters := p.meters
	if meters == nil {
		p.mu.Unlock()

		return
	}

	var tasks []acaScaleTask

	for _, a := range p.apps {
		for _, r := range a.Revisions {
			key := acaRevKey(a, r.Name)

			if rules := p.appQueueRules(a, r); len(rules) > 0 && now.Sub(p.lastPoll[key]) >= acaAppPolling(r) {
				p.lastPoll[key] = now
				tasks = append(tasks, acaScaleTask{key: key, rules: rules})
			}
		}
	}
	p.mu.Unlock()

	for _, t := range tasks {
		want, ok := acaDesiredFromQueues(meters, t.rules)
		if !ok || want == 0 {
			continue
		}

		cur := p.rt.live(t.key)
		if step := min(want, max(acaScaleUpMinStep, acaScaleUpFactor*cur)); step > cur {
			ctx, cancel := context.WithTimeout(context.Background(), acaReadyTimeout)
			err := p.rt.scaleTo(ctx, t.key, step)

			cancel()

			if err != nil {
				p.rt.setFailure(t.key, err.Error())
			}
		}
	}
}

type acaRetry struct {
	cfg aciConfig
}

// settleExecutions retries failed replicas and records the final state of executions that finished, timed out or
// completed, stopping the replicas still running.
func (p *ContainerAppsProvider) settleExecutions(now time.Time) {
	p.opMu.Lock("settleExecutions")
	defer p.opMu.Unlock()

	var (
		retries []acaRetry
		stops   []string
	)

	p.mu.Lock("settleExecutions")

	for _, j := range p.jobs {
		for _, e := range j.Executions {
			if e.Done || e.Stopped {
				continue
			}

			v := p.evaluate(j, e, now)
			retries = append(retries, p.retryReplicas(j, e, v.retry)...)

			if !v.final || v.status == acaExecUnknown {
				continue
			}

			e.Status, e.End, e.Done = v.status, v.end, true

			for i := range max(e.Replicas, 1) {
				stops = append(stops, acaExecKey(j, e.Name, i))
			}
		}
	}
	p.mu.Unlock()

	for _, r := range retries {
		p.rt.engine.Deploy(r.cfg)
	}

	for _, k := range stops {
		p.rt.engine.Stop(k)
	}
}

// retryReplicas redeploys the failed replicas, counting an attempt for each. The caller holds mu.
func (p *ContainerAppsProvider) retryReplicas(j *storedACAJob, e *storedACAExecution, idx []int) []acaRetry {
	if len(idx) == 0 {
		return nil
	}

	props, err := p.jobProps(j, e.Template)
	if err != nil {
		return nil
	}

	domain := ""
	if env := p.envByID(j.EnvironmentID); env != nil {
		domain = env.DefaultDomain
	}

	out := make([]acaRetry, 0, len(idx))

	for _, i := range idx {
		e.Attempts[i]++

		out = append(out, acaRetry{aciConfig{
			Key: acaExecKey(j, e.Name, i), Name: e.Name, Props: jobReplicaProps(props, j.Name, e.Name, domain),
		}})
	}

	return out
}
