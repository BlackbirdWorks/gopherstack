package azurearm

import "strings"

const (
	logicConcDefaultRuns   = 25
	logicConcMaxRuns       = 100
	logicConcMaxWaiting    = 100
	logicConcWaitingExtra  = 10
	logicOptSingleInstance = "SingleInstance"
)

type logicConcurrency struct {
	runs    int
	waiting int
}

type logicQueued struct {
	run    *logicRunRec
	live   *logicLive
	launch func()
}

// triggerConcurrency reads runtimeConfiguration.concurrency; runs is 0 when concurrency is off.
// Limits: learn.microsoft.com/azure/logic-apps/logic-apps-limits-and-config (Concurrency and debatching).
func triggerConcurrency(def map[string]any) logicConcurrency {
	rt, _ := objectOf(def["runtimeConfiguration"])
	cc, hasCC := objectOf(rt["concurrency"])
	single := strings.EqualFold(stringOf(def["operationOptions"]), logicOptSingleInstance)

	if !hasCC && !single {
		return logicConcurrency{}
	}

	runs := logicConcDefaultRuns

	if n, ok := logicInt(normLogic(cc["runs"])); ok && n >= 1 {
		runs = int(min(n, logicConcMaxRuns))
	}

	if single {
		runs = 1
	}

	waiting := min(runs+logicConcWaitingExtra, logicConcMaxWaiting)
	if n, ok := logicInt(normLogic(cc["maximumWaitingRuns"])); ok && n >= 1 {
		waiting = int(min(n, logicConcMaxWaiting))
	}

	return logicConcurrency{runs: runs, waiting: waiting}
}

func countRuns(w *logicWorkflow, trigger, status string) int {
	n := 0

	for _, r := range w.Runs {
		if r.Status == status && r.Trigger.Name == trigger {
			n++
		}
	}

	return n
}

// dispatchQueuedLocked starts waiting runs of a trigger while it is below its concurrent-run limit.
func (p *LogicProvider) dispatchQueuedLocked(wfKey, trigger string) {
	w, ok := p.wf[wfKey]
	if !ok {
		return
	}

	lim := triggerConcurrency(w.triggerDef(trigger))
	qk := wfKey + "|" + trigger

	for len(p.queue[qk]) > 0 && (lim.runs == 0 || countRuns(w, trigger, logicStRunning) < lim.runs) {
		e := p.queue[qk][0]
		p.queue[qk] = p.queue[qk][1:]
		e.run.Status = logicStRunning
		e.launch()
	}

	if len(p.queue[qk]) == 0 {
		delete(p.queue, qk)
	}
}

// dropQueuedLocked cancels a waiting run without ever starting it.
func (p *LogicProvider) dropQueuedLocked(w *logicWorkflow, r *logicRunRec) *logicLive {
	qk := w.key() + "|" + r.Trigger.Name
	kept := p.queue[qk][:0]

	var dropped *logicLive

	for _, e := range p.queue[qk] {
		if e.run == r {
			e.live.cancel()
			close(e.live.done)

			dropped = e.live

			continue
		}

		kept = append(kept, e)
	}

	if len(kept) == 0 {
		delete(p.queue, qk)
	} else {
		p.queue[qk] = kept
	}

	delete(p.live, w.key()+"|"+r.Name)
	r.Status, r.End = logicStCancelled, p.clock.Now().UTC()

	return dropped
}
