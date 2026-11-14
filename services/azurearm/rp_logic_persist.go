package azurearm

import (
	"encoding/json/v2"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// logicSnapshot is the Microsoft.Logic provider's persisted state: workflows with their versions, run history and
// trigger histories.
type logicSnapshot struct {
	Workflows map[string]*logicWorkflow `json:"workflows,omitempty"`
}

// snapshot deep-copies the state (the engine keeps mutating run records under the lock).
func (p *LogicProvider) snapshot() *logicSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	raw, err := json.Marshal(p.wf, jsonx.V1())
	if err != nil {
		return &logicSnapshot{}
	}

	snap := &logicSnapshot{Workflows: map[string]*logicWorkflow{}}
	if json.Unmarshal(raw, &snap.Workflows, jsonx.V1()) != nil {
		return &logicSnapshot{}
	}

	return snap
}

// restore replaces the provider's state; nil clears it. Runs that were in flight are marked cancelled.
func (p *LogicProvider) restore(snap *logicSnapshot) {
	p.mu.Lock("restore")

	for _, t := range p.timers {
		t.Stop()
	}

	p.timers = map[string]gcpscale.Timer{}
	p.wf = map[string]*logicWorkflow{}

	if snap != nil {
		now := p.clock.Now().UTC()

		for k, w := range snap.Workflows {
			if w == nil {
				continue
			}

			prepareRestoredWorkflow(w, now)
			p.wf[k] = w
			p.armWorkflowLocked(w)
		}
	}

	p.mu.Unlock()
}

func prepareRestoredWorkflow(w *logicWorkflow, now time.Time) {
	if w.Triggers == nil {
		w.Triggers = map[string]*logicTriggerState{}
	}

	for _, r := range w.Runs {
		if r.Actions == nil {
			r.Actions = map[string]*logicActionRec{}
		}

		if r.Status != logicStRunning {
			continue
		}

		r.Status, r.Code, r.End = logicStCancelled, "ServiceRestarted", now

		for _, a := range r.Actions {
			if a.Status == logicStRunning {
				a.Status, a.End = logicStCancelled, now
			}
		}
	}
}
