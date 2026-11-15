package ocifunctions

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// prewarm starts the containers that provisioned concurrency reserves for every ACTIVE function.
func (b *InMemoryBackend) prewarm() {
	b.mu.RLock("prewarm")

	var ids []string

	for id, f := range b.funcs {
		if f.State == stateActive && f.Provisioned.Strategy == concConstant && f.Provisioned.Count != nil &&
			*f.Provisioned.Count > 0 {
			ids = append(ids, id)
		}
	}
	b.mu.RUnlock()

	for _, id := range ids {
		if p, aerr := b.plan(id); aerr == nil {
			b.exec.warmUp(p)
		}
	}
}

func (e *executor) warmUp(p execPlan) {
	rt, err := e.runner()
	if err != nil {
		return
	}

	var stale []*warm

	e.mu.Lock("warmUp")

	keep := e.idle[p.fnID][:0:0]

	for _, w := range e.idle[p.fnID] {
		if w.pinned && w.sig != p.sig {
			stale = append(stale, w)
		} else {
			keep = append(keep, w)
		}
	}

	e.idle[p.fnID] = keep

	if e.warming[p.fnID] {
		e.mu.Unlock()
		e.teardownAll(stale)

		return
	}

	e.warming[p.fnID] = true
	e.mu.Unlock()

	e.teardownAll(stale)

	e.wg.Go(func() {
		defer func() {
			e.mu.Lock("warmDone")
			delete(e.warming, p.fnID)
			e.mu.Unlock()
		}()

		for e.addPinned(rt, p) {
		}
	})
}

func (e *executor) teardownAll(ws []*warm) {
	for _, w := range ws {
		e.teardown(w)
	}
}

// addPinned starts one more reserved container while fewer than p.provisioned exist and capacity remains.
func (e *executor) addPinned(rt Runner, p execPlan) bool {
	e.mu.Lock("addPinned")

	if e.runCtx.Err() != nil || e.pinned[p.fnID] >= p.provisioned || e.busy+e.idleCountLocked() >= e.maxLive {
		e.mu.Unlock()

		return false
	}

	e.busy++
	e.mu.Unlock()

	w, err := e.start(e.runCtx, rt, p)
	if err != nil {
		e.abandon()
		logger.Load(e.runCtx).
			WarnContext(context.WithoutCancel(e.runCtx), "ocifunctions: provisioned container failed to start",
				"function", p.fnID, "error", err)

		return false
	}

	w.pinned = true

	e.mu.Lock("addPinned")
	e.busy--
	e.pinned[p.fnID]++
	w.last = e.now().UTC()
	e.idle[p.fnID] = append(e.idle[p.fnID], w)
	e.mu.Unlock()

	return true
}
