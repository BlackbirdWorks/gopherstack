package amp

import (
	"bytes"
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const tickInterval = time.Second

type parsedNamespace struct {
	src  []byte
	defs []parsedGroup
}

// SetSNSPublisher wires the SNS emulator that receives alert manager notifications.
func (b *InMemoryBackend) SetSNSPublisher(p SNSPublisher) {
	b.mu.Lock("SetSNSPublisher")
	defer b.mu.Unlock()

	b.publisher = p
}

func (b *InMemoryBackend) resetRuntimeLocked() {
	b.groups = map[string]*groupRuntime{}
	b.parsed = map[string]*parsedNamespace{}
	b.ams = map[string]*alertManager{}
}

// appendSampleLocked stores one sample in the workspace's series for labels.
func (b *InMemoryBackend) appendSampleLocked(wsID string, labels promql.Labels, s promql.Sample) {
	key := seriesKey(wsID, promql.Fingerprint(labels))

	rec, ok := b.series.Get(key)
	if !ok {
		rec = &SeriesRecord{Key: key, WorkspaceID: wsID, Labels: labels.Clone()}
		b.series.Put(rec)
	}

	insertSample(rec, s)
}

func (b *InMemoryBackend) parsedFor(n *RuleGroupsNamespace) []parsedGroup {
	key := nsKey(n.WorkspaceID, n.Name)
	if p, ok := b.parsed[key]; ok && bytes.Equal(p.src, n.Doc.Data) {
		return p.defs
	}

	defs, err := parseRuleFile(n.Doc.Data)
	if err != nil {
		defs = nil
	}

	b.parsed[key] = &parsedNamespace{src: n.Doc.Data, defs: defs}

	return defs
}

// syncAlertManagersLocked reconciles the per-workspace dispatchers with the stored definitions.
func (b *InMemoryBackend) syncAlertManagersLocked(now time.Time) map[string]*alertManager {
	live := map[string]*alertManager{}

	b.workspaces.Range(func(w *Workspace) bool {
		if m := b.managerFor(w, now); m != nil {
			live[w.ID] = m
		}

		return true
	})

	b.ams = live

	return live
}

func (b *InMemoryBackend) managerFor(w *Workspace, now time.Time) *alertManager {
	d := w.AlertManager
	if d == nil || w.Pending.Kind == statusDeleting || d.Pending.Kind == statusDeleting ||
		b.state(w.Pending) != statusActive || b.state(d.Pending) == statusCreating {
		return nil
	}

	old := b.ams[w.ID]
	if old != nil && bytes.Equal(old.src, d.Data) {
		return old
	}

	raw, err := alertManagerConfigYAML(d.Data)
	if err != nil {
		return old
	}

	cfg, err := parseAlertManagerConfig(raw)
	if err != nil {
		return old
	}

	m := newAlertManager(cfg, d.Data)
	if old != nil {
		m.Receive(old.List(), now)
	}

	return m
}

// Tick runs every due rule group and flushes the alert managers at the backend clock's current time.
func (b *InMemoryBackend) Tick(ctx context.Context) {
	now := b.now()

	b.mu.Lock("Tick")
	b.purgeLocked()
	batches := b.evaluateRulesLocked(now)
	ams := b.syncAlertManagersLocked(now)
	pub := b.publisher
	b.mu.Unlock()

	for _, batch := range batches {
		if m := ams[batch.wsID]; m != nil {
			m.Receive(batch.alerts, now)
		}
	}

	for _, m := range ams {
		deliverNotifications(ctx, pub, m.Flush(now))
	}
}

// StartWorker evaluates rules and flushes alert managers every second until ctx ends.
func (h *Handler) StartWorker(ctx context.Context) error {
	go func() {
		t := time.NewTicker(tickInterval)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				h.Backend.Tick(ctx)
			}
		}
	}()

	return nil
}
