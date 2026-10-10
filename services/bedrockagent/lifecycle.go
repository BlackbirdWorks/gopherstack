package bedrockagent

import "time"

type transition struct {
	start  time.Time
	phases []string
}

// SetLifecycleDelay makes agent preparation, knowledge base creation and
// ingestion jobs dwell in their transitional states for d; 0 (the default)
// settles instantly. Transitions are not persisted.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.lifecycleDelay.Store(int64(d))
}

// beginTransition records a transitional overlay on key. Caller holds b.mu.
func (b *InMemoryBackend) beginTransition(key string, phases ...string) {
	if b.lifecycleDelay.Load() <= 0 {
		return
	}

	if b.transient == nil {
		b.transient = make(map[string]transition)
	}

	b.transient[key] = transition{start: time.Now(), phases: phases}
}

// statusOf returns the transitional status for key if one is still running,
// else final. Caller holds at least b.mu.RLock.
func (b *InMemoryBackend) statusOf(key, final string) string {
	d := time.Duration(b.lifecycleDelay.Load())
	tr, ok := b.transient[key]

	if !ok || d <= 0 {
		return final
	}

	elapsed := time.Since(tr.start)
	if elapsed >= d {
		return final
	}

	return tr.phases[int(elapsed*time.Duration(len(tr.phases))/d)]
}

func (b *InMemoryBackend) agentView(a *Agent) *Agent {
	cp := agentCopy(a)
	cp.AgentStatus = b.statusOf("agent/"+a.AgentID, a.AgentStatus)

	return cp
}

func (b *InMemoryBackend) kbView(kb *KnowledgeBase) *KnowledgeBase {
	cp := kbCopy(kb)
	cp.Status = b.statusOf("kb/"+kb.KnowledgeBaseID, kb.Status)

	return cp
}

func (b *InMemoryBackend) jobView(j *IngestionJob) *IngestionJob {
	cp := jobCopy(j)
	cp.Status = b.statusOf("job/"+jobKey(j.KnowledgeBaseID, j.DataSourceID, j.IngestionJobID), j.Status)

	return cp
}

func (b *InMemoryBackend) ingestionInFlightLocked(kbID, dsID string) bool {
	for _, j := range b.ingestionJobsByDataSource.Get(dsKey(kbID, dsID)) {
		if s := b.jobView(j).Status; s == ingestionJobStarting || s == ingestionJobRunning {
			return true
		}
	}

	return false
}
