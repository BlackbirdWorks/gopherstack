package timestreaminfluxdb

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const engineStartTimeout = 5 * time.Minute

// SetEngine switches the backend to engine mode: new instances run a real InfluxDB server.
func (b *InMemoryBackend) SetEngine(e Engine) {
	b.mu.Lock("SetEngine")
	defer b.mu.Unlock()

	b.engine = e

	if b.live == nil {
		b.live = map[string]EngineHandle{}
	}

	if b.bg == nil {
		b.bg, b.cancel = context.WithCancel(context.Background())
	}
}

// startEngineLocked launches the engine for inst in the background; the instance stays in its
// transitional status until the server accepts connections.
func (b *InMemoryBackend) startEngineLocked(inst *DBInstance, kind string) {
	if b.engine == nil || b.closed {
		return
	}

	inst.Pending = b.pend(kind, true)

	spec := EngineSpec{
		Name: inst.ID, Username: inst.Username, Password: inst.Password,
		Organization: inst.Organization, Bucket: inst.Bucket,
	}
	eng, id := b.engine, inst.ID

	b.wg.Go(func() {
		ctx, cancel := context.WithTimeout(b.bg, engineStartTimeout)
		defer cancel()

		h, err := eng.Start(ctx, spec)
		b.finishEngine(id, h, err)
	})
}

func (b *InMemoryBackend) finishEngine(id string, h EngineHandle, startErr error) {
	b.mu.Lock("finishEngine")

	inst, ok := b.instances.Get(id)
	if startErr != nil {
		defer b.mu.Unlock()

		logger.Load(b.bg).
			WarnContext(b.bg, "timestreaminfluxdb: engine start failed", "instance", id, "error", startErr)

		if ok {
			inst.Failed = startErr.Error()
			inst.Pending = Pending{Kind: statusFailed, Until: b.now().Add(farFuture)}
		}

		return
	}

	if !ok || b.closed || inst.Pending.Kind == statusDeleting {
		b.mu.Unlock()
		b.stopEngines([]EngineHandle{h})

		return
	}

	b.live[id] = h
	inst.EngineHost, inst.EnginePort = h.Host, h.Port
	inst.Pending = Pending{}
	b.mu.Unlock()
}

// detachEnginesLocked removes the live handles of instances; stop them with stopEngines after unlocking.
func (b *InMemoryBackend) detachEnginesLocked(insts []*DBInstance) []EngineHandle {
	var out []EngineHandle

	for _, i := range insts {
		if h, ok := b.live[i.ID]; ok {
			out = append(out, h)

			delete(b.live, i.ID)
		}
	}

	return out
}

func (b *InMemoryBackend) stopEngines(handles []EngineHandle) {
	if b.engine == nil {
		return
	}

	for _, h := range handles {
		if err := b.engine.Stop(context.WithoutCancel(b.bgOrBackground()), h); err != nil {
			logger.Load(b.bgOrBackground()).WarnContext(b.bgOrBackground(),
				"timestreaminfluxdb: engine stop failed", "container", h.ContainerID, "error", err)
		}
	}
}

func (b *InMemoryBackend) bgOrBackground() context.Context {
	if b.bg != nil {
		return b.bg
	}

	return context.Background()
}

// Close stops every engine container and waits for in-flight starts.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	b.closed = true

	if b.cancel != nil {
		b.cancel()
	}

	handles := make([]EngineHandle, 0, len(b.live))
	for id, h := range b.live {
		handles = append(handles, h)

		delete(b.live, id)
	}
	b.mu.Unlock()

	b.wg.Wait()
	b.stopEngines(handles)
}
