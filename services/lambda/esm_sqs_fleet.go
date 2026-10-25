package lambda

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

const (
	sqsPollersDefaultMin = 2
	sqsPollersDefaultMax = 200
	sqsInvokesPerPoller  = 10
	sqsFleetScaleFactor  = 2
	sqsMaxBatchesPerTick = 64
	esmMetricProvPollers = "ProvisionedPollers"
)

var errESMFunctionError = errors.New("function returned an error")

// sqsFleet is the poller count of a provisioned-mode mapping; guarded by EventSourcePoller.mu.
type sqsFleet struct{ pollers int }

func sqsPollerBounds(ppc *ESMProvisionedPollerConfig) (int, int) {
	lo, hi := sqsPollersDefaultMin, sqsPollersDefaultMax

	if ppc.MinimumPollers != nil {
		lo = int(*ppc.MinimumPollers)
	}

	if ppc.MaximumPollers != nil {
		hi = int(*ppc.MaximumPollers)
	}

	return lo, max(hi, lo)
}

// sqsConcurrency returns how many batches the mapping may have in flight this tick.
// Without ScalingConfig or ProvisionedPollerConfig a mapping stays serial.
func (p *EventSourcePoller) sqsConcurrency(m *EventSourceMapping) int {
	limit := 1

	if ppc := m.ProvisionedPollerConfig; ppc != nil {
		lo, hi := sqsPollerBounds(ppc)

		p.mu.Lock("sqsConcurrency")
		f := p.sqsFleets[m.UUID]
		if f == nil {
			f = &sqsFleet{pollers: lo}
			p.sqsFleets[m.UUID] = f
		}

		f.pollers = min(max(f.pollers, lo), hi)
		limit = f.pollers * sqsInvokesPerPoller
		p.mu.Unlock()
	}

	if sc := m.ScalingConfig; sc != nil && sc.MaximumConcurrency != nil {
		if m.ProvisionedPollerConfig == nil {
			return int(*sc.MaximumConcurrency)
		}

		limit = min(limit, int(*sc.MaximumConcurrency))
	}

	return limit
}

// resizeSQSFleet doubles the provisioned pollers when every worker found work and halves
// them when under half did, within the configured bounds.
func (p *EventSourcePoller) resizeSQSFleet(m *EventSourceMapping, busy, workers int) {
	if m.ProvisionedPollerConfig == nil {
		return
	}

	lo, hi := sqsPollerBounds(m.ProvisionedPollerConfig)

	p.mu.Lock("resizeSQSFleet")
	defer p.mu.Unlock()

	f := p.sqsFleets[m.UUID]
	if f == nil {
		return
	}

	switch {
	case busy >= workers:
		f.pollers = min(f.pollers*sqsFleetScaleFactor, hi)
	case busy*sqsFleetScaleFactor < workers:
		f.pollers = max(f.pollers/sqsFleetScaleFactor, lo)
	}

	f.pollers = max(f.pollers, lo)

	if p.lambdaBackend.metrics.Enabled() {
		p.lambdaBackend.metrics.Put(p.lambdaBackend.region, lambdaMetricNamespace, esmMetricProvPollers,
			lambdaUnitCount, float64(f.pollers), cwmetric.Dimension{Name: esmMetricDimUUID, Value: m.UUID})
	}
}

// SQSPollerCount reports the provisioned poller count of a mapping, or 0 when it has none.
func (p *EventSourcePoller) SQSPollerCount(uuid string) int {
	p.mu.RLock("SQSPollerCount")
	defer p.mu.RUnlock()

	if f := p.sqsFleets[uuid]; f != nil {
		return f.pollers
	}

	return 0
}

// processSQSMapping drains the queue with up to sqsConcurrency(m) concurrent batch
// workers; a mapping with no concurrency settings processes one batch per tick.
func (p *EventSourcePoller) processSQSMapping(ctx context.Context, m *EventSourceMapping, reader SQSReader) {
	workers := p.sqsConcurrency(m)
	if workers <= 1 && m.ProvisionedPollerConfig == nil {
		p.processSQSBatch(ctx, m, reader)

		return
	}

	var (
		wg   sync.WaitGroup
		busy atomic.Int32
	)

	for range workers {
		wg.Go(func() {
			for i := range sqsMaxBatchesPerTick {
				if ctx.Err() != nil {
					return
				}

				n := p.processSQSBatch(ctx, m, reader)
				if n == 0 {
					return
				}

				if i == 0 {
					busy.Add(1)
				}
			}
		})
	}

	wg.Wait()
	p.resizeSQSFleet(m, int(busy.Load()), workers)
}

// invokeESMFunctionSync delivers a batch with RequestResponse so the mapping holds its
// concurrency slot until the function finishes; a function error fails the batch.
func (p *EventSourcePoller) invokeESMFunctionSync(
	ctx context.Context, fnName, qualifier string, payload []byte,
) ([]byte, error) {
	result, _, fnErr, _, err := p.lambdaBackend.InvokeFunctionWithQualifier(
		ctx, fnName, qualifier, "", "", InvocationTypeRequestResponse, payload,
	)
	if err != nil {
		return nil, err
	}

	if fnErr != "" {
		return result, errESMFunctionError
	}

	return result, nil
}
