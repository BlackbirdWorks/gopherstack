package cwmetric

import (
	"context"
	"slices"
	"sync/atomic"
	"time"
)

const (
	maxInlineDims = 4
	maxSeries     = 4096
)

// seriesKey identifies one metric series without allocating: dims live inline.
type seriesKey struct {
	region, namespace, name, unit string
	dims                          [maxInlineDims]Dimension
	n                             int
}

type item struct {
	key   seriesKey
	value float64
}

type stats struct {
	samples, sum, min, max float64
}

// Async is a bounded, non-blocking emitter: points queue to one worker and are dropped when full.
// With a window it folds points per series into statistic sets and flushes once per window.
type Async struct {
	ch      chan item
	inner   Emitter
	dropped atomic.Int64
	window  time.Duration
}

// NewAsync starts one worker forwarding to inner until ctx is done; window <= 0 forwards every point as-is.
func NewAsync(ctx context.Context, inner Emitter, size int, window time.Duration) *Async {
	a := &Async{ch: make(chan item, size), inner: inner, window: window}

	go a.run(ctx)

	return a
}

func (a *Async) run(ctx context.Context) {
	var tick <-chan time.Time

	if a.window > 0 {
		t := time.NewTicker(a.window)
		defer t.Stop()

		tick = t.C
	}

	agg := make(map[seriesKey]stats)

	for {
		select {
		case <-ctx.Done():
			return
		case it := <-a.ch:
			if a.window <= 0 {
				_ = a.inner.EmitMetric(it.key.point(it.value, 0, 0, 0, 0))

				continue
			}

			agg[it.key] = fold(agg[it.key], it.value)

			if len(agg) >= maxSeries {
				a.flush(agg)
			}
		case <-tick:
			a.flush(agg)
		}
	}
}

func fold(s stats, v float64) stats {
	if s.samples == 0 {
		return stats{samples: 1, sum: v, min: v, max: v}
	}

	return stats{samples: s.samples + 1, sum: s.sum + v, min: min(s.min, v), max: max(s.max, v)}
}

func (a *Async) flush(agg map[seriesKey]stats) {
	for k, s := range agg {
		_ = a.inner.EmitMetric(k.point(s.sum, s.samples, s.min, s.max, s.sum))
	}

	clear(agg)
}

func (k seriesKey) point(value, samples, lo, hi, sum float64) Point {
	return Point{
		Region: k.region, Namespace: k.namespace, Name: k.name, Unit: k.unit,
		Dimensions: slices.Clone(k.dims[:k.n]), Value: value,
		Samples: samples, Sum: sum, Min: lo, Max: hi,
	}
}

// EmitMetric enqueues p without blocking; a full queue drops it.
func (a *Async) EmitMetric(p Point) error {
	k := seriesKey{region: p.Region, namespace: p.Namespace, name: p.Name, unit: p.Unit}
	if len(p.Dimensions) > maxInlineDims {
		return a.inner.EmitMetric(p)
	}

	k.n = copy(k.dims[:], p.Dimensions)
	a.enqueue(item{key: k, value: p.Value})

	return nil
}

func (a *Async) put(region, namespace, name, unit string, value float64, dims []Dimension) {
	if len(dims) > maxInlineDims {
		_ = a.EmitMetric(Point{
			Region: region, Namespace: namespace, Name: name, Unit: unit,
			Dimensions: slices.Clone(dims), Value: value,
		})

		return
	}

	k := seriesKey{region: region, namespace: namespace, name: name, unit: unit}
	k.n = copy(k.dims[:], dims)
	a.enqueue(item{key: k, value: value})
}

func (a *Async) enqueue(it item) {
	select {
	case a.ch <- it:
	default:
		a.dropped.Add(1)
	}
}

// Dropped reports how many points were discarded because the queue was full.
func (a *Async) Dropped() int64 { return a.dropped.Load() }

// Sink is a lock-free holder for an optional Emitter, safe to read on hot paths.
type Sink struct {
	async atomic.Pointer[Async]
	p     atomic.Pointer[Emitter]
}

// Set installs e; nil disables emission.
func (s *Sink) Set(e Emitter) {
	a, isAsync := e.(*Async)

	switch {
	case e == nil:
		s.async.Store(nil)
		s.p.Store(nil)
	case isAsync:
		s.p.Store(nil)
		s.async.Store(a)
	default:
		s.async.Store(nil)
		s.p.Store(&e)
	}
}

// Emitter returns the installed emitter, or nil.
func (s *Sink) Emitter() Emitter {
	if a := s.async.Load(); a != nil {
		return a
	}

	if e := s.p.Load(); e != nil {
		return *e
	}

	return nil
}

// Enabled reports whether an emitter is installed.
func (s *Sink) Enabled() bool { return s.async.Load() != nil || s.p.Load() != nil }

// Put emits one data point; it is a no-op when no emitter is installed.
func (s *Sink) Put(region, namespace, name, unit string, value float64, dims ...Dimension) {
	if a := s.async.Load(); a != nil {
		a.put(region, namespace, name, unit, value, dims)

		return
	}

	if e := s.p.Load(); e != nil {
		_ = (*e).EmitMetric(Point{
			Region: region, Namespace: namespace, Name: name, Unit: unit, Value: value,
			Dimensions: slices.Clone(dims),
		})
	}
}
