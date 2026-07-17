package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CancelReplay cancels a running or starting replay.
func (b *InMemoryBackend) CancelReplay(ctx context.Context, replayName string) (*Replay, error) {
	if replayName == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CancelReplay")
	defer b.mu.Unlock()

	replay, exists := b.replaysTable(region).Get(replayName)
	if !exists {
		return nil, fmt.Errorf("%w: replay %s not found", ErrNotFound, replayName)
	}

	if replay.State != "RUNNING" && replay.State != replayStateStarting {
		return nil, fmt.Errorf(
			"%w: replay %s is not in a cancellable state (current: %s)",
			ErrInvalidState,
			replayName,
			replay.State,
		)
	}

	replay.State = "CANCELLING"

	cp := *replay

	return &cp, nil
}

// DescribeReplay returns a single replay by name.
func (b *InMemoryBackend) DescribeReplay(ctx context.Context, name string) (*Replay, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeReplay")
	defer b.mu.RUnlock()

	replay, exists := b.replaysTable(region).Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: replay %s not found", ErrNotFound, name)
	}

	cp := *replay

	return &cp, nil
}

// ListReplays returns replays optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListReplays(ctx context.Context, namePrefix, nextToken string) ([]Replay, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListReplays")
	defer b.mu.RUnlock()

	store := b.replaysTable(region)
	all := make([]Replay, 0, store.Len())
	for _, r := range store.All() {
		if namePrefix == "" || strings.HasPrefix(r.ReplayName, namePrefix) {
			all = append(all, *r)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ReplayName < all[j].ReplayName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// StartReplay creates a new replay in the STARTING state.
func (b *InMemoryBackend) StartReplay(ctx context.Context, input StartReplayInput) (*Replay, error) {
	if input.ReplayName == "" {
		return nil, fmt.Errorf("%w: ReplayName is required", ErrInvalidParameter)
	}

	if input.EventSourceArn == "" {
		return nil, fmt.Errorf("%w: EventSourceArn is required", ErrInvalidParameter)
	}

	if !input.EventStartTime.IsZero() && !input.EventEndTime.IsZero() &&
		!input.EventStartTime.Before(input.EventEndTime) {
		return nil, fmt.Errorf(
			"%w: EventStartTime must be before EventEndTime",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("StartReplay")

	replays := b.replaysTable(region)
	if replays.Has(input.ReplayName) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: replay %s already exists", ErrAlreadyExists, input.ReplayName)
	}

	// Validate destination ARN points to a known event bus.
	if input.Destination != nil && input.Destination.Arn != "" {
		found := false
		for _, bus := range b.busesTable(region).All() {
			if bus.Arn == input.Destination.Arn {
				found = true

				break
			}
		}
		if !found {
			b.mu.Unlock()

			return nil, fmt.Errorf(
				"%w: destination ARN %s does not match any event bus",
				ErrInvalidParameter,
				input.Destination.Arn,
			)
		}
	}

	// Find the archive by ARN (EventSourceArn points to an archive ARN).
	var archiveName string
	var archivePattern string
	for _, archive := range b.archivesTable(region).All() {
		if archive.ArchiveArn == input.EventSourceArn {
			archiveName = archive.ArchiveName
			archivePattern = archive.EventPattern

			break
		}
	}

	replay := &Replay{
		EventSourceArn:  input.EventSourceArn,
		EventStartTime:  input.EventStartTime,
		EventEndTime:    input.EventEndTime,
		ReplayArn:       b.replayARN(input.ReplayName),
		ReplayName:      input.ReplayName,
		ReplayStartTime: time.Now(),
		State:           replayStateStarting,
		StateReason:     input.Description,
	}
	replays.Put(replay)

	// Collect archived events to replay filtered by time window and event pattern.
	eventsToReplay := b.filterArchivedEvents(
		region,
		archiveName,
		archivePattern,
		input.EventStartTime,
		input.EventEndTime,
	)

	dt := b.deliveryTargets
	workerSem := b.workerSem
	svcCtx := b.ctx
	delivTimeout := b.deliveryTimeout
	cp := *replay
	b.mu.Unlock()

	// Deliver archived events asynchronously and mark the replay complete.
	if !b.closing.Load() {
		b.scheduleReplayWorker(svcCtx, region, workerSem, input.ReplayName, eventsToReplay, dt, delivTimeout)
	}

	return &cp, nil
}

// filterArchivedEvents returns archived events for the named archive filtered by
// time window [startTime, endTime) and optional event pattern.
// Must be called with b.mu held for reading.
func (b *InMemoryBackend) filterArchivedEvents(
	region, archiveName, pattern string,
	startTime, endTime time.Time,
) []EventEntry {
	if archiveName == "" {
		return nil
	}

	raw := b.archivedEventsStore(region)[archiveName]
	if len(raw) == 0 {
		return nil
	}

	result := make([]EventEntry, 0, len(raw))
	for _, e := range raw {
		t := time.Now()
		if e.Time != nil {
			t = *e.Time
		}
		if !startTime.IsZero() && t.Before(startTime) {
			continue
		}
		if !endTime.IsZero() && !t.Before(endTime) {
			continue
		}
		if pattern != "" {
			envelope := buildEventEnvelope(e)
			if !matchPattern(pattern, envelope) {
				continue
			}
		}
		result = append(result, e)
	}

	return result
}

// scheduleReplayWorker launches a background goroutine that delivers archived events
// and then marks the replay COMPLETED. Extracted to reduce cognitive complexity of StartReplay.
func (b *InMemoryBackend) scheduleReplayWorker(
	ctx context.Context,
	region string,
	workerSem chan struct{},
	replayName string,
	eventsToReplay []EventEntry,
	dt *DeliveryTargets,
	delivTimeout time.Duration,
) {
	b.wg.Go(func() {
		select {
		case workerSem <- struct{}{}:
			defer func() { <-workerSem }()
		case <-ctx.Done():
			return
		}

		if dt != nil && len(eventsToReplay) > 0 {
			b.deliverEvents(ctx, region, eventsToReplay, *dt, delivTimeout)
		}

		b.mu.Lock("StartReplay-complete")
		if r, ok := b.replaysTable(region).Get(replayName); ok && r.State == replayStateStarting {
			r.State = "COMPLETED"
			r.ReplayEndTime = time.Now()
		}
		b.mu.Unlock()
	})
}

// AddReplayInternal adds a replay directly for testing.
func (b *InMemoryBackend) AddReplayInternal(replay *Replay) {
	b.mu.Lock("AddReplayInternal")
	defer b.mu.Unlock()

	if replay.ReplayArn == "" {
		replay.ReplayArn = b.replayARN(replay.ReplayName)
	}

	cp := *replay
	b.replaysTable(b.region).Put(&cp)
}
