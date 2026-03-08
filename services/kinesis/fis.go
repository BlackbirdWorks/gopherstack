package kinesis

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// kinesisThrottleMaxPercentage is the maximum percentage value (100%).
	kinesisThrottleMaxPercentage = 100
	// kinesisThrottleDivisor converts an integer percentage to a probability fraction.
	kinesisThrottleDivisor = float64(kinesisThrottleMaxPercentage)
)

// FISActions returns the FIS action definitions that the Kinesis service supports.
func (h *Handler) FISActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:kinesis:stream-provisioned-throughput-exception",
			Description: "Return ProvisionedThroughputExceededException on PutRecord/GetRecords for the target stream",
			TargetType:  "aws:kinesis:stream",
			Parameters: []service.FISParamDef{
				{Name: "duration", Description: "ISO 8601 duration (e.g. PT5M)", Required: false},
				{
					Name:        "percentage",
					Description: "Percentage of requests to throttle (0-100, default 100)",
					Required:    false,
				},
			},
		},
	}
}

// ExecuteFISAction executes a FIS action against resolved Kinesis targets.
func (h *Handler) ExecuteFISAction(ctx context.Context, action service.FISActionExecution) error {
	if action.ActionID != "aws:kinesis:stream-provisioned-throughput-exception" {
		return nil
	}

	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	prob := parseThrottlePercentage(action.Parameters["percentage"])

	return b.activateThroughputFault(ctx, streamNamesFromARNs(action.Targets), action.Duration, prob)
}

// activateThroughputFault enables the throughput exception on the named streams.
// It always registers a goroutine that clears the fault when ctx is cancelled
// (experiment stopped), and also schedules time-based expiry when dur > 0.
func (b *InMemoryBackend) activateThroughputFault(
	ctx context.Context,
	names []string,
	dur time.Duration,
	prob float64,
) error {
	var expiry time.Time
	if dur > 0 {
		expiry = time.Now().Add(dur)
	}

	b.mu.Lock("FISThroughputException")

	for _, name := range names {
		b.fisThroughputFaults[name] = &kinesisThrottleFault{
			expiry:      expiry,
			probability: prob,
		}
	}

	b.mu.Unlock()

	if dur > 0 {
		// Time-limited: clear after duration or on cancellation.
		go b.scheduleThroughputFaultCleanup(ctx, names, dur)
	} else {
		// Indefinite fault (dur==0): the goroutine blocks on ctx.Done().
		// It terminates when StopExperiment cancels the experiment context,
		// or when the server shuts down (root context is cancelled).
		// This is not a goroutine leak — the goroutine is intentionally
		// bound to the experiment lifetime via ctx.
		go func() {
			<-ctx.Done()

			b.mu.Lock("FISThroughputException-ctxcancel")
			defer b.mu.Unlock()

			for _, name := range names {
				delete(b.fisThroughputFaults, name)
			}
		}()
	}

	return nil
}

// scheduleThroughputFaultCleanup removes throughput faults after the given
// duration or when ctx is cancelled (whichever comes first).
// On ctx cancellation, entries are removed unconditionally so that StopExperiment
// always clears active faults regardless of remaining time.
func (b *InMemoryBackend) scheduleThroughputFaultCleanup(ctx context.Context, names []string, dur time.Duration) {
	ctxCancelled := false

	select {
	case <-ctx.Done():
		ctxCancelled = true
	case <-time.After(dur):
	}

	b.mu.Lock("FISThroughputException-cleanup")
	defer b.mu.Unlock()

	now := time.Now()

	for _, name := range names {
		fault, exists := b.fisThroughputFaults[name]
		if !exists || fault == nil {
			continue
		}

		// On ctx cancellation always remove; on timeout only remove if expired.
		if ctxCancelled || (!fault.expiry.IsZero() && now.After(fault.expiry)) {
			delete(b.fisThroughputFaults, name)
		}
	}
}

// parseThrottlePercentage converts a percentage string (0-100) to a probability (0.0-1.0).
// An empty or invalid string defaults to 100% (1.0). Negative values also default to 1.0.
// "0" returns 0.0 (no fault injection).
func parseThrottlePercentage(s string) float64 {
	if s == "" {
		return 1.0
	}

	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return 1.0
	}

	if v >= kinesisThrottleMaxPercentage {
		return 1.0
	}

	return float64(v) / kinesisThrottleDivisor
}

// streamNamesFromARNs extracts Kinesis stream names from ARNs or bare names.
// ARN format: arn:aws:kinesis:{region}:{account}:stream/{name}.
func streamNamesFromARNs(arns []string) []string {
	names := make([]string, 0, len(arns))

	for _, a := range arns {
		if idx := strings.LastIndex(a, "/"); idx >= 0 {
			name := a[idx+1:]
			if name != "" {
				names = append(names, name)

				continue
			}
		}

		if a != "" {
			names = append(names, a)
		}
	}

	return names
}
