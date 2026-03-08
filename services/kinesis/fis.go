package kinesis

import (
	"context"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
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

	return b.activateThroughputFault(ctx, streamNamesFromARNs(action.Targets), action.Duration)
}

// activateThroughputFault enables the throughput exception on the named streams
// and schedules automatic cleanup after dur (if non-zero).
func (b *InMemoryBackend) activateThroughputFault(ctx context.Context, names []string, dur time.Duration) error {
	var expiry time.Time
	if dur > 0 {
		expiry = time.Now().Add(dur)
	}

	b.mu.Lock("FISThroughputException")

	for _, name := range names {
		b.fisThroughputFaults[name] = expiry
	}

	b.mu.Unlock()

	if dur > 0 {
		go b.scheduleThroughputFaultCleanup(ctx, names, dur)
	}

	return nil
}

// scheduleThroughputFaultCleanup removes expired throughput faults after the
// given duration or when ctx is cancelled.
func (b *InMemoryBackend) scheduleThroughputFaultCleanup(ctx context.Context, names []string, dur time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(dur):
	}

	b.mu.Lock("FISThroughputException-cleanup")
	defer b.mu.Unlock()

	now := time.Now()

	for _, name := range names {
		if exp, exists := b.fisThroughputFaults[name]; exists {
			if !exp.IsZero() && now.After(exp) {
				delete(b.fisThroughputFaults, name)
			}
		}
	}
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
