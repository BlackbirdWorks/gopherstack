package lambda

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// lambdaFISMaxPercentage is the maximum percentage value (100%) for FIS fault injection.
	lambdaFISMaxPercentage = 100
	// lambdaFISDivisor converts a percentage integer to a probability float.
	lambdaFISDivisor = 100.0
	// lambdaARNFuncParts is the expected number of parts after splitting on ":function:".
	lambdaARNFuncParts = 2
	// decimalBase is the base for decimal integer parsing.
	decimalBase = 10
)

// ErrFISInvocationError is returned when a FIS invocation-error action is active for a function.
var ErrFISInvocationError = errors.New("FunctionError: FIS fault injection: invocation error")

// errNotAnInteger is a sentinel error for integer parsing failures.
var errNotAnInteger = errors.New("not an integer")

// FISInvocationFault holds the fault configuration for a Lambda function invocation.
type FISInvocationFault struct {
	Expiry           time.Time
	ErrorProbability float64
	AddDelayMs       int
}

// FISActions returns the FIS action definitions that the Lambda service supports.
func (h *Handler) FISActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:lambda:invocation-error",
			Description: "Force Lambda invocations to return errors for the specified duration",
			TargetType:  "aws:lambda:function",
			Parameters: []service.FISParamDef{
				{Name: "duration", Description: "ISO 8601 duration (e.g. PT5M)", Required: true},
				{
					Name:        "percentage",
					Description: "Percentage of invocations to fault (0-100)",
					Required:    false,
					Default:     "100",
				},
			},
		},
		{
			ActionID:    "aws:lambda:invocation-add-delay",
			Description: "Add latency to Lambda invocations for the specified duration",
			TargetType:  "aws:lambda:function",
			Parameters: []service.FISParamDef{
				{Name: "duration", Description: "ISO 8601 duration (e.g. PT5M)", Required: true},
				{
					Name:        "invocationDelayMilliseconds",
					Description: "Delay in milliseconds to add to each invocation",
					Required:    true,
				},
				{
					Name:        "percentage",
					Description: "Percentage of invocations to affect (0-100)",
					Required:    false,
					Default:     "100",
				},
			},
		},
	}
}

// ExecuteFISAction executes a FIS action against resolved Lambda function targets.
func (h *Handler) ExecuteFISAction(ctx context.Context, action service.FISActionExecution) error {
	names := functionNamesFromARNs(action.Targets)

	switch action.ActionID {
	case "aws:lambda:invocation-error":
		return h.activateLambdaInvocationError(ctx, names, action)
	case "aws:lambda:invocation-add-delay":
		return h.activateLambdaInvocationDelay(ctx, names, action)
	}

	return nil
}

// activateLambdaInvocationError activates invocation-error fault for the given functions.
func (h *Handler) activateLambdaInvocationError(
	ctx context.Context,
	names []string,
	action service.FISActionExecution,
) error {
	prob := parseInvocationPercentage(action.Parameters["percentage"])
	expiry := expiryFromDuration(action.Duration)

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		for _, name := range names {
			b.setFISFault(name, &FISInvocationFault{
				ErrorProbability: prob,
				Expiry:           expiry,
			})
		}

		// Schedule fault removal.
		if action.Duration > 0 {
			go func() {
				select {
				case <-ctx.Done():
				case <-time.After(action.Duration):
				}

				for _, name := range names {
					b.clearFISFault(name)
				}
			}()
		}
	}

	return nil
}

// activateLambdaInvocationDelay activates invocation-add-delay fault for the given functions.
func (h *Handler) activateLambdaInvocationDelay(
	ctx context.Context,
	names []string,
	action service.FISActionExecution,
) error {
	delayMs := parseInvocationDelayMs(action.Parameters["invocationDelayMilliseconds"])
	expiry := expiryFromDuration(action.Duration)

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		for _, name := range names {
			b.setFISFault(name, &FISInvocationFault{
				AddDelayMs: delayMs,
				Expiry:     expiry,
			})
		}

		if action.Duration > 0 {
			go func() {
				select {
				case <-ctx.Done():
				case <-time.After(action.Duration):
				}

				for _, name := range names {
					b.clearFISFault(name)
				}
			}()
		}
	}

	return nil
}

// setFISFault registers a FIS fault for a Lambda function.
func (b *InMemoryBackend) setFISFault(name string, fault *FISInvocationFault) {
	b.mu.Lock("setFISFault")
	defer b.mu.Unlock()

	b.fisFaults[name] = fault
}

// clearFISFault removes a FIS fault for a Lambda function.
func (b *InMemoryBackend) clearFISFault(name string) {
	b.mu.Lock("clearFISFault")
	defer b.mu.Unlock()

	delete(b.fisFaults, name)
}

// checkFISFault returns the active FIS fault for a function, or nil if none.
// Expired faults are automatically cleared.
func (b *InMemoryBackend) checkFISFault(name string) *FISInvocationFault {
	b.mu.Lock("checkFISFault")
	defer b.mu.Unlock()

	fault := b.fisFaults[name]
	if fault == nil {
		return nil
	}

	if !fault.Expiry.IsZero() && time.Now().After(fault.Expiry) {
		delete(b.fisFaults, name)

		return nil
	}

	return fault
}

// applyFISFault checks if a FIS fault applies to an invocation and returns an error
// or adds a delay accordingly. Returns nil if no fault is active.
func (b *InMemoryBackend) applyFISFault(name string) (time.Duration, error) {
	fault := b.checkFISFault(name)
	if fault == nil {
		return 0, nil
	}

	// Apply probability.
	prob := fault.ErrorProbability
	if prob <= 0 && fault.AddDelayMs <= 0 {
		return 0, nil
	}

	var delay time.Duration

	// Apply delay with probability (if no separate delay probability, always apply).
	if fault.AddDelayMs > 0 {
		delayProb := fault.ErrorProbability
		if delayProb <= 0 {
			delayProb = 1.0
		}

		//nolint:gosec // weak random intentional for fault injection
		if rand.Float64() < delayProb {
			delay = time.Duration(fault.AddDelayMs) * time.Millisecond
		}
	}

	if fault.ErrorProbability > 0 {
		//nolint:gosec // weak random intentional for fault injection
		if rand.Float64() < fault.ErrorProbability {
			return delay, ErrFISInvocationError
		}
	}

	return delay, nil
}

// functionNamesFromARNs extracts Lambda function names from ARNs.
// ARN format: arn:aws:lambda:{region}:{account}:function:{name}.
func functionNamesFromARNs(arns []string) []string {
	names := make([]string, 0, len(arns))

	for _, a := range arns {
		if strings.Contains(a, ":function:") {
			parts := strings.SplitN(a, ":function:", lambdaARNFuncParts)
			if len(parts) == lambdaARNFuncParts && parts[1] != "" {
				names = append(names, parts[1])

				continue
			}
		}

		// Not an ARN — treat as a bare function name.
		if a != "" {
			names = append(names, a)
		}
	}

	return names
}

// parseInvocationPercentage converts a percentage string (0-100) to a probability (0.0-1.0).
func parseInvocationPercentage(s string) float64 {
	if s == "" {
		return 1.0
	}

	var v int

	if err := parseIntSafe(s, &v); err != nil {
		return 1.0
	}

	if v <= 0 {
		return 1.0
	}

	if v >= lambdaFISMaxPercentage {
		return 1.0
	}

	return float64(v) / lambdaFISDivisor
}

// parseInvocationDelayMs parses the delay in milliseconds.
func parseInvocationDelayMs(s string) int {
	if s == "" {
		return 0
	}

	var v int

	_ = parseIntSafe(s, &v)

	return v
}

func parseIntSafe(s string, out *int) error {
	var v int

	for _, c := range s {
		if c < '0' || c > '9' {
			return errNotAnInteger
		}

		v = v*decimalBase + int(c-'0')
	}

	*out = v

	return nil
}

// expiryFromDuration returns the expiry time for a given duration; zero if duration is 0.
func expiryFromDuration(d time.Duration) time.Time {
	if d <= 0 {
		return time.Time{}
	}

	return time.Now().Add(d)
}
