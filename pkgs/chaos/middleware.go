package chaos

import (
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// minSigV4CredentialParts is the minimum number of slash-delimited fields in
// an AWS SigV4 Credential value: AKID / date / region / service / aws4_request.
const minSigV4CredentialParts = 5

// extractServiceFromRequest extracts the lowercase AWS service name from the
// SigV4 Authorization header (e.g. "dynamodb", "s3", "sqs").
// Returns an empty string when the header is absent or malformed.
func extractServiceFromRequest(r interface {
	Header(string) string
}) string {
	auth := r.Header("Authorization")
	if auth == "" {
		return ""
	}

	if !strings.Contains(auth, "Credential=") {
		return ""
	}

	_, after, found := strings.Cut(auth, "Credential=")
	if !found {
		return ""
	}

	// Credential value ends at the next comma (before SignedHeaders).
	credOnly, _, _ := strings.Cut(after, ",")
	parts := strings.Split(credOnly, "/")

	// Format: AKID / date / region / service / aws4_request
	if len(parts) < minSigV4CredentialParts {
		return ""
	}

	return strings.ToLower(parts[3])
}

// extractRegionFromRequest extracts the AWS region from the SigV4 Authorization
// header, falling back to the X-Amz-Region header.
func extractRegionFromRequest(r interface {
	Header(string) string
}) string {
	auth := r.Header("Authorization")
	if auth != "" && strings.Contains(auth, "Credential=") {
		_, after, found := strings.Cut(auth, "Credential=")
		if found {
			credOnly, _, _ := strings.Cut(after, ",")
			parts := strings.Split(credOnly, "/")

			if len(parts) >= minSigV4CredentialParts {
				return parts[2]
			}
		}
	}

	return r.Header("X-Amz-Region")
}

// extractOperationFromRequest tries to determine the AWS operation name from
// request headers or returns an empty string when it cannot.
//
// For JSON-protocol services (DynamoDB, CloudWatch, etc.) the operation is
// encoded in the X-Amz-Target header as "ServiceVersion.OperationName".
// For REST-based services (S3, Lambda, …) there is no universal header, so
// the chaos middleware falls back to matching with an empty operation string
// (which matches any operation in the fault rules).
func extractOperationFromRequest(r interface {
	Header(string) string
}) string {
	target := r.Header("X-Amz-Target")
	if target == "" {
		return ""
	}

	// Format is "DynamoDB_20120810.GetItem" or "Logs_20140328.CreateLogGroup".
	_, op, found := strings.Cut(target, ".")
	if !found {
		return target
	}

	return op
}

// echoRequestAdapter wraps an echo.Context to implement the header interface
// used by the extraction helpers.
type echoRequestAdapter struct {
	c *echo.Context
}

func (a echoRequestAdapter) Header(key string) string {
	return a.c.Request().Header.Get(key)
}

// Middleware returns an Echo middleware that evaluates fault rules and network
// effects from the provided FaultStore against each incoming AWS API request.
//
// The middleware is intended to be registered via Registry.Use() so that it
// runs as an outer wrapper around the telemetry+handler chain. It extracts the
// AWS service, operation, and region directly from the HTTP request headers —
// specifically the SigV4 Authorization header (for service and region) and the
// X-Amz-Target header (for JSON-protocol service operations) — so that it does
// not depend on any context values that are set only after the handler is called.
//
// Request lifecycle when a fault fires:
//  1. Middleware extracts service / operation / region from request headers.
//  2. FaultStore.Match selects the first matching rule.
//  3. FaultRule.ShouldTrigger applies the probability check.
//  4. If triggered, an error response is written and the chain is short-circuited.
//
// Network effects (latency) are applied regardless of fault injection and run
// before the rest of the handler chain.
func Middleware(store *FaultStore) func(echo.HandlerFunc) echo.HandlerFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := c.Request().Context()
			log := logger.Load(ctx)

			adapter := echoRequestAdapter{c: c}
			svc := extractServiceFromRequest(adapter)
			op := extractOperationFromRequest(adapter)
			region := extractRegionFromRequest(adapter)

			// Apply network effects latency before forwarding.
			effects := store.GetEffects()
			if delayMs := effects.TotalDelayMs(); delayMs > 0 {
				delay := time.Duration(delayMs) * time.Millisecond
				timer := time.NewTimer(delay)

				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()

					return ctx.Err()
				}
			}

			// Check fault rules.
			rule, matched := store.Match(svc, op, region)
			if matched {
				triggered := rule.ShouldTrigger()

				event := ActivityEvent{
					Timestamp:   time.Now(),
					Service:     svc,
					Operation:   op,
					Region:      region,
					Probability: rule.Probability,
					Triggered:   triggered,
				}

				if triggered {
					fe := rule.EffectiveError()
					event.FaultApplied = fe.Code

					log.InfoContext(ctx, "chaos: injecting fault",
						"service", svc,
						"operation", op,
						"region", region,
						"status_code", fe.StatusCode,
						"error_code", fe.Code,
					)

					store.RecordActivity(event)

					return respondWithFault(c, fe)
				}

				store.RecordActivity(event)
			}

			return next(c)
		}
	}
}
