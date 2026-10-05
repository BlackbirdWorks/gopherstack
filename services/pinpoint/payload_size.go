package pinpoint

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// Payload size ceilings sourced from
// https://docs.aws.amazon.com/pinpoint/latest/developerguide/quotas.html
// (API request quotas / Event ingestion quotas / Endpoint quotas sections),
// NOT the pinned SDK -- aws-sdk-go-v2/service/pinpoint has no numeric size
// fields to verify these against; see PARITY.md. AWS states these as
// decimal "MB"/"KB" figures; this repo already treats AWS request/response
// size quotas as binary when wiring them (pkgs/httputils.MaxRequestBodyBytes
// documents Lambda's "6 MB" as 6 MiB), so the same convention is used here.
const (
	// maxInvocationPayloadBytes is the API request quotas section's general
	// ceiling: "the maximum size of an invocation (request and response)
	// payload is 7 MB, unless otherwise specified for a particular type of
	// resource". Applied to every op below that models PayloadTooLargeException
	// and has no more specific documented limit.
	maxInvocationPayloadBytes = 7 * 1024 * 1024

	// maxPutEventsRequestBytes is the Event ingestion quotas section's
	// "maximum size of a request", which supersedes the general 7 MB ceiling
	// for PutEvents.
	maxPutEventsRequestBytes = 4 * 1024 * 1024

	// maxPutEventBytes is the same table's "maximum size of an individual event".
	maxPutEventBytes = 1000 * 1024

	// maxEndpointRequestBytes is the Endpoint quotas section's "maximum
	// endpoint size", which supersedes the general 7 MB ceiling for a
	// single UpdateEndpoint request body.
	maxEndpointRequestBytes = 15 * 1024
)

// checkPayloadSize writes a 413 PayloadTooLargeException response and
// returns false when body exceeds limit; callers must return immediately.
func checkPayloadSize(c *echo.Context, body []byte, limit int) bool {
	if len(body) <= limit {
		return true
	}

	_ = writeErrorResponse(c, http.StatusRequestEntityTooLarge, "PayloadTooLargeException",
		"the request payload exceeds the maximum allowed size")

	return false
}

// putEventsExceedsEventQuota reports whether any single event in a PutEvents body is over the per-event quota.
func putEventsExceedsEventQuota(body []byte) bool {
	var raw struct {
		BatchItem map[string]struct {
			Events map[string]json.RawMessage `json:"Events"`
		} `json:"BatchItem"`
	}
	if json.Unmarshal(body, &raw) != nil {
		return false
	}

	for _, item := range raw.BatchItem {
		for _, ev := range item.Events {
			if len(ev) > maxPutEventBytes {
				return true
			}
		}
	}

	return false
}
