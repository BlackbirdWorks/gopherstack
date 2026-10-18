package cosmosdb

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// Request-unit model (an approximation of the documented per-operation costs for a 1 KB item with default
// indexing; sizes are the JSON payload length):
//
//	point read    1 + 0.09 per KB beyond the first
//	create/upsert 5.5 + 0.73 per KB beyond the first
//	replace       2x create
//	delete        5.5 flat
//	read feed     point-read cost of the returned payload
//	query         2.3 + 0.005 per scanned item + 0.1 per KB returned
//	metadata      1 flat
const (
	ruReadBase        = 1.0
	ruReadPerKB       = 0.09
	ruWriteBase       = 5.5
	ruWritePerKB      = 0.73
	ruReplaceFactor   = 2.0
	ruDelete          = 5.5
	ruQueryBase       = 2.3
	ruQueryPerScanned = 0.005
	ruQueryPerKB      = 0.1
	ruMetadata        = 1.0
	bytesPerKB        = 1024.0
)

func kbBeyondFirst(size int) float64 {
	return max(float64(size)/bytesPerKB-1, 0)
}

func readCharge(size int) float64   { return ruReadBase + ruReadPerKB*kbBeyondFirst(size) }
func createCharge(size int) float64 { return ruWriteBase + ruWritePerKB*kbBeyondFirst(size) }
func replaceCharge(size int) float64 {
	return ruReplaceFactor * createCharge(size)
}

func queryCharge(scanned, resultBytes int) float64 {
	return ruQueryBase + ruQueryPerScanned*float64(scanned) + ruQueryPerKB*float64(resultBytes)/bytesPerKB
}

// formatCharge renders RUs with at most two decimals, as the service does.
func formatCharge(ru float64) string {
	s := strconv.FormatFloat(ru, 'f', 2, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")

	if s == "" || s == "0" {
		return "0.01"
	}

	return s
}

func setCharge(c *echo.Context, ru float64) {
	c.Response().Header().Set(headerRequestCharge, formatCharge(ru))
}

// writeJSONCharged marshals v, sets x-ms-request-charge from the payload size, and writes the response.
func (h *Handler) writeJSONCharged(c *echo.Context, status int, v any, charge func(size int) float64) error {
	body, err := jsonMarshal(v)
	if err != nil {
		return h.writeErrorNoRecurse(c, http.StatusInternalServerError, "InternalError", "Failed to marshal response.")
	}

	setCharge(c, charge(len(body)))

	return c.Blob(status, "application/json", body)
}
