package cloudfront

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// storeKVS adapts the backend's Key Value Stores to a running function.
type storeKVS struct {
	b   *InMemoryBackend
	ids []string
}

func (s *storeKVS) KVSIDs() []string { return s.ids }

func (s *storeKVS) Value(id, key string) (string, bool) {
	v, _, err := s.b.GetKVSValue(id, key)

	return v, err == nil
}

// kvsReaderFor builds the reader for a function's associated stores.
func (b *InMemoryBackend) kvsReaderFor(fn *Function) kvsReader {
	ids := make([]string, 0, len(fn.KVSARNs))
	for _, a := range fn.KVSARNs {
		ids = append(ids, a[strings.LastIndex(a, "/")+1:])
	}

	return &storeKVS{b: b, ids: ids}
}

// ExecuteFunction runs a function (as seen at stage; "" is the current record) against event.
func (b *InMemoryBackend) ExecuteFunction(name, stage string, event any) (*Function, fnResult, error) {
	fn, err := b.GetFunctionAtStage(name, stage)
	if err != nil {
		return nil, fnResult{}, err
	}

	return fn, runCloudFrontFunction(fn.Runtime, fn.FunctionCode, event, b.kvsReaderFor(fn)), nil
}

func (h *Handler) runTestFunction(c *echo.Context, name, stage string, eventObject []byte) error {
	var event any
	if err := json.Unmarshal(eventObject, &event); err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("InvalidArgument", "EventObject must be valid JSON"))
	}

	fn, res, err := h.Backend.ExecuteFunction(name, stage, event)
	if err != nil {
		return h.handleError(c, err)
	}

	return xmlResp(c, http.StatusOK, testResultXML(fn, res))
}

func testResultXML(fn *Function, res fnResult) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, `<?xml version="1.0" encoding="UTF-8"?><TestResult xmlns="%s">`, cfNS)
	sb.WriteString(functionSummaryXML(fn))
	sb.WriteString("<ComputeUtilization>" + res.Utilization + "</ComputeUtilization>")
	sb.WriteString("<FunctionExecutionLogs>")

	for _, l := range res.Logs {
		sb.WriteString("<member>" + html.EscapeString(l) + "</member>")
	}

	sb.WriteString("</FunctionExecutionLogs>")

	if res.ErrMsg != "" {
		sb.WriteString("<FunctionErrorMessage>" + html.EscapeString(res.ErrMsg) + "</FunctionErrorMessage>")
	}

	sb.WriteString("<FunctionOutput>" + html.EscapeString(res.Output) + "</FunctionOutput></TestResult>")

	return sb.String()
}
