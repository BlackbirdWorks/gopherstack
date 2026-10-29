package bedrockruntime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	stopReasonToolUse   = "tool_use"
	stopReasonMaxTokens = "max_tokens"
	ollamaDoneLength    = "length"
	toolUseIDPrefix     = "tooluse_"
)

// errUnsupportedContent marks Converse content Ollama cannot represent.
var errUnsupportedContent = errors.New("unsupported content block")

type cvBlock struct {
	Text     *string       `json:"text"`
	Image    *cvImage      `json:"image"`
	Document *cvDocument   `json:"document"`
	ToolUse  *cvToolUse    `json:"toolUse"`
	Result   *cvToolResult `json:"toolResult"`
}

type cvImage struct {
	Source struct {
		Bytes string `json:"bytes"`
	} `json:"source"`
}

type cvDocument struct {
	Format string `json:"format"`
	Name   string `json:"name"`
	Source struct {
		Bytes string `json:"bytes"`
		Text  string `json:"text"`
	} `json:"source"`
}

type cvToolUse struct {
	ToolUseID string          `json:"toolUseId"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
}

type cvToolResult struct {
	ToolUseID string `json:"toolUseId"`
	Content   []struct {
		Text *string         `json:"text"`
		JSON json.RawMessage `json:"json"`
	} `json:"content"`
}

type cvMessage struct {
	Role    string    `json:"role"`
	Content []cvBlock `json:"content"`
}

type cvSystemBlock struct {
	Text *string `json:"text"`
}

type cvRequest struct {
	InferenceConfig json.RawMessage `json:"inferenceConfig"`
	ToolConfig      json.RawMessage `json:"toolConfig"`
	Messages        []cvMessage     `json:"messages"`
	System          []cvSystemBlock `json:"system"`
}

func textDocumentFormat(f string) bool {
	switch f {
	case "txt", "md", "csv", "html":
		return true
	default:
		return false
	}
}

func decodeDocumentText(d *cvDocument) (string, error) {
	if !textDocumentFormat(d.Format) {
		return "", fmt.Errorf("%w: document format %q cannot be sent to ollama (supported: txt, md, csv, html)",
			errUnsupportedContent, d.Format)
	}

	if d.Source.Text != "" {
		return d.Source.Text, nil
	}

	raw, err := base64.StdEncoding.DecodeString(d.Source.Bytes)
	if err != nil {
		return "", fmt.Errorf("%w: document bytes are not base64", errUnsupportedContent)
	}

	return string(raw), nil
}

func toolResultText(r *cvToolResult) string {
	parts := make([]string, 0, len(r.Content))

	for _, c := range r.Content {
		switch {
		case c.Text != nil:
			parts = append(parts, *c.Text)
		case len(c.JSON) > 0:
			parts = append(parts, string(c.JSON))
		}
	}

	return strings.Join(parts, "\n")
}

// ollamaMessages maps Converse system prompts and turns to Ollama chat messages.
func ollamaMessages(req *cvRequest) ([]ollamaMessage, error) {
	var out []ollamaMessage

	var sys []string

	for _, s := range req.System {
		if s.Text != nil {
			sys = append(sys, *s.Text)
		}
	}

	if len(sys) > 0 {
		out = append(out, ollamaMessage{Role: "system", Content: strings.Join(sys, "\n")})
	}

	toolNames := map[string]string{}

	for _, m := range req.Messages {
		var (
			texts   []string
			images  []string
			calls   []ollamaToolCall
			results []ollamaMessage
		)

		for _, b := range m.Content {
			switch {
			case b.Text != nil:
				texts = append(texts, *b.Text)
			case b.Image != nil:
				images = append(images, b.Image.Source.Bytes)
			case b.Document != nil:
				t, err := decodeDocumentText(b.Document)
				if err != nil {
					return nil, err
				}

				texts = append(texts, fmt.Sprintf("%s:\n%s", b.Document.Name, t))
			case b.ToolUse != nil:
				toolNames[b.ToolUse.ToolUseID] = b.ToolUse.Name
				calls = append(calls, ollamaToolCall{
					ID:       b.ToolUse.ToolUseID,
					Function: ollamaToolFunc{Name: b.ToolUse.Name, Arguments: objectOrEmpty(b.ToolUse.Input)},
				})
			case b.Result != nil:
				results = append(results, ollamaMessage{
					Role: "tool", Content: toolResultText(b.Result), ToolName: toolNames[b.Result.ToolUseID],
				})
			}
		}

		out = append(out, results...)

		if len(texts) > 0 || len(images) > 0 || len(calls) > 0 {
			out = append(out, ollamaMessage{
				Role: m.Role, Content: strings.Join(texts, "\n"), Images: images, ToolCalls: calls,
			})
		}
	}

	return out, nil
}

func objectOrEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("{}")
	}

	return raw
}

func ollamaTools(raw json.RawMessage) []ollamaTool {
	if len(raw) == 0 {
		return nil
	}

	var tc struct {
		Tools []struct {
			ToolSpec *struct {
				Name        string `json:"name"`
				Description string `json:"description"`
				InputSchema struct {
					JSON json.RawMessage `json:"json"`
				} `json:"inputSchema"`
			} `json:"toolSpec"`
		} `json:"tools"`
	}
	if json.Unmarshal(raw, &tc) != nil {
		return nil
	}

	var out []ollamaTool

	for _, t := range tc.Tools {
		if t.ToolSpec == nil {
			continue
		}

		out = append(out, ollamaTool{Type: "function", Function: ollamaToolFunc{
			Name: t.ToolSpec.Name, Description: t.ToolSpec.Description, Parameters: t.ToolSpec.InputSchema.JSON,
		}})
	}

	return out
}

func ollamaOptionsFromInference(raw json.RawMessage) *ollamaOptions {
	if len(raw) == 0 {
		return nil
	}

	var ic inferenceConfigBody
	if json.Unmarshal(raw, &ic) != nil {
		return nil
	}

	return &ollamaOptions{NumPredict: ic.MaxTokens, Temperature: ic.Temperature, TopP: ic.TopP, Stop: ic.StopSequences}
}

func (o *ollamaBackend) converseCall(modelID string, body []byte) (*ollamaCall, error) {
	target, ok := o.resolve(modelID)
	if !ok {
		return nil, errUnmappedModel
	}

	var req cvRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errUnsupportedContent, err)
	}

	msgs, err := ollamaMessages(&req)
	if err != nil {
		return nil, err
	}

	return &ollamaCall{chat: &ollamaChatReq{
		Model:    target,
		Messages: msgs,
		Tools:    ollamaTools(req.ToolConfig),
		Options:  ollamaOptionsFromInference(req.InferenceConfig),
	}}, nil
}

// converseStopReason maps an Ollama outcome to types.StopReason. Ollama reports stopStop
// for both end of turn and stop-sequence hits, so stop_sequence is never produced.
func converseStopReason(res *ollamaResult) string {
	switch {
	case len(res.ToolCalls) > 0:
		return stopReasonToolUse
	case res.DoneReason == ollamaDoneLength:
		return stopReasonMaxTokens
	default:
		return stopReasonEndTurn
	}
}

func toolUseBlock(tc ollamaToolCall) map[string]any {
	id := tc.ID
	if id == "" {
		id = awsid.New(toolUseIDPrefix)
	}

	return map[string]any{"toolUseId": id, keyName: tc.Function.Name, keyInput: objectOrEmpty(tc.Function.Arguments)}
}

func tokenUsage(res *ollamaResult) map[string]any {
	return map[string]any{
		keyInputTokens:   res.PromptTokens,
		convOutputTokens: res.EvalTokens,
		convTotalTokens:  res.PromptTokens + res.EvalTokens,
	}
}

func buildOllamaConverseResponse(res *ollamaResult, took time.Duration) map[string]any {
	content := []map[string]any{}
	if res.Text != "" {
		content = append(content, map[string]any{keyText: res.Text})
	}

	for _, tc := range res.ToolCalls {
		content = append(content, map[string]any{keyToolUse: toolUseBlock(tc)})
	}

	return map[string]any{
		keyOutput:         map[string]any{keyMessage: map[string]any{keyRole: roleAssistant, keyContent: content}},
		convStopReasonKey: converseStopReason(res),
		keyUsage:          tokenUsage(res),
		keyMetrics:        map[string]any{keyLatencyMs: took.Milliseconds()},
	}
}

func (h *Handler) handleOllamaConverse(
	c *echo.Context,
	o *ollamaBackend,
	modelID string,
	body []byte,
	req *converseRequest,
) error {
	call, err := o.converseCall(modelID, body)
	if err != nil {
		return writeLLMError(c, err)
	}

	start := time.Now()

	res, err := o.run(c.Request().Context(), call, nil, nil)
	if err != nil {
		return writeLLMError(c, err)
	}

	resp := buildOllamaConverseResponse(res, time.Since(start))
	echoConverseConfig(resp, req)

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation(opConverse, modelID, truncateString(string(body)), truncateString(string(out)))

	return c.JSONBlob(http.StatusOK, out)
}

// converseStreamer turns Ollama deltas into the documented ConverseStream event order.
type converseStreamer struct {
	emit     func(eventType string, payload any)
	onBegin  func()
	started  bool
	textOpen bool
	idx      int
}

func (s *converseStreamer) begin() {
	if s.started {
		return
	}

	s.started = true

	if s.onBegin != nil {
		s.onBegin()
	}

	s.emit("messageStart", map[string]any{keyRole: roleAssistant})
}

func (s *converseStreamer) closeText() {
	if s.textOpen {
		s.emit("contentBlockStop", map[string]any{convContentIdx: s.idx})
		s.idx++
		s.textOpen = false
	}
}

func (s *converseStreamer) text(t string) error {
	s.begin()

	if !s.textOpen {
		s.emit("contentBlockStart", map[string]any{convContentIdx: s.idx})
		s.textOpen = true
	}

	s.emit("contentBlockDelta", map[string]any{convContentIdx: s.idx, keyDelta: map[string]any{keyText: t}})

	return nil
}

func (s *converseStreamer) tool(tc ollamaToolCall) error {
	s.begin()
	s.closeText()

	tu := toolUseBlock(tc)
	s.emit("contentBlockStart", map[string]any{
		convContentIdx: s.idx,
		"start":        map[string]any{keyToolUse: map[string]any{"toolUseId": tu["toolUseId"], keyName: tu[keyName]}},
	})
	s.emit("contentBlockDelta", map[string]any{
		convContentIdx: s.idx,
		keyDelta: map[string]any{
			keyToolUse: map[string]any{keyInput: string(objectOrEmpty(tc.Function.Arguments))},
		},
	})
	s.emit("contentBlockStop", map[string]any{convContentIdx: s.idx})
	s.idx++

	return nil
}

func (s *converseStreamer) finish(res *ollamaResult, meta map[string]any) {
	s.begin()
	s.closeText()
	s.emit("messageStop", map[string]any{convStopReasonKey: converseStopReason(res)})

	meta[keyUsage] = tokenUsage(res)
	s.emit("metadata", meta)
}

func frameWriter(c *echo.Context) (func(string, any), func(string, string)) {
	write := func(headers [][2]string, payload any) {
		data, err := json.Marshal(payload)
		if err != nil {
			return
		}

		_, _ = c.Response().Write(encodeEventStreamMsg(headers, data))
		flushResponse(c.Response())
	}

	emit := func(eventType string, payload any) {
		write([][2]string{
			{hdrMessageType, hdrMessageTypeEvent},
			{hdrEventType, eventType},
			{hdrContentType, contentTypeJSON},
		}, payload)
	}
	exception := func(excType, msg string) {
		write([][2]string{
			{hdrMessageType, "exception"},
			{":exception-type", excType},
			{hdrContentType, contentTypeJSON},
		}, map[string]string{keyMessage: msg})
	}

	return emit, exception
}

func (h *Handler) handleOllamaConverseStream(
	c *echo.Context, o *ollamaBackend, modelID string, body []byte, req *converseRequest,
) error {
	call, err := o.converseCall(modelID, body)
	if err != nil {
		return writeLLMError(c, err)
	}

	emit, exception := frameWriter(c)
	s := &converseStreamer{emit: emit, onBegin: func() {
		c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		c.Response().WriteHeader(http.StatusOK)
	}}
	start := time.Now()

	res, err := o.run(c.Request().Context(), call, s.text, s.tool)
	if err != nil {
		if !s.started {
			return writeLLMError(c, err)
		}

		exception("modelStreamErrorException", err.Error())

		return nil
	}

	took := time.Since(start)
	meta := map[string]any{keyMetrics: map[string]any{keyLatencyMs: took.Milliseconds()}}
	echoConverseConfig(meta, req)
	s.finish(res, meta)

	h.Backend.RecordInvocation(opConverseStream, modelID, truncateString(string(body)),
		truncateString(string(mustJSON(buildOllamaConverseResponse(res, took)))))

	return nil
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)

	return b
}

func llmErrorBody(excType, msg string, extra map[string]any) map[string]any {
	m := map[string]any{"__type": excType, keyMessage: msg}
	maps.Copy(m, extra)

	return m
}

// writeLLMError maps backend failures to the exceptions the Bedrock runtime models:
// unreachable -> ServiceUnavailableException (503), model not pulled ->
// ModelNotReadyException (429), unmapped model -> ResourceNotFoundException (404).
func writeLLMError(c *echo.Context, err error) error {
	var se *ollamaStatusError

	switch {
	case errors.Is(err, errUnmappedModel):
		return c.JSON(http.StatusNotFound, errorResponse("ResourceNotFoundException",
			"The specified model is not mapped to an Ollama model. Set "+EnvModelMap+"."))
	case errors.Is(err, errUnsupportedContent):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", err.Error()))
	case errors.Is(err, context.Canceled):
		return c.JSON(http.StatusServiceUnavailable, errorResponse("ServiceUnavailableException", "request canceled"))
	case errors.Is(err, errOllamaUnreachable):
		return c.JSON(http.StatusServiceUnavailable, errorResponse("ServiceUnavailableException", err.Error()))
	case errors.As(err, &se) && se.status == http.StatusNotFound:
		return c.JSON(http.StatusTooManyRequests, errorResponse("ModelNotReadyException", se.msg))
	case errors.As(err, &se) && se.status == http.StatusBadRequest:
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", se.msg))
	case errors.As(err, &se):
		return c.JSON(http.StatusFailedDependency, llmErrorBody("ModelErrorException", se.msg,
			map[string]any{"originalStatusCode": se.status}))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}
}
