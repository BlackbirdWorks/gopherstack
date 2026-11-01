package bedrockruntime

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	keyInvocationMetrics = "amazon-bedrock-invocationMetrics"
	cohereComplete       = "COMPLETE"
	cohereMaxTokens      = "MAX_TOKENS"
	titanFinish          = "FINISH"
	titanLength          = "LENGTH"
	stopLength           = "length"
	typeToolUse          = "tool_use"
	typeText             = "text"
	typeEvent            = "type"
	keyIndex             = "index"
	chatTurnPair         = 2
)

var (
	errMalformed   = errors.New("malformed input request")
	errMissingKey  = errors.New("required key not found")
	errImageSource = errors.New("image block requires source")
)

func missingKey(name string) error {
	return malformed(fmt.Errorf("%w: [%s]", errMissingKey, name))
}

// invokePlan is one family's translation of an InvokeModel body.
type invokePlan struct {
	call     *ollamaCall
	render   func(res *ollamaResult) any
	streamer func(modelID string) invokeStreamer
}

// invokeStreamer yields the family's documented chunk bodies for Ollama deltas.
type invokeStreamer interface {
	begin() []any
	text(string) []any
	tool(ollamaToolCall) []any
	end(res *ollamaResult, took time.Duration) []any
}

type familyBuilder func(target, modelID string, body []byte) (*invokePlan, error)

func malformed(err error) error { return fmt.Errorf("%w: %w", errMalformed, err) }

func familyFor(id string) familyBuilder {
	switch {
	case strings.HasPrefix(id, "anthropic.claude"):
		return buildAnthropic
	case strings.HasPrefix(id, "amazon.titan-embed-text"):
		return buildTitanEmbed
	case strings.HasPrefix(id, "amazon.titan-text"):
		return buildTitanText
	case strings.HasPrefix(id, "amazon.nova-micro"), strings.HasPrefix(id, "amazon.nova-lite"),
		strings.HasPrefix(id, "amazon.nova-pro"), strings.HasPrefix(id, "amazon.nova-premier"):
		return buildNova
	case strings.HasPrefix(id, "meta.llama"):
		return buildLlama
	case strings.HasPrefix(id, "mistral."):
		return buildMistral
	case strings.HasPrefix(id, "cohere.embed"):
		return buildCohereEmbed
	case strings.HasPrefix(id, "cohere.command-r"):
		return buildCohereChat
	default:
		return nil
	}
}

func parseBody(body []byte, v any) error {
	if err := json.Unmarshal(body, v); err != nil {
		return malformed(err)
	}

	return nil
}

func opts(maxTokens *int, temp, topP *float64, topK *int, stop []string) *ollamaOptions {
	return &ollamaOptions{NumPredict: maxTokens, Temperature: temp, TopP: topP, TopK: topK, Stop: stop}
}

func metricsBlock(res *ollamaResult, took time.Duration) map[string]any {
	ms := took.Milliseconds()

	return map[string]any{
		"inputTokenCount": res.PromptTokens, "outputTokenCount": res.EvalTokens,
		"invocationLatency": ms, "firstByteLatency": ms,
	}
}

// --- Anthropic ---------------------------------------------------------------

type anthropicBlock struct {
	Source *struct {
		Data string `json:"data"`
	} `json:"source"`
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	ToolUseID string          `json:"tool_use_id"`
	Input     json.RawMessage `json:"input"`
	Content   json.RawMessage `json:"content"`
}

type anthropicRequest struct {
	MaxTokens         *int     `json:"max_tokens"`
	MaxTokensToSample *int     `json:"max_tokens_to_sample"`
	Temperature       *float64 `json:"temperature"`
	TopP              *float64 `json:"top_p"`
	TopK              *int     `json:"top_k"`
	Prompt            string   `json:"prompt"`
	Messages          []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	System        json.RawMessage `json:"system"`
	Tools         []anthropicTool `json:"tools"`
	StopSequences []string        `json:"stop_sequences"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// anthropicBlocks reads a content value that is either a string or an array of blocks.
func anthropicBlocks(raw json.RawMessage) ([]anthropicBlock, error) {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return []anthropicBlock{{Type: typeText, Text: s}}, nil
	}

	var blocks []anthropicBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, malformed(err)
	}

	return blocks, nil
}

func anthropicToCv(role string, raw json.RawMessage) (cvMessage, error) {
	blocks, err := anthropicBlocks(raw)
	if err != nil {
		return cvMessage{}, err
	}

	m := cvMessage{Role: role}

	for _, b := range blocks {
		switch b.Type {
		case typeText:
			t := b.Text
			m.Content = append(m.Content, cvBlock{Text: &t})
		case "image":
			if b.Source == nil {
				return m, malformed(errImageSource)
			}

			img := &cvImage{}
			img.Source.Bytes = b.Source.Data
			m.Content = append(m.Content, cvBlock{Image: img})
		case typeToolUse:
			m.Content = append(m.Content, cvBlock{ToolUse: &cvToolUse{ToolUseID: b.ID, Name: b.Name, Input: b.Input}})
		case "tool_result":
			inner, ierr := anthropicBlocks(b.Content)
			if ierr != nil && len(b.Content) > 0 {
				return m, ierr
			}

			res := &cvToolResult{ToolUseID: b.ToolUseID}

			for _, ib := range inner {
				t := ib.Text
				res.Content = append(res.Content, struct {
					Text *string         `json:"text"`
					JSON json.RawMessage `json:"json"`
				}{Text: &t})
			}

			m.Content = append(m.Content, cvBlock{Result: res})
		default:
			return m, fmt.Errorf("%w: content block type %q", errUnsupportedContent, b.Type)
		}
	}

	return m, nil
}

func anthropicSystem(raw json.RawMessage) ([]cvSystemBlock, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	blocks, err := anthropicBlocks(raw)
	if err != nil {
		return nil, err
	}

	out := make([]cvSystemBlock, 0, len(blocks))

	for _, b := range blocks {
		t := b.Text
		out = append(out, cvSystemBlock{Text: &t})
	}

	return out, nil
}

func buildAnthropic(target, modelID string, body []byte) (*invokePlan, error) {
	var req anthropicRequest
	if err := parseBody(body, &req); err != nil {
		return nil, err
	}

	if len(req.Messages) == 0 && req.Prompt != "" {
		return buildAnthropicCompletion(target, &req), nil
	}

	cv := &cvRequest{}

	sys, err := anthropicSystem(req.System)
	if err != nil {
		return nil, err
	}

	cv.System = sys

	for _, m := range req.Messages {
		cm, merr := anthropicToCv(m.Role, m.Content)
		if merr != nil {
			return nil, merr
		}

		cv.Messages = append(cv.Messages, cm)
	}

	msgs, err := ollamaMessages(cv)
	if err != nil {
		return nil, err
	}

	var tools []ollamaTool
	for _, t := range req.Tools {
		tools = append(tools, ollamaTool{Type: "function", Function: ollamaToolFunc{
			Name: t.Name, Description: t.Description, Parameters: t.InputSchema,
		}})
	}

	call := &ollamaCall{chat: &ollamaChatReq{
		Model: target, Messages: msgs, Tools: tools,
		Options: opts(req.MaxTokens, req.Temperature, req.TopP, req.TopK, req.StopSequences),
	}}

	return &invokePlan{
		call: call,
		render: func(res *ollamaResult) any {
			return map[string]any{
				"id": awsid.New("msg_"), typeEvent: keyMessage, keyRole: roleAssistant, keyModel: modelID,
				keyContent: anthropicContent(res), keyStopReason: anthropicStopReason(res), keyStopSequence: nil,
				keyUsage: map[string]any{keyInputCountSnake: res.PromptTokens, keyOutputCountSnake: res.EvalTokens},
			}
		},
		streamer: func(id string) invokeStreamer { return &anthropicStreamer{modelID: id} },
	}, nil
}

func buildAnthropicCompletion(target string, req *anthropicRequest) *invokePlan {
	return &invokePlan{
		call: &ollamaCall{gen: &ollamaGenerateReq{
			Model: target, Prompt: req.Prompt,
			Options: opts(req.MaxTokensToSample, req.Temperature, req.TopP, req.TopK, req.StopSequences),
		}},
		render: func(res *ollamaResult) any {
			out := map[string]any{keyCompletion: res.Text, keyStopReason: keyStopSequence, stopStop: "\n\nHuman:"}
			if res.DoneReason == stopLength {
				out[keyStopReason], out[stopStop] = stopReasonMaxTokens, nil
			}

			return out
		},
	}
}

func anthropicStopReason(res *ollamaResult) string {
	switch {
	case len(res.ToolCalls) > 0:
		return typeToolUse
	case res.DoneReason == stopLength:
		return stopReasonMaxTokens
	default:
		return stopReasonEndTurn
	}
}

func anthropicToolUse(tc ollamaToolCall) map[string]any {
	tu := toolUseBlock(tc)

	return map[string]any{typeEvent: typeToolUse, "id": tu["toolUseId"], keyName: tu[keyName], keyInput: tu[keyInput]}
}

func anthropicContent(res *ollamaResult) []map[string]any {
	out := []map[string]any{}
	if res.Text != "" {
		out = append(out, map[string]any{typeEvent: typeText, keyText: res.Text})
	}

	for _, tc := range res.ToolCalls {
		out = append(out, anthropicToolUse(tc))
	}

	return out
}

type anthropicStreamer struct {
	modelID  string
	idx      int
	textOpen bool
	started  bool
}

func (s *anthropicStreamer) begin() []any {
	if s.started {
		return nil
	}

	s.started = true

	return []any{map[string]any{
		typeEvent: "message_start",
		keyMessage: map[string]any{
			"id": awsid.New("msg_"), typeEvent: keyMessage, keyRole: roleAssistant, keyModel: s.modelID,
			keyContent: []any{}, keyStopReason: nil, keyStopSequence: nil,
			keyUsage: map[string]any{keyInputCountSnake: 0, keyOutputCountSnake: 0},
		},
	}}
}

func (s *anthropicStreamer) closeText() []any {
	if !s.textOpen {
		return nil
	}

	ev := map[string]any{typeEvent: "content_block_stop", keyIndex: s.idx}
	s.idx++
	s.textOpen = false

	return []any{ev}
}

func (s *anthropicStreamer) text(t string) []any {
	out := s.begin()

	if !s.textOpen {
		s.textOpen = true
		out = append(out, map[string]any{
			typeEvent: "content_block_start", keyIndex: s.idx,
			"content_block": map[string]any{typeEvent: typeText, keyText: ""},
		})
	}

	return append(out, map[string]any{
		typeEvent: "content_block_delta", keyIndex: s.idx,
		keyDelta: map[string]any{typeEvent: "text_delta", keyText: t},
	})
}

func (s *anthropicStreamer) tool(tc ollamaToolCall) []any {
	out := append(s.begin(), s.closeText()...)
	tu := anthropicToolUse(tc)
	input := tu[keyInput]
	tu[keyInput] = map[string]any{}

	out = append(out,
		map[string]any{typeEvent: "content_block_start", keyIndex: s.idx, "content_block": tu},
		map[string]any{
			typeEvent: "content_block_delta", keyIndex: s.idx,
			keyDelta: map[string]any{typeEvent: "input_json_delta", "partial_json": string(mustJSON(input))},
		},
		map[string]any{typeEvent: "content_block_stop", keyIndex: s.idx},
	)
	s.idx++

	return out
}

func (s *anthropicStreamer) end(res *ollamaResult, took time.Duration) []any {
	out := append(s.begin(), s.closeText()...)

	return append(out,
		map[string]any{
			typeEvent: "message_delta",
			keyDelta:  map[string]any{keyStopReason: anthropicStopReason(res), keyStopSequence: nil},
			keyUsage:  map[string]any{keyOutputCountSnake: res.EvalTokens},
		},
		map[string]any{typeEvent: "message_stop", keyInvocationMetrics: metricsBlock(res, took)},
	)
}

// --- Amazon Titan ------------------------------------------------------------

func buildTitanText(target, _ string, body []byte) (*invokePlan, error) {
	var req struct {
		Config *struct {
			MaxTokenCount *int     `json:"maxTokenCount"`
			Temperature   *float64 `json:"temperature"`
			TopP          *float64 `json:"topP"`
			StopSequences []string `json:"stopSequences"`
		} `json:"textGenerationConfig"`
		InputText string `json:"inputText"`
	}
	if err := parseBody(body, &req); err != nil {
		return nil, err
	}

	if req.InputText == "" {
		return nil, missingKey("inputText")
	}

	o := &ollamaOptions{}
	if c := req.Config; c != nil {
		o = opts(c.MaxTokenCount, c.Temperature, c.TopP, nil, c.StopSequences)
	}

	return &invokePlan{
		call: &ollamaCall{gen: &ollamaGenerateReq{Model: target, Prompt: req.InputText, Options: o}},
		render: func(res *ollamaResult) any {
			return map[string]any{
				keyInputTextCount: res.PromptTokens,
				keyResults: []map[string]any{{
					"tokenCount": res.EvalTokens, keyOutputText: res.Text, keyCompletionReason: titanReason(res),
				}},
			}
		},
		streamer: func(string) invokeStreamer { return &titanStreamer{} },
	}, nil
}

func titanReason(res *ollamaResult) string {
	if res.DoneReason == stopLength {
		return titanLength
	}

	return titanFinish
}

type titanStreamer struct{ n int }

func (s *titanStreamer) begin() []any { return nil }

func (s *titanStreamer) text(t string) []any {
	s.n++

	return []any{map[string]any{keyOutputText: t, keyIndex: s.n - 1, keyCompletionReason: nil}}
}

func (*titanStreamer) tool(ollamaToolCall) []any { return nil }

func (s *titanStreamer) end(res *ollamaResult, took time.Duration) []any {
	return []any{map[string]any{
		keyOutputText: "", keyIndex: s.n, "totalOutputTextTokenCount": res.EvalTokens,
		keyCompletionReason: titanReason(res), keyInputTextCount: res.PromptTokens,
		keyInvocationMetrics: metricsBlock(res, took),
	}}
}

func buildTitanEmbed(target, modelID string, body []byte) (*invokePlan, error) {
	var req struct {
		Normalize      *bool    `json:"normalize"`
		Dimensions     *int     `json:"dimensions"`
		InputText      string   `json:"inputText"`
		EmbeddingTypes []string `json:"embeddingTypes"`
	}
	if err := parseBody(body, &req); err != nil {
		return nil, err
	}

	if req.InputText == "" {
		return nil, missingKey("inputText")
	}

	for _, t := range req.EmbeddingTypes {
		if t != keyFloat {
			return nil, fmt.Errorf("%w: embeddingTypes %q is not supported by the ollama backend (float only)",
				errUnsupportedContent, t)
		}
	}

	v2 := strings.Contains(modelID, "-v2")
	e := &ollamaEmbedReq{Model: target, Input: []string{req.InputText}}

	if v2 {
		e.Dimensions = req.Dimensions
	}

	return &invokePlan{
		call: &ollamaCall{embed: e},
		render: func(res *ollamaResult) any {
			out := map[string]any{"embedding": firstEmbedding(res), keyInputTextCount: res.PromptTokens}
			if v2 {
				out["embeddingsByType"] = map[string]any{keyFloat: firstEmbedding(res)}
			}

			return out
		},
	}, nil
}

func firstEmbedding(res *ollamaResult) []float64 {
	if len(res.Embeddings) == 0 {
		return []float64{}
	}

	return res.Embeddings[0]
}

// --- Amazon Nova ---------------------------------------------------------------

func buildNova(target, _ string, body []byte) (*invokePlan, error) {
	var req struct {
		Inference *struct {
			MaxNewTokens  *int     `json:"max_new_tokens"`
			Temperature   *float64 `json:"temperature"`
			TopP          *float64 `json:"top_p"`
			TopK          *int     `json:"top_k"`
			StopSequences []string `json:"stopSequences"`
		} `json:"inferenceConfig"`
		cvRequest
	}
	if err := parseBody(body, &req); err != nil {
		return nil, err
	}

	msgs, err := ollamaMessages(&req.cvRequest)
	if err != nil {
		return nil, err
	}

	o := &ollamaOptions{}
	if c := req.Inference; c != nil {
		o = opts(c.MaxNewTokens, c.Temperature, c.TopP, c.TopK, c.StopSequences)
	}

	return &invokePlan{
		call: &ollamaCall{chat: &ollamaChatReq{
			Model: target, Messages: msgs, Tools: ollamaTools(req.ToolConfig), Options: o,
		}},
		render: func(res *ollamaResult) any {
			out := buildOllamaConverseResponse(res, 0)
			delete(out, keyMetrics)

			return out
		},
		streamer: func(string) invokeStreamer { return &novaStreamer{} },
	}, nil
}

// novaStreamer wraps ConverseStream-shaped events, which is what Nova emits for InvokeModel streams.
type novaStreamer struct {
	conv converseStreamer
	buf  []any
}

func (s *novaStreamer) init() {
	if s.conv.emit == nil {
		s.conv.emit = func(eventType string, payload any) { s.buf = append(s.buf, map[string]any{eventType: payload}) }
	}
}

func (s *novaStreamer) drain() []any {
	out := s.buf
	s.buf = nil

	return out
}

func (s *novaStreamer) begin() []any {
	s.init()
	s.conv.begin()

	return s.drain()
}

func (s *novaStreamer) text(t string) []any {
	s.init()
	_ = s.conv.text(t)

	return s.drain()
}

func (s *novaStreamer) tool(tc ollamaToolCall) []any {
	s.init()
	_ = s.conv.tool(tc)

	return s.drain()
}

func (s *novaStreamer) end(res *ollamaResult, took time.Duration) []any {
	s.init()
	s.conv.finish(res, map[string]any{keyInvocationMetrics: metricsBlock(res, took)})

	return s.drain()
}

// --- Meta Llama / Mistral ------------------------------------------------------

func buildLlama(target, _ string, body []byte) (*invokePlan, error) {
	var req struct {
		MaxGenLen   *int     `json:"max_gen_len"`
		Temperature *float64 `json:"temperature"`
		TopP        *float64 `json:"top_p"`
		Prompt      string   `json:"prompt"`
	}
	if err := parseBody(body, &req); err != nil {
		return nil, err
	}

	if req.Prompt == "" {
		return nil, missingKey("prompt")
	}

	reason := func(res *ollamaResult) string {
		if res.DoneReason == stopLength {
			return stopLength
		}

		return stopStop
	}

	return &invokePlan{
		call: &ollamaCall{gen: &ollamaGenerateReq{
			Model: target, Prompt: req.Prompt, Options: opts(req.MaxGenLen, req.Temperature, req.TopP, nil, nil),
		}},
		render: func(res *ollamaResult) any {
			return map[string]any{
				keyGeneration: res.Text, keyPromptCount: res.PromptTokens,
				keyGenCount: res.EvalTokens, keyStopReason: reason(res),
			}
		},
		streamer: func(string) invokeStreamer {
			return &simpleStreamer{
				delta: func(t string) any {
					return map[string]any{
						keyGeneration:  t,
						keyPromptCount: nil,
						keyGenCount:    nil,
						keyStopReason:  nil,
					}
				},
				last: func(res *ollamaResult, took time.Duration) any {
					return map[string]any{
						keyGeneration: "", keyPromptCount: res.PromptTokens,
						keyGenCount: res.EvalTokens, keyStopReason: reason(res),
						keyInvocationMetrics: metricsBlock(res, took),
					}
				},
			}
		},
	}, nil
}

func buildMistral(target, _ string, body []byte) (*invokePlan, error) {
	var req struct {
		MaxTokens   *int     `json:"max_tokens"`
		Temperature *float64 `json:"temperature"`
		TopP        *float64 `json:"top_p"`
		TopK        *int     `json:"top_k"`
		Prompt      string   `json:"prompt"`
		Stop        []string `json:"stop"`
	}
	if err := parseBody(body, &req); err != nil {
		return nil, err
	}

	if req.Prompt == "" {
		return nil, missingKey("prompt")
	}

	reason := func(res *ollamaResult) string {
		if res.DoneReason == stopLength {
			return stopLength
		}

		return stopStop
	}
	output := func(t string, stop any) any {
		return map[string]any{keyOutputs: []map[string]any{{keyText: t, keyStopReason: stop}}}
	}

	return &invokePlan{
		call: &ollamaCall{gen: &ollamaGenerateReq{
			Model: target, Prompt: req.Prompt,
			Options: opts(req.MaxTokens, req.Temperature, req.TopP, req.TopK, req.Stop),
		}},
		render: func(res *ollamaResult) any { return output(res.Text, reason(res)) },
		streamer: func(string) invokeStreamer {
			return &simpleStreamer{
				delta: func(t string) any { return output(t, nil) },
				last: func(res *ollamaResult, took time.Duration) any {
					o, _ := output("", reason(res)).(map[string]any)
					o[keyInvocationMetrics] = metricsBlock(res, took)

					return o
				},
			}
		},
	}, nil
}

// simpleStreamer emits one chunk per text delta plus a closing chunk.
type simpleStreamer struct {
	delta func(string) any
	last  func(*ollamaResult, time.Duration) any
}

func (*simpleStreamer) begin() []any                                 { return nil }
func (s *simpleStreamer) text(t string) []any                        { return []any{s.delta(t)} }
func (*simpleStreamer) tool(ollamaToolCall) []any                    { return nil }
func (s *simpleStreamer) end(r *ollamaResult, d time.Duration) []any { return []any{s.last(r, d)} }

// --- Cohere ----------------------------------------------------------------------

func buildCohereEmbed(target, _ string, body []byte) (*invokePlan, error) {
	var req struct {
		Texts          []string `json:"texts"`
		Images         []string `json:"images"`
		EmbeddingTypes []string `json:"embedding_types"`
	}
	if err := parseBody(body, &req); err != nil {
		return nil, err
	}

	if len(req.Images) > 0 {
		return nil, fmt.Errorf("%w: image embeddings are not supported by the ollama backend", errUnsupportedContent)
	}

	if len(req.Texts) == 0 {
		return nil, missingKey("texts")
	}

	for _, t := range req.EmbeddingTypes {
		if t != keyFloat {
			return nil, fmt.Errorf("%w: embedding_types %q is not supported by the ollama backend (float only)",
				errUnsupportedContent, t)
		}
	}

	byType := len(req.EmbeddingTypes) > 0

	return &invokePlan{
		call: &ollamaCall{embed: &ollamaEmbedReq{Model: target, Input: req.Texts}},
		render: func(res *ollamaResult) any {
			out := map[string]any{"id": awsid.New(""), "texts": req.Texts}
			if byType {
				out["response_type"] = "embeddings_by_type"
				out["embeddings"] = map[string]any{keyFloat: res.Embeddings}
			} else {
				out["response_type"] = "embeddings_floats"
				out["embeddings"] = res.Embeddings
			}

			return out
		},
	}, nil
}

func buildCohereChat(target, _ string, body []byte) (*invokePlan, error) {
	var req struct {
		MaxTokens   *int     `json:"max_tokens"`
		Temperature *float64 `json:"temperature"`
		P           *float64 `json:"p"`
		K           *float64 `json:"k"`
		History     []struct {
			Role    string `json:"role"`
			Message string `json:"message"`
		} `json:"chat_history"`
		Message       string   `json:"message"`
		Preamble      string   `json:"preamble"`
		StopSequences []string `json:"stop_sequences"`
	}
	if err := parseBody(body, &req); err != nil {
		return nil, err
	}

	if req.Message == "" {
		return nil, missingKey("message")
	}

	var msgs []ollamaMessage
	if req.Preamble != "" {
		msgs = append(msgs, ollamaMessage{Role: "system", Content: req.Preamble})
	}

	for _, h := range req.History {
		role := "user"
		if strings.EqualFold(h.Role, "CHATBOT") {
			role = roleAssistant
		}

		msgs = append(msgs, ollamaMessage{Role: role, Content: h.Message})
	}

	msgs = append(msgs, ollamaMessage{Role: "user", Content: req.Message})

	var topK *int
	if req.K != nil {
		k := int(*req.K)
		topK = &k
	}

	history := func(res *ollamaResult) []map[string]string {
		h := make([]map[string]string, 0, len(req.History)+chatTurnPair)
		for _, m := range req.History {
			h = append(h, map[string]string{keyRole: strings.ToUpper(m.Role), keyMessage: m.Message})
		}

		return append(h,
			map[string]string{keyRole: "USER", keyMessage: req.Message},
			map[string]string{keyRole: "CHATBOT", keyMessage: res.Text})
	}
	finish := func(res *ollamaResult) string {
		if res.DoneReason == stopLength {
			return cohereMaxTokens
		}

		return cohereComplete
	}
	final := func(res *ollamaResult) map[string]any {
		return map[string]any{
			"response_id": awsid.New(""), "text": res.Text, "generation_id": awsid.New(""),
			"chat_history": history(res), keyFinishReason: finish(res),
		}
	}

	return &invokePlan{
		call: &ollamaCall{chat: &ollamaChatReq{
			Model:    target,
			Messages: msgs,
			Options:  opts(req.MaxTokens, req.Temperature, req.P, topK, req.StopSequences),
		}},
		render: func(res *ollamaResult) any { return final(res) },
		streamer: func(string) invokeStreamer {
			return &simpleStreamer{
				delta: func(t string) any {
					return map[string]any{"is_finished": false, "event_type": "text-generation", keyText: t}
				},
				last: func(res *ollamaResult, _ time.Duration) any {
					return map[string]any{
						"is_finished": true, "event_type": "stream-end",
						keyFinishReason: finish(res), "response": final(res),
					}
				},
			}
		},
	}, nil
}

// --- handlers ----------------------------------------------------------------------

func (o *ollamaBackend) invokePlanFor(modelID string, body []byte) (*invokePlan, error) {
	target, ok := o.resolve(modelID)
	if !ok {
		return nil, errUnmappedModel
	}

	build := familyFor(normalizedModelID(modelID))
	if build == nil {
		return nil, fmt.Errorf("%w: model %q is not supported by the ollama backend", errUnsupportedContent, modelID)
	}

	return build(target, modelID, body)
}

func writeInvokeError(c *echo.Context, err error) error {
	if errors.Is(err, errMalformed) {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException",
			"Malformed input request: "+strings.TrimPrefix(err.Error(), errMalformed.Error()+": ")+
				", please reformat your input and try again."))
	}

	return writeLLMError(c, err)
}

func (h *Handler) handleOllamaInvoke(c *echo.Context, o *ollamaBackend, modelID string, body []byte) error {
	plan, err := o.invokePlanFor(modelID, body)
	if err != nil {
		return writeInvokeError(c, err)
	}

	res, err := o.run(c.Request().Context(), plan.call, nil, nil)
	if err != nil {
		return writeInvokeError(c, err)
	}

	out, err := json.Marshal(plan.render(res))
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation(opInvokeModel, modelID, truncateString(string(body)), truncateString(string(out)))

	c.Response().Header().Set("Content-Type", resolveResponseContentType(c.Request()))
	c.Response().Header().Set(hdrBedrockInputTokenCount, strconv.Itoa(res.PromptTokens))
	c.Response().Header().Set(hdrBedrockOutputTokenCount, strconv.Itoa(res.EvalTokens))
	echoPerformanceConfigLatency(c)

	return c.JSONBlob(http.StatusOK, out)
}

func (h *Handler) handleOllamaInvokeStream(c *echo.Context, o *ollamaBackend, modelID string, body []byte) error {
	plan, err := o.invokePlanFor(modelID, body)
	if err != nil {
		return writeInvokeError(c, err)
	}

	if plan.streamer == nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException",
			"The model does not support response streaming for this request."))
	}

	st := plan.streamer(modelID)
	emit, exception := frameWriter(c)
	started := false

	send := func(chunks []any) {
		if len(chunks) == 0 {
			return
		}

		if !started {
			started = true

			c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
			c.Response().Header().Set(hdrBedrockContentType, resolveStreamResponseContentType(c.Request()))
			echoPerformanceConfigLatency(c)
			c.Response().WriteHeader(http.StatusOK)
		}

		for _, ch := range chunks {
			emit("chunk", json.RawMessage(modelResponsePayloadPart(mustJSON(ch))))
		}
	}

	start := time.Now()

	res, err := o.run(c.Request().Context(), plan.call,
		func(t string) error {
			send(append(st.begin(), st.text(t)...))

			return nil
		},
		func(tc ollamaToolCall) error {
			send(append(st.begin(), st.tool(tc)...))

			return nil
		},
	)
	if err != nil {
		if !started {
			return writeInvokeError(c, err)
		}

		exception("modelStreamErrorException", err.Error())

		return nil
	}

	send(append(st.begin(), st.end(res, time.Since(start))...))

	h.Backend.RecordInvocation(opInvokeModelWithResponseStream, modelID,
		truncateString(string(body)), truncateString(string(mustJSON(plan.render(res)))))

	return nil
}
