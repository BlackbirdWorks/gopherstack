package bedrockruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxOllamaErrBody = 4096

type ollamaOptions struct {
	NumPredict  *int     `json:"num_predict,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
	TopP        *float64 `json:"top_p,omitempty"`
	TopK        *int     `json:"top_k,omitempty"`
	Stop        []string `json:"stop,omitempty"`
}

type ollamaToolFunc struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Arguments   json.RawMessage `json:"arguments,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type ollamaToolCall struct {
	ID       string         `json:"id,omitempty"`
	Function ollamaToolFunc `json:"function"`
}

type ollamaTool struct {
	Type     string         `json:"type"`
	Function ollamaToolFunc `json:"function"`
}

type ollamaMessage struct {
	Role      string           `json:"role"`
	Content   string           `json:"content"`
	ToolName  string           `json:"tool_name,omitempty"`
	Images    []string         `json:"images,omitempty"`
	ToolCalls []ollamaToolCall `json:"tool_calls,omitempty"`
}

type ollamaChatReq struct {
	Options  *ollamaOptions  `json:"options,omitempty"`
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Tools    []ollamaTool    `json:"tools,omitempty"`
	Stream   bool            `json:"stream"`
}

type ollamaGenerateReq struct {
	Options *ollamaOptions `json:"options,omitempty"`
	Model   string         `json:"model"`
	Prompt  string         `json:"prompt"`
	System  string         `json:"system,omitempty"`
	Stream  bool           `json:"stream"`
}

type ollamaEmbedReq struct {
	Dimensions *int     `json:"dimensions,omitempty"`
	Model      string   `json:"model"`
	Input      []string `json:"input"`
}

type ollamaChunk struct {
	Message         ollamaMessage `json:"message"`
	Model           string        `json:"model"`
	Response        string        `json:"response"`
	DoneReason      string        `json:"done_reason"`
	Error           string        `json:"error"`
	Embeddings      [][]float64   `json:"embeddings"`
	PromptEvalCount int           `json:"prompt_eval_count"`
	EvalCount       int           `json:"eval_count"`
	Done            bool          `json:"done"`
}

// ollamaCall is exactly one of the three Ollama requests.
type ollamaCall struct {
	chat  *ollamaChatReq
	gen   *ollamaGenerateReq
	embed *ollamaEmbedReq
}

func (c *ollamaCall) endpoint() (string, any) {
	switch {
	case c.chat != nil:
		return "/api/chat", c.chat
	case c.gen != nil:
		return "/api/generate", c.gen
	default:
		return "/api/embed", c.embed
	}
}

func (c *ollamaCall) setStream(on bool) {
	switch {
	case c.chat != nil:
		c.chat.Stream = on
	case c.gen != nil:
		c.gen.Stream = on
	}
}

// ollamaResult is the merged outcome of one Ollama call.
type ollamaResult struct {
	Text         string
	DoneReason   string
	ToolCalls    []ollamaToolCall
	Embeddings   [][]float64
	PromptTokens int
	EvalTokens   int
}

var (
	errOllamaUnreachable = errors.New("ollama backend unreachable")
	errUnmappedModel     = errors.New("no ollama model mapped")
)

// ollamaStatusError is a non-2xx answer from Ollama.
type ollamaStatusError struct {
	msg    string
	status int
}

func (e *ollamaStatusError) Error() string {
	return fmt.Sprintf("ollama returned %d: %s", e.status, e.msg)
}

func (o *ollamaBackend) post(ctx context.Context, call *ollamaCall) (*http.Response, error) {
	endpoint, payload := call.endpoint()

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal ollama request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errOllamaUnreachable, err)
	}

	req.Header.Set("Content-Type", contentTypeJSON)

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errOllamaUnreachable, err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		defer resp.Body.Close()

		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxOllamaErrBody))
		msg := strings.TrimSpace(string(b))

		var e ollamaChunk
		if json.Unmarshal(b, &e) == nil && e.Error != "" {
			msg = e.Error
		}

		return nil, &ollamaStatusError{status: resp.StatusCode, msg: msg}
	}

	return resp, nil
}

// run executes call, invoking onText/onTool for streamed output when non-nil.
func (o *ollamaBackend) run(
	ctx context.Context,
	call *ollamaCall,
	onText func(string) error,
	onTool func(ollamaToolCall) error,
) (*ollamaResult, error) {
	call.setStream(onText != nil || onTool != nil)

	resp, err := o.post(ctx, call)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	res := &ollamaResult{}
	dec := json.NewDecoder(resp.Body)

	for {
		var ch ollamaChunk

		if derr := dec.Decode(&ch); derr != nil {
			return nil, fmt.Errorf("%w: reading ollama response: %w", errOllamaUnreachable, derr)
		}

		if ch.Error != "" {
			return nil, &ollamaStatusError{status: http.StatusInternalServerError, msg: ch.Error}
		}

		if aerr := res.absorb(&ch, onText, onTool); aerr != nil {
			return nil, aerr
		}

		if ch.Done || call.embed != nil {
			return res, nil
		}
	}
}

func (r *ollamaResult) absorb(ch *ollamaChunk, onText func(string) error, onTool func(ollamaToolCall) error) error {
	text := ch.Message.Content + ch.Response
	if text != "" {
		r.Text += text

		if onText != nil {
			if err := onText(text); err != nil {
				return err
			}
		}
	}

	for _, tc := range ch.Message.ToolCalls {
		r.ToolCalls = append(r.ToolCalls, tc)

		if onTool != nil {
			if err := onTool(tc); err != nil {
				return err
			}
		}
	}

	if len(ch.Embeddings) > 0 {
		r.Embeddings = ch.Embeddings
	}

	if ch.Done {
		r.DoneReason = ch.DoneReason
		if r.DoneReason == "" {
			r.DoneReason = stopStop
		}

		r.PromptTokens, r.EvalTokens = ch.PromptEvalCount, ch.EvalCount
	}

	if ch.PromptEvalCount > 0 && r.PromptTokens == 0 {
		r.PromptTokens = ch.PromptEvalCount
	}

	return nil
}
