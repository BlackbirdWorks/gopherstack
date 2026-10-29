package bedrockruntime_test

import (
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bedrockruntimesdk "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

type fakeReply struct {
	errMsg string
	lines  []map[string]any
	status int
}

type ollamaRecorder struct {
	reqs []map[string]any
	path []string
	mu   sync.Mutex
}

func (r *ollamaRecorder) last(t *testing.T) (string, map[string]any) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.NotEmpty(t, r.reqs, "ollama was never called")

	return r.path[len(r.path)-1], r.reqs[len(r.reqs)-1]
}

func collapse(lines []map[string]any) map[string]any {
	out := map[string]any{}

	var content, resp strings.Builder

	var calls []any

	for _, l := range lines {
		maps.Copy(out, l)

		if m, isMsg := l["message"].(map[string]any); isMsg {
			content.WriteString(m["content"].(string))

			if tc, hasCalls := m["tool_calls"].([]any); hasCalls {
				calls = append(calls, tc...)
			}
		}

		if r, isResp := l["response"].(string); isResp {
			resp.WriteString(r)
		}
	}

	if _, hasMsg := out["message"]; hasMsg {
		msg := map[string]any{"role": "assistant", "content": content.String()}
		if len(calls) > 0 {
			msg["tool_calls"] = calls
		}

		out["message"] = msg
	}

	if _, hasResp := out["response"]; hasResp {
		out["response"] = resp.String()
	}

	return out
}

func newFakeOllama(t *testing.T, reply func(path string, req map[string]any) fakeReply) (string, *ollamaRecorder) {
	t.Helper()
	rec := &ollamaRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(raw, &req)
		rec.mu.Lock()
		rec.reqs = append(rec.reqs, req)
		rec.path = append(rec.path, r.URL.Path)
		rec.mu.Unlock()

		rp := reply(r.URL.Path, req)
		if rp.status != 0 {
			w.WriteHeader(rp.status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": rp.errMsg})

			return
		}

		w.Header().Set("Content-Type", "application/x-ndjson")
		enc := json.NewEncoder(w)
		if stream, _ := req["stream"].(bool); !stream {
			_ = enc.Encode(collapse(rp.lines))

			return
		}
		for _, l := range rp.lines {
			_ = enc.Encode(l)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	t.Cleanup(srv.Close)

	return srv.URL, rec
}

func newOllamaSDKClient(t *testing.T, cfg bedrockruntime.LLMConfig) *bedrockruntimesdk.Client {
	t.Helper()

	backend := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.SetLLM(cfg))
	h := bedrockruntime.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	awsCfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return bedrockruntimesdk.NewFromConfig(awsCfg, func(o *bedrockruntimesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = sdktest.PlainBodyHTTPClient()
		o.Retryer = aws.NopRetryer{}
	})
}

const testModelMap = "anthropic.claude-*=llama3.2,amazon.titan-embed-*=nomic-embed-text," +
	"amazon.titan-text-*=llama3.2,amazon.nova-*=llama3.2,meta.llama*=llama3.2,mistral.*=mistral," +
	"cohere.embed-*=nomic-embed-text," +
	"cohere.command-r*=llama3.2,ai21.*=llama3.2"

func chatLines(parts ...string) []map[string]any {
	var out []map[string]any
	for _, p := range parts {
		out = append(
			out,
			map[string]any{
				"model":   "llama3.2",
				"message": map[string]any{"role": "assistant", "content": p},
				"done":    false,
			},
		)
	}

	return append(out, map[string]any{
		"model": "llama3.2", "message": map[string]any{"role": "assistant", "content": ""},
		"done": true, "done_reason": "stop", "prompt_eval_count": 7, "eval_count": 3,
	})
}

func toolLines() []map[string]any {
	return []map[string]any{
		{"message": map[string]any{"role": "assistant", "content": "", "tool_calls": []any{
			map[string]any{
				"function": map[string]any{"name": "get_weather", "arguments": map[string]any{"city": "Paris"}},
			},
		}}, "done": false},
		{
			"message":           map[string]any{"role": "assistant", "content": ""},
			"done":              true,
			"done_reason":       "stop",
			"prompt_eval_count": 11,
			"eval_count":        5,
		},
	}
}

func genLines(parts ...string) []map[string]any {
	var out []map[string]any
	for _, p := range parts {
		out = append(out, map[string]any{"response": p, "done": false})
	}

	return append(
		out,
		map[string]any{"response": "", "done": true, "done_reason": "stop", "prompt_eval_count": 4, "eval_count": 2},
	)
}

func userText(s string) types.Message {
	return types.Message{
		Role:    types.ConversationRoleUser,
		Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: s}},
	}
}

func weatherTool() *types.ToolConfiguration {
	return &types.ToolConfiguration{Tools: []types.Tool{&types.ToolMemberToolSpec{Value: types.ToolSpecification{
		Name: aws.String("get_weather"), Description: aws.String("weather"),
		InputSchema: &types.ToolInputSchemaMemberJson{Value: document.NewLazyDocument(map[string]any{
			"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}},
		})},
	}}}}
}

func TestOllamaBackend_Converse(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in         func() *bedrockruntimesdk.ConverseInput
		checkReq   func(t *testing.T, req map[string]any)
		checkOut   func(t *testing.T, out *bedrockruntimesdk.ConverseOutput)
		name       string
		wantStop   types.StopReason
		reply      []map[string]any
		wantInput  int32
		wantOutput int32
	}{
		{
			name:  "text",
			reply: chatLines("Hello ", "there"),
			in: func() *bedrockruntimesdk.ConverseInput {
				return &bedrockruntimesdk.ConverseInput{
					ModelId:  aws.String("anthropic.claude-3-haiku-20240307-v1:0"),
					System:   []types.SystemContentBlock{&types.SystemContentBlockMemberText{Value: "be brief"}},
					Messages: []types.Message{userText("hi")},
					InferenceConfig: &types.InferenceConfiguration{
						MaxTokens: aws.Int32(
							50,
						), Temperature: aws.Float32(0.5), TopP: aws.Float32(0.5), StopSequences: []string{"END"},
					},
				}
			},
			checkReq: func(t *testing.T, req map[string]any) {
				t.Helper()
				assert.Equal(t, "llama3.2", req["model"])
				msgs, _ := req["messages"].([]any)
				require.Len(t, msgs, 2)
				assert.Equal(t, "system", msgs[0].(map[string]any)["role"])
				assert.Equal(t, "be brief", msgs[0].(map[string]any)["content"])
				opts, _ := req["options"].(map[string]any)
				assert.InDelta(t, 50, opts["num_predict"], 0)
				assert.InDelta(t, 0.5, opts["temperature"], 0)
				assert.Equal(t, []any{"END"}, opts["stop"])
			},
			checkOut: func(t *testing.T, out *bedrockruntimesdk.ConverseOutput) {
				t.Helper()
				msg := out.Output.(*types.ConverseOutputMemberMessage).Value
				require.Len(t, msg.Content, 1)
				assert.Equal(t, "Hello there", msg.Content[0].(*types.ContentBlockMemberText).Value)
				assert.NotNil(t, out.Metrics)
			},
			wantStop: types.StopReasonEndTurn, wantInput: 7, wantOutput: 3,
		},
		{
			name: "max tokens",
			reply: []map[string]any{{
				"message": map[string]any{"role": "assistant", "content": "cut"}, "done": true,
				"done_reason": "length", "prompt_eval_count": 1, "eval_count": 2,
			}},
			in: func() *bedrockruntimesdk.ConverseInput {
				return &bedrockruntimesdk.ConverseInput{
					ModelId:  aws.String("anthropic.claude-3-haiku-20240307-v1:0"),
					Messages: []types.Message{userText("hi")},
				}
			},
			wantStop: types.StopReasonMaxTokens, wantInput: 1, wantOutput: 2,
		},
		{
			name:  "tool use",
			reply: toolLines(),
			in: func() *bedrockruntimesdk.ConverseInput {
				return &bedrockruntimesdk.ConverseInput{
					ModelId: aws.String(
						"us.anthropic.claude-3-haiku-20240307-v1:0",
					), Messages: []types.Message{userText("weather?")},
					ToolConfig: weatherTool(),
				}
			},
			checkReq: func(t *testing.T, req map[string]any) {
				t.Helper()
				tools, _ := req["tools"].([]any)
				require.Len(t, tools, 1)
				fn := tools[0].(map[string]any)["function"].(map[string]any)
				assert.Equal(t, "get_weather", fn["name"])
				assert.Equal(t, "object", fn["parameters"].(map[string]any)["type"])
			},
			checkOut: func(t *testing.T, out *bedrockruntimesdk.ConverseOutput) {
				t.Helper()
				msg := out.Output.(*types.ConverseOutputMemberMessage).Value
				require.Len(t, msg.Content, 1)
				tu := msg.Content[0].(*types.ContentBlockMemberToolUse).Value
				assert.Equal(t, "get_weather", aws.ToString(tu.Name))
				assert.NotEmpty(t, aws.ToString(tu.ToolUseId))
				var in map[string]any
				require.NoError(t, tu.Input.UnmarshalSmithyDocument(&in))
				assert.Equal(t, "Paris", in["city"])
			},
			wantStop: types.StopReasonToolUse, wantInput: 11, wantOutput: 5,
		},
		{
			name:  "tool result round trip and image",
			reply: chatLines("sunny"),
			in: func() *bedrockruntimesdk.ConverseInput {
				return &bedrockruntimesdk.ConverseInput{
					ModelId:    aws.String("anthropic.claude-3-haiku-20240307-v1:0"),
					ToolConfig: weatherTool(),
					Messages: []types.Message{
						{Role: types.ConversationRoleUser, Content: []types.ContentBlock{
							&types.ContentBlockMemberText{Value: "weather in Paris, and this picture?"},
							&types.ContentBlockMemberImage{Value: types.ImageBlock{
								Format: types.ImageFormatPng,
								Source: &types.ImageSourceMemberBytes{Value: []byte("PNGDATA")},
							}},
						}},
						{
							Role: types.ConversationRoleAssistant,
							Content: []types.ContentBlock{&types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{
								ToolUseId: aws.String("tu-1"), Name: aws.String("get_weather"),
								Input: document.NewLazyDocument(map[string]any{"city": "Paris"}),
							}}},
						},
						{
							Role: types.ConversationRoleUser,
							Content: []types.ContentBlock{
								&types.ContentBlockMemberToolResult{Value: types.ToolResultBlock{
									ToolUseId: aws.String("tu-1"),
									Content: []types.ToolResultContentBlock{
										&types.ToolResultContentBlockMemberText{Value: "22C"},
									},
								}},
							},
						},
					},
				}
			},
			checkReq: func(t *testing.T, req map[string]any) {
				t.Helper()
				msgs, _ := req["messages"].([]any)
				require.Len(t, msgs, 3)
				first := msgs[0].(map[string]any)
				assert.Equal(t, []any{"UE5HREFUQQ=="}, first["images"])
				asst := msgs[1].(map[string]any)
				calls := asst["tool_calls"].([]any)
				assert.Equal(t, "get_weather", calls[0].(map[string]any)["function"].(map[string]any)["name"])
				tool := msgs[2].(map[string]any)
				assert.Equal(t, "tool", tool["role"])
				assert.Equal(t, "22C", tool["content"])
				assert.Equal(t, "get_weather", tool["tool_name"])
			},
			checkOut: func(t *testing.T, out *bedrockruntimesdk.ConverseOutput) {
				t.Helper()
				msg := out.Output.(*types.ConverseOutputMemberMessage).Value
				assert.Equal(t, "sunny", msg.Content[0].(*types.ContentBlockMemberText).Value)
			},
			wantStop: types.StopReasonEndTurn, wantInput: 7, wantOutput: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			url, rec := newFakeOllama(t, func(string, map[string]any) fakeReply { return fakeReply{lines: tc.reply} })
			client := newOllamaSDKClient(
				t,
				bedrockruntime.LLMConfig{Backend: "ollama", OllamaURL: url, ModelMap: testModelMap},
			)

			out, err := client.Converse(t.Context(), tc.in())
			require.NoError(t, err)

			path, req := rec.last(t)
			assert.Equal(t, "/api/chat", path)
			assert.Equal(t, false, req["stream"])
			if tc.checkReq != nil {
				tc.checkReq(t, req)
			}
			if tc.checkOut != nil {
				tc.checkOut(t, out)
			}
			assert.Equal(t, tc.wantStop, out.StopReason)
			assert.Equal(t, tc.wantInput, aws.ToInt32(out.Usage.InputTokens))
			assert.Equal(t, tc.wantOutput, aws.ToInt32(out.Usage.OutputTokens))
			assert.Equal(t, tc.wantInput+tc.wantOutput, aws.ToInt32(out.Usage.TotalTokens))
		})
	}
}

func TestOllamaBackend_ConverseStream(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		reply []map[string]any
		want  []string
		tools bool
	}{
		{
			name: "text", reply: chatLines("Hel", "lo"),
			want: []string{
				"messageStart",
				"contentBlockStart",
				"contentBlockDelta",
				"contentBlockDelta",
				"contentBlockStop",
				"messageStop",
				"metadata",
			},
		},
		{
			name: "tool use", reply: toolLines(), tools: true,
			want: []string{
				"messageStart",
				"contentBlockStart",
				"contentBlockDelta",
				"contentBlockStop",
				"messageStop",
				"metadata",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			url, rec := newFakeOllama(t, func(string, map[string]any) fakeReply { return fakeReply{lines: tc.reply} })
			client := newOllamaSDKClient(
				t,
				bedrockruntime.LLMConfig{Backend: "ollama", OllamaURL: url, ModelMap: testModelMap},
			)

			in := &bedrockruntimesdk.ConverseStreamInput{
				ModelId: aws.String(
					"anthropic.claude-3-haiku-20240307-v1:0",
				), Messages: []types.Message{userText("hi")},
			}
			if tc.tools {
				in.ToolConfig = weatherTool()
			}

			out, err := client.ConverseStream(t.Context(), in)
			require.NoError(t, err)
			stream := out.GetStream()
			t.Cleanup(func() { _ = stream.Close() })

			var got []string
			var text string
			var stop types.StopReason
			var usage *types.TokenUsage
			var toolName, toolInput string

			for ev := range stream.Events() {
				switch e := ev.(type) {
				case *types.ConverseStreamOutputMemberMessageStart:
					got = append(got, "messageStart")
				case *types.ConverseStreamOutputMemberContentBlockStart:
					got = append(got, "contentBlockStart")
					if tu, ok := e.Value.Start.(*types.ContentBlockStartMemberToolUse); ok {
						toolName = aws.ToString(tu.Value.Name)
						assert.NotEmpty(t, aws.ToString(tu.Value.ToolUseId))
					}
				case *types.ConverseStreamOutputMemberContentBlockDelta:
					got = append(got, "contentBlockDelta")
					switch d := e.Value.Delta.(type) {
					case *types.ContentBlockDeltaMemberText:
						text += d.Value
					case *types.ContentBlockDeltaMemberToolUse:
						toolInput += aws.ToString(d.Value.Input)
					}
				case *types.ConverseStreamOutputMemberContentBlockStop:
					got = append(got, "contentBlockStop")
				case *types.ConverseStreamOutputMemberMessageStop:
					got = append(got, "messageStop")
					stop = e.Value.StopReason
				case *types.ConverseStreamOutputMemberMetadata:
					got = append(got, "metadata")
					usage = e.Value.Usage
				case *types.UnknownUnionMember:
					t.Fatalf("unknown event %q", e.Tag)
				}
			}

			require.NoError(t, stream.Err())
			assert.Equal(t, tc.want, got)
			_, req := rec.last(t)
			assert.Equal(t, true, req["stream"])
			require.NotNil(t, usage)

			if tc.tools {
				assert.Equal(t, types.StopReasonToolUse, stop)
				assert.Equal(t, "get_weather", toolName)
				assert.JSONEq(t, `{"city":"Paris"}`, toolInput)
				assert.Equal(t, int32(11), aws.ToInt32(usage.InputTokens))
			} else {
				assert.Equal(t, types.StopReasonEndTurn, stop)
				assert.Equal(t, "Hello", text)
				assert.Equal(t, int32(7), aws.ToInt32(usage.InputTokens))
				assert.Equal(t, int32(3), aws.ToInt32(usage.OutputTokens))
			}
		})
	}
}

func TestOllamaBackend_InvokeModel(t *testing.T) {
	t.Parallel()

	embed := map[string]any{"embeddings": []any{[]any{0.1, 0.2}, []any{0.3, 0.4}}, "prompt_eval_count": 6}

	cases := []struct {
		check    func(t *testing.T, req map[string]any, out map[string]any)
		name     string
		modelID  string
		body     string
		wantPath string
		reply    []map[string]any
	}{
		{
			name:     "anthropic messages",
			modelID:  "anthropic.claude-3-sonnet-20240229-v1:0",
			wantPath: "/api/chat",
			reply:    chatLines("Hi ", "you"),
			body: `{"anthropic_version":"bedrock-2023-05-31","max_tokens":20,"system":"sys",
"temperature":0.2,"stop_sequences":["X"],
"messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				msgs := req["messages"].([]any)
				assert.Equal(t, "sys", msgs[0].(map[string]any)["content"])
				assert.Equal(t, "hello", msgs[1].(map[string]any)["content"])
				assert.InDelta(t, 20, req["options"].(map[string]any)["num_predict"], 0)
				assert.Equal(t, "message", out["type"])
				assert.Equal(t, "assistant", out["role"])
				assert.Equal(t, "end_turn", out["stop_reason"])
				c := out["content"].([]any)[0].(map[string]any)
				assert.Equal(t, "Hi you", c["text"])
				assert.InDelta(t, 7, out["usage"].(map[string]any)["input_tokens"], 0)
				assert.InDelta(t, 3, out["usage"].(map[string]any)["output_tokens"], 0)
			},
		},
		{
			name:     "anthropic tool use",
			modelID:  "anthropic.claude-3-sonnet-20240229-v1:0",
			wantPath: "/api/chat",
			reply:    toolLines(),
			body: `{"anthropic_version":"bedrock-2023-05-31","max_tokens":20,"messages":[{"role":"user","content":"w?"}],
"tools":[{"name":"get_weather","description":"d","input_schema":{"type":"object"}}]}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				assert.Equal(
					t,
					"get_weather",
					req["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["name"],
				)
				assert.Equal(t, "tool_use", out["stop_reason"])
				c := out["content"].([]any)[0].(map[string]any)
				assert.Equal(t, "tool_use", c["type"])
				assert.Equal(t, "get_weather", c["name"])
				assert.NotEmpty(t, c["id"])
				assert.Equal(t, map[string]any{"city": "Paris"}, c["input"])
			},
		},
		{
			name:     "titan text",
			modelID:  "amazon.titan-text-express-v1",
			wantPath: "/api/generate",
			reply:    genLines("titan ", "out"),
			body: `{"inputText":"hey","textGenerationConfig":{"maxTokenCount":9,"temperature":0.1,
"topP":0.9,"stopSequences":["Z"]}}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				assert.Equal(t, "hey", req["prompt"])
				assert.InDelta(t, 9, req["options"].(map[string]any)["num_predict"], 0)
				r := out["results"].([]any)[0].(map[string]any)
				assert.Equal(t, "titan out", r["outputText"])
				assert.Equal(t, "FINISH", r["completionReason"])
				assert.InDelta(t, 2, r["tokenCount"], 0)
				assert.InDelta(t, 4, out["inputTextTokenCount"], 0)
			},
		},
		{
			name:     "titan embed v1",
			modelID:  "amazon.titan-embed-text-v1",
			wantPath: "/api/embed",
			reply:    []map[string]any{embed},
			body:     `{"inputText":"embed me"}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				assert.Equal(t, "nomic-embed-text", req["model"])
				assert.Equal(t, []any{"embed me"}, req["input"])
				assert.Equal(t, []any{0.1, 0.2}, out["embedding"])
				assert.InDelta(t, 6, out["inputTextTokenCount"], 0)
				assert.NotContains(t, out, "embeddingsByType")
			},
		},
		{
			name:     "titan embed v2",
			modelID:  "amazon.titan-embed-text-v2:0",
			wantPath: "/api/embed",
			reply:    []map[string]any{embed},
			body:     `{"inputText":"embed me","dimensions":256,"embeddingTypes":["float"]}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				assert.InDelta(t, 256, req["dimensions"], 0)
				assert.Equal(t, map[string]any{"float": []any{0.1, 0.2}}, out["embeddingsByType"])
			},
		},
		{
			name:     "nova",
			modelID:  "amazon.nova-lite-v1:0",
			wantPath: "/api/chat",
			reply:    chatLines("nova"),
			body: `{"schemaVersion":"messages-v1","system":[{"text":"s"}],"messages":[{"role":"user","content":[{"text":"hi"}]}],
"inferenceConfig":{"max_new_tokens":33,"temperature":0.3}}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				assert.InDelta(t, 33, req["options"].(map[string]any)["num_predict"], 0)
				msg := out["output"].(map[string]any)["message"].(map[string]any)
				assert.Equal(t, "nova", msg["content"].([]any)[0].(map[string]any)["text"])
				assert.Equal(t, "end_turn", out["stopReason"])
				assert.InDelta(t, 10, out["usage"].(map[string]any)["totalTokens"], 0)
			},
		},
		{
			name:     "llama",
			modelID:  "meta.llama3-70b-instruct-v1:0",
			wantPath: "/api/generate",
			reply:    genLines("llama"),
			body:     `{"prompt":"p","max_gen_len":5,"temperature":0.5,"top_p":0.9}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				assert.InDelta(t, 5, req["options"].(map[string]any)["num_predict"], 0)
				assert.Equal(t, "llama", out["generation"])
				assert.Equal(t, "stop", out["stop_reason"])
				assert.InDelta(t, 4, out["prompt_token_count"], 0)
				assert.InDelta(t, 2, out["generation_token_count"], 0)
			},
		},
		{
			name:     "mistral",
			modelID:  "mistral.mistral-7b-instruct-v0:2",
			wantPath: "/api/generate",
			reply:    genLines("m"),
			body:     `{"prompt":"<s>[INST] hi [/INST]","max_tokens":8,"stop":["S"],"top_k":40}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				assert.Equal(t, "mistral", req["model"])
				assert.InDelta(t, 40, req["options"].(map[string]any)["top_k"], 0)
				o := out["outputs"].([]any)[0].(map[string]any)
				assert.Equal(t, "m", o["text"])
				assert.Equal(t, "stop", o["stop_reason"])
			},
		},
		{
			name:     "cohere embed",
			modelID:  "cohere.embed-english-v3",
			wantPath: "/api/embed",
			reply:    []map[string]any{embed},
			body:     `{"texts":["a","b"],"input_type":"search_document"}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				assert.Equal(t, []any{"a", "b"}, req["input"])
				assert.Equal(t, "embeddings_floats", out["response_type"])
				assert.Len(t, out["embeddings"], 2)
				assert.Equal(t, []any{"a", "b"}, out["texts"])
				assert.NotEmpty(t, out["id"])
			},
		},
		{
			name:     "cohere embed by type",
			modelID:  "cohere.embed-english-v3",
			wantPath: "/api/embed",
			reply:    []map[string]any{embed},
			body:     `{"texts":["a","b"],"input_type":"search_query","embedding_types":["float"]}`,
			check: func(t *testing.T, _, out map[string]any) {
				t.Helper()
				assert.Equal(t, "embeddings_by_type", out["response_type"])
				assert.Len(t, out["embeddings"].(map[string]any)["float"], 2)
			},
		},
		{
			name: "cohere command r", modelID: "cohere.command-r-v1:0", wantPath: "/api/chat", reply: chatLines("ok"),
			body: `{"message":"next","preamble":"pre","max_tokens":12,"k":10,
"chat_history":[{"role":"USER","message":"q"},{"role":"CHATBOT","message":"a"}]}`,
			check: func(t *testing.T, req, out map[string]any) {
				t.Helper()
				msgs := req["messages"].([]any)
				require.Len(t, msgs, 4)
				assert.Equal(t, "assistant", msgs[2].(map[string]any)["role"])
				assert.Equal(t, "ok", out["text"])
				assert.Equal(t, "COMPLETE", out["finish_reason"])
				assert.Len(t, out["chat_history"], 4)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			url, rec := newFakeOllama(t, func(string, map[string]any) fakeReply { return fakeReply{lines: tc.reply} })
			client := newOllamaSDKClient(
				t,
				bedrockruntime.LLMConfig{Backend: "ollama", OllamaURL: url, ModelMap: testModelMap},
			)

			resp, err := client.InvokeModel(t.Context(), &bedrockruntimesdk.InvokeModelInput{
				ModelId: aws.String(tc.modelID), Body: []byte(tc.body), ContentType: aws.String("application/json"),
			})
			require.NoError(t, err)

			path, req := rec.last(t)
			assert.Equal(t, tc.wantPath, path)

			var out map[string]any
			require.NoError(t, json.Unmarshal(resp.Body, &out))
			tc.check(t, req, out)
		})
	}
}

func TestOllamaBackend_InvokeModelStream(t *testing.T) {
	t.Parallel()

	cases := []struct {
		check   func(t *testing.T, chunks []map[string]any)
		name    string
		modelID string
		body    string
		reply   []map[string]any
	}{
		{
			name:    "titan",
			modelID: "amazon.titan-text-express-v1",
			reply:   genLines("a", "b"),
			body:    `{"inputText":"x"}`,
			check: func(t *testing.T, ch []map[string]any) {
				t.Helper()
				require.Len(t, ch, 3)
				assert.Equal(t, "a", ch[0]["outputText"])
				assert.Nil(t, ch[0]["completionReason"])
				assert.Equal(t, "FINISH", ch[2]["completionReason"])
				assert.InDelta(t, 2, ch[2]["totalOutputTextTokenCount"], 0)
				assert.Contains(t, ch[2], "amazon-bedrock-invocationMetrics")
			},
		},
		{
			name: "llama", modelID: "meta.llama3-8b-instruct-v1:0", reply: genLines("a", "b"), body: `{"prompt":"x"}`,
			check: func(t *testing.T, ch []map[string]any) {
				t.Helper()
				require.Len(t, ch, 3)
				assert.Equal(t, "b", ch[1]["generation"])
				assert.Equal(t, "stop", ch[2]["stop_reason"])
				assert.InDelta(t, 4, ch[2]["prompt_token_count"], 0)
			},
		},
		{
			name:    "mistral",
			modelID: "mistral.mixtral-8x7b-instruct-v0:1",
			reply:   genLines("a"),
			body:    `{"prompt":"x"}`,
			check: func(t *testing.T, ch []map[string]any) {
				t.Helper()
				require.Len(t, ch, 2)
				assert.Equal(t, "a", ch[0]["outputs"].([]any)[0].(map[string]any)["text"])
				assert.Equal(t, "stop", ch[1]["outputs"].([]any)[0].(map[string]any)["stop_reason"])
			},
		},
		{
			name:    "anthropic",
			modelID: "anthropic.claude-3-haiku-20240307-v1:0",
			reply:   chatLines("a", "b"),
			body:    `{"anthropic_version":"bedrock-2023-05-31","max_tokens":5,"messages":[{"role":"user","content":"x"}]}`,
			check: func(t *testing.T, ch []map[string]any) {
				t.Helper()
				types := make([]any, 0, len(ch))
				for _, c := range ch {
					types = append(types, c["type"])
				}
				assert.Equal(
					t,
					[]any{"message_start", "content_block_start", "content_block_delta", "content_block_delta",
						"content_block_stop", "message_delta", "message_stop"},
					types,
				)
				assert.Equal(t, "text_delta", ch[2]["delta"].(map[string]any)["type"])
				assert.Equal(t, "end_turn", ch[5]["delta"].(map[string]any)["stop_reason"])
				assert.InDelta(t, 3, ch[5]["usage"].(map[string]any)["output_tokens"], 0)
				assert.Contains(t, ch[6], "amazon-bedrock-invocationMetrics")
			},
		},
		{
			name: "nova", modelID: "amazon.nova-pro-v1:0", reply: chatLines("a"),
			body: `{"schemaVersion":"messages-v1","messages":[{"role":"user","content":[{"text":"x"}]}]}`,
			check: func(t *testing.T, ch []map[string]any) {
				t.Helper()
				require.GreaterOrEqual(t, len(ch), 5)
				assert.Contains(t, ch[0], "messageStart")
				assert.Contains(t, ch[len(ch)-2], "messageStop")
				assert.Contains(t, ch[len(ch)-1], "metadata")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			url, rec := newFakeOllama(t, func(string, map[string]any) fakeReply { return fakeReply{lines: tc.reply} })
			client := newOllamaSDKClient(
				t,
				bedrockruntime.LLMConfig{Backend: "ollama", OllamaURL: url, ModelMap: testModelMap},
			)

			resp, err := client.InvokeModelWithResponseStream(
				t.Context(),
				&bedrockruntimesdk.InvokeModelWithResponseStreamInput{
					ModelId: aws.String(tc.modelID), Body: []byte(tc.body), ContentType: aws.String("application/json"),
				},
			)
			require.NoError(t, err)
			stream := resp.GetStream()
			t.Cleanup(func() { _ = stream.Close() })

			var chunks []map[string]any
			for ev := range stream.Events() {
				part, ok := ev.(*types.ResponseStreamMemberChunk)
				require.True(t, ok, "unexpected event %T", ev)
				var m map[string]any
				require.NoError(t, json.Unmarshal(part.Value.Bytes, &m))
				chunks = append(chunks, m)
			}
			require.NoError(t, stream.Err())

			_, req := rec.last(t)
			assert.Equal(t, true, req["stream"])
			tc.check(t, chunks)
		})
	}
}

func isErr[E error](err error) bool {
	var target E

	return errors.As(err, &target)
}

func TestOllamaBackend_Errors(t *testing.T) {
	t.Parallel()

	converse := func(client *bedrockruntimesdk.Client, id string) error {
		_, err := client.Converse(
			t.Context(),
			&bedrockruntimesdk.ConverseInput{ModelId: aws.String(id), Messages: []types.Message{userText("hi")}},
		)

		return err
	}
	invoke := func(client *bedrockruntimesdk.Client, id, body string) error {
		_, err := client.InvokeModel(
			t.Context(),
			&bedrockruntimesdk.InvokeModelInput{ModelId: aws.String(id), Body: []byte(body)},
		)

		return err
	}

	cases := []struct {
		call    func(c *bedrockruntimesdk.Client) error
		wantErr func(error) bool
		name    string
		reply   fakeReply
		down    bool
	}{
		{
			name:    "ollama down converse",
			down:    true,
			call:    func(c *bedrockruntimesdk.Client) error { return converse(c, "anthropic.claude-3-haiku-20240307-v1:0") },
			wantErr: isErr[*types.ServiceUnavailableException],
		},
		{name: "ollama down invoke", down: true, call: func(c *bedrockruntimesdk.Client) error {
			return invoke(c, "meta.llama3-8b-instruct-v1:0", `{"prompt":"x"}`)
		}, wantErr: isErr[*types.ServiceUnavailableException]},
		{
			name:    "model not pulled",
			reply:   fakeReply{status: 404, errMsg: "model not found, try pulling it first"},
			call:    func(c *bedrockruntimesdk.Client) error { return converse(c, "anthropic.claude-3-haiku-20240307-v1:0") },
			wantErr: isErr[*types.ModelNotReadyException],
		},
		{
			name:    "ollama bad request",
			reply:   fakeReply{status: 400, errMsg: "does not support tools"},
			call:    func(c *bedrockruntimesdk.Client) error { return converse(c, "anthropic.claude-3-haiku-20240307-v1:0") },
			wantErr: isErr[*types.ValidationException],
		},
		{
			name:    "ollama 500",
			reply:   fakeReply{status: 500, errMsg: "boom"},
			call:    func(c *bedrockruntimesdk.Client) error { return converse(c, "anthropic.claude-3-haiku-20240307-v1:0") },
			wantErr: isErr[*types.ModelErrorException],
		},
		{name: "embedding stream", call: func(c *bedrockruntimesdk.Client) error {
			_, err := c.InvokeModelWithResponseStream(
				t.Context(),
				&bedrockruntimesdk.InvokeModelWithResponseStreamInput{
					ModelId: aws.String("amazon.titan-embed-text-v1"), Body: []byte(`{"inputText":"x"}`),
				},
			)

			return err
		}, wantErr: isErr[*types.ValidationException]},
		{
			name:    "unmapped model",
			call:    func(c *bedrockruntimesdk.Client) error { return converse(c, "cohere.command-text-v14") },
			wantErr: isErr[*types.ResourceNotFoundException],
		},
		{
			name:    "unsupported family",
			call:    func(c *bedrockruntimesdk.Client) error { return invoke(c, "ai21.jamba-1-5-large-v1:0", `{}`) },
			wantErr: isErr[*types.ValidationException],
		},
		{
			name:    "malformed body",
			call:    func(c *bedrockruntimesdk.Client) error { return invoke(c, "meta.llama3-8b-instruct-v1:0", `{not json`) },
			wantErr: isErr[*types.ValidationException],
		},
		{
			name:    "missing prompt",
			call:    func(c *bedrockruntimesdk.Client) error { return invoke(c, "meta.llama3-8b-instruct-v1:0", `{}`) },
			wantErr: isErr[*types.ValidationException],
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			url, _ := newFakeOllama(t, func(string, map[string]any) fakeReply { return tc.reply })
			if tc.down {
				url = "http://127.0.0.1:1"
			}
			client := newOllamaSDKClient(
				t,
				bedrockruntime.LLMConfig{Backend: "ollama", OllamaURL: url, ModelMap: testModelMap},
			)

			err := tc.call(client)
			require.Error(t, err)
			assert.True(t, tc.wantErr(err), "wrong error type: %T %v", err, err)
		})
	}
}

func TestOllamaBackend_MockDefaultUnchanged(t *testing.T) {
	t.Parallel()

	for _, backend := range []string{"", "mock"} {
		t.Run("backend="+backend, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			client := newOllamaSDKClient(t, bedrockruntime.LLMConfig{Backend: backend, OllamaURL: "http://127.0.0.1:1"})
			out, err := client.Converse(t.Context(), &bedrockruntimesdk.ConverseInput{
				ModelId: aws.String(
					"anthropic.claude-3-haiku-20240307-v1:0",
				), Messages: []types.Message{userText("hi")},
			})
			require.NoError(t, err)
			msg := out.Output.(*types.ConverseOutputMemberMessage).Value
			assert.Equal(
				t,
				"This is a mock response from Gopherstack.",
				msg.Content[0].(*types.ContentBlockMemberText).Value,
			)
		})
	}
}

func TestOllamaBackend_ModelMapMatching(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		modelID string
		want    string
	}{
		{name: "plain", modelID: "anthropic.claude-3-haiku-20240307-v1:0", want: "llama3.2"},
		{name: "geo profile", modelID: "eu.anthropic.claude-3-haiku-20240307-v1:0", want: "llama3.2"},
		{
			name:    "profile arn",
			modelID: "arn:aws:bedrock:us-east-1:111122223333:inference-profile/us.anthropic.claude-3-sonnet-20240229-v1:0",
			want:    "llama3.2",
		},
		{name: "second rule", modelID: "amazon.titan-embed-text-v2:0", want: "nomic-embed-text"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			url, rec := newFakeOllama(t, func(p string, _ map[string]any) fakeReply {
				if p == "/api/embed" {
					return fakeReply{lines: []map[string]any{{"embeddings": []any{[]any{1.0}}}}}
				}

				return fakeReply{lines: chatLines("x")}
			})
			client := newOllamaSDKClient(
				t,
				bedrockruntime.LLMConfig{Backend: "ollama", OllamaURL: url, ModelMap: testModelMap},
			)

			var err error
			if tc.want == "nomic-embed-text" {
				_, err = client.InvokeModel(
					t.Context(),
					&bedrockruntimesdk.InvokeModelInput{
						ModelId: aws.String(tc.modelID),
						Body:    []byte(`{"inputText":"a"}`),
					},
				)
			} else {
				_, err = client.Converse(
					t.Context(),
					&bedrockruntimesdk.ConverseInput{
						ModelId:  aws.String(tc.modelID),
						Messages: []types.Message{userText("hi")},
					},
				)
			}
			require.NoError(t, err)

			_, req := rec.last(t)
			assert.Equal(t, tc.want, req["model"])
		})
	}
}

func TestOllamaBackend_SetLLMValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cfg  bedrockruntime.LLMConfig
		bad  bool
	}{
		{name: "mock", cfg: bedrockruntime.LLMConfig{Backend: "mock"}},
		{name: "ollama defaults", cfg: bedrockruntime.LLMConfig{Backend: "ollama"}},
		{name: "unknown backend", cfg: bedrockruntime.LLMConfig{Backend: "bedrock"}, bad: true},
		{name: "bad url", cfg: bedrockruntime.LLMConfig{Backend: "ollama", OllamaURL: "localhost:11434"}, bad: true},
		{name: "bad map entry", cfg: bedrockruntime.LLMConfig{Backend: "ollama", ModelMap: "anthropic.*"}, bad: true},
		{name: "bad glob", cfg: bedrockruntime.LLMConfig{Backend: "ollama", ModelMap: "[=x"}, bad: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			err := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1").SetLLM(tc.cfg)
			if tc.bad {
				require.ErrorIs(t, err, bedrockruntime.ErrInvalidLLMConfig)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestOllamaBackend_Live talks to a real Ollama; set GOPHERSTACK_TEST_OLLAMA_URL and
// GOPHERSTACK_TEST_OLLAMA_MODEL (a pulled chat model) to run it.
func TestOllamaBackend_Live(t *testing.T) {
	t.Parallel()

	url, model := os.Getenv("GOPHERSTACK_TEST_OLLAMA_URL"), os.Getenv("GOPHERSTACK_TEST_OLLAMA_MODEL")
	if url == "" || model == "" {
		t.Skip("GOPHERSTACK_TEST_OLLAMA_URL / GOPHERSTACK_TEST_OLLAMA_MODEL not set")
	}

	client := newOllamaSDKClient(t, bedrockruntime.LLMConfig{Backend: "ollama", OllamaURL: url, ModelMap: "*=" + model})
	out, err := client.Converse(t.Context(), &bedrockruntimesdk.ConverseInput{
		ModelId:  aws.String("anthropic.claude-3-haiku-20240307-v1:0"),
		Messages: []types.Message{userText("Reply with the single word: pong")},
	})
	require.NoError(t, err)
	msg := out.Output.(*types.ConverseOutputMemberMessage).Value
	require.NotEmpty(t, msg.Content)
	assert.Positive(t, aws.ToInt32(out.Usage.OutputTokens))
}
