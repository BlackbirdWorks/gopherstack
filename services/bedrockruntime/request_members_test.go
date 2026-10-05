package bedrockruntime_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrockruntimesdk "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func userMessage() []types.Message {
	return []types.Message{{
		Role:    types.ConversationRoleUser,
		Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: "hello"}},
	}}
}

func TestRealClient_ConverseTierAndLatency(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		tier        types.ServiceTierType
		latency     types.PerformanceConfigLatency
		wantErr     bool
		wantNoTier  bool
		wantNoLatcy bool
	}{
		{name: "echoed", tier: types.ServiceTierTypeFlex, latency: types.PerformanceConfigLatencyOptimized},
		{name: "omitted", wantNoTier: true, wantNoLatcy: true},
		{name: "bad_tier", tier: "gold", wantErr: true},
		{name: "bad_latency", latency: "turbo", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestBedrockRuntimeSDKClient(t, newTestHandler(t))
			in := &bedrockruntimesdk.ConverseInput{
				ModelId:  aws.String("amazon.nova-pro-v1:0"),
				Messages: userMessage(),
			}
			if tc.tier != "" {
				in.ServiceTier = &types.ServiceTier{Type: tc.tier}
			}

			if tc.latency != "" {
				in.PerformanceConfig = &types.PerformanceConfiguration{Latency: tc.latency}
			}

			out, err := client.Converse(t.Context(), in)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tc.wantNoTier {
				assert.Nil(t, out.ServiceTier)
			} else {
				require.NotNil(t, out.ServiceTier)
				assert.Equal(t, tc.tier, out.ServiceTier.Type)
			}

			if tc.wantNoLatcy {
				assert.Nil(t, out.PerformanceConfig)
			} else {
				require.NotNil(t, out.PerformanceConfig)
				assert.Equal(t, tc.latency, out.PerformanceConfig.Latency)
			}
		})
	}
}

func TestRealClient_InvokeModelServiceTierAndTrace(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		tier    types.ServiceTierType
		trace   types.Trace
		wantErr bool
	}{
		{name: "tier_echoed", tier: types.ServiceTierTypePriority, trace: types.TraceEnabled},
		{name: "bad_tier", tier: "gold", wantErr: true},
		{name: "bad_trace", trace: "VERBOSE", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestBedrockRuntimeSDKClient(t, newTestHandler(t))
			out, err := client.InvokeModel(t.Context(), &bedrockruntimesdk.InvokeModelInput{
				ModelId:     aws.String("amazon.titan-text-express-v1"),
				Body:        []byte(`{"inputText":"hi"}`),
				ContentType: aws.String("application/json"),
				ServiceTier: tc.tier,
				Trace:       tc.trace,
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.tier, out.ServiceTier)
		})
	}
}

func TestHandler_ApplyGuardrailRequiredMembers(t *testing.T) {
	t.Parallel()

	content := []map[string]any{{"text": map[string]any{"text": "hello"}}}
	cases := []struct {
		body map[string]any
		name string
		want int
	}{
		{name: "valid", body: map[string]any{"source": "INPUT", "content": content}, want: http.StatusOK},
		{name: "missing_source", body: map[string]any{"content": content}, want: http.StatusBadRequest},
		{name: "bad_source", body: map[string]any{"source": "BOTH", "content": content}, want: http.StatusBadRequest},
		{name: "missing_content", body: map[string]any{"source": "INPUT"}, want: http.StatusBadRequest},
		{
			name: "bad_output_scope",
			body: map[string]any{"source": "INPUT", "content": content, "outputScope": "ALL"},
			want: http.StatusBadRequest,
		},
		{
			name: "full_output_scope",
			body: map[string]any{"source": "OUTPUT", "content": content, "outputScope": "FULL"},
			want: http.StatusOK,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), http.MethodPost, "/guardrail/g1/version/DRAFT/apply", tc.body)
			assert.Equal(t, tc.want, rec.Code)
		})
	}
}

func TestRealClient_StartAsyncInvokeTokenReplay(t *testing.T) {
	t.Parallel()

	start := func(client *bedrockruntimesdk.Client, uri string) (*bedrockruntimesdk.StartAsyncInvokeOutput, error) {
		return client.StartAsyncInvoke(t.Context(), &bedrockruntimesdk.StartAsyncInvokeInput{
			ModelId:            aws.String("amazon.nova-reel-v1:0"),
			ModelInput:         document.NewLazyDocument(map[string]any{"prompt": "a cat"}),
			ClientRequestToken: aws.String("tok-1"),
			OutputDataConfig: &types.AsyncInvokeOutputDataConfigMemberS3OutputDataConfig{
				Value: types.AsyncInvokeS3OutputDataConfig{S3Uri: aws.String(uri)},
			},
		})
	}

	cases := []struct {
		name         string
		secondURI    string
		wantConflict bool
	}{
		{name: "same_params_replays", secondURI: "s3://bucket/a"},
		{name: "different_params_conflict", secondURI: "s3://bucket/b", wantConflict: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestBedrockRuntimeSDKClient(t, newTestHandler(t))

			first, err := start(client, "s3://bucket/a")
			require.NoError(t, err)

			second, err := start(client, tc.secondURI)
			if tc.wantConflict {
				var conflict *types.ConflictException
				require.ErrorAs(t, err, &conflict)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.InvocationArn), aws.ToString(second.InvocationArn))
		})
	}
}

func TestHandler_ListAsyncInvokesEnumQuery(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		query string
		want  int
	}{
		{name: "valid_sort_by", query: "?sortBy=SubmissionTime&sortOrder=Descending&statusEquals=Completed", want: 200},
		{name: "bad_sort_by", query: "?sortBy=Size", want: http.StatusBadRequest},
		{name: "bad_sort_order", query: "?sortOrder=Sideways", want: http.StatusBadRequest},
		{name: "bad_status", query: "?statusEquals=Paused", want: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), http.MethodGet, "/async-invoke"+tc.query, nil)
			assert.Equal(t, tc.want, rec.Code)
		})
	}
}
