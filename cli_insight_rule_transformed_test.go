package main

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestInitializeServices_InsightRuleTransformedLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		message     string
		wantKeys    [][]string
		transform   bool
		transformed bool
	}{
		{name: "original-events-have-no-key", message: `{"user":"a"}`, transform: true, wantKeys: [][]string{}},
		{
			name:        "transformed-events-gain-key",
			message:     `{"user":"a"}`,
			transform:   true,
			transformed: true,
			wantKeys:    [][]string{{"emu"}},
		},
		{
			name:        "group-without-transformer-unchanged",
			message:     `{"team":"x"}`,
			transformed: true,
			wantKeys:    [][]string{{"x"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			services, err := initializeServices(newTestAppContext(t, 20000, 20100))
			require.NoError(t, err)

			byName := serviceByName(services)

			logsH, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler)
			require.True(t, ok)

			logs, ok := logsH.Backend.(*cwlogsbackend.InMemoryBackend)
			require.True(t, ok)

			cwH, ok := byName["CloudWatch"].(*cwbackend.Handler)
			require.True(t, ok)

			ctx := t.Context()
			start := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Minute)

			_, err = logs.CreateLogGroup(ctx, "app-1", "", "")
			require.NoError(t, err)
			_, err = logs.CreateLogStream(ctx, "app-1", "s1")
			require.NoError(t, err)

			if tt.transform {
				require.NoError(t, logs.PutTransformer(ctx, "app-1", []map[string]any{
					{"addKeys": map[string]any{"entries": []any{map[string]any{"key": "team", "value": "emu"}}}},
				}))
			}

			_, err = logs.PutLogEvents(ctx, "app-1", "s1", "", []cwlogsbackend.InputLogEvent{
				{Message: tt.message, Timestamp: start.UnixMilli()},
			})
			require.NoError(t, err)

			e := echo.New()
			registry := service.NewRegistry()
			require.NoError(t, registry.Register(cwH))
			e.Use(service.NewServiceRouter(registry).RouteHandler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			cfg, err := awscfg.LoadDefaultConfig(
				ctx,
				awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
			)
			require.NoError(t, err)

			client := cwsdk.NewFromConfig(cfg, func(o *cwsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

			_, err = client.PutInsightRule(ctx, &cwsdk.PutInsightRuleInput{
				RuleName: aws.String("r"),
				RuleDefinition: aws.String(
					`{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogGroupNames":["app-*"],` +
						`"LogFormat":"JSON","Contribution":{"Keys":["$.team"]},"AggregateOn":"Count"}`,
				),
				ApplyOnTransformedLogs: aws.Bool(tt.transformed),
			})
			require.NoError(t, err)

			out, err := client.GetInsightRuleReport(ctx, &cwsdk.GetInsightRuleReportInput{
				RuleName:  aws.String("r"),
				StartTime: aws.Time(start),
				EndTime:   aws.Time(start.Add(10 * time.Minute)),
				Period:    aws.Int32(60),
			})
			require.NoError(t, err)

			keys := make([][]string, 0, len(out.Contributors))
			for _, c := range out.Contributors {
				keys = append(keys, c.Keys)
			}

			assert.Equal(t, tt.wantKeys, keys)
		})
	}
}
