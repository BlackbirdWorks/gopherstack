package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func devEndpointsServer(t *testing.T) (*httptest.Server, aws.Config) {
	t.Helper()

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(&service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
	)
	require.NoError(t, err)

	return srv, cfg
}

func devDo(t *testing.T, method, url, accept string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, http.NoBody)
	require.NoError(t, err)

	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, b
}

func sendSESMail(ctx context.Context, t *testing.T, cfg aws.Config, from string) string {
	t.Helper()

	c := ses.NewFromConfig(cfg)
	_, err := c.VerifyEmailIdentity(ctx, &ses.VerifyEmailIdentityInput{EmailAddress: aws.String(from)})
	require.NoError(t, err)

	out, err := c.SendEmail(ctx, &ses.SendEmailInput{
		Source: aws.String(from),
		Destination: &sestypes.Destination{
			ToAddresses: []string{"to@example.com"},
			CcAddresses: []string{"cc@example.com"},
		},
		Message: &sestypes.Message{
			Subject: &sestypes.Content{Data: aws.String("hello")},
			Body:    &sestypes.Body{Text: &sestypes.Content{Data: aws.String("plain")}},
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.MessageId)
}

func TestSESRetrospectionEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query  func(id string) string
		name   string
		wantN  int
		delete bool
	}{
		{name: "all", query: func(string) string { return "" }, wantN: 1},
		{name: "by id", query: func(id string) string { return "?id=" + id }, wantN: 1},
		{name: "by email", query: func(string) string { return "?email=sender@example.com" }, wantN: 1},
		{name: "unmatched email", query: func(string) string { return "?email=other@example.com" }, wantN: 0},
		{name: "delete by id", query: func(id string) string { return "?id=" + id }, wantN: 0, delete: true},
		{name: "delete all", query: func(string) string { return "" }, wantN: 0, delete: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, cfg := devEndpointsServer(t)
			id := sendSESMail(t.Context(), t, cfg, "sender@example.com")

			if tt.delete {
				code, _ := devDo(t, http.MethodDelete, srv.URL+"/_aws/ses"+tt.query(id), "")
				require.Equal(t, http.StatusNoContent, code)
			}

			code, body := devDo(t, http.MethodGet, srv.URL+"/_aws/ses"+func() string {
				if tt.delete {
					return ""
				}

				return tt.query(id)
			}(), "")
			require.Equal(t, http.StatusOK, code)

			var got struct {
				Messages []map[string]any `json:"messages"`
			}
			require.NoError(t, json.Unmarshal(body, &got))
			require.Len(t, got.Messages, tt.wantN)

			if tt.wantN == 0 {
				return
			}

			m := got.Messages[0]
			assert.Equal(t, id, m["Id"])
			assert.Equal(t, "us-east-1", m["Region"])
			assert.Equal(t, "sender@example.com", m["Source"])
			assert.Equal(t, "hello", m["Subject"])
			assert.Equal(t, map[string]any{"text_part": "plain", "html_part": nil}, m["Body"])
			assert.Equal(t, map[string]any{
				"ToAddresses": []any{"to@example.com"}, "CcAddresses": []any{"cc@example.com"}, "BccAddresses": []any{},
			}, m["Destination"])

			_, err := time.Parse(time.RFC3339Nano, m["Timestamp"].(string))
			require.NoError(t, err)
		})
	}
}

func TestSQSInspectEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path    func(queueURL string) string
		name    string
		accept  string
		wantErr bool
	}{
		{name: "query xml", path: func(u string) string { return "/_aws/sqs/messages?QueueUrl=" + u }},
		{
			name:   "query json",
			accept: "application/json",
			path:   func(u string) string { return "/_aws/sqs/messages?QueueUrl=" + u },
		},
		{name: "path form", path: func(string) string { return "/_aws/sqs/messages/us-east-1/000000000000/insp-q" }},
		{
			name:    "missing queue",
			wantErr: true,
			path:    func(string) string { return "/_aws/sqs/messages/us-east-1/000000000000/nope" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, cfg := devEndpointsServer(t)
			c := sqs.NewFromConfig(cfg)

			q, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("insp-q")})
			require.NoError(t, err)

			_, err = c.SendMessage(t.Context(), &sqs.SendMessageInput{
				QueueUrl: q.QueueUrl, MessageBody: aws.String("payload"),
				MessageAttributes: map[string]sqstypes.MessageAttributeValue{
					"k": {DataType: aws.String("String"), StringValue: aws.String("v")},
				},
			})
			require.NoError(t, err)

			code, body := devDo(t, http.MethodGet, srv.URL+tt.path(aws.ToString(q.QueueUrl)), tt.accept)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, code)
				assert.Contains(t, string(body), "NonExistentQueue")

				return
			}

			require.Equal(t, http.StatusOK, code)

			if tt.accept == "application/json" {
				var got struct {
					Messages []struct {
						Attributes map[string]string `json:"Attributes"`
						Body       string            `json:"Body"`
					} `json:"Messages"`
				}
				require.NoError(t, json.Unmarshal(body, &got))
				require.Len(t, got.Messages, 1)
				assert.Equal(t, "payload", got.Messages[0].Body)
				assert.Equal(t, "0", got.Messages[0].Attributes["ApproximateReceiveCount"])
				assert.NotEmpty(t, got.Messages[0].Attributes["SentTimestamp"])
			} else {
				var got ReceiveXML
				require.NoError(t, xml.Unmarshal(body, &got))
				require.Len(t, got.Messages, 1)
				assert.Equal(t, "payload", got.Messages[0].Body)
				assert.Contains(t, string(body), "ApproximateReceiveCount")
			}

			for range 3 {
				devDo(t, http.MethodGet, srv.URL+tt.path(aws.ToString(q.QueueUrl)), tt.accept)
			}

			// Inspection must not hide the message nor bump its receive count.
			rm, err := c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
				QueueUrl:                    q.QueueUrl,
				MaxNumberOfMessages:         1,
				MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"All"},
			})
			require.NoError(t, err)
			require.Len(t, rm.Messages, 1)
			assert.Equal(t, "1", rm.Messages[0].Attributes["ApproximateReceiveCount"])

			code, body = devDo(t, http.MethodGet, srv.URL+tt.path(aws.ToString(q.QueueUrl)), "application/json")
			require.Equal(t, http.StatusOK, code)
			assert.JSONEq(t, "{}", string(body), "in-flight message hidden without ShowInvisible")

			code, body = devDo(t, http.MethodGet, srv.URL+tt.path(aws.ToString(q.QueueUrl))+
				func() string {
					if strings.Contains(tt.path(""), "?") {
						return "&ShowInvisible=true"
					}

					return "?ShowInvisible=true"
				}(), "application/json")
			require.Equal(t, http.StatusOK, code)
			assert.Contains(t, string(body), `"ApproximateReceiveCount":"1"`)
		})
	}
}

type ReceiveXML struct {
	XMLName  xml.Name `xml:"ReceiveMessageResponse"`
	Messages []struct {
		Body string `xml:"Body"`
	} `xml:"ReceiveMessageResult>Message"`
}

func TestCloudWatchRawMetricsEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDim any
		name    string
		dims    []cwtypes.Dimension
	}{
		{name: "no dims", wantDim: nil},
		{
			name: "dims",
			dims: []cwtypes.Dimension{
				{Name: aws.String("B"), Value: aws.String("2")}, {Name: aws.String("A"), Value: aws.String("1")},
			},
			wantDim: "A=1\tB=2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := time.Now().Add(-time.Minute).Truncate(time.Second)
			srv, cfg := devEndpointsServer(t)
			_, err := cloudwatch.NewFromConfig(cfg).PutMetricData(t.Context(), &cloudwatch.PutMetricDataInput{
				Namespace: aws.String("Test/NS"),
				MetricData: []cwtypes.MetricDatum{{
					MetricName: aws.String("m1"), Value: aws.Float64(42), Dimensions: tt.dims,
					Timestamp: aws.Time(ts),
				}},
			})
			require.NoError(t, err)

			code, body := devDo(t, http.MethodGet, srv.URL+"/_aws/cloudwatch/metrics/raw", "")
			require.Equal(t, http.StatusOK, code)

			var got struct {
				Metrics []map[string]any `json:"metrics"`
			}
			require.NoError(t, json.Unmarshal(body, &got))

			var found map[string]any

			for _, m := range got.Metrics {
				if m["ns"] == "Test/NS" {
					found = m
				}
			}

			require.NotNil(t, found)
			assert.Equal(t, "m1", found["n"])
			assert.InDelta(t, 42, found["v"], 0)
			assert.InDelta(t, ts.Unix(), found["t"], 0)
			assert.Equal(t, tt.wantDim, found["d"])
			assert.Equal(t, "000000000000", found["account"])
			assert.Equal(t, "us-east-1", found["region"])
		})
	}
}

func TestLocalstackStateResetAlias(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, path string }{
		{name: "localstack alias", path: "/_localstack/state/reset"},
		{name: "gopherstack native", path: "/_gopherstack/reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, cfg := devEndpointsServer(t)
			sendSESMail(t.Context(), t, cfg, "sender@example.com")

			code, _ := devDo(t, http.MethodPost, srv.URL+tt.path, "")
			require.Equal(t, http.StatusOK, code)

			_, body := devDo(t, http.MethodGet, srv.URL+"/_aws/ses", "")
			assert.JSONEq(t, `{"messages":[]}`, string(body))
		})
	}
}
