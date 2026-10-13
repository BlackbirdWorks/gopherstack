package sns_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

const testSQSQueueARN = "arn:aws:sqs:us-east-1:000000000000:q"

func hasErrCode(err error, code string) bool {
	return err != nil && strings.Contains(err.Error(), code)
}

func TestMaximumMessageSize_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, c *snssdk.Client, arn string)
		name     string
		setValue string
		message  string
		wantSet  string
		wantPub  string
	}{
		{name: "default limit ok", message: strings.Repeat("a", 262144)},
		{name: "default limit exceeded", message: strings.Repeat("a", 262145), wantPub: "InvalidParameter"},
		{name: "set too small", setValue: "1023", wantSet: "InvalidParameter"},
		{name: "set too large", setValue: "1048577", wantSet: "InvalidParameter"},
		{name: "set not a number", setValue: "big", wantSet: "InvalidParameter"},
		{
			name:     "lowered limit exceeded",
			setValue: "2048",
			message:  strings.Repeat("a", 2049),
			wantPub:  "InvalidParameter",
		},
		{name: "lowered limit ok", setValue: "2048", message: strings.Repeat("a", 2048)},
		{name: "raised limit ok", setValue: "524288", message: strings.Repeat("a", 400000)},
		{
			name:     "raised limit exceeded",
			setValue: "524288",
			message:  strings.Repeat("a", 524289),
			wantPub:  "InvalidParameter",
		},
		{
			name: "raised with http subscription", setValue: "524288", wantSet: "InvalidParameter",
			setup: func(t *testing.T, c *snssdk.Client, arn string) {
				t.Helper()

				_, err := c.Subscribe(t.Context(), &snssdk.SubscribeInput{
					TopicArn: aws.String(
						arn,
					),
					Protocol: aws.String("http"),
					Endpoint: aws.String("http://127.0.0.1:9/x"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "raised with sqs subscription", setValue: "524288",
			setup: func(t *testing.T, c *snssdk.Client, arn string) {
				t.Helper()

				_, err := c.Subscribe(t.Context(), &snssdk.SubscribeInput{
					TopicArn: aws.String(arn), Protocol: aws.String("sqs"), Endpoint: aws.String(testSQSQueueARN),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealismClient(t)
			topic, err := c.CreateTopic(t.Context(), &snssdk.CreateTopicInput{Name: aws.String("big")})
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(t, c, aws.ToString(topic.TopicArn))
			}

			if tc.setValue != "" {
				_, err = c.SetTopicAttributes(t.Context(), &snssdk.SetTopicAttributesInput{
					TopicArn: topic.TopicArn, AttributeName: aws.String("MaximumMessageSize"),
					AttributeValue: aws.String(tc.setValue),
				})
				if tc.wantSet != "" {
					require.Error(t, err)
					assert.True(t, hasErrCode(err, tc.wantSet), err.Error())

					return
				}

				require.NoError(t, err)
			}

			if tc.message == "" {
				return
			}

			_, err = c.Publish(
				t.Context(),
				&snssdk.PublishInput{TopicArn: topic.TopicArn, Message: aws.String(tc.message)},
			)
			if tc.wantPub == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.True(t, hasErrCode(err, tc.wantPub), err.Error())
		})
	}
}

func TestMaximumMessageSize_SubscribeAndCreate_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createVal string
		protocol  string
		endpoint  string
		wantErr   bool
	}{
		{name: "create invalid", createVal: "10", wantErr: true},
		{
			name:      "http on large topic",
			createVal: "524288",
			protocol:  "http",
			endpoint:  "http://127.0.0.1:9/x",
			wantErr:   true,
		},
		{name: "sqs on large topic", createVal: "524288", protocol: "sqs", endpoint: testSQSQueueARN},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealismClient(t)
			topic, err := c.CreateTopic(t.Context(), &snssdk.CreateTopicInput{
				Name: aws.String("big"), Attributes: map[string]string{"MaximumMessageSize": tc.createVal},
			})

			if tc.protocol == "" {
				require.Error(t, err)
				assert.True(t, hasErrCode(err, "InvalidParameter"), err.Error())

				return
			}

			require.NoError(t, err)

			_, err = c.Subscribe(t.Context(), &snssdk.SubscribeInput{
				TopicArn: topic.TopicArn, Protocol: aws.String(tc.protocol), Endpoint: aws.String(tc.endpoint),
			})
			if tc.wantErr {
				require.Error(t, err)
				assert.True(t, hasErrCode(err, "InvalidParameter"), err.Error())

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestPublishBatch_SizeLimitIncludesAttributes_SDK(t *testing.T) {
	t.Parallel()

	attr := func(n int) map[string]types.MessageAttributeValue {
		return map[string]types.MessageAttributeValue{
			"a": {DataType: aws.String("String"), StringValue: aws.String(strings.Repeat("x", n))},
		}
	}

	tests := []struct {
		name    string
		bodyLen int
		attrLen int
		wantErr bool
	}{
		{name: "within limit", bodyLen: 100000, attrLen: 10000},
		{name: "attributes push over", bodyLen: 100000, attrLen: 70000, wantErr: true},
		{name: "bodies alone over", bodyLen: 140000, attrLen: 1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealismClient(t)
			topic, err := c.CreateTopic(t.Context(), &snssdk.CreateTopicInput{Name: aws.String("batch")})
			require.NoError(t, err)

			entries := make([]types.PublishBatchRequestEntry, 0, 2)
			for _, id := range []string{"1", "2"} {
				entries = append(entries, types.PublishBatchRequestEntry{
					Id: aws.String(id), Message: aws.String(strings.Repeat("m", tc.bodyLen)),
					MessageAttributes: attr(tc.attrLen),
				})
			}

			_, err = c.PublishBatch(t.Context(), &snssdk.PublishBatchInput{
				TopicArn: topic.TopicArn, PublishBatchRequestEntries: entries,
			})
			if !tc.wantErr {
				require.NoError(t, err)

				return
			}

			var tooLong *types.BatchRequestTooLongException
			require.ErrorAs(t, err, &tooLong)
		})
	}
}

func TestSubscribe_EndpointScreening_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		protocol string
		endpoint string
		allow    bool
		wantErr  bool
	}{
		{name: "metadata v4", protocol: "http", endpoint: "http://169.254.169.254/latest", wantErr: true},
		{name: "ecs metadata", protocol: "http", endpoint: "http://169.254.170.2/v2", wantErr: true},
		{name: "link local v6", protocol: "https", endpoint: "https://[fe80::1]/x", wantErr: true},
		{name: "ec2 metadata v6", protocol: "http", endpoint: "http://[fd00:ec2::254]/x", wantErr: true},
		{name: "mapped v4", protocol: "http", endpoint: "http://[::ffff:169.254.169.254]/x", wantErr: true},
		{name: "scheme mismatch", protocol: "http", endpoint: "ftp://example.com/x", wantErr: true},
		{name: "no host", protocol: "https", endpoint: "https:///x", wantErr: true},
		{name: "loopback allowed", protocol: "http", endpoint: "http://127.0.0.1:9/x"},
		{name: "opt out allows metadata", protocol: "http", endpoint: "http://169.254.169.254/x", allow: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			backend.SetAllowLinkLocalDelivery(tc.allow)
			c := newTestSNSClient(t, sns.NewHandler(backend))

			topic, err := c.CreateTopic(t.Context(), &snssdk.CreateTopicInput{Name: aws.String("screen")})
			require.NoError(t, err)

			_, err = c.Subscribe(t.Context(), &snssdk.SubscribeInput{
				TopicArn: topic.TopicArn, Protocol: aws.String(tc.protocol), Endpoint: aws.String(tc.endpoint),
			})
			if !tc.wantErr {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.True(t, hasErrCode(err, "InvalidParameter"), err.Error())
		})
	}
}
