package sns_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func newRealismClient(t *testing.T) *snssdk.Client {
	t.Helper()

	return newTestSNSClient(t, sns.NewHandler(sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")))
}

func tagList(n int) []types.Tag {
	tags := make([]types.Tag, 0, n)
	for i := range n {
		tags = append(tags, types.Tag{Key: aws.String(fmt.Sprintf("k%d", i)), Value: aws.String("v")})
	}

	return tags
}

func TestCreateTopic_ExistingWithDifferentAttributes_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs   map[string]string
		name    string
		wantErr bool
	}{
		{name: "no attrs", attrs: nil},
		{name: "same attrs", attrs: map[string]string{"DisplayName": "one"}},
		{name: "different attrs", attrs: map[string]string{"DisplayName": "two"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealismClient(t)
			first, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{
				Name: aws.String("dup"), Attributes: map[string]string{"DisplayName": "one"},
			})
			require.NoError(t, err)

			second, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{
				Name: aws.String("dup"), Attributes: tc.attrs,
			})
			if tc.wantErr {
				var target *types.InvalidParameterException
				require.ErrorAs(t, err, &target)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.TopicArn), aws.ToString(second.TopicArn))
		})
	}
}

func TestTagResource_Errors_SDK(t *testing.T) {
	t.Parallel()

	const missing = "arn:aws:sns:us-east-1:000000000000:missing"

	tests := []struct {
		want any
		run  func(t *testing.T, client *snssdk.Client, topicArn string) error
		name string
	}{
		{
			name: "tag missing topic",
			want: new(*types.ResourceNotFoundException),
			run: func(t *testing.T, client *snssdk.Client, _ string) error {
				t.Helper()
				_, err := client.TagResource(t.Context(), &snssdk.TagResourceInput{
					ResourceArn: aws.String(missing), Tags: tagList(1),
				})

				return err
			},
		},
		{
			name: "untag missing topic",
			want: new(*types.ResourceNotFoundException),
			run: func(t *testing.T, client *snssdk.Client, _ string) error {
				t.Helper()
				_, err := client.UntagResource(t.Context(), &snssdk.UntagResourceInput{
					ResourceArn: aws.String(missing), TagKeys: []string{"a"},
				})

				return err
			},
		},
		{
			name: "list missing topic",
			want: new(*types.ResourceNotFoundException),
			run: func(t *testing.T, client *snssdk.Client, _ string) error {
				t.Helper()
				_, err := client.ListTagsForResource(t.Context(), &snssdk.ListTagsForResourceInput{
					ResourceArn: aws.String(missing),
				})

				return err
			},
		},
		{
			name: "over 50 tags",
			want: new(*types.TagLimitExceededException),
			run: func(t *testing.T, client *snssdk.Client, topicArn string) error {
				t.Helper()
				_, err := client.TagResource(t.Context(), &snssdk.TagResourceInput{
					ResourceArn: aws.String(topicArn), Tags: tagList(51),
				})

				return err
			},
		},
		{
			name: "incremental over 50",
			want: new(*types.TagLimitExceededException),
			run: func(t *testing.T, client *snssdk.Client, topicArn string) error {
				t.Helper()
				_, err := client.TagResource(t.Context(), &snssdk.TagResourceInput{
					ResourceArn: aws.String(topicArn), Tags: tagList(50),
				})
				require.NoError(t, err)

				_, err = client.TagResource(t.Context(), &snssdk.TagResourceInput{
					ResourceArn: aws.String(topicArn),
					Tags:        []types.Tag{{Key: aws.String("extra"), Value: aws.String("v")}},
				})

				return err
			},
		},
		{
			name: "create with over 50 tags",
			want: new(*types.TagLimitExceededException),
			run: func(t *testing.T, client *snssdk.Client, _ string) error {
				t.Helper()
				_, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{
					Name: aws.String("toomany"), Tags: tagList(51),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealismClient(t)
			topic, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{Name: aws.String("tagged")})
			require.NoError(t, err)

			err = tc.run(t, client, aws.ToString(topic.TopicArn))
			require.Error(t, err)

			switch target := tc.want.(type) {
			case **types.ResourceNotFoundException:
				require.ErrorAs(t, err, target)
			case **types.TagLimitExceededException:
				require.ErrorAs(t, err, target)
			}
		})
	}
}

func TestSubscribe_HTTPConfirmationFlow_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		viaURL   bool
		badToken bool
	}{
		{name: "confirm with token"},
		{name: "confirm via subscribe url", viaURL: true},
		{name: "wrong token", badToken: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			confirmations := make(chan map[string]string, 1)
			endpoint := newConfirmationEndpoint(confirmations)
			t.Cleanup(endpoint.Close)

			client := newRealismClient(t)
			topic, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{Name: aws.String("flow")})
			require.NoError(t, err)

			sub, err := client.Subscribe(t.Context(), &snssdk.SubscribeInput{
				TopicArn: topic.TopicArn, Protocol: aws.String("http"), Endpoint: aws.String(endpoint.URL),
				ReturnSubscriptionArn: true,
			})
			require.NoError(t, err)

			msg := <-confirmations
			assert.Equal(t, "SubscriptionConfirmation", msg["Type"])
			assert.Equal(t, aws.ToString(topic.TopicArn), msg["TopicArn"])
			assert.NotEmpty(t, msg["Token"])
			assert.NotEmpty(t, msg["Signature"])
			assert.Contains(t, msg["SubscribeURL"], "Action=ConfirmSubscription")

			switch {
			case tc.badToken:
				_, err = client.ConfirmSubscription(t.Context(), &snssdk.ConfirmSubscriptionInput{
					TopicArn: topic.TopicArn, Token: aws.String("bogus"),
				})
				var target *types.InvalidParameterException
				require.ErrorAs(t, err, &target)

				return
			case tc.viaURL:
				req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, msg["SubscribeURL"], nil)
				require.NoError(t, reqErr)

				resp, getErr := http.DefaultClient.Do(req)
				require.NoError(t, getErr)
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
				assert.Contains(t, string(body), aws.ToString(sub.SubscriptionArn))
			default:
				out, cErr := client.ConfirmSubscription(t.Context(), &snssdk.ConfirmSubscriptionInput{
					TopicArn: topic.TopicArn, Token: aws.String(msg["Token"]),
				})
				require.NoError(t, cErr)
				assert.Equal(t, aws.ToString(sub.SubscriptionArn), aws.ToString(out.SubscriptionArn))
			}

			attrs, err := client.GetSubscriptionAttributes(t.Context(), &snssdk.GetSubscriptionAttributesInput{
				SubscriptionArn: sub.SubscriptionArn,
			})
			require.NoError(t, err)
			assert.Equal(t, "false", attrs.Attributes["PendingConfirmation"])
		})
	}
}

func TestSubscribe_InvalidAttributeRollsBack_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs map[string]string
		name  string
	}{
		{name: "unknown attribute", attrs: map[string]string{"Bogus": "x"}},
		{name: "bad redrive policy", attrs: map[string]string{"RedrivePolicy": "{"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRealismClient(t)
			topic, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{Name: aws.String("attr")})
			require.NoError(t, err)

			_, err = client.Subscribe(t.Context(), &snssdk.SubscribeInput{
				TopicArn: topic.TopicArn, Protocol: aws.String("sqs"),
				Endpoint:   aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
				Attributes: tc.attrs,
			})
			var target *types.InvalidParameterException
			require.ErrorAs(t, err, &target)

			list, err := client.ListSubscriptionsByTopic(t.Context(), &snssdk.ListSubscriptionsByTopicInput{
				TopicArn: topic.TopicArn,
			})
			require.NoError(t, err)
			assert.Empty(t, list.Subscriptions)
		})
	}
}

func newConfirmationEndpoint(out chan<- map[string]string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		var m map[string]string
		if json.Unmarshal(body, &m) == nil && m["Type"] == "SubscriptionConfirmation" {
			select {
			case out <- m:
			default:
			}
		}

		w.WriteHeader(http.StatusOK)
	}))
}
