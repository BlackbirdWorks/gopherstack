package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// iotRequest is a helper that issues an HTTP request to the IoT control-plane.
func iotRequest(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()

	var reqBody io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, endpoint+path, reqBody)
	require.NoError(t, err)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

// connectMQTT connects a paho MQTT client to the shared broker and returns it.
func connectMQTT(t *testing.T, clientID string) pahomqtt.Client {
	t.Helper()

	opts := pahomqtt.NewClientOptions().
		AddBroker(mqttEndpoint).
		SetClientID(clientID).
		SetConnectTimeout(5 * time.Second)

	client := pahomqtt.NewClient(opts)

	token := client.Connect()
	require.True(t, token.WaitTimeout(5*time.Second), "mqtt connect timeout")
	require.NoError(t, token.Error(), "mqtt connect error")

	return client
}

//nolint:tparallel // steps are sequential and order-dependent; inner subtests cannot be parallel
func TestIntegration_IoT_ThingLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	thingName := "test-thing-" + uuid.NewString()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_thing",
			run: func(t *testing.T) {
				t.Helper()

				resp := iotRequest(t, http.MethodPost, "/things/"+thingName, map[string]any{
					"thingTypeName": "SensorType",
					"attributePayload": map[string]any{
						"attributes": map[string]string{"location": "lab"},
					},
				})
				defer resp.Body.Close()

				assert.Equal(t, http.StatusOK, resp.StatusCode)

				var out map[string]string
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
				assert.Equal(t, thingName, out["thingName"])
				assert.NotEmpty(t, out["thingArn"])
				assert.NotEmpty(t, out["thingId"])
			},
		},
		{
			name: "describe_thing",
			run: func(t *testing.T) {
				t.Helper()

				resp := iotRequest(t, http.MethodGet, "/things/"+thingName, nil)
				defer resp.Body.Close()

				assert.Equal(t, http.StatusOK, resp.StatusCode)

				var out map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
				assert.Equal(t, thingName, out["thingName"])
			},
		},
		{
			name: "delete_thing",
			run: func(t *testing.T) {
				t.Helper()

				resp := iotRequest(t, http.MethodDelete, "/things/"+thingName, nil)
				defer resp.Body.Close()

				assert.Equal(t, http.StatusNoContent, resp.StatusCode)
			},
		},
		{
			name: "describe_after_delete_returns_404",
			run: func(t *testing.T) {
				t.Helper()

				resp := iotRequest(t, http.MethodGet, "/things/"+thingName, nil)
				defer resp.Body.Close()

				assert.Equal(t, http.StatusNotFound, resp.StatusCode)
			},
		},
	}

	// Execute sequentially — each step depends on the previous.
	for _, tt := range tests { //nolint:paralleltest // steps are sequential, each depends on prior state
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

//nolint:tparallel // steps are sequential and order-dependent; inner subtests cannot be parallel
func TestIntegration_IoT_PolicyAndRule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	policyName := "test-policy-" + uuid.NewString()
	ruleName := "test-rule-" + uuid.NewString()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_policy",
			run: func(t *testing.T) {
				t.Helper()

				resp := iotRequest(t, http.MethodPost, "/policies/"+policyName, map[string]any{
					"policyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`,
				})
				defer resp.Body.Close()

				assert.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "create_rule",
			run: func(t *testing.T) {
				t.Helper()

				resp := iotRequest(t, http.MethodPost, "/rules/"+ruleName, map[string]any{
					"sql":         "SELECT * FROM 'sensor/#'",
					"description": "forward all sensor readings",
					"actions":     []any{},
				})
				defer resp.Body.Close()

				assert.Equal(t, http.StatusOK, resp.StatusCode)
			},
		},
		{
			name: "get_rule",
			run: func(t *testing.T) {
				t.Helper()

				resp := iotRequest(t, http.MethodGet, "/rules/"+ruleName, nil)
				defer resp.Body.Close()

				assert.Equal(t, http.StatusOK, resp.StatusCode)

				var out map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
				assert.Equal(t, ruleName, out["ruleName"])
			},
		},
		{
			name: "delete_rule",
			run: func(t *testing.T) {
				t.Helper()

				resp := iotRequest(t, http.MethodDelete, "/rules/"+ruleName, nil)
				defer resp.Body.Close()

				assert.Equal(t, http.StatusNoContent, resp.StatusCode)
			},
		},
	}

	for _, tt := range tests { //nolint:paralleltest // steps are sequential, each depends on prior state
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func TestIntegration_IoT_DescribeEndpoint(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name         string
		endpointType string
	}{
		{name: "data_ats", endpointType: "iot:Data-ATS"},
		{name: "data", endpointType: "iot:Data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := iotRequest(t, http.MethodGet, "/endpoint?endpointType="+tt.endpointType, nil)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]string
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			assert.NotEmpty(t, out["endpointAddress"])
		})
	}
}

func TestIntegration_IoT_MQTTPublishAndSubscribe(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	if mqttEndpoint == "" {
		t.Skip("mqttEndpoint not configured")
	}

	topic := "sensor/temperature/" + uuid.NewString()
	received := make(chan []byte, 1)

	subscriber := connectMQTT(t, "test-sub-"+uuid.NewString())
	defer subscriber.Disconnect(250)

	subToken := subscriber.Subscribe(topic, 0, func(_ pahomqtt.Client, msg pahomqtt.Message) {
		select {
		case received <- msg.Payload():
		default:
		}
	})
	require.True(t, subToken.WaitTimeout(5*time.Second), "subscribe timeout")
	require.NoError(t, subToken.Error())

	publisher := connectMQTT(t, "test-pub-"+uuid.NewString())
	defer publisher.Disconnect(250)

	payload := `{"temperature": 42}`
	pubToken := publisher.Publish(topic, 0, false, payload)
	require.True(t, pubToken.WaitTimeout(5*time.Second), "publish timeout")
	require.NoError(t, pubToken.Error())

	tests := []struct {
		name    string
		want    string
		timeout time.Duration
	}{
		{name: "receive_message", want: payload, timeout: 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			select {
			case msg := <-received:
				assert.Equal(t, tt.want, string(msg))
			case <-time.After(tt.timeout):
				require.Fail(t, fmt.Sprintf("did not receive MQTT message within %s", tt.timeout))
			}
		})
	}
}

func TestIntegration_IoT_DataPlane_Publish(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	if mqttEndpoint == "" {
		t.Skip("mqttEndpoint not configured")
	}

	topic := "device/status/" + uuid.NewString()
	received := make(chan []byte, 1)

	subscriber := connectMQTT(t, "test-dp-sub-"+uuid.NewString())
	defer subscriber.Disconnect(250)

	subToken := subscriber.Subscribe(topic, 0, func(_ pahomqtt.Client, msg pahomqtt.Message) {
		select {
		case received <- msg.Payload():
		default:
		}
	})
	require.True(t, subToken.WaitTimeout(5*time.Second), "subscribe timeout")
	require.NoError(t, subToken.Error())

	tests := []struct {
		name    string
		payload string
	}{
		{name: "publish_via_data_plane", payload: `{"status":"online"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := iotRequest(t, http.MethodPost, "/topics/"+topic, map[string]any{
				"payload": tt.payload,
			})
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			select {
			case msg := <-received:
				assert.NotEmpty(t, msg)
			case <-time.After(5 * time.Second):
				require.Fail(t, "did not receive MQTT message via data plane")
			}
		})
	}
}

func TestIntegration_IoT_Rule_ForwardsToSQS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	if mqttEndpoint == "" {
		t.Skip("mqttEndpoint not configured")
	}

	// Create an SQS queue to receive rule-forwarded messages.
	sqsClient := createSQSClient(t)
	queueName := "iot-rule-queue-" + uuid.NewString()
	createOut, err := sqsClient.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: &queueName,
	})
	require.NoError(t, err)
	queueURL := *createOut.QueueUrl

	t.Cleanup(func() {
		_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: &queueURL})
	})

	// Create an IoT rule that forwards matching messages to the SQS queue.
	ruleName := "fwd-rule-" + uuid.NewString()
	topic := "sensor/temp/" + uuid.NewString()

	//nolint:unqueryvet // IoT SQL rules use SELECT *; not a database query
	ruleSQL := "SELECT * FROM '" + topic + "' WHERE temperature > 50"

	resp := iotRequest(t, http.MethodPost, "/rules/"+ruleName, map[string]any{
		"sql":         ruleSQL,
		"description": "forward high-temp readings to SQS",
		"actions": []map[string]any{
			{"sqs": map[string]any{"queueUrl": queueURL, "roleArn": "arn:aws:iam::000000000000:role/IoTRule"}},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Publish a matching message via MQTT.
	publisher := connectMQTT(t, "test-rule-pub-"+uuid.NewString())
	defer publisher.Disconnect(250)

	matchingPayload := `{"temperature": 75}`
	pubToken := publisher.Publish(topic, 0, false, matchingPayload)
	require.True(t, pubToken.WaitTimeout(5*time.Second), "publish timeout")
	require.NoError(t, pubToken.Error())

	tests := []struct {
		name string
	}{
		{name: "message_forwarded_to_sqs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Poll SQS for the forwarded message.
			var receivedBody string

			deadline := time.Now().Add(10 * time.Second)

			for time.Now().Before(deadline) {
				msgOut, receiveErr := sqsClient.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
					QueueUrl:            &queueURL,
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     2,
				})

				if receiveErr != nil || len(msgOut.Messages) == 0 {
					time.Sleep(500 * time.Millisecond)

					continue
				}

				receivedBody = *msgOut.Messages[0].Body

				break
			}

			assert.NotEmpty(t, receivedBody, "expected SQS message forwarded by IoT rule")
			assert.Equal(t, matchingPayload, receivedBody)
		})
	}
}
