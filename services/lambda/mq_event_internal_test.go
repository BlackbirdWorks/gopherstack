package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	mqTestBrokerARN  = "arn:aws:mq:us-east-2:111122223333:broker:pizzaBroker:b-9bcfa592-423a-4942-879d-eb284b418fc8"
	mqTestSecretARN  = "arn:aws:secretsmanager:us-east-1:000000000000:secret:mq-creds-AbCdEf"
	mqTestSecretJSON = `{"username":"admin","password":"s3cretpassword"}`
)

func TestMQEventPayloadShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		vhost  string
		want   string
		msgs   []mqMessage
	}{
		{
			name:   "activemq follows the documented aws:mq event",
			source: mqEventSourceActiveMQ,
			msgs: []mqMessage{{
				MessageID: "ID:b-1:1:1:1:1", MessageType: "jms/text-message", DeliveryMode: 1, Expiration: "60000",
				Priority: 1, CorrelationID: "myJMSCoID", Queue: "testQueue", Body: []byte("ABC:AAAA"),
				Timestamp: time.UnixMilli(1598827811958), DeliveredAt: time.UnixMilli(1598827811959),
				Properties: map[string]string{"index": "1", "myCustomProperty": "value"},
			}},
			want: `{"eventSource":"aws:mq","eventSourceArn":"` + mqTestBrokerARN + `","messages":[{
				"messageID":"ID:b-1:1:1:1:1","messageType":"jms/text-message","deliveryMode":1,"replyTo":null,
				"type":null,"expiration":"60000","priority":1,"correlationId":"myJMSCoID","redelivered":false,
				"destination":{"physicalName":"testQueue"},"data":"QUJDOkFBQUE=","timestamp":1598827811958,
				"brokerInTime":1598827811958,"brokerOutTime":1598827811959,
				"properties":{"index":"1","myCustomProperty":"value"}}]}`,
		},
		{
			name:   "rabbitmq follows the documented aws:rmq event",
			source: mqEventSourceRabbitMQ,
			vhost:  "/",
			msgs: []mqMessage{{
				Queue: "pizzaQueue", ContentType: "text/plain", DeliveryMode: 1, Priority: 34, Expiration: "60000",
				UserID: "AIDACKCEVSQ6C2EXAMPLE", Timestamp: time.Unix(2021, 0),
				Headers: map[string]any{"header1": "value1", "numberInHeader": int32(10)},
				Body:    []byte(`{"timeout":0,"data":"CZrmf0Gw8Ov4bqLQxD4E"}`),
			}},
			want: `{"eventSource":"aws:rmq","eventSourceArn":"` + mqTestBrokerARN + `","rmqMessagesByQueue":{
				"pizzaQueue::/":[{"basicProperties":{"contentType":"text/plain","contentEncoding":null,
				"headers":{"header1":{"bytes":[118,97,108,117,101,49]},"numberInHeader":10},"deliveryMode":1,
				"priority":34,"correlationId":null,"replyTo":null,"expiration":"60000","messageId":null,
				"timestamp":"Jan 1, 1970, 12:33:41 AM","type":null,"userId":"AIDACKCEVSQ6C2EXAMPLE","appId":null,
				"clusterId":null,"bodySize":43},"redelivered":false,
				"data":"eyJ0aW1lb3V0IjowLCJkYXRhIjoiQ1pybWYwR3c4T3Y0YnFMUXhENEUifQ=="}]}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := buildMQEventPayload(tt.source, mqTestBrokerARN, tt.vhost, tt.msgs)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(raw))
		})
	}
}

var errMQSecretNotFound = errors.New("not found")

type fakeMQSources struct {
	endpoints map[string][2]string
	secrets   map[string]string
	mu        sync.Mutex
}

func (f *fakeMQSources) MQConsumerEndpoint(arn string) (string, string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.endpoints[arn]

	return e[0], e[1], ok
}

func (f *fakeMQSources) SecretString(_ context.Context, arn string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if s, ok := f.secrets[arn]; ok {
		return s, nil
	}

	return "", errMQSecretNotFound
}

func mqInput(queue string, cfg SourceAccessConfiguration) CreateEventSourceMappingInput {
	return CreateEventSourceMappingInput{
		Queues:                     []string{queue},
		SourceAccessConfigurations: []SourceAccessConfiguration{cfg},
	}
}

func TestMQSpecResolution(t *testing.T) {
	t.Parallel()

	basic := SourceAccessConfiguration{Type: "BASIC_AUTH", URI: mqTestSecretARN}

	tests := []struct {
		endpoints  map[string][2]string
		secrets    map[string]string
		name       string
		wantReason string
		wantSource string
		wantVHost  string
		wantQueue  string
		in         CreateEventSourceMappingInput
	}{
		{
			name:      "rabbitmq with virtual host",
			endpoints: map[string][2]string{mqTestBrokerARN: {"RABBITMQ", "127.0.0.1:5672"}},
			secrets:   map[string]string{mqTestSecretARN: mqTestSecretJSON},
			in: CreateEventSourceMappingInput{
				Queues:                     []string{"pizza"},
				SourceAccessConfigurations: []SourceAccessConfiguration{basic, {Type: "VIRTUAL_HOST", URI: "orders"}},
			},
			wantSource: "aws:rmq", wantVHost: "orders", wantQueue: "pizza",
		},
		{
			name:      "activemq defaults the virtual host",
			endpoints: map[string][2]string{mqTestBrokerARN: {"ACTIVEMQ", "127.0.0.1:61613"}},
			secrets:   map[string]string{mqTestSecretARN: mqTestSecretJSON},
			in: CreateEventSourceMappingInput{
				Queues: []string{"pizza"}, SourceAccessConfigurations: []SourceAccessConfiguration{basic},
			},
			wantSource: "aws:mq", wantVHost: "/", wantQueue: "pizza",
		},
		{
			name:       "metadata only broker is not polled",
			secrets:    map[string]string{mqTestSecretARN: mqTestSecretJSON},
			in:         mqInput("q", basic),
			wantReason: "no real broker behind this source",
		},
		{
			name:       "missing secret",
			endpoints:  map[string][2]string{mqTestBrokerARN: {"RABBITMQ", "127.0.0.1:5672"}},
			in:         mqInput("q", basic),
			wantReason: "BASIC_AUTH secret unavailable",
		},
		{
			name:       "missing queue",
			endpoints:  map[string][2]string{mqTestBrokerARN: {"RABBITMQ", "127.0.0.1:5672"}},
			in:         CreateEventSourceMappingInput{SourceAccessConfigurations: []SourceAccessConfiguration{basic}},
			wantReason: "no queue configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := NewEventSourcePoller(newKafkaTestBackend(t), nil)
			src := &fakeMQSources{endpoints: tt.endpoints, secrets: tt.secrets}
			p.SetMQBrokerResolver(src)
			p.SetMQSecretResolver(src)

			m := &EventSourceMapping{
				UUID: "u1", EventSourceARN: mqTestBrokerARN, FunctionARN: "fn", BatchSize: 50,
				Queues: tt.in.Queues, SourceAccessConfigurations: tt.in.SourceAccessConfigurations,
			}

			spec, reason := p.mqSpecFor(t.Context(), m)
			assert.Equal(t, tt.wantReason, reason)

			if tt.wantReason != "" {
				return
			}

			assert.Equal(t, tt.wantSource, spec.EventSource)
			assert.Equal(t, tt.wantVHost, spec.Consumer.VHost)
			assert.Equal(t, tt.wantQueue, spec.Consumer.Queue)
			assert.Equal(t, "admin", spec.Consumer.Username)
			assert.Equal(t, "s3cretpassword", spec.Consumer.Password)
			assert.Equal(t, 50, spec.BatchSize)
			assert.Equal(t, kafkaDefaultWindow, spec.Window)
		})
	}
}

func TestMQESMPollsRealBrokerOnly(t *testing.T) {
	t.Parallel()

	b := newKafkaTestBackend(t)
	src := &fakeMQSources{secrets: map[string]string{mqTestSecretARN: mqTestSecretJSON}}
	created := make(chan mqConsumerConfig, 4)
	deliveries := make(chan []byte, 4)

	p := NewEventSourcePoller(b, nil)
	p.SetMQBrokerResolver(src)
	p.SetMQSecretResolver(src)
	p.mqFactory = func(cfg mqConsumerConfig, _ int) (mqConsumer, error) {
		created <- cfg

		return newFakeMQConsumer(mqTestMessages("hello")...), nil
	}
	p.kafkaInvoker = func(_ context.Context, _ string, payload []byte) error {
		deliveries <- payload

		return nil
	}

	t.Cleanup(p.stopAllKafka)

	_, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
		FunctionName: "fn", EventSourceARN: mqTestBrokerARN, Enabled: true, Queues: []string{"pizza"}, BatchSize: 1,
		SourceAccessConfigurations: []SourceAccessConfiguration{{Type: "BASIC_AUTH", URI: mqTestSecretARN}},
	})
	require.NoError(t, err)

	assert.Equal(t, 1, p.poll(t.Context()))
	assert.Empty(t, p.mqWorkers, "metadata-only broker is not polled")
	assert.Len(t, p.kafkaUnsupported, 1)

	src.mu.Lock()
	src.endpoints = map[string][2]string{mqTestBrokerARN: {"RABBITMQ", "127.0.0.1:5672"}}
	src.mu.Unlock()

	p.poll(t.Context())

	cfg := <-created
	assert.Equal(t, mqConsumerConfig{
		Engine: "RABBITMQ", Addr: "127.0.0.1:5672", Queue: "pizza", VHost: "/",
		Username: "admin", Password: "s3cretpassword",
	}, cfg)

	var ev map[string]any

	require.NoError(t, json.Unmarshal(<-deliveries, &ev))
	assert.Equal(t, "aws:rmq", ev["eventSource"])
	assert.Equal(t, mqTestBrokerARN, ev["eventSourceArn"])
	assert.Contains(t, ev["rmqMessagesByQueue"], "q::/")
}
