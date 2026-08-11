package iotdataplane_test

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PublishMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/topics/test", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
func TestHandler_PublishEmptyTopic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/topics/", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// mockMQTTPublisher implements MQTTPublisher for testing. retain/qos are
// captured (not just topic/payload) so tests can assert on them too -- e.g.
// SendDirectMessage's confirmation-to-QoS mapping.
//
// clientSubs/knownClients simulate the broker's live per-client session
// state (mochi-mqtt's Clients table): a clientID present in knownClients is
// "connected" for ClientSubscriptions/SendToClient purposes, with whatever
// subscriptions (if any) clientSubs[clientID] holds. sendToClientTopic/
// sendToClientPayload/sendToClientQos record the last SendToClient call
// separately from Publish's topic/payload/qos, so tests can tell direct
// per-client delivery apart from a topic broadcast. props/sendToClientProps
// record the MQTT5Properties passed to the *WithProperties variants.
type mockMQTTPublisher struct {
	sendToClientErr     error
	clientSubs          map[string]map[string]byte
	knownClients        map[string]bool
	topic               string
	sendToClientTopic   string
	sendToClientClient  string
	payload             []byte
	sendToClientPayload []byte
	props               iotdataplane.MQTT5Properties
	sendToClientProps   iotdataplane.MQTT5Properties
	qos                 byte
	sendToClientQos     byte
	retain              bool
}

func (m *mockMQTTPublisher) Publish(topic string, payload []byte, retain bool, qos byte) error {
	return m.PublishWithProperties(topic, payload, retain, qos, iotdataplane.MQTT5Properties{})
}

func (m *mockMQTTPublisher) PublishWithProperties(
	topic string, payload []byte, retain bool, qos byte, props iotdataplane.MQTT5Properties,
) error {
	m.topic = topic
	m.payload = payload
	m.retain = retain
	m.qos = qos
	m.props = props

	return nil
}

func (m *mockMQTTPublisher) ClientSubscriptions(clientID string) (map[string]byte, bool) {
	if !m.knownClients[clientID] {
		return nil, false
	}

	subs := m.clientSubs[clientID]
	if subs == nil {
		subs = map[string]byte{}
	}

	return subs, true
}

func (m *mockMQTTPublisher) SendToClient(
	clientID, topic string,
	payload []byte,
	qos byte,
) (bool, error) {
	return m.SendToClientWithProperties(
		clientID,
		topic,
		payload,
		qos,
		iotdataplane.MQTT5Properties{},
	)
}

func (m *mockMQTTPublisher) SendToClientWithProperties(
	clientID, topic string, payload []byte, qos byte, props iotdataplane.MQTT5Properties,
) (bool, error) {
	if m.sendToClientErr != nil {
		return false, m.sendToClientErr
	}

	if !m.knownClients[clientID] {
		return false, nil
	}

	m.sendToClientClient = clientID
	m.sendToClientTopic = topic
	m.sendToClientPayload = payload
	m.sendToClientQos = qos
	m.sendToClientProps = props

	return true, nil
}
func TestBackend_Publish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topic     string
		payload   []byte
		wantErr   bool
		setupMock bool
	}{
		{
			name:      "publish with broker",
			topic:     "test/topic",
			payload:   []byte("hello"),
			wantErr:   false,
			setupMock: true,
		},
		{
			name:    "publish without broker",
			topic:   "test/topic",
			payload: []byte("hello"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()

			if tt.setupMock {
				mock := &mockMQTTPublisher{}
				b.SetBroker(mock)
			}

			err := b.Publish(tt.topic, tt.payload, 0, false, iotdataplane.MQTT5Properties{})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
func TestHandler_PublishWithBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		body     []byte
		wantCode int
	}{
		{
			name:     "publish plain payload",
			path:     "/topics/test/topic",
			body:     []byte("hello"),
			wantCode: http.StatusOK,
		},
		{
			name:     "publish json payload",
			path:     "/topics/json/topic",
			body:     []byte(`{"payload":"world"}`),
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			b.SetBroker(&mockMQTTPublisher{})
			h := iotdataplane.NewHandler(b)

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
func TestHandler_PublishWithRetain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		path          string
		retainedTopic string
		body          []byte
		wantCode      int
		wantRetained  bool
	}{
		{
			name:          "publish_with_retain_true_stores_message",
			path:          "/topics/sensor/data?retain=true",
			body:          []byte(`{"temp":25}`),
			wantCode:      http.StatusOK,
			wantRetained:  true,
			retainedTopic: "sensor/data",
		},
		{
			name:          "publish_without_retain_does_not_store",
			path:          "/topics/sensor/data",
			body:          []byte(`{"temp":25}`),
			wantCode:      http.StatusOK,
			wantRetained:  false,
			retainedTopic: "sensor/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			b.SetBroker(&mockMQTTPublisher{})
			h := iotdataplane.NewHandler(b)

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			_, err := b.GetRetainedMessage(tt.retainedTopic)
			if tt.wantRetained {
				require.NoError(t, err, "retained message should be stored")
			} else {
				require.Error(t, err, "retained message should not be stored")
			}
		})
	}
}

// TestParityAccuracy_Publish_Returns200 verifies that a valid Publish request
// returns HTTP 200, matching real AWS IoT Data Plane behavior.
func Test_Publish_Returns200(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "simple_topic",
			path:       "/topics/devices/sensor1/data",
			body:       []byte(`{"temp":22}`),
			wantStatus: http.StatusOK,
		},
		{
			name:       "binary_payload",
			path:       "/topics/telemetry/raw",
			body:       []byte{0x01, 0x02, 0x03},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_payload",
			path:       "/topics/devices/cmd",
			body:       []byte{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "qos_0",
			path:       "/topics/test?qos=0",
			body:       []byte(`{}`),
			wantStatus: http.StatusOK,
		},
		{
			name:       "qos_1",
			path:       "/topics/test?qos=1",
			body:       []byte(`{}`),
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, "path: %s", tt.path)
		})
	}
}

// TestParityAccuracy_Publish_TopicValidation verifies that invalid topics are
// rejected per AWS IoT rules: wildcards, reserved shadow topics, empty segments.
func Test_Publish_TopicValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "wildcard_hash_rejected",
			path:       "/topics/devices/#",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "wildcard_plus_rejected",
			path:       "/topics/devices/+/data",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "qos_invalid_2",
			path:       "/topics/test?qos=2",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "qos_invalid_negative",
			path:       "/topics/test?qos=-1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "shadow_reserved_topic_rejected",
			path:       "/topics/$aws/things/mydevice/shadow/update",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, []byte(`{}`))
			assert.Equal(t, tt.wantStatus, rec.Code, "path: %s", tt.path)
		})
	}
}
func Test_Publish_NoBroker_Returns200(t *testing.T) {
	t.Parallel()

	// When no broker is configured publish should silently succeed (HTTP 200),
	// not return 500. This matches localstack behaviour where publish is fire-and-forget.
	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	rec := doRequest(t, h, http.MethodPost, "/topics/test/topic", []byte("hello"))
	assert.Equal(t, http.StatusOK, rec.Code)
}
func Test_Publish_QosParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		qos      string
		wantCode int
	}{
		{name: "qos_0", qos: "0", wantCode: http.StatusOK},
		{name: "qos_1", qos: "1", wantCode: http.StatusOK},
		{name: "qos_invalid", qos: "2", wantCode: http.StatusBadRequest},
		{name: "qos_negative", qos: "-1", wantCode: http.StatusBadRequest},
		{name: "qos_string", qos: "abc", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/topics/t?qos="+tt.qos, []byte("x"))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
func Test_Publish_RetainStores(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	// Publish with retain=true should store a retained message.
	rec := doRequest(t, h, http.MethodPost, "/topics/sensor/temp?retain=true", []byte("42"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b))

	// Publish without retain should not store.
	b.Reset()
	rec = doRequest(t, h, http.MethodPost, "/topics/sensor/temp", []byte("42"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))
}
func Test_TopicValidation_Wildcards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		topic    string
		wantCode int
	}{
		{name: "hash_wildcard", topic: "sensor/#", wantCode: http.StatusBadRequest},
		{name: "plus_wildcard", topic: "sensor/+/temp", wantCode: http.StatusBadRequest},
		{name: "multi_wildcard", topic: "#", wantCode: http.StatusBadRequest},
		{name: "valid_topic", topic: "sensor/temperature", wantCode: http.StatusOK},
		{name: "valid_nested", topic: "a/b/c/d", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequest(t, h, http.MethodPost, "/topics/"+tt.topic, []byte("x"))
			assert.Equal(t, tt.wantCode, rec.Code, "topic: %q", tt.topic)
		})
	}
}
func Test_TopicValidation_Length(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// 257-character topic must be rejected.
	longTopic := strings.Repeat("a", 257)
	rec := doRequest(t, h, http.MethodPost, "/topics/"+longTopic, []byte("x"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// 256-character topic must be accepted.
	exactTopic := strings.Repeat("a", 256)
	rec = doRequest(t, h, http.MethodPost, "/topics/"+exactTopic, []byte("x"))
	assert.Equal(t, http.StatusOK, rec.Code)
}
func Test_TopicValidation_EmptySegments(t *testing.T) {
	t.Parallel()

	h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())

	// Topic with empty segment (consecutive slashes) must be rejected.
	rec := doRequest(t, h, http.MethodPost, "/topics/a//b", []byte("x"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// doRequestCT sends a request with a specific Content-Type header.
func doRequestCT(
	t *testing.T, h *iotdataplane.Handler, method, path string, body []byte, ct string,
) *httptest.ResponseRecorder {
	t.Helper()

	var r *bytes.Reader
	if body != nil {
		r = bytes.NewReader(body)
	} else {
		r = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Content-Type", ct)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestRefinement3_TopicValidation_ControlChars validates per-segment control byte rejection.
// Control characters cannot be embedded in HTTP URLs, so we test validateTopic directly.
func Test_TopicValidation_ControlChars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{name: "nul_byte", topic: "sensor/\x00data", wantErr: true},
		{name: "tab", topic: "sensor/\tdata", wantErr: true},
		{name: "newline", topic: "sensor/\ndata", wantErr: true},
		{name: "carriage_return", topic: "a/\r/b", wantErr: true},
		{name: "del_0x7f", topic: "a/\x7f", wantErr: true},
		{name: "unit_separator_0x1f", topic: "a/\x1f/b", wantErr: true},
		{name: "printable_ascii", topic: "sensor/temp", wantErr: false},
		{name: "space_0x20_allowed", topic: "sensor/temp data", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateTopic(tt.topic)
			if tt.wantErr {
				require.Error(t, err, "topic %q should fail validation", tt.topic)
				assert.ErrorIs(t, err, iotdataplane.ErrValidation)
			} else {
				assert.NoError(t, err, "topic %q should pass validation", tt.topic)
			}
		})
	}
}
func Test_TopicValidation_AwsShadowReserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{
			name:    "shadow_update_reserved",
			topic:   "$aws/things/mydevice/shadow/update",
			wantErr: true,
		},
		{
			name:    "shadow_accepted_reserved",
			topic:   "$aws/things/mydevice/shadow/update/accepted",
			wantErr: true,
		},
		{
			name:    "aws_non_shadow_allowed",
			topic:   "$aws/things/mydevice/jobs/get",
			wantErr: false,
		},
		{
			name:    "non_aws_dollar_allowed",
			topic:   "$local/sensor/data",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateTopic(tt.topic)
			if tt.wantErr {
				require.Error(t, err, "topic %q should fail validation", tt.topic)
				assert.ErrorIs(t, err, iotdataplane.ErrValidation)
			} else {
				assert.NoError(t, err, "topic %q should pass", tt.topic)
			}
		})
	}
}
func Test_PublishPayload_ContentTypeGate(t *testing.T) {
	t.Parallel()

	// The JSON envelope {"payload":"..."} should only be unwrapped for JSON content types,
	// not for application/octet-stream (binary).
	tests := []struct {
		name     string
		ct       string
		body     []byte
		wantCode int
	}{
		{
			name:     "json_content_type_unwraps",
			ct:       "application/json",
			body:     []byte(`{"payload":"hello"}`),
			wantCode: http.StatusOK,
		},
		{
			name:     "no_content_type_unwraps",
			ct:       "",
			body:     []byte(`{"payload":"hello"}`),
			wantCode: http.StatusOK,
		},
		{
			name:     "octet_stream_passes_raw",
			ct:       "application/octet-stream",
			body:     []byte(`{"payload":"should-stay-raw"}`),
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequestCT(t, h, http.MethodPost, "/topics/test/topic", tt.body, tt.ct)
			assert.Equal(t, tt.wantCode, rec.Code, "ct: %q", tt.ct)
		})
	}
}
func Test_RetainMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retain    string
		payload   []byte
		wantCount int
		preseed   bool
	}{
		{
			name:      "non_empty_retain_true_stores",
			payload:   []byte("data"),
			retain:    "true",
			wantCount: 1,
		},
		{
			name:      "non_empty_retain_false_no_store",
			payload:   []byte("data"),
			retain:    "false",
			wantCount: 0,
		},
		{
			name:      "empty_retain_true_deletes",
			payload:   []byte{},
			retain:    "true",
			preseed:   true,
			wantCount: 0,
		},
		{
			name:      "empty_retain_false_no_store",
			payload:   []byte{},
			retain:    "false",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			if tt.preseed {
				require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("old"), 0, nil))
			}

			h := iotdataplane.NewHandler(b)
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/topics/sensor/temp?retain="+tt.retain,
				tt.payload,
			)
			require.Equal(t, http.StatusOK, rec.Code)

			assert.Equal(t, tt.wantCount, iotdataplane.RetainedMessageCount(b))
		})
	}
}
func Test_Publish_BinaryContentType_NoUnwrap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        []byte
		retain      string
		wantPayload []byte
	}{
		{
			name:        "binary_payload_preserved",
			contentType: "application/octet-stream",
			body:        []byte{0x00, 0x01, 0xFF, 0xFE},
			retain:      "true",
			wantPayload: []byte{0x00, 0x01, 0xFF, 0xFE},
		},
		{
			name:        "json_payload_unwrapped",
			contentType: "application/json",
			body:        []byte(`{"payload":"hello"}`),
			retain:      "true",
			wantPayload: []byte("hello"),
		},
		{
			name:        "json_payload_no_envelope_preserved",
			contentType: "application/json",
			body:        []byte(`{"key":"value"}`),
			retain:      "true",
			wantPayload: []byte(`{"key":"value"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotdataplane.NewInMemoryBackend()
			h := iotdataplane.NewHandler(b)

			rec := doRequestWithContentType(t, h, http.MethodPost,
				"/topics/test/topic?retain="+tt.retain, tt.contentType, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			msg, err := b.GetRetainedMessage("test/topic")
			require.NoError(t, err)
			assert.Equal(t, tt.wantPayload, msg.Payload)
		})
	}
}

// doRequestHeaders issues a request like doRequest but with extra headers set
// (used to exercise the MQTT5 Publish headers: X-Amz-Mqtt5-*).
// doRequestHeaders always issues a POST -- every current caller exercises a
// POST-only wire path (Publish, SendDirectMessage); add a method parameter
// back if a future caller needs another verb.
func doRequestHeaders(
	t *testing.T,
	h *iotdataplane.Handler,
	path string,
	body []byte,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var r *bytes.Reader
	if body != nil {
		r = bytes.NewReader(body)
	} else {
		r = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(http.MethodPost, path, r)
	req.Header.Set("Content-Type", "application/octet-stream")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// Test_Publish_MQTT5Fields_AcceptedAndValidated exercises every MQTT5 Publish
// field beyond topic/qos/payload/retain against the real SDK's wire locations
// (aws-sdk-go-v2/service/iotdataplane/serializers.go,
// awsRestjson1_serializeOpHttpBindingsPublishInput): contentType/messageExpiry/
// responseTopic are query params; correlationData/payloadFormatIndicator/
// userProperties are X-Amz-Mqtt5-* headers.
func Test_Publish_MQTT5Fields_AcceptedAndValidated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers  map[string]string
		name     string
		query    string
		wantCode int
	}{
		{
			name:  "all_fields_valid",
			query: "?contentType=text%2Fplain&messageExpiry=60&responseTopic=reply/topic",
			headers: map[string]string{
				"X-Amz-Mqtt5-Correlation-Data":         "aGVsbG8=", // valid base64 ("hello") -- see decodeCorrelationData
				"X-Amz-Mqtt5-Payload-Format-Indicator": "UTF8_DATA",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "no_optional_fields",
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid_payload_format_indicator",
			headers:  map[string]string{"X-Amz-Mqtt5-Payload-Format-Indicator": "NOT_A_REAL_ENUM"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unspecified_bytes_indicator_valid",
			headers: map[string]string{
				"X-Amz-Mqtt5-Payload-Format-Indicator": "UNSPECIFIED_BYTES",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "negative_message_expiry_rejected",
			query:    "?messageExpiry=-1",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non_numeric_message_expiry_rejected",
			query:    "?messageExpiry=soon",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "response_topic_with_wildcard_rejected",
			query:    "?responseTopic=reply/%23",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_base64_user_properties_rejected",
			headers:  map[string]string{"X-Amz-Mqtt5-User-Properties": "not-valid-base64!!"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_base64_correlation_data_rejected",
			headers:  map[string]string{"X-Amz-Mqtt5-Correlation-Data": "not-valid-base64!!"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "user_properties_wrong_shape_rejected",
			headers: map[string]string{
				// valid base64, but decodes to a single multi-key object rather
				// than an array of single-key objects (see parseUserProperties).
				"X-Amz-Mqtt5-User-Properties": base64.StdEncoding.EncodeToString(
					[]byte(`{"deviceName":"alpha"}`),
				),
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "user_properties_well_formed_accepted",
			headers: map[string]string{
				"X-Amz-Mqtt5-User-Properties": base64.StdEncoding.EncodeToString(
					[]byte(`[{"deviceName":"alpha"},{"deviceCnt":"45"}]`)),
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
			rec := doRequestHeaders(
				t,
				h,
				"/topics/test/topic"+tt.query,
				[]byte("payload"),
				tt.headers,
			)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// Test_Publish_UserProperties_PersistedOnRetainedMessage verifies that the
// X-Amz-Mqtt5-User-Properties header (base64-encoded per AWS docs) survives a
// retain=true Publish and is echoed back by GetRetainedMessage, matching
// GetRetainedMessageOutput.UserProperties in the real SDK.
func Test_Publish_UserProperties_PersistedOnRetainedMessage(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	rawProps := `[{"deviceName":"alpha"}]`
	encoded := base64.StdEncoding.EncodeToString([]byte(rawProps))

	rec := doRequestHeaders(t, h, "/topics/sensor/temp?retain=true", []byte("25"),
		map[string]string{"X-Amz-Mqtt5-User-Properties": encoded})
	require.Equal(t, http.StatusOK, rec.Code)

	msg, err := b.GetRetainedMessage("sensor/temp")
	require.NoError(t, err)
	assert.Equal(t, []byte(rawProps), msg.UserProperties)
}

// Test_Publish_NoUserProperties_RetainedMessageHasNone verifies a Publish
// without the userProperties header leaves the retained message's
// UserProperties nil (AWS docs: "...or null if the retained message doesn't
// include any user properties").
func Test_Publish_NoUserProperties_RetainedMessageHasNone(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	rec := doRequest(t, h, http.MethodPost, "/topics/sensor/temp?retain=true", []byte("25"))
	require.Equal(t, http.StatusOK, rec.Code)

	msg, err := b.GetRetainedMessage("sensor/temp")
	require.NoError(t, err)
	assert.Nil(t, msg.UserProperties)
}

// Test_Publish_MQTT5Fields_ForwardedToBroker verifies every optional MQTT5
// Publish field (contentType/correlationData/messageExpiry/
// payloadFormatIndicator/responseTopic/userProperties) now reaches the
// broker as a real iotdataplane.MQTT5Properties value via
// MQTTPublisher.PublishWithProperties. Before this, MQTTPublisher.Publish
// carried only topic/payload/retain/qos -- there was no properties-carrying
// method at all, so none of these fields could reach a live subscriber
// (see PARITY.md's now-resolved gap).
func Test_Publish_MQTT5Fields_ForwardedToBroker(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	mock := &mockMQTTPublisher{}
	b.SetBroker(mock)
	h := iotdataplane.NewHandler(b)

	query := "?contentType=text%2Fplain&messageExpiry=60&responseTopic=reply/topic"
	headers := map[string]string{
		"X-Amz-Mqtt5-Correlation-Data": base64.StdEncoding.EncodeToString(
			[]byte("corr-id-1"),
		),
		"X-Amz-Mqtt5-Payload-Format-Indicator": "UTF8_DATA",
		"X-Amz-Mqtt5-User-Properties": base64.StdEncoding.EncodeToString(
			[]byte(`[{"deviceName":"alpha"},{"deviceCnt":"45"}]`)),
	}

	rec := doRequestHeaders(
		t,
		h,
		"/topics/test/topic"+query,
		[]byte("payload"),
		headers,
	)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	assert.Equal(t, "text/plain", mock.props.ContentType)
	assert.Equal(t, "reply/topic", mock.props.ResponseTopic)
	assert.Equal(t, "UTF8_DATA", mock.props.PayloadFormatIndicator)
	assert.Equal(t, []byte("corr-id-1"), mock.props.CorrelationData)
	assert.EqualValues(t, 60, mock.props.MessageExpiry)
	require.Len(t, mock.props.UserProperties, 2)
	assert.Equal(
		t,
		iotdataplane.MQTT5UserProperty{Key: "deviceName", Value: "alpha"},
		mock.props.UserProperties[0],
	)
	assert.Equal(
		t,
		iotdataplane.MQTT5UserProperty{Key: "deviceCnt", Value: "45"},
		mock.props.UserProperties[1],
	)
}

func Test_TopicValidation_Matrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{name: "simple", topic: "home/sensors/temperature", wantErr: false},
		{name: "single_segment", topic: "temperature", wantErr: false},
		{name: "dollar_not_shadow", topic: "$aws/jobs/start", wantErr: false},
		{name: "shadow_reserved", topic: "$aws/things/dev/shadow/update", wantErr: true},
		{name: "wildcard_hash", topic: "home/#", wantErr: true},
		{name: "wildcard_plus", topic: "home/+/temp", wantErr: true},
		{name: "empty_level_leading", topic: "/home/temp", wantErr: true},
		{name: "empty_level_trailing", topic: "home/temp/", wantErr: true},
		{name: "empty_level_middle", topic: "home//temp", wantErr: true},
		{name: "too_long", topic: strings.Repeat("a", 257), wantErr: true},
		{name: "exactly_256", topic: strings.Repeat("a", 256), wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := iotdataplane.ValidateTopic(tt.topic)
			if tt.wantErr {
				require.ErrorIs(
					t,
					err,
					iotdataplane.ErrValidation,
					"topic %q should be invalid",
					tt.topic,
				)
			} else {
				assert.NoError(t, err, "topic %q should be valid", tt.topic)
			}
		})
	}
}

// doRequestWithContentType issues a handler request with a specific Content-Type header.
func doRequestWithContentType(
	t *testing.T, h *iotdataplane.Handler, method, path, contentType string, body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}
