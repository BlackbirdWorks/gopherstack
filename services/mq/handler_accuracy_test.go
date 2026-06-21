package mq_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newAccuracyMQBackend(t *testing.T) *mq.InMemoryBackend {
	t.Helper()

	return mq.NewInMemoryBackend("123456789012", "us-east-1")
}

func newAccuracyMQHandler(t *testing.T) *mq.Handler {
	t.Helper()

	return mq.NewHandler(newAccuracyMQBackend(t))
}

func doAccuracyMQ(t *testing.T, h *mq.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var reqBody *strings.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = strings.NewReader(string(raw))
	} else {
		reqBody = strings.NewReader("")
	}

	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func parseAccuracyMQ(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// createAccuracyBroker creates a broker via the HTTP handler and returns the broker ID.
func createAccuracyBroker(t *testing.T, h *mq.Handler, name, engineType string) string {
	t.Helper()

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": name,
		"engineType": engineType,
	})
	require.Equal(t, http.StatusOK, rec.Code, "CreateBroker %s failed: %s", name, rec.Body.String())

	resp := parseAccuracyMQ(t, rec)

	return resp["brokerId"].(string)
}

// createAccuracyConfig creates a configuration and returns its ID.
func createAccuracyConfig(t *testing.T, h *mq.Handler, name, engineType string) string {
	t.Helper()

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       name,
		"engineType": engineType,
	})
	require.Equal(t, http.StatusOK, rec.Code, "CreateConfiguration %s failed: %s", name, rec.Body.String())

	resp := parseAccuracyMQ(t, rec)

	return resp["id"].(string)
}

// ── Issue 1: Broker name validation ──────────────────────────────────────────

func TestAccuracy_CreateBroker_NameTooLong(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	longName := strings.Repeat("a", 51)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": longName,
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"broker name >50 chars must return 400")

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, "BadRequestException", out["__type"])
}

func TestAccuracy_CreateBroker_NameExactly50Chars(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	name := strings.Repeat("a", 50)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": name,
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "50-char broker name must be accepted: %s", rec.Body.String())
}

func TestAccuracy_CreateBroker_NameStartsWithHyphen(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "-invalid-name",
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"broker name starting with hyphen must return 400")
}

func TestAccuracy_CreateBroker_NameContainsInvalidChars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		brokerName string
	}{
		{"spaces", "broker name"},
		{"slash", "broker/name"},
		{"at_sign", "broker@name"},
		{"dot", "broker.name"},
		{"colon", "broker:name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyMQHandler(t)
			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": tt.brokerName,
				"engineType": mq.EngineTypeActiveMQ,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"broker name %q must return 400", tt.brokerName)
		})
	}
}

func TestAccuracy_CreateBroker_ValidNameFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		brokerName string
	}{
		{"alphanumeric_only", "mybroker"},
		{"with_hyphens", "my-broker-123"},
		{"with_underscores", "my_broker_123"},
		{"mixed", "My-Broker_01"},
		{"single_char", "x"},
		{"starts_with_digit", "1broker"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyMQHandler(t)
			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": tt.brokerName,
				"engineType": mq.EngineTypeActiveMQ,
			})
			assert.Equal(t, http.StatusOK, rec.Code,
				"valid broker name %q should succeed: %s", tt.brokerName, rec.Body.String())
		})
	}
}

func TestAccuracy_CreateBroker_NameValidation_Backend(t *testing.T) {
	t.Parallel()

	b := newAccuracyMQBackend(t)

	tests := []struct {
		name       string
		brokerName string
		wantErr    bool
	}{
		{"too_long", strings.Repeat("x", 51), true},
		{"empty", "", true},
		{"starts_with_hyphen", "-broker", true},
		{"contains_space", "my broker", true},
		{"valid_with_hyphen", "my-broker", false},
		{"valid_with_underscore", "my_broker", false},
		{"valid_50_chars", strings.Repeat("a", 50), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.CreateBroker(
				tt.brokerName, mq.DeploymentModeSingleInstance,
				mq.EngineTypeActiveMQ, "", "", false, false, nil, nil, nil, nil,
			)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, mq.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ── Issue 2: Deployment mode and engine type compatibility ────────────────────

func TestAccuracy_CreateBroker_DeploymentMode_Invalid(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":     "mode-test",
		"engineType":     mq.EngineTypeActiveMQ,
		"deploymentMode": "INVALID_MODE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"invalid deploymentMode must return 400")

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, "BadRequestException", out["__type"])
}

func TestAccuracy_CreateBroker_RabbitMQ_RejectsActiveStandby(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":     "rabbitmq-broker",
		"engineType":     mq.EngineTypeRabbitMQ,
		"deploymentMode": mq.DeploymentModeActiveStandby,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"RabbitMQ does not support ACTIVE_STANDBY_MULTI_AZ")
}

func TestAccuracy_CreateBroker_ActiveMQ_RejectsClusterMultiAZ(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":     "activemq-broker",
		"engineType":     mq.EngineTypeActiveMQ,
		"deploymentMode": mq.DeploymentModeCluster,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"ActiveMQ does not support CLUSTER_MULTI_AZ")
}

func TestAccuracy_CreateBroker_RabbitMQ_AcceptsClusterMultiAZ(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":     "rabbit-cluster",
		"engineType":     mq.EngineTypeRabbitMQ,
		"deploymentMode": mq.DeploymentModeCluster,
	})
	assert.Equal(t, http.StatusOK, rec.Code,
		"RabbitMQ + CLUSTER_MULTI_AZ should succeed: %s", rec.Body.String())
}

func TestAccuracy_CreateBroker_ActiveMQ_AcceptsActiveStandby(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":     "activemq-standby",
		"engineType":     mq.EngineTypeActiveMQ,
		"deploymentMode": mq.DeploymentModeActiveStandby,
	})
	assert.Equal(t, http.StatusOK, rec.Code,
		"ActiveMQ + ACTIVE_STANDBY_MULTI_AZ should succeed: %s", rec.Body.String())
}

func TestAccuracy_CreateBroker_DeploymentMode_BothEngines_SingleInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
	}{
		{"activemq", mq.EngineTypeActiveMQ},
		{"rabbitmq", mq.EngineTypeRabbitMQ},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyMQHandler(t)
			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName":     tt.name + "-single",
				"engineType":     tt.engineType,
				"deploymentMode": mq.DeploymentModeSingleInstance,
			})
			assert.Equal(t, http.StatusOK, rec.Code,
				"%s + SINGLE_INSTANCE should succeed: %s", tt.engineType, rec.Body.String())
		})
	}
}

func TestAccuracy_CreateBroker_DeploymentMode_Backend_Validation(t *testing.T) {
	t.Parallel()

	b := newAccuracyMQBackend(t)

	tests := []struct {
		name           string
		engineType     string
		deploymentMode string
		wantErr        bool
	}{
		{"rabbitmq_active_standby", mq.EngineTypeRabbitMQ, mq.DeploymentModeActiveStandby, true},
		{"activemq_cluster", mq.EngineTypeActiveMQ, mq.DeploymentModeCluster, true},
		{"invalid_mode", mq.EngineTypeActiveMQ, "UNKNOWN_MODE", true},
		{"rabbitmq_cluster_ok", mq.EngineTypeRabbitMQ, mq.DeploymentModeCluster, false},
		{"activemq_standby_ok", mq.EngineTypeActiveMQ, mq.DeploymentModeActiveStandby, false},
		{"both_single_ok", mq.EngineTypeRabbitMQ, mq.DeploymentModeSingleInstance, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.CreateBroker(
				tt.name, tt.deploymentMode, tt.engineType,
				"", "", false, false, nil, nil, nil, nil,
			)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, mq.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ── Issue 3: Broker endpoint format ──────────────────────────────────────────

func TestAccuracy_CreateBroker_EndpointContainsBrokerID(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "endpoint-test",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseAccuracyMQ(t, desc)
	instances, ok := out["brokerInstances"].([]any)
	require.True(t, ok && len(instances) > 0, "brokerInstances must be non-empty")

	instance := instances[0].(map[string]any)
	endpoints, ok := instance["endpoints"].([]any)
	require.True(t, ok && len(endpoints) > 0, "endpoints must be non-empty")

	endpoint := endpoints[0].(string)
	assert.Contains(t, endpoint, brokerID,
		"endpoint must contain the broker ID, not just the name")
}

func TestAccuracy_CreateBroker_ActiveMQ_EndpointFormat(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "amq-endpoint",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseAccuracyMQ(t, desc)
	instances := out["brokerInstances"].([]any)
	instance := instances[0].(map[string]any)
	endpoints := instance["endpoints"].([]any)
	endpoint := endpoints[0].(string)

	assert.True(t, strings.HasPrefix(endpoint, "ssl://"),
		"ActiveMQ endpoint must use ssl:// scheme, got %s", endpoint)
	assert.True(t, strings.HasSuffix(endpoint, ":61617"),
		"ActiveMQ endpoint must use port 61617, got %s", endpoint)
	assert.Contains(t, endpoint, "us-east-1",
		"endpoint must contain the region")
}

func TestAccuracy_CreateBroker_RabbitMQ_EndpointFormat(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "rmq-endpoint",
		"engineType": mq.EngineTypeRabbitMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseAccuracyMQ(t, desc)
	instances := out["brokerInstances"].([]any)
	instance := instances[0].(map[string]any)
	endpoints := instance["endpoints"].([]any)
	endpoint := endpoints[0].(string)

	assert.True(t, strings.HasPrefix(endpoint, "amqps://"),
		"RabbitMQ endpoint must use amqps:// scheme, got %s", endpoint)
	assert.True(t, strings.HasSuffix(endpoint, ":5671"),
		"RabbitMQ endpoint must use port 5671, got %s", endpoint)
	assert.Contains(t, endpoint, "us-east-1",
		"endpoint must contain the region")
	assert.Contains(t, endpoint, brokerID,
		"endpoint must contain broker ID")
}

func TestAccuracy_CreateBroker_Endpoint_RegionFromBackend(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("111122223333", "eu-west-1")
	h := mq.NewHandler(b)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "eu-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseAccuracyMQ(t, desc)
	instances := out["brokerInstances"].([]any)
	instance := instances[0].(map[string]any)
	endpoints := instance["endpoints"].([]any)
	endpoint := endpoints[0].(string)

	assert.Contains(t, endpoint, "eu-west-1",
		"endpoint must use the backend region, got %s", endpoint)
}

// ── Issue 4: UpdateBroker response accuracy ──────────────────────────────────

func TestAccuracy_UpdateBroker_ResponseContainsBrokerID(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "update-test", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"engineVersion": "5.16.7",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, brokerID, out["brokerId"], "UpdateBroker must return brokerId")
}

func TestAccuracy_UpdateBroker_ResponseContainsEngineVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "update-ev", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"engineVersion": "5.16.7",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, "5.16.7", out["engineVersion"],
		"UpdateBroker response must include engineVersion")
}

func TestAccuracy_UpdateBroker_ResponseContainsHostInstanceType(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "update-hit", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"hostInstanceType": "mq.m5.xlarge",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, "mq.m5.xlarge", out["hostInstanceType"],
		"UpdateBroker response must include hostInstanceType")
}

func TestAccuracy_UpdateBroker_ResponseContainsAutoMinorVersionUpgrade(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "update-amvu", mq.EngineTypeActiveMQ)

	enabled := true
	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"autoMinorVersionUpgrade": enabled,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	val, hasKey := out["autoMinorVersionUpgrade"]
	assert.True(t, hasKey, "UpdateBroker response must include autoMinorVersionUpgrade")
	assert.Equal(t, true, val)
}

func TestAccuracy_UpdateBroker_ResponseContainsSecurityGroups(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "update-sg", mq.EngineTypeActiveMQ)

	sgs := []string{"sg-aabbccdd", "sg-11223344"}
	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"securityGroups": sgs,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	gotSGs, ok := out["securityGroups"].([]any)
	require.True(t, ok, "UpdateBroker response must include securityGroups")
	require.Len(t, gotSGs, 2)
	assert.Equal(t, "sg-aabbccdd", gotSGs[0])
}

func TestAccuracy_UpdateBroker_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/nonexistent-id", map[string]any{
		"engineVersion": "5.16.7",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_UpdateBroker_UpdatesReflectedInDescribe(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "update-reflect", mq.EngineTypeActiveMQ)

	// Update engine version.
	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"engineVersion": "5.18.3",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the change is visible in DescribeBroker.
	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	descOut := parseAccuracyMQ(t, desc)
	assert.Equal(t, "5.18.3", descOut["engineVersion"],
		"updated engineVersion must appear in DescribeBroker")
}

// ── Issue 5: ActiveMQ user password validation ────────────────────────────────

func TestAccuracy_CreateUser_ActiveMQ_ShortPassword_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "pwd-test", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/testuser", map[string]any{
			"password": "short",
		})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"ActiveMQ password <12 chars must return 400")
}

func TestAccuracy_CreateUser_ActiveMQ_PasswordWithComma_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "comma-pwd-test", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/commauser", map[string]any{
			"password": "password,with,comma",
		})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"ActiveMQ password with commas must return 400")
}

func TestAccuracy_CreateUser_ActiveMQ_PasswordFewUniqueChars_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "unique-pwd-test", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/weakuser", map[string]any{
			"password": "aaaaaaaaaaaa",
		})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"ActiveMQ password with <4 unique chars must return 400")
}

func TestAccuracy_CreateUser_ActiveMQ_ValidPassword_Returns201(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "valid-pwd-test", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/gooduser", map[string]any{
			"password": "GoodPassword123!",
		})
	assert.Equal(t, http.StatusCreated, rec.Code,
		"valid ActiveMQ password must return 201: %s", rec.Body.String())
}

func TestAccuracy_CreateUser_ActiveMQ_ExactMinLength_Returns201(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "min-pwd-test", mq.EngineTypeActiveMQ)

	// Exactly 12 characters, 4+ unique chars
	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/minuser", map[string]any{
			"password": "Pass1234abcd",
		})
	assert.Equal(t, http.StatusCreated, rec.Code,
		"12-char ActiveMQ password must return 201: %s", rec.Body.String())
}

func TestAccuracy_CreateUser_RabbitMQ_ShortPassword_Returns201(t *testing.T) {
	t.Parallel()

	// RabbitMQ does NOT have the same password constraints as ActiveMQ.
	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "rmq-pwd-test", mq.EngineTypeRabbitMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/rmquser", map[string]any{
			"password": "short",
		})
	assert.Equal(t, http.StatusCreated, rec.Code,
		"RabbitMQ does not enforce 12-char password rule: %s", rec.Body.String())
}

func TestAccuracy_UpdateUser_ActiveMQ_ShortPassword_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "upd-pwd-test", mq.EngineTypeActiveMQ)

	// Create user with valid password first.
	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/upduser", map[string]any{
			"password": "ValidPassword1!",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Update with short password.
	rec = doAccuracyMQ(t, h, http.MethodPut,
		"/v1/brokers/"+brokerID+"/users/upduser", map[string]any{
			"password": "tooshort",
		})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"UpdateUser with short ActiveMQ password must return 400")
}

func TestAccuracy_CreateUser_ActiveMQ_Password_Backend_Validation(t *testing.T) {
	t.Parallel()

	b := newAccuracyMQBackend(t)
	br, err := b.CreateBroker(
		"pwdval-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "", false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{"too_short", "short1A!", true},
		{"comma_in_password", "ValidPassword1,!", true},
		// 11 'a' + 'A' = 2 unique chars, fails the 4-unique-char rule.
		{"too_few_unique", strings.Repeat("a", 11) + "A", true},
		{"valid", "GoodPassword123!", false},
		{"exact_12_chars", "Pass1234abcd", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			username := tt.name + "-user"
			createErr := b.CreateUser(br.BrokerID, username, tt.password, nil, false)

			if tt.wantErr {
				require.Error(t, createErr)
				require.ErrorIs(t, createErr, mq.ErrValidation)
			} else {
				require.NoError(t, createErr)
			}
		})
	}
}

// ── Issue 6: CreateConfiguration response includes engineType/engineVersion ──

func TestAccuracy_CreateConfiguration_ResponseIncludesEngineType(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "cfg-engine-type",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, mq.EngineTypeActiveMQ, out["engineType"],
		"CreateConfiguration response must include engineType")
}

func TestAccuracy_CreateConfiguration_ResponseIncludesEngineVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":          "cfg-engine-ver",
		"engineType":    mq.EngineTypeActiveMQ,
		"engineVersion": "5.18.3",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, "5.18.3", out["engineVersion"],
		"CreateConfiguration response must include engineVersion")
}

func TestAccuracy_CreateConfiguration_ResponseDefaultEngineVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "cfg-default-ver",
		"engineType": mq.EngineTypeRabbitMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.NotEmpty(t, out["engineVersion"],
		"CreateConfiguration response engineVersion must not be empty when not specified")
}

func TestAccuracy_CreateConfiguration_ResponseFieldsPresent(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "cfg-fields",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)

	requiredFields := []string{"id", "arn", "name", "engineType", "engineVersion", "latestRevision"}
	for _, field := range requiredFields {
		assert.Contains(t, out, field, "CreateConfiguration response must contain %q field", field)
	}
}

func TestAccuracy_CreateConfiguration_RabbitMQ_EngineTypeRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "rmq-cfg",
		"engineType": mq.EngineTypeRabbitMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, mq.EngineTypeRabbitMQ, out["engineType"])

	// Verify DescribeConfiguration also returns engineType.
	configID := out["id"].(string)
	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	descOut := parseAccuracyMQ(t, desc)
	assert.Equal(t, mq.EngineTypeRabbitMQ, descOut["engineType"])
}

// ── Issue 7: ListBrokers pagination ──────────────────────────────────────────

func TestAccuracy_ListBrokers_DefaultReturnsAll(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	for i := range 5 {
		createAccuracyBroker(t, h, "broker-list-"+string(rune('a'+i)), mq.EngineTypeActiveMQ)
	}

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	summaries := out["brokerSummaries"].([]any)
	assert.Len(t, summaries, 5)

	// No nextToken when all results fit.
	_, hasToken := out["nextToken"]
	assert.False(t, hasToken, "nextToken must not appear when all results fit in one page")
}

func TestAccuracy_ListBrokers_MaxResults_Limits_Response(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	for i := range 5 {
		createAccuracyBroker(t, h, "broker-page-"+string(rune('a'+i)), mq.EngineTypeActiveMQ)
	}

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers?maxResults=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	summaries := out["brokerSummaries"].([]any)
	assert.Len(t, summaries, 3, "maxResults=3 must return only 3 brokers")

	// nextToken must be present because there are more.
	assert.Contains(t, out, "nextToken", "nextToken must appear when page is truncated")
}

func TestAccuracy_ListBrokers_NextToken_Pagination(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	names := []string{
		"broker-pag-a", "broker-pag-b", "broker-pag-c", "broker-pag-d", "broker-pag-e",
	}
	for _, name := range names {
		createAccuracyBroker(t, h, name, mq.EngineTypeActiveMQ)
	}

	// First page: 2 results.
	rec1 := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	page1 := parseAccuracyMQ(t, rec1)

	summaries1 := page1["brokerSummaries"].([]any)
	require.Len(t, summaries1, 2)

	nextToken, ok := page1["nextToken"].(string)
	require.True(t, ok && nextToken != "", "page 1 must return a nextToken")

	// Second page using the token.
	rec2 := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers?maxResults=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	page2 := parseAccuracyMQ(t, rec2)

	summaries2 := page2["brokerSummaries"].([]any)
	require.Len(t, summaries2, 2)

	// Third page: 1 remaining.
	nextToken2, ok := page2["nextToken"].(string)
	require.True(t, ok && nextToken2 != "", "page 2 must return a nextToken")

	rec3 := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers?maxResults=2&nextToken="+nextToken2, nil)
	require.Equal(t, http.StatusOK, rec3.Code)
	page3 := parseAccuracyMQ(t, rec3)

	summaries3 := page3["brokerSummaries"].([]any)
	require.Len(t, summaries3, 1)

	// No nextToken on last page.
	_, hasMore := page3["nextToken"]
	assert.False(t, hasMore, "last page must not have a nextToken")

	// All 5 brokers should appear across the 3 pages.
	allNames := make([]string, 0, 5)
	for _, page := range [][]any{summaries1, summaries2, summaries3} {
		for _, s := range page {
			sm := s.(map[string]any)
			allNames = append(allNames, sm["brokerName"].(string))
		}
	}
	assert.Len(t, allNames, 5, "all 5 brokers must appear across pages")
}

func TestAccuracy_ListBrokers_MaxResults_ExceedingTotal(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	createAccuracyBroker(t, h, "only-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers?maxResults=100", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	summaries := out["brokerSummaries"].([]any)
	assert.Len(t, summaries, 1)
	_, hasToken := out["nextToken"]
	assert.False(t, hasToken)
}

// ── Issue 8: ListConfigurations pagination ────────────────────────────────────

func TestAccuracy_ListConfigurations_MaxResults(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	for i := range 5 {
		createAccuracyConfig(t, h, "cfg-paginate-"+string(rune('a'+i)), mq.EngineTypeActiveMQ)
	}

	rec := doAccuracyMQ(
		t, h, http.MethodGet,
		"/v1/configurations?maxResults=3",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	cfgs := out["configurations"].([]any)
	assert.Len(t, cfgs, 3, "maxResults=3 must return 3 configurations")
	assert.Contains(t, out, "nextToken")
}

func TestAccuracy_ListConfigurations_Pagination(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	engineTypes := []string{mq.EngineTypeActiveMQ, mq.EngineTypeRabbitMQ, mq.EngineTypeActiveMQ, mq.EngineTypeRabbitMQ}

	for i := range 4 {
		createAccuracyConfig(t, h, "cfg-page2-"+string(rune('a'+i)), engineTypes[i])
	}

	// Page 1.
	rec1 := doAccuracyMQ(t, h, http.MethodGet, "/v1/configurations?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	page1 := parseAccuracyMQ(t, rec1)
	cfgs1 := page1["configurations"].([]any)
	require.Len(t, cfgs1, 2)
	nextToken := page1["nextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Page 2.
	rec2 := doAccuracyMQ(t, h, http.MethodGet, "/v1/configurations?maxResults=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	page2 := parseAccuracyMQ(t, rec2)
	cfgs2 := page2["configurations"].([]any)
	require.Len(t, cfgs2, 2)
	_, hasMore := page2["nextToken"]
	assert.False(t, hasMore, "last page must not have nextToken")
}

// ── Issue 9: DescribeBroker response field accuracy ───────────────────────────

func TestAccuracy_DescribeBroker_AllCoreFieldsPresent(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":              "full-describe-broker",
		"engineType":              mq.EngineTypeActiveMQ,
		"engineVersion":           "5.18.3",
		"hostInstanceType":        "mq.m5.large",
		"deploymentMode":          mq.DeploymentModeSingleInstance,
		"publiclyAccessible":      true,
		"autoMinorVersionUpgrade": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseAccuracyMQ(t, desc)

	fields := []string{
		"brokerId", "brokerArn", "brokerName", "brokerState",
		"engineType", "engineVersion", "hostInstanceType",
		"deploymentMode", "publiclyAccessible", "autoMinorVersionUpgrade",
		"created",
	}
	for _, f := range fields {
		assert.Contains(t, out, f, "DescribeBroker response must contain %q", f)
	}

	assert.Equal(t, "full-describe-broker", out["brokerName"])
	assert.Equal(t, mq.EngineTypeActiveMQ, out["engineType"])
	assert.Equal(t, "5.18.3", out["engineVersion"])
	assert.Equal(t, mq.BrokerStateRunning, out["brokerState"])
	assert.Equal(t, true, out["publiclyAccessible"])
	assert.Equal(t, true, out["autoMinorVersionUpgrade"])
}

func TestAccuracy_DescribeBroker_BrokerARN_Format(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "arn-test-broker", mq.EngineTypeActiveMQ)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseAccuracyMQ(t, desc)
	brokerARN := out["brokerArn"].(string)

	assert.True(t, strings.HasPrefix(brokerARN, "arn:aws:mq:"),
		"brokerArn must start with arn:aws:mq:, got %s", brokerARN)
	assert.Contains(t, brokerARN, "123456789012",
		"brokerArn must contain the account ID")
	assert.Contains(t, brokerARN, "us-east-1",
		"brokerArn must contain the region")
}

func TestAccuracy_DescribeBroker_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/nonexistent-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, "NotFoundException", out["__type"])
}

func TestAccuracy_DescribeBroker_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "tagged-describe-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"env": "test", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseAccuracyMQ(t, desc)
	tags, ok := out["tags"].(map[string]any)
	require.True(t, ok, "tags must be present in DescribeBroker response")
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "platform", tags["team"])
}

// ── Issue 10: Configuration revision accuracy ─────────────────────────────────

func TestAccuracy_UpdateConfiguration_RevisionIncrements(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	configID := createAccuracyConfig(t, h, "rev-test-cfg", mq.EngineTypeActiveMQ)

	// Initial revision is 1.
	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, desc.Code)
	out := parseAccuracyMQ(t, desc)
	latestRev := out["latestRevision"].(map[string]any)
	assert.Equal(t, 1, int(latestRev["revision"].(float64)))

	// Update bumps to revision 2.
	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/configurations/"+configID, map[string]any{
		"data":        "updated-config-data",
		"description": "second revision",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc2 := doAccuracyMQ(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, desc2.Code)
	out2 := parseAccuracyMQ(t, desc2)
	latestRev2 := out2["latestRevision"].(map[string]any)
	assert.Equal(t, 2, int(latestRev2["revision"].(float64)))
}

func TestAccuracy_DescribeConfigurationRevision_DataRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	configID := createAccuracyConfig(t, h, "data-rt-cfg", mq.EngineTypeActiveMQ)

	testData := "<activemq-config>test</activemq-config>"
	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/configurations/"+configID, map[string]any{
		"data":        testData,
		"description": "test revision",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	revRec := doAccuracyMQ(t, h, http.MethodGet,
		"/v1/configurations/"+configID+"/revisions/2", nil)
	require.Equal(t, http.StatusOK, revRec.Code)

	out := parseAccuracyMQ(t, revRec)
	assert.Equal(t, testData, out["data"],
		"configuration data must round-trip through update and revision fetch")
	assert.Equal(t, 2, int(out["revision"].(float64)))
	assert.Equal(t, "test revision", out["description"])
}

func TestAccuracy_ListConfigurationRevisions_ReturnsAllRevisions(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	configID := createAccuracyConfig(t, h, "multi-rev-cfg", mq.EngineTypeActiveMQ)

	for i := range 3 {
		rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/configurations/"+configID, map[string]any{
			"data":        "revision-" + string(rune('a'+i)),
			"description": "rev " + string(rune('a'+i)),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doAccuracyMQ(t, h, http.MethodGet,
		"/v1/configurations/"+configID+"/revisions", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	out := parseAccuracyMQ(t, listRec)
	revisions, ok := out["revisions"].([]any)
	require.True(t, ok, "response must contain revisions array")
	assert.Len(t, revisions, 4, "should have 4 revisions (initial + 3 updates)")
}

// ── Issue 11: Tag operations accuracy ────────────────────────────────────────

func TestAccuracy_ListTags_EmptyTagMap(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "no-tags-broker", mq.EngineTypeActiveMQ)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc.Code)
	brokerARN := parseAccuracyMQ(t, desc)["brokerArn"].(string)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	tags, ok := out["tags"].(map[string]any)
	require.True(t, ok, "tags field must be present")
	assert.Empty(t, tags, "tags must be empty for broker with no tags")
}

func TestAccuracy_CreateTags_AppendsToExisting(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "add-tags-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"initial": "yes"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	brokerARN := parseAccuracyMQ(t, desc)["brokerArn"].(string)

	// Add another tag.
	addRec := doAccuracyMQ(t, h, http.MethodPost, "/v1/tags/"+brokerARN, map[string]any{
		"tags": map[string]string{"added": "later"},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	listRec := doAccuracyMQ(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	tags := parseAccuracyMQ(t, listRec)["tags"].(map[string]any)
	assert.Equal(t, "yes", tags["initial"])
	assert.Equal(t, "later", tags["added"])
}

func TestAccuracy_DeleteTags_RemovesSpecified(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "del-tags-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"keep": "yes", "remove": "no"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	brokerARN := parseAccuracyMQ(t, desc)["brokerArn"].(string)

	// Delete the "remove" tag via query param.
	e := echo.New()
	req := httptest.NewRequest(http.MethodDelete, "/v1/tags/"+brokerARN+"?tagKeys=remove", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
	rec2 := httptest.NewRecorder()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	listRec := doAccuracyMQ(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	tags := parseAccuracyMQ(t, listRec)["tags"].(map[string]any)
	assert.Equal(t, "yes", tags["keep"])
	_, hasRemove := tags["remove"]
	assert.False(t, hasRemove, "deleted tag must not appear in ListTags")
}

// ── Issue 12: User lifecycle accuracy ────────────────────────────────────────

func TestAccuracy_DescribeUser_AllFieldsPresent(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "user-describe-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/myuser", map[string]any{
			"password":      "MySecurePass1!",
			"consoleAccess": true,
			"groups":        []string{"admins", "operators"},
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	desc := doAccuracyMQ(t, h, http.MethodGet,
		"/v1/brokers/"+brokerID+"/users/myuser", nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseAccuracyMQ(t, desc)
	assert.Equal(t, brokerID, out["brokerId"])
	assert.Equal(t, "myuser", out["username"])
	assert.Equal(t, true, out["consoleAccess"])

	groups, ok := out["groups"].([]any)
	require.True(t, ok, "groups must be present")
	assert.Len(t, groups, 2)
}

func TestAccuracy_ListUsers_SortedByUsername(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "sorted-users", mq.EngineTypeActiveMQ)

	for _, name := range []string{"zebra", "alpha", "middle"} {
		rec := doAccuracyMQ(t, h, http.MethodPost,
			"/v1/brokers/"+brokerID+"/users/"+name, map[string]any{
				"password": "SortedUsers123!",
			})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	listRec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	out := parseAccuracyMQ(t, listRec)
	users, ok := out["users"].([]any)
	require.True(t, ok && len(users) == 3)

	names := make([]string, len(users))
	for i, u := range users {
		names[i] = u.(map[string]any)["username"].(string)
	}
	assert.Equal(t, []string{"alpha", "middle", "zebra"}, names,
		"ListUsers must return users sorted by username")
}

func TestAccuracy_CreateUser_AlreadyExists_Returns409(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "dup-user-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/dupuser", map[string]any{
			"password": "DupUserPassword1!",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/dupuser", map[string]any{
			"password": "DupUserPassword1!",
		})
	assert.Equal(t, http.StatusConflict, rec2.Code,
		"duplicate CreateUser must return 409 Conflict")
}

func TestAccuracy_DeleteUser_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "del-user-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodDelete,
		"/v1/brokers/"+brokerID+"/users/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ── Issue 13: Broker engine types and instance options ────────────────────────

func TestAccuracy_DescribeBrokerEngineTypes_ReturnsActiveMQAndRabbitMQ(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/broker-engine-types", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	types, ok := out["brokerEngineTypes"].([]any)
	require.True(t, ok)

	engineTypeNames := make([]string, 0, len(types))
	for _, et := range types {
		etMap := et.(map[string]any)
		engineTypeNames = append(engineTypeNames, etMap["engineType"].(string))
	}

	assert.Contains(t, engineTypeNames, mq.EngineTypeActiveMQ)
	assert.Contains(t, engineTypeNames, mq.EngineTypeRabbitMQ)
}

func TestAccuracy_DescribeBrokerEngineTypes_FilterByEngineType(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet,
		"/v1/broker-engine-types?engineType=ACTIVEMQ", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	types := out["brokerEngineTypes"].([]any)
	require.Len(t, types, 1, "filter by ACTIVEMQ must return exactly one entry")
	assert.Equal(t, mq.EngineTypeActiveMQ, types[0].(map[string]any)["engineType"])
}

func TestAccuracy_DescribeBrokerEngineTypes_ActiveMQ_HasEngineVersions(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet,
		"/v1/broker-engine-types?engineType=ACTIVEMQ", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	types := out["brokerEngineTypes"].([]any)
	engineType := types[0].(map[string]any)
	versions, ok := engineType["engineVersions"].([]any)
	require.True(t, ok && len(versions) > 0, "ACTIVEMQ must have engine versions")

	// Each version must have a "name" field.
	for _, v := range versions {
		vMap := v.(map[string]any)
		assert.NotEmpty(t, vMap["name"], "engine version name must not be empty")
	}
}

func TestAccuracy_DescribeBrokerInstanceOptions_ActiveMQ(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet,
		"/v1/broker-instance-options?engineType=ACTIVEMQ", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	opts, ok := out["brokerInstanceOptions"].([]any)
	require.True(t, ok && len(opts) > 0, "must return ActiveMQ instance options")

	opt := opts[0].(map[string]any)
	assert.Equal(t, mq.EngineTypeActiveMQ, opt["engineType"])
	assert.NotEmpty(t, opt["hostInstanceType"])
	assert.NotEmpty(t, opt["storageType"])
	assert.NotEmpty(t, opt["availabilityZones"])
}

// ── Issue 14: DeleteBroker accuracy ──────────────────────────────────────────

func TestAccuracy_DeleteBroker_Returns200WithBrokerID(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "del-test-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodDelete, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, brokerID, out["brokerId"],
		"DeleteBroker must return the deleted broker ID")
}

func TestAccuracy_DeleteBroker_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodDelete, "/v1/brokers/nonexistent-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_DeleteBroker_NotInListAfterDelete(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "del-list-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodDelete, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	out := parseAccuracyMQ(t, listRec)
	summaries := out["brokerSummaries"].([]any)
	for _, s := range summaries {
		sm := s.(map[string]any)
		assert.NotEqual(t, brokerID, sm["brokerId"],
			"deleted broker must not appear in ListBrokers")
	}
}

// ── Issue 15: Promote accuracy ────────────────────────────────────────────────

func TestAccuracy_Promote_ValidModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode string
	}{
		{mq.PromoteModeFailover},
		{mq.PromoteModeSwitchover},
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyMQHandler(t)
			brokerID := createAccuracyBroker(t, h, "promote-"+strings.ToLower(tt.mode), mq.EngineTypeActiveMQ)

			rec := doAccuracyMQ(t, h, http.MethodPost,
				"/v1/brokers/"+brokerID+"/promote", map[string]any{
					"mode": tt.mode,
				})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracyMQ(t, rec)
			assert.Equal(t, brokerID, out["brokerId"])
		})
	}
}

func TestAccuracy_Promote_InvalidMode_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "promote-invalid", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/promote", map[string]any{
			"mode": "UPGRADE",
		})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ── Issue 16: RebootBroker state transition ───────────────────────────────────

func TestAccuracy_RebootBroker_TransitionsToRebooting(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "reboot-state", mq.EngineTypeActiveMQ)

	// Confirm initial state is RUNNING.
	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	assert.Equal(t, mq.BrokerStateRunning, parseAccuracyMQ(t, desc)["brokerState"])

	// Reboot the broker.
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// First DescribeBroker after reboot shows REBOOT_IN_PROGRESS.
	desc2 := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc2.Code)
	assert.Equal(t, mq.BrokerStateRebooting, parseAccuracyMQ(t, desc2)["brokerState"],
		"broker state must be REBOOT_IN_PROGRESS immediately after reboot")
}

func TestAccuracy_RebootBroker_SettlesBackToRunning(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	brokerID := createAccuracyBroker(t, h, "reboot-settle", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// First read: REBOOT_IN_PROGRESS.
	desc1 := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc1.Code)

	// Second read: RUNNING (stub promotes on next read after the first).
	desc2 := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, desc2.Code)
	assert.Equal(t, mq.BrokerStateRunning, parseAccuracyMQ(t, desc2)["brokerState"],
		"broker must return to RUNNING state after the reboot transition is observed")
}

// ── Issue 17: DeleteConfiguration accuracy ────────────────────────────────────

func TestAccuracy_DeleteConfiguration_Returns200(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	configID := createAccuracyConfig(t, h, "del-cfg-test", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodDelete, "/v1/configurations/"+configID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAccuracy_DeleteConfiguration_NotFoundAfterDelete(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	configID := createAccuracyConfig(t, h, "del-cfg-gone", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodDelete, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	assert.Equal(t, http.StatusNotFound, desc.Code)
}

// ── Issue 18: ListBrokers response structure ──────────────────────────────────

func TestAccuracy_ListBrokers_SummaryContainsRequiredFields(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	createAccuracyBroker(t, h, "summary-fields-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	summaries := out["brokerSummaries"].([]any)
	require.Len(t, summaries, 1)

	summary := summaries[0].(map[string]any)
	requiredFields := []string{
		"brokerArn", "brokerId", "brokerName", "brokerState",
		"deploymentMode", "engineType", "hostInstanceType", "created",
	}

	for _, f := range requiredFields {
		assert.Contains(t, summary, f, "broker summary must contain %q", f)
	}
}

func TestAccuracy_ListBrokers_SortedByName(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	for _, name := range []string{"broker-z", "broker-a", "broker-m"} {
		createAccuracyBroker(t, h, name, mq.EngineTypeActiveMQ)
	}

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	summaries := out["brokerSummaries"].([]any)
	require.Len(t, summaries, 3)

	names := make([]string, len(summaries))
	for i, s := range summaries {
		names[i] = s.(map[string]any)["brokerName"].(string)
	}
	assert.Equal(t, []string{"broker-a", "broker-m", "broker-z"}, names,
		"ListBrokers must return brokers sorted by name")
}

func TestAccuracy_ListBrokers_Empty_ReturnsBrokerSummariesKey(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	_, hasKey := out["brokerSummaries"]
	assert.True(t, hasKey, "brokerSummaries key must always be present")
}

// ── Issue 19: Configuration create accuracy ───────────────────────────────────

func TestAccuracy_CreateConfiguration_DuplicateName_Returns409(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	createAccuracyConfig(t, h, "dup-cfg", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "dup-cfg",
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusConflict, rec.Code,
		"duplicate configuration name must return 409")
}

func TestAccuracy_CreateConfiguration_MissingName_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CreateConfiguration_InvalidEngineType_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "bad-engine-cfg",
		"engineType": "KAFKA",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ── Issue 20: Snapshot / Restore accuracy ────────────────────────────────────

func TestAccuracy_Snapshot_RestorePreservesEndpoints(t *testing.T) {
	t.Parallel()

	b1 := newAccuracyMQBackend(t)
	br, err := b1.CreateBroker(
		"snap-endpoint-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, br.BrokerInstances)

	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mq.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	require.NotEmpty(t, restored.BrokerInstances,
		"broker instances must survive snapshot/restore")
	assert.Equal(t, br.BrokerInstances[0].Endpoints, restored.BrokerInstances[0].Endpoints)
}

func TestAccuracy_Snapshot_RestorePreservesDeploymentMode(t *testing.T) {
	t.Parallel()

	b1 := newAccuracyMQBackend(t)
	br, err := b1.CreateBroker(
		"snap-mode-broker", mq.DeploymentModeActiveStandby,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, mq.DeploymentModeActiveStandby, br.DeploymentMode)

	snap := b1.Snapshot(t.Context())
	b2 := mq.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, mq.DeploymentModeActiveStandby, restored.DeploymentMode)
}
