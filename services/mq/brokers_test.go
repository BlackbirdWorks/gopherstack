package mq_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestBrokerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		brokerName string
		engineType string
		wantStatus int
	}{
		{
			name:       "create_activemq",
			brokerName: "my-activemq-broker",
			engineType: "ACTIVEMQ",
			wantStatus: http.StatusOK,
		},
		{
			name:       "create_rabbitmq",
			brokerName: "my-rabbitmq-broker",
			engineType: "RABBITMQ",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create broker.
			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName":       tt.brokerName,
				"engineType":       tt.engineType,
				"hostInstanceType": "mq.m5.large",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			createResp := parseResponse(t, rec)
			assert.NotEmpty(t, createResp["brokerId"])
			assert.NotEmpty(t, createResp["brokerArn"])

			brokerID := createResp["brokerId"].(string)

			// Describe broker.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			descResp := parseResponse(t, rec)
			assert.Equal(t, tt.brokerName, descResp["brokerName"])
			assert.Equal(t, "RUNNING", descResp["brokerState"])

			// List brokers.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			summaries, ok := parseResponse(t, rec)["brokerSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, 1)

			// Delete broker.
			rec = doRequest(t, h, http.MethodDelete, "/v1/brokers/"+brokerID, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify deletion.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestBrokerNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		method string
		path   string
	}{
		{
			name:   "describe_nonexistent",
			method: http.MethodGet,
			path:   "/v1/brokers/nonexistent-id",
		},
		{
			name:   "delete_nonexistent",
			method: http.MethodDelete,
			path:   "/v1/brokers/nonexistent-id",
		},
		{
			name:   "reboot_nonexistent",
			method: http.MethodPost,
			path:   "/v1/brokers/nonexistent-id/reboot",
		},
		{
			name:   "update_nonexistent_broker",
			method: http.MethodPut,
			path:   "/v1/brokers/nonexistent-id",
			body:   map[string]any{"engineVersion": "5.16.7"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestCreateBroker_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing_broker_name",
			body:       map[string]any{"engineType": "ACTIVEMQ"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_engine_type",
			body:       map[string]any{"brokerName": "my-broker"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate_broker",
			body:       map[string]any{"brokerName": "my-broker", "engineType": "ACTIVEMQ"},
			wantStatus: http.StatusConflict,
		},
		{
			name:       "invalid_engine_type",
			body:       map[string]any{"brokerName": "bad-broker", "engineType": "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_broker" {
				rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
					"brokerName": "my-broker",
					"engineType": "ACTIVEMQ",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCreateBroker_InvalidEngineType_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
	}{
		{name: "empty", engineType: ""},
		{name: "invalid", engineType: "KAFKA"},
		{name: "lowercase", engineType: "activemq"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			_, err := b.CreateBroker(
				"my-broker", mq.DeploymentModeSingleInstance,
				tt.engineType, "", "",
				false, false, nil, nil, nil, nil,
			)
			require.ErrorIs(t, err, mq.ErrValidation)
		})
	}
}

func TestCreateBroker_NameValidation(t *testing.T) {
	t.Parallel()

	t.Run("valid_name_formats", func(t *testing.T) {
		t.Parallel()

		tests := []struct{ name, brokerName string }{
			{"alphanumeric_only", "mybroker"},
			{"with_hyphens", "my-broker-123"},
			{"with_underscores", "my_broker_123"},
			{"mixed", "My-Broker_01"},
			{"single_char", "x"},
			{"starts_with_digit", "1broker"},
			{"exactly_50_chars", strings.Repeat("a", 50)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				h := newTestHandler(t)
				rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
					"brokerName": tt.brokerName,
					"engineType": mq.EngineTypeActiveMQ,
				})
				assert.Equal(t, http.StatusOK, rec.Code,
					"valid broker name %q should succeed: %s", tt.brokerName, rec.Body.String())
			})
		}
	})

	t.Run("invalid_name_formats", func(t *testing.T) {
		t.Parallel()

		tests := []struct{ name, brokerName string }{
			{"too_long", strings.Repeat("a", 51)},
			{"starts_with_hyphen", "-invalid-name"},
			{"spaces", "broker name"},
			{"slash", "broker/name"},
			{"at_sign", "broker@name"},
			{"dot", "broker.name"},
			{"colon", "broker:name"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				h := newTestHandler(t)
				rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
					"brokerName": tt.brokerName,
					"engineType": mq.EngineTypeActiveMQ,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code,
					"broker name %q must return 400", tt.brokerName)
				assert.Equal(t, "BadRequestException", parseResponse(t, rec)["__type"])
			})
		}
	})
}

func TestCreateBroker_NameValidation_Backend(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

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

func TestCreateBroker_DeploymentMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		engineType     string
		deploymentMode string
		wantStatus     int
	}{
		{"invalid_mode", mq.EngineTypeActiveMQ, "INVALID_MODE", http.StatusBadRequest},
		{
			"rabbitmq_rejects_active_standby", mq.EngineTypeRabbitMQ,
			mq.DeploymentModeActiveStandby, http.StatusBadRequest,
		},
		{"activemq_rejects_cluster", mq.EngineTypeActiveMQ, mq.DeploymentModeCluster, http.StatusBadRequest},
		{"rabbitmq_accepts_cluster", mq.EngineTypeRabbitMQ, mq.DeploymentModeCluster, http.StatusOK},
		{"activemq_accepts_active_standby", mq.EngineTypeActiveMQ, mq.DeploymentModeActiveStandby, http.StatusOK},
		{"activemq_single_instance", mq.EngineTypeActiveMQ, mq.DeploymentModeSingleInstance, http.StatusOK},
		{"rabbitmq_single_instance", mq.EngineTypeRabbitMQ, mq.DeploymentModeSingleInstance, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName":     tt.name,
				"engineType":     tt.engineType,
				"deploymentMode": tt.deploymentMode,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func TestCreateBroker_DeploymentMode_Backend(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

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

func TestCreateBroker_EndpointContainsBrokerID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "endpoint-test", mq.EngineTypeActiveMQ)
	out := describeTestBroker(t, h, brokerID)

	instances, ok := out["brokerInstances"].([]any)
	require.True(t, ok && len(instances) > 0, "brokerInstances must be non-empty")

	instance := instances[0].(map[string]any)
	endpoints, ok := instance["endpoints"].([]any)
	require.True(t, ok && len(endpoints) > 0, "endpoints must be non-empty")

	endpoint := endpoints[0].(string)
	assert.Contains(t, endpoint, brokerID,
		"endpoint must contain the broker ID, not just the name")
}

func TestCreateBroker_EndpointFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
		wantScheme string
		wantPort   string
	}{
		{"activemq", mq.EngineTypeActiveMQ, "ssl://", ":61617"},
		{"rabbitmq", mq.EngineTypeRabbitMQ, "amqps://", ":5671"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			brokerID := createTestBroker(t, h, tt.name+"-endpoint", tt.engineType)
			out := describeTestBroker(t, h, brokerID)

			instances := out["brokerInstances"].([]any)
			instance := instances[0].(map[string]any)
			endpoints := instance["endpoints"].([]any)
			endpoint := endpoints[0].(string)

			assert.True(t, strings.HasPrefix(endpoint, tt.wantScheme),
				"%s endpoint must use %s scheme, got %s", tt.engineType, tt.wantScheme, endpoint)
			assert.True(t, strings.HasSuffix(endpoint, tt.wantPort),
				"%s endpoint must use port %s, got %s", tt.engineType, tt.wantPort, endpoint)
			assert.Contains(t, endpoint, "us-east-1", "endpoint must contain the region")
		})
	}
}

func TestCreateBroker_Endpoint_RegionFromBackend(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("111122223333", "eu-west-1")
	h := mq.NewHandler(b)

	brokerID := createTestBroker(t, h, "eu-broker", mq.EngineTypeActiveMQ)
	out := describeTestBroker(t, h, brokerID)

	instances := out["brokerInstances"].([]any)
	instance := instances[0].(map[string]any)
	endpoints := instance["endpoints"].([]any)
	endpoint := endpoints[0].(string)

	assert.Contains(t, endpoint, "eu-west-1",
		"endpoint must use the backend region, got %s", endpoint)
}

func TestUpdateBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "update-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"engineVersion": "5.16.7",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, brokerID, parseResponse(t, rec)["brokerId"])
}

func TestUpdateBroker_ResponseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantValue any
		body      map[string]any
		name      string
		fieldName string
	}{
		{
			name: "engine_version", body: map[string]any{"engineVersion": "5.16.7"},
			fieldName: "engineVersion", wantValue: "5.16.7",
		},
		{
			name: "host_instance_type", body: map[string]any{"hostInstanceType": "mq.m5.xlarge"},
			fieldName: "hostInstanceType", wantValue: "mq.m5.xlarge",
		},
		{
			name: "auto_minor_version_upgrade", body: map[string]any{"autoMinorVersionUpgrade": true},
			fieldName: "autoMinorVersionUpgrade", wantValue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			brokerID := createTestBroker(t, h, "update-"+tt.name, mq.EngineTypeActiveMQ)

			rec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseResponse(t, rec)
			assert.Equal(t, brokerID, out["brokerId"], "UpdateBroker must return brokerId")
			assert.Equal(t, tt.wantValue, out[tt.fieldName])
		})
	}

	t.Run("security_groups", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		brokerID := createTestBroker(t, h, "update-sg", mq.EngineTypeActiveMQ)

		sgs := []string{"sg-aabbccdd", "sg-11223344"}
		rec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
			"securityGroups": sgs,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		gotSGs, ok := parseResponse(t, rec)["securityGroups"].([]any)
		require.True(t, ok, "UpdateBroker response must include securityGroups")
		require.Len(t, gotSGs, 2)
		assert.Equal(t, "sg-aabbccdd", gotSGs[0])
	})
}

func TestUpdateBroker_UpdatesReflectedInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "update-reflect", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"engineVersion": "5.18.3",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descOut := describeTestBroker(t, h, brokerID)
	assert.Equal(t, "5.18.3", descOut["engineVersion"],
		"updated engineVersion must appear in DescribeBroker")
}

func TestPromote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "promote_failover",
			mode:       "FAILOVER",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "promote_switchover",
			mode:       "SWITCHOVER",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "promote_nonexistent_broker",
			mode:       "FAILOVER",
			preCreate:  false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_mode",
			mode:       "UPGRADE",
			preCreate:  true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			brokerID := "nonexistent-broker"
			if tt.preCreate {
				brokerID = createTestBroker(t, h, "promotable-"+tt.name, mq.EngineTypeActiveMQ)
			}

			rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/promote", map[string]any{
				"mode": tt.mode,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, brokerID, parseResponse(t, rec)["brokerId"])
			}
		})
	}
}

func TestPromote_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "promote-invalid-body-broker", mq.EngineTypeActiveMQ)

	rec := doRawJSONRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/promote", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPromote_Backend_InvalidMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
	}{
		{name: "empty", mode: ""},
		{name: "invalid", mode: "UPGRADE"},
		{name: "lowercase", mode: "failover"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			_, err := b.Promote("some-broker", tt.mode)
			require.ErrorIs(t, err, mq.ErrValidation)
		})
	}
}

func TestPromote_Backend_ValidModeBrokerNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
	}{
		{name: "failover", mode: mq.PromoteModeFailover},
		{name: "switchover", mode: mq.PromoteModeSwitchover},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			_, err := b.Promote("nonexistent", tt.mode)
			require.ErrorIs(t, err, mq.ErrNotFound)
		})
	}
}

func TestCreateBrokerWithOptions_StorageAndIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		engine      string
		opts        *mq.CreateBrokerOptions
		wantStorage string
		wantErr     bool
	}{
		{
			name:        "rabbitmq_default_ebs",
			engine:      mq.EngineTypeRabbitMQ,
			opts:        nil,
			wantStorage: mq.StorageTypeEBS,
		},
		{
			name:        "activemq_default_efs",
			engine:      mq.EngineTypeActiveMQ,
			opts:        nil,
			wantStorage: mq.StorageTypeEFS,
		},
		{
			name:    "rabbitmq_rejects_efs",
			engine:  mq.EngineTypeRabbitMQ,
			opts:    &mq.CreateBrokerOptions{StorageType: mq.StorageTypeEFS},
			wantErr: true,
		},
		{
			name:        "activemq_accepts_ebs",
			engine:      mq.EngineTypeActiveMQ,
			opts:        &mq.CreateBrokerOptions{StorageType: mq.StorageTypeEBS},
			wantStorage: mq.StorageTypeEBS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			br, err := b.CreateBrokerWithOptions(
				"b-"+tt.name, mq.DeploymentModeSingleInstance, tt.engine,
				"", "", false, false, nil, nil, nil, nil, tt.opts,
			)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStorage, br.StorageType)
		})
	}
}

func TestCreateBroker_CreatorRequestIDIdempotency(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	first, err := b.CreateBrokerWithOptions(
		"idem-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{CreatorRequestID: "req-1"},
	)
	require.NoError(t, err)

	second, err := b.CreateBrokerWithOptions(
		"idem-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{CreatorRequestID: "req-1"},
	)
	require.NoError(t, err)
	assert.Equal(t, first.BrokerID, second.BrokerID)

	// Same name, no idempotency token => duplicate-name error.
	_, err = b.CreateBrokerWithOptions(
		"idem-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil, nil,
	)
	require.Error(t, err)
}

func TestDefaultHostInstanceType(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	br, err := b.CreateBroker(
		"default-instance-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "", // empty hostInstanceType
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "mq.m5.large", br.HostInstanceType)
}

func TestBrokerUsersInResponseAreSorted(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	br, err := b.CreateBroker(
		"sorted-users-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil,
		[]*mq.User{
			{Username: "zebra"},
			{Username: "alpha"},
			{Username: "middle"},
		},
		nil,
	)
	require.NoError(t, err)

	h := mq.NewHandler(b)
	out := describeTestBroker(t, h, br.BrokerID)

	usersRaw, ok := out["users"].([]any)
	require.True(t, ok)
	require.Len(t, usersRaw, 3)

	names := make([]string, len(usersRaw))
	for i, u := range usersRaw {
		names[i] = u.(map[string]any)["username"].(string)
	}

	assert.Equal(t, []string{"alpha", "middle", "zebra"}, names)
}

func TestCreatorRequestID_SameID_ReturnsSameBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"brokerName":       "idem-http-broker",
		"engineType":       mq.EngineTypeActiveMQ,
		"creatorRequestId": "unique-request-id-123",
	}

	rec1 := doRequest(t, h, http.MethodPost, "/v1/brokers", body)
	require.Equal(t, http.StatusOK, rec1.Code)
	id1 := parseResponse(t, rec1)["brokerId"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/v1/brokers", body)
	require.Equal(t, http.StatusOK, rec2.Code)
	id2 := parseResponse(t, rec2)["brokerId"].(string)

	assert.Equal(t, id1, id2, "same CreatorRequestId must return same broker ID")
}

func TestCreatorRequestID_DifferentID_CreatesDifferentBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec1 := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":       "idem-diff-a",
		"engineType":       mq.EngineTypeActiveMQ,
		"creatorRequestId": "req-aaa",
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	id1 := parseResponse(t, rec1)["brokerId"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":       "idem-diff-b",
		"engineType":       mq.EngineTypeActiveMQ,
		"creatorRequestId": "req-bbb",
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	id2 := parseResponse(t, rec2)["brokerId"].(string)

	assert.NotEqual(t, id1, id2)
}

func TestCreateBroker_ReturnsHTTP200(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "status-code-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "CreateBroker must return 200, not 202: %s", rec.Body.String())
}

// TestWireConstants pins the wire-visible string values of every MQ enum
// against the real aws-sdk-go-v2/service/mq typed constants, so an
// accidental rename or casing change is caught immediately.
func TestWireConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"deployment_mode_single_instance", mq.DeploymentModeSingleInstance, "SINGLE_INSTANCE"},
		{"deployment_mode_active_standby", mq.DeploymentModeActiveStandby, "ACTIVE_STANDBY_MULTI_AZ"},
		{"deployment_mode_cluster", mq.DeploymentModeCluster, "CLUSTER_MULTI_AZ"},
		{"promote_mode_failover", mq.PromoteModeFailover, "FAILOVER"},
		{"promote_mode_switchover", mq.PromoteModeSwitchover, "SWITCHOVER"},
		{"storage_type_efs", mq.StorageTypeEFS, "EFS"},
		{"storage_type_ebs", mq.StorageTypeEBS, "EBS"},
		{"broker_state_running", mq.BrokerStateRunning, "RUNNING"},
		{"broker_state_creating", mq.BrokerStateCreating, "CREATION_IN_PROGRESS"},
		{"broker_state_deleting", mq.BrokerStateDeleting, "DELETION_IN_PROGRESS"},
		{"broker_state_rebooting", mq.BrokerStateRebooting, "REBOOT_IN_PROGRESS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

// TestStorageType_WireValueIsUppercase verifies that DescribeBroker echoes
// storageType back using the SDK's uppercase enum casing (see
// aws-sdk-go-v2/service/mq/types.BrokerStorageTypeEfs/BrokerStorageTypeEbs).
func TestStorageType_WireValueIsUppercase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		engineType  string
		wantStorage string
	}{
		{name: "activemq_default", engineType: mq.EngineTypeActiveMQ, wantStorage: "EFS"},
		{name: "rabbitmq_default", engineType: mq.EngineTypeRabbitMQ, wantStorage: "EBS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			brokerID := createTestBroker(t, h, "storage-type-"+tt.name, tt.engineType)
			out := describeTestBroker(t, h, brokerID)
			assert.Equal(t, tt.wantStorage, out["storageType"])
		})
	}
}

func TestStorageType_Validation(t *testing.T) {
	t.Parallel()

	t.Run("rabbitmq_rejects_efs", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
			"brokerName":  "rmq-efs-broker",
			"engineType":  mq.EngineTypeRabbitMQ,
			"storageType": mq.StorageTypeEFS,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code,
			"RabbitMQ with EFS storageType must return 400")
	})

	t.Run("activemq_accepts_ebs", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
			"brokerName":  "amq-ebs-broker",
			"engineType":  mq.EngineTypeActiveMQ,
			"storageType": mq.StorageTypeEBS,
		})
		require.Equal(t, http.StatusOK, rec.Code, "ActiveMQ with EBS must succeed: %s", rec.Body.String())

		brokerID := parseResponse(t, rec)["brokerId"].(string)
		out := describeTestBroker(t, h, brokerID)
		assert.Equal(t, mq.StorageTypeEBS, out["storageType"])
	})

	t.Run("rabbitmq_default_ebs", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		brokerID := createTestBroker(t, h, "rmq-default-storage", mq.EngineTypeRabbitMQ)
		out := describeTestBroker(t, h, brokerID)
		assert.Equal(t, mq.StorageTypeEBS, out["storageType"],
			"RabbitMQ default storageType must be ebs")
	})

	t.Run("activemq_default_efs", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		brokerID := createTestBroker(t, h, "amq-default-storage", mq.EngineTypeActiveMQ)
		out := describeTestBroker(t, h, brokerID)
		assert.Equal(t, mq.StorageTypeEFS, out["storageType"],
			"ActiveMQ default storageType must be efs")
	})
}
