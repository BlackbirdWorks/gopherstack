package mq_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// ── CreateBroker / CreateTags HTTP status codes ────────────────────────────────
//
// Real Amazon MQ's CreateBroker returns HTTP 200 (not 202) despite being an
// asynchronous operation, and CreateTags returns HTTP 204 (not 200). See the
// mq-2017-11-27 service-2.json model: CreateBroker's "http" trait specifies
// responseCode 200, CreateTags specifies 204.

func Test_CreateBroker_ReturnsHTTP200(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "status-code-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "CreateBroker must return 200, not 202: %s", rec.Body.String())
}

func Test_CreateTags_ReturnsHTTP204(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "create-tags-status-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := parseAccuracyMQ(t, rec)["brokerArn"].(string)

	tagRec := doAccuracyMQ(t, h, http.MethodPost, "/v1/tags/"+brokerARN, map[string]any{
		"tags": map[string]string{"env": "prod"},
	})
	assert.Equal(t, http.StatusNoContent, tagRec.Code, "CreateTags must return 204, not 200: %s", tagRec.Body.String())
}

// ── BrokerStorageType enum casing ───────────────────────────────────────────────
//
// AWS MQ's BrokerStorageType enum is uppercase ("EFS"/"EBS"); see
// aws-sdk-go-v2/service/mq/types.BrokerStorageTypeEfs/BrokerStorageTypeEbs. A
// lowercase value round-trips through JSON fine but silently fails any
// client-side comparison against the SDK's typed enum constants.

func Test_StorageType_WireValueIsUppercase(t *testing.T) {
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

			h := newAccuracyMQHandler(t)
			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "storage-type-" + tt.name,
				"engineType": tt.engineType,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

			desc := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
			require.Equal(t, http.StatusOK, desc.Code)
			out := parseAccuracyMQ(t, desc)
			assert.Equal(t, tt.wantStorage, out["storageType"])
		})
	}
}

func Test_StorageType_Constants_MatchSDKEnumCasing(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "EFS", mq.StorageTypeEFS)
	assert.Equal(t, "EBS", mq.StorageTypeEBS)
}

// ── ActiveMQ password validation ────────────────────────────────────────────────
//
// Per the CreateUserRequest.Password docs: "must not contain commas, colons,
// or equal signs (,:=)". Only commas were previously rejected.

func Test_CreateUser_PasswordValidation_RejectsColonAndEquals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		password   string
		wantStatus int
	}{
		{name: "valid_password", password: "GoodPassword123", wantStatus: http.StatusOK},
		{name: "comma_rejected", password: "Bad,Password123", wantStatus: http.StatusBadRequest},
		{name: "colon_rejected", password: "Bad:Password123", wantStatus: http.StatusBadRequest},
		{name: "equals_rejected", password: "Bad=Password123", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyMQHandler(t)
			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "pwd-validation-" + tt.name,
				"engineType": mq.EngineTypeActiveMQ,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

			userRec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/tester", map[string]any{
				"password": tt.password,
			})
			assert.Equal(t, tt.wantStatus, userRec.Code, "password=%q: %s", tt.password, userRec.Body.String())
		})
	}
}

// ── ActiveMQ username charset ───────────────────────────────────────────────────
//
// Per the User.Username docs, ActiveMQ usernames allow alphanumeric
// characters, dashes, periods, underscores, and tildes (- . _ ~). Periods and
// tildes were previously rejected, causing gopherstack to reject usernames a
// real AWS MQ broker would accept.

func Test_CreateUser_UsernameCharset_AllowsPeriodAndTilde(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		username   string
		wantStatus int
	}{
		{name: "period_allowed", username: "john.doe", wantStatus: http.StatusOK},
		{name: "tilde_allowed", username: "svc~user", wantStatus: http.StatusOK},
		{name: "hyphen_underscore_still_allowed", username: "svc-user_1", wantStatus: http.StatusOK},
		{name: "invalid_char_still_rejected", username: "bad$user", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyMQHandler(t)
			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "username-charset-" + tt.name,
				"engineType": mq.EngineTypeActiveMQ,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

			userPath := "/v1/brokers/" + brokerID + "/users/" + tt.username
			userRec := doAccuracyMQ(t, h, http.MethodPost, userPath, map[string]any{
				"password": "ValidPassword123",
			})
			assert.Equal(t, tt.wantStatus, userRec.Code, "username=%q: %s", tt.username, userRec.Body.String())
		})
	}
}

// ── UpdateBroker response: data replication fields ──────────────────────────────
//
// Real UpdateBrokerOutput includes dataReplicationMode, dataReplicationMetadata,
// pendingDataReplicationMode, and pendingDataReplicationMetadata (see
// aws-sdk-go-v2/service/mq/api_op_UpdateBroker.go). These were previously
// omitted from the handler's JSON response entirely.

func Test_UpdateBroker_ResponseIncludesDataReplicationMode(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "crdr-update-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	updateRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"dataReplicationMode": "CRDR",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	out := parseAccuracyMQ(t, updateRec)
	assert.Equal(t, "CRDR", out["dataReplicationMode"],
		"UpdateBroker response must echo back dataReplicationMode: %s", updateRec.Body.String())
}
