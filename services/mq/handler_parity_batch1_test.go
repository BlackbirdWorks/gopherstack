package mq_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newBatch1Handler(t *testing.T) *mq.Handler {
	t.Helper()

	return mq.NewHandler(mq.NewInMemoryBackend("123456789012", "us-east-1"))
}

func newBatch1Backend(t *testing.T) *mq.InMemoryBackend {
	t.Helper()

	return mq.NewInMemoryBackend("123456789012", "us-east-1")
}

func createBatch1Broker(t *testing.T, h *mq.Handler, name, engineType string) string {
	t.Helper()

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": name,
		"engineType": engineType,
	})
	require.Equal(t, http.StatusAccepted, rec.Code, "CreateBroker %s: %s", name, rec.Body.String())

	return parseAccuracyMQ(t, rec)["brokerId"].(string)
}

func createBatch1Config(t *testing.T, h *mq.Handler, name string) string {
	t.Helper()

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       name,
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code, "CreateConfiguration %s: %s", name, rec.Body.String())

	return parseAccuracyMQ(t, rec)["id"].(string)
}

func describeBatch1Broker(t *testing.T, h *mq.Handler, brokerID string) map[string]any {
	t.Helper()

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	return parseAccuracyMQ(t, rec)
}

// ── EncryptionOptions ─────────────────────────────────────────────────────────

func TestBatch1_EncryptionOptions_KMSKey_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "enc-kms-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"encryptionOptions": map[string]any{
			"kmsKeyId":       "arn:aws:kms:us-east-1:123456789012:key/test-key-id",
			"useAwsOwnedKey": false,
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	enc, ok := out["encryptionOptions"].(map[string]any)
	require.True(t, ok, "encryptionOptions must be present in DescribeBroker")
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/test-key-id", enc["kmsKeyId"])
	assert.Equal(t, false, enc["useAwsOwnedKey"])
}

func TestBatch1_EncryptionOptions_UseAwsOwnedKey_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "enc-owned-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"encryptionOptions": map[string]any{
			"useAwsOwnedKey": true,
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	enc, ok := out["encryptionOptions"].(map[string]any)
	require.True(t, ok, "encryptionOptions must be present")
	assert.Equal(t, true, enc["useAwsOwnedKey"])
}

func TestBatch1_EncryptionOptions_AbsentWhenNotSpecified(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "no-enc-broker", mq.EngineTypeActiveMQ)
	out := describeBatch1Broker(t, h, brokerID)

	_, hasEnc := out["encryptionOptions"]
	assert.False(t, hasEnc, "encryptionOptions must be absent when not specified")
}

func TestBatch1_EncryptionOptions_Backend_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	enc := &mq.EncryptionOptions{
		KMSKeyID:       "key-abc123",
		UseAWSOwnedKey: false,
	}
	br, err := b.CreateBrokerWithOptions(
		"enc-backend", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{EncryptionOptions: enc},
	)
	require.NoError(t, err)
	require.NotNil(t, br.EncryptionOptions)
	assert.Equal(t, "key-abc123", br.EncryptionOptions.KMSKeyID)

	got, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	require.NotNil(t, got.EncryptionOptions)
	assert.Equal(t, "key-abc123", got.EncryptionOptions.KMSKeyID)
}

func TestBatch1_EncryptionOptions_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"enc-snap-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{
			EncryptionOptions: &mq.EncryptionOptions{
				KMSKeyID:       "snap-key",
				UseAWSOwnedKey: false,
			},
		},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mq.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	require.NotNil(t, restored.EncryptionOptions)
	assert.Equal(t, "snap-key", restored.EncryptionOptions.KMSKeyID)
}

// ── LoggingOptions / LogsSummary ──────────────────────────────────────────────

func TestBatch1_Logs_CreateBroker_GeneralLogGroup_Present(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "logs-gen-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"logs": map[string]any{
			"general": true,
			"audit":   false,
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	logs, ok := out["logs"].(map[string]any)
	require.True(t, ok, "logs must be present in DescribeBroker")
	assert.Equal(t, true, logs["general"])
	assert.Equal(t, false, logs["audit"])
	assert.NotEmpty(t, logs["generalLogGroup"], "generalLogGroup must be set")
}

func TestBatch1_Logs_CreateBroker_AuditLogGroup_ContainsBrokerID(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "logs-audit-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"logs": map[string]any{
			"general": true,
			"audit":   true,
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	logs := out["logs"].(map[string]any)
	auditLogGroup := logs["auditLogGroup"].(string)
	assert.Contains(t, auditLogGroup, brokerID, "auditLogGroup must contain broker ID")
	assert.True(t, strings.HasPrefix(auditLogGroup, "/aws/amazonmq/broker/"),
		"auditLogGroup must start with /aws/amazonmq/broker/")
}

func TestBatch1_Logs_LogGroupName_Format(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "logs-fmt-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"logs": map[string]any{
			"general": true,
			"audit":   true,
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)
	logs := out["logs"].(map[string]any)

	generalLogGroup := logs["generalLogGroup"].(string)
	assert.Equal(t, "/aws/amazonmq/broker/"+brokerID+"/general", generalLogGroup)
}

func TestBatch1_Logs_UpdateBroker_UpdatesLogsSummary(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "logs-upd-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"logs": map[string]any{
			"general": true,
			"audit":   false,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := describeBatch1Broker(t, h, brokerID)
	logs, ok := out["logs"].(map[string]any)
	require.True(t, ok, "logs must be present after UpdateBroker")
	assert.Equal(t, true, logs["general"])
	assert.NotEmpty(t, logs["generalLogGroup"])
}

func TestBatch1_Logs_Backend_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"logs-be-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{
			Logs: &mq.Logs{General: true, Audit: true},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, br.Logs)
	assert.True(t, br.Logs.General)
	assert.True(t, br.Logs.Audit)
	require.NotNil(t, br.LogsSummary)
	assert.True(t, br.LogsSummary.General)
}

func TestBatch1_Logs_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"logs-snap-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{
			Logs: &mq.Logs{General: true, Audit: false},
		},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	b2 := mq.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	require.NotNil(t, restored.Logs)
	assert.True(t, restored.Logs.General)
	require.NotNil(t, restored.LogsSummary)
	assert.True(t, restored.LogsSummary.General)
}

// ── AuthenticationStrategy ────────────────────────────────────────────────────

func TestBatch1_AuthStrategy_Simple_CreateBroker(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "auth-simple-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "SIMPLE",
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, "SIMPLE", out["authenticationStrategy"])
}

func TestBatch1_AuthStrategy_LDAP_CreateBroker(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "auth-ldap-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "LDAP",
		"ldapServerMetadata": map[string]any{
			"hosts":              []string{"ldap.example.com:389"},
			"roleBase":           "ou=roles,dc=example,dc=com",
			"roleSearchMatching": "(member=uid={0},ou=users,dc=example,dc=com)",
			"userBase":           "ou=users,dc=example,dc=com",
			"userSearchMatching": "(uid={0})",
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, "LDAP", out["authenticationStrategy"])
}

func TestBatch1_AuthStrategy_UpdateBroker_ChangesStrategy(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "auth-upd-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "SIMPLE",
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"authenticationStrategy": "LDAP",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, "LDAP", out["authenticationStrategy"])
}

func TestBatch1_AuthStrategy_InDescribeBrokerResponse(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"auth-describe-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{AuthenticationStrategy: "SIMPLE"},
	)
	require.NoError(t, err)

	got, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "SIMPLE", got.AuthenticationStrategy)
}

func TestBatch1_AuthStrategy_UpdateBroker_Response_Contains_Strategy(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "auth-resp-broker", mq.EngineTypeActiveMQ)

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"authenticationStrategy": "SIMPLE",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	updOut := parseAccuracyMQ(t, updRec)
	assert.Equal(t, "SIMPLE", updOut["authenticationStrategy"])
}

// ── LdapServerMetadata ────────────────────────────────────────────────────────

func TestBatch1_LDAP_CreateBroker_Hosts_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "ldap-hosts-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "LDAP",
		"ldapServerMetadata": map[string]any{
			"hosts":              []string{"ldap1.example.com:389", "ldap2.example.com:389"},
			"roleBase":           "ou=roles,dc=example,dc=com",
			"roleSearchMatching": "(member={0})",
			"userBase":           "ou=users,dc=example,dc=com",
			"userSearchMatching": "(uid={0})",
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	ldap, ok := out["ldapServerMetadata"].(map[string]any)
	require.True(t, ok, "ldapServerMetadata must be in DescribeBroker")

	hosts := ldap["hosts"].([]any)
	assert.Len(t, hosts, 2)
	assert.Equal(t, "ldap1.example.com:389", hosts[0])
}

func TestBatch1_LDAP_ServiceAccountPassword_Not_Exposed(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "ldap-pwd-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "LDAP",
		"ldapServerMetadata": map[string]any{
			"hosts":                  []string{"ldap.example.com:389"},
			"roleBase":               "ou=roles,dc=example,dc=com",
			"roleSearchMatching":     "(member={0})",
			"userBase":               "ou=users,dc=example,dc=com",
			"userSearchMatching":     "(uid={0})",
			"serviceAccountUsername": "cn=admin,dc=example,dc=com",
			"serviceAccountPassword": "super-secret-password",
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	ldap := out["ldapServerMetadata"].(map[string]any)
	_, hasPassword := ldap["serviceAccountPassword"]
	assert.False(t, hasPassword, "serviceAccountPassword must NOT be exposed in DescribeBroker")

	username, hasUsername := ldap["serviceAccountUsername"]
	assert.True(t, hasUsername, "serviceAccountUsername must be exposed")
	assert.Equal(t, "cn=admin,dc=example,dc=com", username)
}

func TestBatch1_LDAP_Backend_ServiceAccountPassword_Not_In_JSON(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"ldap-json-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{
			AuthenticationStrategy: "LDAP",
			LdapServerMetadata: &mq.LdapServerMetadata{
				Hosts:                  []string{"ldap.example.com:389"},
				RoleBase:               "ou=roles",
				RoleSearchMatching:     "(member={0})",
				UserBase:               "ou=users",
				UserSearchMatching:     "(uid={0})",
				ServiceAccountUsername: "admin",
				ServiceAccountPassword: "topsecret",
			},
		},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	assert.NotContains(t, string(snap), "topsecret",
		"serviceAccountPassword must not appear in JSON snapshot")
	assert.NotContains(t, string(snap), "serviceAccountPassword")

	_ = br
}

func TestBatch1_LDAP_UpdateBroker_SetsMetadata(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "ldap-upd-broker", mq.EngineTypeActiveMQ)

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"authenticationStrategy": "LDAP",
		"ldapServerMetadata": map[string]any{
			"hosts":              []string{"newldap.example.com:636"},
			"roleBase":           "ou=groups,dc=corp,dc=com",
			"roleSearchMatching": "(member={0})",
			"userBase":           "ou=people,dc=corp,dc=com",
			"userSearchMatching": "(sAMAccountName={0})",
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	updOut := parseAccuracyMQ(t, updRec)
	ldap, ok := updOut["ldapServerMetadata"].(map[string]any)
	require.True(t, ok, "ldapServerMetadata must be in UpdateBroker response")

	hosts := ldap["hosts"].([]any)
	assert.Equal(t, "newldap.example.com:636", hosts[0])
}

func TestBatch1_LDAP_DescribeBroker_After_UpdateBroker(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "ldap-desc-upd-broker", mq.EngineTypeActiveMQ)

	doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"ldapServerMetadata": map[string]any{
			"hosts":              []string{"ldap.corp.com:389"},
			"roleBase":           "ou=roles",
			"roleSearchMatching": "(member={0})",
			"userBase":           "ou=users",
			"userSearchMatching": "(uid={0})",
		},
	})

	out := describeBatch1Broker(t, h, brokerID)
	ldap, ok := out["ldapServerMetadata"].(map[string]any)
	require.True(t, ok, "ldapServerMetadata must appear in DescribeBroker after update")

	hosts := ldap["hosts"].([]any)
	assert.Equal(t, "ldap.corp.com:389", hosts[0])
}

// ── MaintenanceWindowStartTime ────────────────────────────────────────────────

func TestBatch1_MaintenanceWindow_CreateBroker_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "maint-create-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "MONDAY",
			"timeOfDay": "03:00",
			"timeZone":  "UTC",
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	mw, ok := out["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must be in DescribeBroker")
	assert.Equal(t, "MONDAY", mw["dayOfWeek"])
	assert.Equal(t, "03:00", mw["timeOfDay"])
	assert.Equal(t, "UTC", mw["timeZone"])
}

func TestBatch1_MaintenanceWindow_UpdateBroker_ChangesWindow(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "maint-upd-broker", mq.EngineTypeActiveMQ)

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "SUNDAY",
			"timeOfDay": "22:00",
			"timeZone":  "America/New_York",
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	updOut := parseAccuracyMQ(t, updRec)
	mw, ok := updOut["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must be in UpdateBroker response")
	assert.Equal(t, "SUNDAY", mw["dayOfWeek"])
	assert.Equal(t, "22:00", mw["timeOfDay"])
}

func TestBatch1_MaintenanceWindow_DescribeBroker_After_UpdateBroker(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "maint-desc-broker", mq.EngineTypeActiveMQ)

	doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "WEDNESDAY",
			"timeOfDay": "01:30",
			"timeZone":  "UTC",
		},
	})

	out := describeBatch1Broker(t, h, brokerID)
	mw, ok := out["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must appear in DescribeBroker after update")
	assert.Equal(t, "WEDNESDAY", mw["dayOfWeek"])
}

func TestBatch1_MaintenanceWindow_Backend_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"maint-be-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{
			MaintenanceWindowStartTime: &mq.WeeklyStartTime{
				DayOfWeek: "FRIDAY",
				TimeOfDay: "05:00",
				TimeZone:  "UTC",
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, br.MaintenanceWindowStartTime)
	assert.Equal(t, "FRIDAY", br.MaintenanceWindowStartTime.DayOfWeek)

	got, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	require.NotNil(t, got.MaintenanceWindowStartTime)
	assert.Equal(t, "FRIDAY", got.MaintenanceWindowStartTime.DayOfWeek)
}

func TestBatch1_MaintenanceWindow_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"maint-snap-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		&mq.CreateBrokerOptions{
			MaintenanceWindowStartTime: &mq.WeeklyStartTime{
				DayOfWeek: "TUESDAY",
				TimeOfDay: "02:00",
				TimeZone:  "UTC",
			},
		},
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	b2 := mq.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	require.NotNil(t, restored.MaintenanceWindowStartTime)
	assert.Equal(t, "TUESDAY", restored.MaintenanceWindowStartTime.DayOfWeek)
}

// ── DataReplicationMode ───────────────────────────────────────────────────────

func TestBatch1_DataReplicationMode_NONE_CreateBroker(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "drm-none-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	_, hasMode := out["dataReplicationMode"]
	assert.False(t, hasMode, "dataReplicationMode must be absent when not set")
}

func TestBatch1_DataReplicationMode_Backend_CRDR_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"drm-crdr-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.UpdateBrokerWithOptions(
		br.BrokerID, "", "", nil, nil,
		&mq.UpdateBrokerOptions{DataReplicationMode: "CRDR"},
	)
	require.NoError(t, err)

	got, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "CRDR", got.DataReplicationMode)
}

func TestBatch1_DataReplicationMode_UpdateBroker_Sets_Mode(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "drm-upd-broker", mq.EngineTypeActiveMQ)

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"dataReplicationMode": "CRDR",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, "CRDR", out["dataReplicationMode"])
}

func TestBatch1_DataReplicationMode_DescribeBroker_After_Update(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "drm-desc-broker", mq.EngineTypeActiveMQ)

	doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"dataReplicationMode": "CRDR",
	})

	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, "CRDR", out["dataReplicationMode"],
		"dataReplicationMode must appear in DescribeBroker after UpdateBroker")
}

func TestBatch1_DataReplicationMode_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBrokerWithOptions(
		"drm-snap-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.UpdateBrokerWithOptions(br.BrokerID, "", "", nil, nil,
		&mq.UpdateBrokerOptions{DataReplicationMode: "CRDR"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	b2 := mq.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "CRDR", restored.DataReplicationMode)
}

// ── ConfigurationAssociation ──────────────────────────────────────────────────

func TestBatch1_ConfigAssoc_CreateBroker_WithConfiguration(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	configID := createBatch1Config(t, h, "assoc-cfg")

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "assoc-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"configuration": map[string]any{
			"id":       configID,
			"revision": 1,
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	configurations, ok := out["configurations"].(map[string]any)
	require.True(t, ok, "configurations must be in DescribeBroker")

	current, ok := configurations["current"].(map[string]any)
	require.True(t, ok, "configurations.current must be set")
	assert.Equal(t, configID, current["id"])
	assert.InEpsilon(t, float64(1), current["revision"], 0.01)
}

func TestBatch1_ConfigAssoc_UpdateBroker_SetsCurrentConfig(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "assoc-upd-broker", mq.EngineTypeActiveMQ)
	configID := createBatch1Config(t, h, "assoc-upd-cfg")

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"configuration": map[string]any{
			"id":       configID,
			"revision": 1,
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	updOut := parseAccuracyMQ(t, updRec)
	cfg, ok := updOut["configuration"].(map[string]any)
	require.True(t, ok, "configuration must be in UpdateBroker response")
	assert.Equal(t, configID, cfg["id"])
}

func TestBatch1_ConfigAssoc_DescribeBroker_After_UpdateBroker(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "assoc-desc-broker", mq.EngineTypeActiveMQ)
	configID := createBatch1Config(t, h, "assoc-desc-cfg")

	doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"configuration": map[string]any{
			"id":       configID,
			"revision": 1,
		},
	})

	out := describeBatch1Broker(t, h, brokerID)
	configurations := out["configurations"].(map[string]any)
	current := configurations["current"].(map[string]any)
	assert.Equal(t, configID, current["id"])
}

func TestBatch1_ConfigRevision_DataSizeCap_Rejected(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	cfg, err := b.CreateConfiguration("cap-cfg", "", mq.EngineTypeActiveMQ, "", nil)
	require.NoError(t, err)

	oversized := strings.Repeat("x", 256*1024+1)
	_, err = b.UpdateConfiguration(cfg.ID, "", oversized)
	require.Error(t, err)
	require.ErrorIs(t, err, mq.ErrValidation, "oversized configuration data must return ErrValidation")
}

func TestBatch1_ConfigRevision_DataRoundTrip_AtSizeLimit(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	cfg, err := b.CreateConfiguration("limit-cfg", "", mq.EngineTypeActiveMQ, "", nil)
	require.NoError(t, err)

	atLimit := strings.Repeat("a", 256*1024)
	updated, err := b.UpdateConfiguration(cfg.ID, "at limit", atLimit)
	require.NoError(t, err, "exactly 256KiB must be accepted")
	assert.Equal(t, int32(2), updated.LatestRevision.Revision)
}

// ── UpdateBroker Extended Fields ──────────────────────────────────────────────

func TestBatch1_UpdateBroker_MultipleFields_SameRequest(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "multi-upd-broker", mq.EngineTypeActiveMQ)

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"engineVersion":          "5.18.3",
		"authenticationStrategy": "SIMPLE",
		"logs": map[string]any{
			"general": true,
			"audit":   false,
		},
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "SATURDAY",
			"timeOfDay": "04:00",
			"timeZone":  "UTC",
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, "5.18.3", out["engineVersion"])
	assert.Equal(t, "SIMPLE", out["authenticationStrategy"])
	assert.NotNil(t, out["logs"])
	assert.NotNil(t, out["maintenanceWindowStartTime"])
}

func TestBatch1_UpdateBroker_PartialUpdate_PreservesOtherFields(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "partial-upd-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "SIMPLE",
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "MONDAY",
			"timeOfDay": "02:00",
			"timeZone":  "UTC",
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)

	doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"engineVersion": "5.16.7",
	})

	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, "5.16.7", out["engineVersion"])
	assert.Equal(t, "SIMPLE", out["authenticationStrategy"], "authenticationStrategy must be preserved")

	mw, ok := out["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must be preserved after partial update")
	assert.Equal(t, "MONDAY", mw["dayOfWeek"])
}

func TestBatch1_UpdateBroker_Response_Contains_Logs(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "logs-resp-broker", mq.EngineTypeActiveMQ)

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"logs": map[string]any{
			"general": true,
			"audit":   true,
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	updOut := parseAccuracyMQ(t, updRec)
	logs, ok := updOut["logs"].(map[string]any)
	require.True(t, ok, "logs must be in UpdateBroker response")
	assert.Equal(t, true, logs["general"])
	assert.Equal(t, true, logs["audit"])
}

func TestBatch1_UpdateBroker_Response_Contains_MaintenanceWindow(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "mw-resp-broker", mq.EngineTypeActiveMQ)

	updRec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "THURSDAY",
			"timeOfDay": "06:00",
			"timeZone":  "UTC",
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	updOut := parseAccuracyMQ(t, updRec)
	mw, ok := updOut["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must be in UpdateBroker response")
	assert.Equal(t, "THURSDAY", mw["dayOfWeek"])
}

func TestBatch1_UpdateBrokerWithOptions_Backend_AllFields(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br, err := b.CreateBroker(
		"opts-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "",
		false, false, nil, nil, nil, nil,
	)
	require.NoError(t, err)

	configID := "c-testcfg01"
	mq.AddConfigurationInternal(b, &mq.Configuration{
		ID:             configID,
		Name:           "opts-cfg",
		Arn:            "arn:aws:mq:us-east-1:123456789012:configuration:" + configID,
		EngineType:     mq.EngineTypeActiveMQ,
		LatestRevision: &mq.ConfigurationRevision{Revision: 1, Created: "2024-01-01T00:00:00Z"},
		Revisions:      []mq.ConfigurationRevision{{Revision: 1, Created: "2024-01-01T00:00:00Z"}},
	})

	updated, err := b.UpdateBrokerWithOptions(
		br.BrokerID, "5.18.3", "mq.m5.xlarge",
		nil, []string{"sg-11223344"},
		&mq.UpdateBrokerOptions{
			AuthenticationStrategy: "SIMPLE",
			Logs:                   &mq.Logs{General: true, Audit: false},
			MaintenanceWindowStartTime: &mq.WeeklyStartTime{
				DayOfWeek: "FRIDAY",
				TimeOfDay: "03:00",
				TimeZone:  "UTC",
			},
			Configuration:       &mq.ConfigurationID{ID: configID, Revision: 1},
			DataReplicationMode: "CRDR",
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "5.18.3", updated.EngineVersion)
	assert.Equal(t, "mq.m5.xlarge", updated.HostInstanceType)
	assert.Equal(t, "SIMPLE", updated.AuthenticationStrategy)
	assert.Equal(t, "CRDR", updated.DataReplicationMode)
	require.NotNil(t, updated.Logs)
	assert.True(t, updated.Logs.General)
	require.NotNil(t, updated.MaintenanceWindowStartTime)
	assert.Equal(t, "FRIDAY", updated.MaintenanceWindowStartTime.DayOfWeek)
	require.NotNil(t, updated.Configurations)
	assert.Equal(t, configID, updated.Configurations.Current.ID)
}

// ── Security / Privacy ────────────────────────────────────────────────────────

func TestBatch1_DescribeUser_DoesNotReturn_Password(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "pwd-sec-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/secuser", map[string]any{
			"password": "SecurePassword1!",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doAccuracyMQ(t, h, http.MethodGet,
		"/v1/brokers/"+brokerID+"/users/secuser", nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	body := descRec.Body.String()
	assert.NotContains(t, body, "SecurePassword1!", "password must NOT appear in DescribeUser response")
	assert.NotContains(t, body, "password", "password key must NOT appear in DescribeUser response")
}

func TestBatch1_ListUsers_DoesNotReturn_Passwords(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "list-pwd-broker", mq.EngineTypeActiveMQ)

	for i, name := range []string{"user1", "user2", "user3"} {
		_ = i
		rec := doAccuracyMQ(t, h, http.MethodPost,
			"/v1/brokers/"+brokerID+"/users/"+name, map[string]any{
				"password": "Password123!",
			})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	body := listRec.Body.String()
	assert.NotContains(t, body, "Password123!", "passwords must NOT appear in ListUsers response")
}

func TestBatch1_DescribeBroker_Password_Not_In_Users(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "br-userpwd",
		"engineType": mq.EngineTypeActiveMQ,
		"users": []map[string]any{
			{
				"username": "admin",
				"password": "AdminPassword123!",
				"groups":   []string{"admins"},
			},
		},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	body, _ := out["users"].([]any)
	for _, u := range body {
		um := u.(map[string]any)
		_, hasPassword := um["password"]
		assert.False(t, hasPassword, "password must NOT appear in DescribeBroker users list")
	}
}

// ── BrokerState Transitions ───────────────────────────────────────────────────

func TestBatch1_BrokerState_InitiallyRunning(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "state-init-broker", mq.EngineTypeActiveMQ)

	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, mq.BrokerStateRunning, out["brokerState"])
}

func TestBatch1_BrokerState_AfterDelete_BrokerGone(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "state-del-broker", mq.EngineTypeActiveMQ)

	delRec := doAccuracyMQ(t, h, http.MethodDelete, "/v1/brokers/"+brokerID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	descRec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+brokerID, nil)
	assert.Equal(t, http.StatusNotFound, descRec.Code,
		"deleted broker must return 404 on describe")
}

func TestBatch1_BrokerState_Reboot_FullCycle(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "state-reboot-broker", mq.EngineTypeActiveMQ)

	rebootRec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rebootRec.Code)

	desc1 := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, mq.BrokerStateRebooting, desc1["brokerState"],
		"broker must be REBOOT_IN_PROGRESS immediately after reboot call")

	desc2 := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, mq.BrokerStateRunning, desc2["brokerState"],
		"broker must return to RUNNING on subsequent DescribeBroker")
}

// ── CreatorRequestID Idempotency ──────────────────────────────────────────────

func TestBatch1_CreatorRequestID_SameID_ReturnsSameBroker(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	body := map[string]any{
		"brokerName":       "idem-batch1-broker",
		"engineType":       mq.EngineTypeActiveMQ,
		"creatorRequestId": "unique-request-id-123",
	}

	rec1 := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", body)
	require.Equal(t, http.StatusAccepted, rec1.Code)
	id1 := parseAccuracyMQ(t, rec1)["brokerId"].(string)

	rec2 := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", body)
	require.Equal(t, http.StatusAccepted, rec2.Code)
	id2 := parseAccuracyMQ(t, rec2)["brokerId"].(string)

	assert.Equal(t, id1, id2, "same CreatorRequestId must return same broker ID")
}

func TestBatch1_CreatorRequestID_DifferentID_CreatesDifferentBroker(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)

	rec1 := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":       "idem-diff-a",
		"engineType":       mq.EngineTypeActiveMQ,
		"creatorRequestId": "req-aaa",
	})
	require.Equal(t, http.StatusAccepted, rec1.Code)
	id1 := parseAccuracyMQ(t, rec1)["brokerId"].(string)

	rec2 := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":       "idem-diff-b",
		"engineType":       mq.EngineTypeActiveMQ,
		"creatorRequestId": "req-bbb",
	})
	require.Equal(t, http.StatusAccepted, rec2.Code)
	id2 := parseAccuracyMQ(t, rec2)["brokerId"].(string)

	assert.NotEqual(t, id1, id2)
}

// ── DescribeBroker by Name ─────────────────────────────────────────────────────

func TestBatch1_DescribeBroker_ByName(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "by-name-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/by-name-broker", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	assert.Equal(t, brokerID, out["brokerId"])
	assert.Equal(t, "by-name-broker", out["brokerName"])
}

func TestBatch1_DescribeBroker_ByName_NotFound(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/nonexistent-name", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ── ConfigurationRevision Cap ─────────────────────────────────────────────────

func TestBatch1_ConfigRevision_Cap_50_OldestPruned(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	cfg, err := b.CreateConfiguration("rev-cap-cfg", "init", mq.EngineTypeActiveMQ, "", nil)
	require.NoError(t, err)

	for i := range 55 {
		_, err = b.UpdateConfiguration(cfg.ID, "desc", strings.Repeat("x", i+1))
		require.NoError(t, err)
	}

	revisions, err := b.ListConfigurationRevisions(cfg.ID)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(revisions), 50,
		"configuration must not keep more than 50 revisions")

	firstRev := revisions[0].Revision
	assert.Greater(t, firstRev, int32(1),
		"oldest revisions must have been pruned; first surviving revision must be > 1")
}

func TestBatch1_ConfigRevision_OldestRevisionData_Pruned(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	cfg, err := b.CreateConfiguration("rev-prune-cfg", "init", mq.EngineTypeActiveMQ, "", nil)
	require.NoError(t, err)

	for range 55 {
		_, err = b.UpdateConfiguration(cfg.ID, "", "data")
		require.NoError(t, err)
	}

	_, _, err = b.DescribeConfigurationRevision(cfg.ID, 1)
	require.Error(t, err, "revision 1 must be pruned after 55 updates")
	require.ErrorIs(t, err, mq.ErrNotFound)
}

// ── ActionsRequired ───────────────────────────────────────────────────────────

func TestBatch1_ActionsRequired_Field_Present_In_DescribeBroker(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br := &mq.Broker{
		BrokerID:    "b-actions-001",
		BrokerName:  "actions-broker",
		BrokerArn:   "arn:aws:mq:us-east-1:123456789012:broker:actions-broker",
		BrokerState: mq.BrokerStateRunning,
		EngineType:  mq.EngineTypeActiveMQ,
		ActionsRequired: []mq.ActionRequired{
			{
				ActionRequiredCode: "ACTION_UPDATE",
				ActionRequiredInfo: "Broker requires upgrade to maintain support.",
			},
		},
	}
	mq.AddBrokerInternal(b, br)

	got, err := b.DescribeBroker("b-actions-001")
	require.NoError(t, err)
	require.Len(t, got.ActionsRequired, 1)
	assert.Equal(t, "ACTION_UPDATE", got.ActionsRequired[0].ActionRequiredCode)
}

func TestBatch1_ActionsRequired_HTTP_In_DescribeBroker_Response(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	br := &mq.Broker{
		BrokerID:    "b-actions-002",
		BrokerName:  "actions-http-broker",
		BrokerArn:   "arn:aws:mq:us-east-1:123456789012:broker:actions-http-broker",
		BrokerState: mq.BrokerStateRunning,
		EngineType:  mq.EngineTypeActiveMQ,
		ActionsRequired: []mq.ActionRequired{
			{ActionRequiredCode: "NEED_REBOOT", ActionRequiredInfo: "A reboot is required."},
		},
	}
	mq.AddBrokerInternal(b, br)

	h := mq.NewHandler(b)
	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/b-actions-002", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	actions, ok := out["actionsRequired"].([]any)
	require.True(t, ok, "actionsRequired must be in DescribeBroker response")
	require.Len(t, actions, 1)

	action := actions[0].(map[string]any)
	assert.Equal(t, "NEED_REBOOT", action["actionRequiredCode"])
}

// ── StorageType Validation ─────────────────────────────────────────────────────

func TestBatch1_StorageType_RabbitMQ_Rejects_EFS(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":  "rmq-efs-broker",
		"engineType":  mq.EngineTypeRabbitMQ,
		"storageType": mq.StorageTypeEFS,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"RabbitMQ with EFS storageType must return 400")
}

func TestBatch1_StorageType_ActiveMQ_Accepts_EBS(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":  "amq-ebs-broker",
		"engineType":  mq.EngineTypeActiveMQ,
		"storageType": mq.StorageTypeEBS,
	})
	require.Equal(t, http.StatusAccepted, rec.Code, "ActiveMQ with EBS must succeed: %s", rec.Body.String())

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, mq.StorageTypeEBS, out["storageType"])
}

func TestBatch1_StorageType_RabbitMQ_Default_EBS(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "rmq-default-storage", mq.EngineTypeRabbitMQ)
	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, mq.StorageTypeEBS, out["storageType"],
		"RabbitMQ default storageType must be ebs")
}

func TestBatch1_StorageType_ActiveMQ_Default_EFS(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	brokerID := createBatch1Broker(t, h, "amq-default-storage", mq.EngineTypeActiveMQ)
	out := describeBatch1Broker(t, h, brokerID)
	assert.Equal(t, mq.StorageTypeEFS, out["storageType"],
		"ActiveMQ default storageType must be efs")
}

// ── Misc / Edge Cases ─────────────────────────────────────────────────────────

func TestBatch1_CreateBroker_WithSecurityGroups_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":     "sg-broker",
		"engineType":     mq.EngineTypeActiveMQ,
		"securityGroups": []string{"sg-aabbccdd", "sg-11223344"},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	sgs, ok := out["securityGroups"].([]any)
	require.True(t, ok, "securityGroups must be in DescribeBroker")
	assert.Len(t, sgs, 2)
}

func TestBatch1_CreateBroker_WithSubnetIDs_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "subnet-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"subnetIds":  []string{"subnet-11111111", "subnet-22222222"},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)

	brokerID := parseAccuracyMQ(t, rec)["brokerId"].(string)
	out := describeBatch1Broker(t, h, brokerID)

	subnets, ok := out["subnetIds"].([]any)
	require.True(t, ok, "subnetIds must be in DescribeBroker")
	assert.Len(t, subnets, 2)
}

func TestBatch1_UpdateBroker_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPut, "/v1/brokers/nonexistent-broker", map[string]any{
		"engineVersion": "5.18.3",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch1_UpdateBrokerWithOptions_NotFound_Backend(t *testing.T) {
	t.Parallel()

	b := newBatch1Backend(t)
	_, err := b.UpdateBrokerWithOptions("no-such-broker", "", "", nil, nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, mq.ErrNotFound)
}

func TestBatch1_DescribeBrokerInstanceOptions_AllEngineTypes(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/broker-instance-options", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	opts, ok := out["brokerInstanceOptions"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, opts, "broker instance options must not be empty")

	engineTypes := make(map[string]bool)
	for _, o := range opts {
		om := o.(map[string]any)
		engineTypes[om["engineType"].(string)] = true
	}

	assert.True(t, engineTypes[mq.EngineTypeActiveMQ], "must have ActiveMQ instance options")
	assert.True(t, engineTypes[mq.EngineTypeRabbitMQ], "must have RabbitMQ instance options")
}

func TestBatch1_DescribeBrokerInstanceOptions_FilterHostInstanceType(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodGet,
		"/v1/broker-instance-options?hostInstanceType=mq.m5.large", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	opts, ok := out["brokerInstanceOptions"].([]any)
	require.True(t, ok)

	for _, o := range opts {
		om := o.(map[string]any)
		assert.Equal(t, "mq.m5.large", om["hostInstanceType"])
	}
}

func TestBatch1_Configuration_ARN_Format(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "arn-cfg",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracyMQ(t, rec)
	arn := out["arn"].(string)
	assert.True(t, strings.HasPrefix(arn, "arn:aws:mq:"),
		"configuration ARN must start with arn:aws:mq:, got %s", arn)
	assert.Contains(t, arn, "123456789012")
	assert.Contains(t, arn, "us-east-1")
}
