package mq_test

// Optional CreateBroker/UpdateBroker fields: encryption options, logs,
// authentication strategy, LDAP metadata, maintenance window,
// data-replication mode, and configuration association -- including
// their snapshot/restore persistence.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestCreateBrokerWithOptions_PersistsLogsAndEncryption(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	br, err := b.CreateBrokerWithOptions(
		"feat-broker", mq.DeploymentModeSingleInstance,
		mq.EngineTypeActiveMQ, "", "", false, false,
		nil, nil, nil, nil,
		&mq.CreateBrokerOptions{
			AuthenticationStrategy: "SIMPLE",
			EncryptionOptions:      &mq.EncryptionOptions{UseAWSOwnedKey: true},
			Logs:                   &mq.Logs{General: true, Audit: true},
			MaintenanceWindowStartTime: &mq.WeeklyStartTime{
				DayOfWeek: "MONDAY", TimeOfDay: "03:00", TimeZone: "UTC",
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "SIMPLE", br.AuthenticationStrategy)
	require.NotNil(t, br.EncryptionOptions)
	assert.True(t, br.EncryptionOptions.UseAWSOwnedKey)
	require.NotNil(t, br.LogsSummary)
	assert.True(t, br.LogsSummary.General)
	assert.Contains(t, br.LogsSummary.GeneralLogGroup, br.BrokerID)
	require.NotNil(t, br.MaintenanceWindowStartTime)
	assert.Equal(t, "MONDAY", br.MaintenanceWindowStartTime.DayOfWeek)
}

func TestEncryptionOptions_KMSKey_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "enc-kms-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"encryptionOptions": map[string]any{
			"kmsKeyId":       "arn:aws:kms:us-east-1:123456789012:key/test-key-id",
			"useAwsOwnedKey": false,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)

	enc, ok := out["encryptionOptions"].(map[string]any)
	require.True(t, ok, "encryptionOptions must be present in DescribeBroker")
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/test-key-id", enc["kmsKeyId"])
	assert.Equal(t, false, enc["useAwsOwnedKey"])
}

func TestEncryptionOptions_UseAwsOwnedKey_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "enc-owned-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"encryptionOptions": map[string]any{
			"useAwsOwnedKey": true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)

	enc, ok := out["encryptionOptions"].(map[string]any)
	require.True(t, ok, "encryptionOptions must be present")
	assert.Equal(t, true, enc["useAwsOwnedKey"])
}

func TestEncryptionOptions_AbsentWhenNotSpecified(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "no-enc-broker", mq.EngineTypeActiveMQ)
	out := describeTestBroker(t, h, brokerID)

	_, hasEnc := out["encryptionOptions"]
	assert.False(t, hasEnc, "encryptionOptions must be absent when not specified")
}

func TestEncryptionOptions_Backend_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestLogs_CreateBroker_GeneralLogGroup_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "logs-gen-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"logs": map[string]any{
			"general": true,
			"audit":   false,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)

	logs, ok := out["logs"].(map[string]any)
	require.True(t, ok, "logs must be present in DescribeBroker")
	assert.Equal(t, true, logs["general"])
	assert.Equal(t, false, logs["audit"])
	assert.NotEmpty(t, logs["generalLogGroup"], "generalLogGroup must be set")
}

func TestLogs_CreateBroker_AuditLogGroup_ContainsBrokerID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "logs-audit-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"logs": map[string]any{
			"general": true,
			"audit":   true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)

	logs := out["logs"].(map[string]any)
	auditLogGroup := logs["auditLogGroup"].(string)
	assert.Contains(t, auditLogGroup, brokerID, "auditLogGroup must contain broker ID")
	assert.True(t, strings.HasPrefix(auditLogGroup, "/aws/amazonmq/broker/"),
		"auditLogGroup must start with /aws/amazonmq/broker/")
}

func TestLogs_LogGroupName_Format(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "logs-fmt-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"logs": map[string]any{
			"general": true,
			"audit":   true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)
	logs := out["logs"].(map[string]any)

	generalLogGroup := logs["generalLogGroup"].(string)
	assert.Equal(t, "/aws/amazonmq/broker/"+brokerID+"/general", generalLogGroup)
}

func TestLogs_UpdateBroker_UpdatesLogsSummary(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "logs-upd-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"logs": map[string]any{
			"general": true,
			"audit":   false,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Real Amazon MQ only takes a Logs change live on the next reboot -- the
	// top-level logs.general/audit stay at their pre-update value and the
	// requested change surfaces under logs.pending until then (see
	// DescribeBrokerOutput.Logs, a LogsSummary with a nested Pending field).
	out := describeTestBroker(t, h, brokerID)
	logs, ok := out["logs"].(map[string]any)
	require.True(t, ok, "logs must be present after UpdateBroker")
	assert.Equal(t, false, logs["general"], "logs.general must not apply before reboot")
	assert.NotEmpty(t, logs["generalLogGroup"])

	pending, ok := logs["pending"].(map[string]any)
	require.True(t, ok, "logs.pending must be present after UpdateBroker")
	assert.Equal(t, true, pending["general"])

	rec = doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	describeTestBroker(t, h, brokerID) // first Describe after reboot observes REBOOT_IN_PROGRESS and promotes.

	settled := describeTestBroker(t, h, brokerID)
	settledLogs := settled["logs"].(map[string]any)
	assert.Equal(t, true, settledLogs["general"], "logs.general must apply after reboot")
	_, hasPending := settledLogs["pending"]
	assert.False(t, hasPending, "logs.pending must clear after reboot")
}

func TestLogs_UpdateBroker_ResponseContainsLogs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "logs-resp-broker", mq.EngineTypeActiveMQ)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"logs": map[string]any{
			"general": true,
			"audit":   true,
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	// UpdateBrokerOutput.logs is the plain Logs shape (not LogsSummary) and
	// echoes the target value -- see updateBrokerResponse's doc.
	logs, ok := parseResponse(t, updRec)["logs"].(map[string]any)
	require.True(t, ok, "logs must be in UpdateBroker response")
	assert.Equal(t, true, logs["general"])
	assert.Equal(t, true, logs["audit"])
}

func TestLogs_Backend_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestAuthStrategy_Simple_CreateBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "auth-simple-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "SIMPLE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)
	assert.Equal(t, "SIMPLE", out["authenticationStrategy"])
}

func TestAuthStrategy_LDAP_CreateBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
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
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)
	assert.Equal(t, "LDAP", out["authenticationStrategy"])
}

func TestAuthStrategy_UpdateBroker_ChangesStrategy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "auth-upd-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "SIMPLE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"authenticationStrategy": "LDAP",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	// authenticationStrategy only takes effect on the next reboot.
	out := describeTestBroker(t, h, brokerID)
	assert.Equal(t, "SIMPLE", out["authenticationStrategy"], "authenticationStrategy must not apply before reboot")
	assert.Equal(t, "LDAP", out["pendingAuthenticationStrategy"])

	rec = doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	describeTestBroker(t, h, brokerID) // first Describe after reboot observes REBOOT_IN_PROGRESS and promotes.

	settled := describeTestBroker(t, h, brokerID)
	assert.Equal(t, "LDAP", settled["authenticationStrategy"], "authenticationStrategy must apply after reboot")
}

func TestAuthStrategy_InDescribeBrokerResponse(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestAuthStrategy_UpdateBroker_ResponseContainsStrategy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "auth-resp-broker", mq.EngineTypeActiveMQ)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"authenticationStrategy": "SIMPLE",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	assert.Equal(t, "SIMPLE", parseResponse(t, updRec)["authenticationStrategy"])
}

func TestLDAP_CreateBroker_Hosts_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
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
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)

	ldap, ok := out["ldapServerMetadata"].(map[string]any)
	require.True(t, ok, "ldapServerMetadata must be in DescribeBroker")

	hosts := ldap["hosts"].([]any)
	assert.Len(t, hosts, 2)
	assert.Equal(t, "ldap1.example.com:389", hosts[0])
}

func TestLDAP_ServiceAccountPassword_NotExposed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
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
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)

	ldap := out["ldapServerMetadata"].(map[string]any)
	_, hasPassword := ldap["serviceAccountPassword"]
	assert.False(t, hasPassword, "serviceAccountPassword must NOT be exposed in DescribeBroker")

	username, hasUsername := ldap["serviceAccountUsername"]
	assert.True(t, hasUsername, "serviceAccountUsername must be exposed")
	assert.Equal(t, "cn=admin,dc=example,dc=com", username)
}

func TestLDAP_Backend_ServiceAccountPassword_NotInJSON(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestLDAP_UpdateBroker_SetsMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "ldap-upd-broker", mq.EngineTypeActiveMQ)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
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

	updOut := parseResponse(t, updRec)
	ldap, ok := updOut["ldapServerMetadata"].(map[string]any)
	require.True(t, ok, "ldapServerMetadata must be in UpdateBroker response")

	hosts := ldap["hosts"].([]any)
	assert.Equal(t, "newldap.example.com:636", hosts[0])
}

func TestLDAP_DescribeBroker_AfterUpdateBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "ldap-desc-upd-broker", mq.EngineTypeActiveMQ)

	doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"ldapServerMetadata": map[string]any{
			"hosts":              []string{"ldap.corp.com:389"},
			"roleBase":           "ou=roles",
			"roleSearchMatching": "(member={0})",
			"userBase":           "ou=users",
			"userSearchMatching": "(uid={0})",
		},
	})

	// ldapServerMetadata only takes effect on the next reboot; before that it
	// surfaces as pendingLdapServerMetadata and the top-level field stays
	// unset (this broker was created without LDAP configured).
	out := describeTestBroker(t, h, brokerID)
	_, hasCurrent := out["ldapServerMetadata"]
	assert.False(t, hasCurrent, "ldapServerMetadata must stay unset before reboot")

	pending, ok := out["pendingLdapServerMetadata"].(map[string]any)
	require.True(t, ok, "pendingLdapServerMetadata must appear in DescribeBroker after update")
	hosts := pending["hosts"].([]any)
	assert.Equal(t, "ldap.corp.com:389", hosts[0])

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	describeTestBroker(t, h, brokerID) // first Describe after reboot observes REBOOT_IN_PROGRESS and promotes.

	settled := describeTestBroker(t, h, brokerID)
	settledLdap, ok := settled["ldapServerMetadata"].(map[string]any)
	require.True(t, ok, "ldapServerMetadata must appear in DescribeBroker after reboot")
	settledHosts := settledLdap["hosts"].([]any)
	assert.Equal(t, "ldap.corp.com:389", settledHosts[0])
}

func TestMaintenanceWindow_CreateBroker_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "maint-create-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "MONDAY",
			"timeOfDay": "03:00",
			"timeZone":  "UTC",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)

	mw, ok := out["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must be in DescribeBroker")
	assert.Equal(t, "MONDAY", mw["dayOfWeek"])
	assert.Equal(t, "03:00", mw["timeOfDay"])
	assert.Equal(t, "UTC", mw["timeZone"])
}

func TestMaintenanceWindow_UpdateBroker_ChangesWindow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "maint-upd-broker", mq.EngineTypeActiveMQ)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "SUNDAY",
			"timeOfDay": "22:00",
			"timeZone":  "America/New_York",
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	updOut := parseResponse(t, updRec)
	mw, ok := updOut["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must be in UpdateBroker response")
	assert.Equal(t, "SUNDAY", mw["dayOfWeek"])
	assert.Equal(t, "22:00", mw["timeOfDay"])
}

func TestMaintenanceWindow_DescribeBroker_AfterUpdateBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "maint-desc-broker", mq.EngineTypeActiveMQ)

	doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "WEDNESDAY",
			"timeOfDay": "01:30",
			"timeZone":  "UTC",
		},
	})

	out := describeTestBroker(t, h, brokerID)
	mw, ok := out["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must appear in DescribeBroker after update")
	assert.Equal(t, "WEDNESDAY", mw["dayOfWeek"])
}

func TestMaintenanceWindow_Backend_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestDataReplicationMode_NONE_CreateBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "drm-none-broker", mq.EngineTypeActiveMQ)
	out := describeTestBroker(t, h, brokerID)

	_, hasMode := out["dataReplicationMode"]
	assert.False(t, hasMode, "dataReplicationMode must be absent when not set")
}

func TestDataReplicationMode_Backend_CRDRRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

	// UpdateBrokerOutput/DescribeBrokerOutput's dataReplicationMode reflects
	// the CURRENT (pre-reboot) mode; pendingDataReplicationMode carries the
	// target -- unlike engineVersion/hostInstanceType/etc, which have no
	// dedicated Pending* output field and so echo the target directly.
	got, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Empty(t, got.DataReplicationMode)
	assert.Equal(t, "CRDR", got.PendingDataReplicationMode)

	require.NoError(t, b.RebootBroker(br.BrokerID))
	_, err = b.DescribeBroker(br.BrokerID) // observes REBOOT_IN_PROGRESS and promotes.
	require.NoError(t, err)

	settled, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "CRDR", settled.DataReplicationMode)
	assert.Empty(t, settled.PendingDataReplicationMode)
}

func TestDataReplicationMode_UpdateBroker_SetsMode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "drm-upd-broker", mq.EngineTypeActiveMQ)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"dataReplicationMode": "CRDR",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	out := describeTestBroker(t, h, brokerID)
	assert.NotEqual(t, "CRDR", out["dataReplicationMode"], "dataReplicationMode must not apply before reboot")
	assert.Equal(t, "CRDR", out["pendingDataReplicationMode"])
}

func TestDataReplicationMode_DescribeBroker_AfterUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "drm-desc-broker", mq.EngineTypeActiveMQ)

	doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"dataReplicationMode": "CRDR",
	})

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	describeTestBroker(t, h, brokerID) // first Describe after reboot observes REBOOT_IN_PROGRESS and promotes.

	out := describeTestBroker(t, h, brokerID)
	assert.Equal(t, "CRDR", out["dataReplicationMode"],
		"dataReplicationMode must appear in DescribeBroker after the broker reboots")
}

func TestConfigAssoc_CreateBroker_WithConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "assoc-cfg", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "assoc-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"configuration": map[string]any{
			"id":       configID,
			"revision": 1,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)
	out := describeTestBroker(t, h, brokerID)

	configurations, ok := out["configurations"].(map[string]any)
	require.True(t, ok, "configurations must be in DescribeBroker")

	current, ok := configurations["current"].(map[string]any)
	require.True(t, ok, "configurations.current must be set")
	assert.Equal(t, configID, current["id"])
	assert.InEpsilon(t, float64(1), current["revision"], 0.01)
}

func TestConfigAssoc_UpdateBroker_SetsCurrentConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "assoc-upd-broker", mq.EngineTypeActiveMQ)
	configID := createTestConfig(t, h, "assoc-upd-cfg", mq.EngineTypeActiveMQ)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"configuration": map[string]any{
			"id":       configID,
			"revision": 1,
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	cfg, ok := parseResponse(t, updRec)["configuration"].(map[string]any)
	require.True(t, ok, "configuration must be in UpdateBroker response")
	assert.Equal(t, configID, cfg["id"])
}

func TestConfigAssoc_DescribeBroker_AfterUpdateBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "assoc-desc-broker", mq.EngineTypeActiveMQ)
	configID := createTestConfig(t, h, "assoc-desc-cfg", mq.EngineTypeActiveMQ)

	doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"configuration": map[string]any{
			"id":       configID,
			"revision": 1,
		},
	})

	// Real Amazon MQ only swaps a new configuration association into
	// Configurations.current on the next reboot; until then it sits in
	// Configurations.pending (see DescribeBrokerOutput.Configurations).
	out := describeTestBroker(t, h, brokerID)
	configurations := out["configurations"].(map[string]any)
	_, hasCurrent := configurations["current"]
	assert.False(t, hasCurrent, "configurations.current must stay unset before reboot")

	pending := configurations["pending"].(map[string]any)
	assert.Equal(t, configID, pending["id"])

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	describeTestBroker(t, h, brokerID) // first Describe after reboot observes REBOOT_IN_PROGRESS and promotes.

	settled := describeTestBroker(t, h, brokerID)
	settledConfigurations := settled["configurations"].(map[string]any)
	current := settledConfigurations["current"].(map[string]any)
	assert.Equal(t, configID, current["id"], "configurations.current must hold the new config after reboot")
	_, hasPending := settledConfigurations["pending"]
	assert.False(t, hasPending, "configurations.pending must clear after reboot")
}

func TestUpdateBroker_MultipleFields_SameRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "multi-upd-broker", mq.EngineTypeActiveMQ)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
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

	// engineVersion/authenticationStrategy/logs only take effect on the next
	// reboot; maintenanceWindowStartTime has no Pending* counterpart in the
	// SDK and applies immediately (see applyUpdateBrokerOptions).
	out := describeTestBroker(t, h, brokerID)
	assert.NotEqual(t, "5.18.3", out["engineVersion"], "engineVersion must not apply before reboot")
	assert.Equal(t, "5.18.3", out["pendingEngineVersion"])
	assert.NotEqual(t, "SIMPLE", out["authenticationStrategy"], "authenticationStrategy must not apply before reboot")
	assert.Equal(t, "SIMPLE", out["pendingAuthenticationStrategy"])
	assert.NotNil(t, out["logs"])
	assert.NotNil(t, out["maintenanceWindowStartTime"])

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/reboot", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	describeTestBroker(t, h, brokerID) // first Describe after reboot observes REBOOT_IN_PROGRESS and promotes.

	settled := describeTestBroker(t, h, brokerID)
	assert.Equal(t, "5.18.3", settled["engineVersion"])
	assert.Equal(t, "SIMPLE", settled["authenticationStrategy"])
}

func TestUpdateBroker_PartialUpdate_PreservesOtherFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":             "partial-upd-broker",
		"engineType":             mq.EngineTypeActiveMQ,
		"authenticationStrategy": "SIMPLE",
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "MONDAY",
			"timeOfDay": "02:00",
			"timeZone":  "UTC",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	brokerID := parseResponse(t, rec)["brokerId"].(string)

	doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"engineVersion": "5.16.7",
	})

	// engineVersion is staged (not yet live); authenticationStrategy and
	// maintenanceWindowStartTime were untouched by this update and must be
	// preserved exactly as set at broker creation.
	out := describeTestBroker(t, h, brokerID)
	assert.Equal(t, "5.16.7", out["pendingEngineVersion"])
	assert.Equal(t, "SIMPLE", out["authenticationStrategy"], "authenticationStrategy must be preserved")

	mw, ok := out["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must be preserved after partial update")
	assert.Equal(t, "MONDAY", mw["dayOfWeek"])
}

func TestUpdateBroker_ResponseContainsMaintenanceWindow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "mw-resp-broker", mq.EngineTypeActiveMQ)

	updRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"maintenanceWindowStartTime": map[string]any{
			"dayOfWeek": "THURSDAY",
			"timeOfDay": "06:00",
			"timeZone":  "UTC",
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	mw, ok := parseResponse(t, updRec)["maintenanceWindowStartTime"].(map[string]any)
	require.True(t, ok, "maintenanceWindowStartTime must be in UpdateBroker response")
	assert.Equal(t, "THURSDAY", mw["dayOfWeek"])
}

func TestUpdateBrokerWithOptions_Backend_AllFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

	// EngineVersion/HostInstanceType/AuthenticationStrategy/DataReplicationMode/
	// Logs/Configuration all stage into their Pending* slot and only take
	// effect on the next reboot; MaintenanceWindowStartTime has no Pending*
	// counterpart in the SDK and applies immediately. The broker's live
	// EngineVersion/HostInstanceType stay at CreateBroker's defaults until
	// then.
	assert.Equal(t, "5.15.14", updated.EngineVersion)
	assert.Equal(t, "5.18.3", updated.PendingEngineVersion)
	assert.Equal(t, "mq.m5.large", updated.HostInstanceType)
	assert.Equal(t, "mq.m5.xlarge", updated.PendingHostInstanceType)
	assert.Empty(t, updated.AuthenticationStrategy)
	assert.Equal(t, "SIMPLE", updated.PendingAuthStrategy)
	assert.Empty(t, updated.DataReplicationMode)
	assert.Equal(t, "CRDR", updated.PendingDataReplicationMode)
	require.NotNil(t, updated.LogsSummary)
	assert.False(t, updated.LogsSummary.General)
	require.NotNil(t, updated.LogsSummary.Pending)
	assert.True(t, updated.LogsSummary.Pending.General)
	require.NotNil(t, updated.MaintenanceWindowStartTime)
	assert.Equal(t, "FRIDAY", updated.MaintenanceWindowStartTime.DayOfWeek)
	require.NotNil(t, updated.Configurations)
	require.Nil(t, updated.Configurations.Current)
	require.NotNil(t, updated.Configurations.Pending)
	assert.Equal(t, configID, updated.Configurations.Pending.ID)

	require.NoError(t, b.RebootBroker(br.BrokerID))
	_, err = b.DescribeBroker(br.BrokerID) // observes REBOOT_IN_PROGRESS and promotes.
	require.NoError(t, err)

	settled, err := b.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "5.18.3", settled.EngineVersion)
	assert.Equal(t, "mq.m5.xlarge", settled.HostInstanceType)
	assert.Equal(t, "SIMPLE", settled.AuthenticationStrategy)
	assert.Equal(t, "CRDR", settled.DataReplicationMode)
	require.NotNil(t, settled.LogsSummary)
	assert.True(t, settled.LogsSummary.General)
	require.NotNil(t, settled.Configurations.Current)
	assert.Equal(t, configID, settled.Configurations.Current.ID)
}

func TestUpdateBroker_ResponseIncludesDataReplicationMode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "crdr-update-broker", mq.EngineTypeActiveMQ)

	updateRec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID, map[string]any{
		"dataReplicationMode": "CRDR",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// UpdateBrokerOutput.dataReplicationMode is the CURRENT (pre-reboot)
	// value -- unlike most other UpdateBroker response fields, it has a real
	// pendingDataReplicationMode counterpart to carry the target.
	out := parseResponse(t, updateRec)
	assert.NotEqual(t, "CRDR", out["dataReplicationMode"])
	assert.Equal(t, "CRDR", out["pendingDataReplicationMode"],
		"UpdateBroker response must echo back pendingDataReplicationMode")
}

func TestSnapshotRestore_PreservesEncryptionOptions(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestSnapshotRestore_PreservesLogs(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestSnapshotRestore_PreservesMaintenanceWindow(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestSnapshotRestore_PreservesDataReplicationMode(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

	// The mode is still staged (no reboot happened), so it must round-trip
	// as pendingDataReplicationMode, not the live dataReplicationMode.
	restored, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Empty(t, restored.DataReplicationMode)
	assert.Equal(t, "CRDR", restored.PendingDataReplicationMode)

	require.NoError(t, b2.RebootBroker(br.BrokerID))
	_, err = b2.DescribeBroker(br.BrokerID) // observes REBOOT_IN_PROGRESS and promotes.
	require.NoError(t, err)

	settled, err := b2.DescribeBroker(br.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "CRDR", settled.DataReplicationMode)
}
