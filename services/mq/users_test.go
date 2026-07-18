package mq_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// makeGroups builds n distinct group names, used to exercise the
// ActiveMQ max-groups-per-user limit.
func makeGroups(n int) []string {
	groups := make([]string, n)
	for i := range groups {
		groups[i] = fmt.Sprintf("group%d", i)
	}

	return groups
}

func TestUserLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		username string
	}{
		{
			name:     "create_and_delete_user",
			username: "testuser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			brokerID := createTestBroker(t, h, "test-broker", mq.EngineTypeActiveMQ)

			// Create user.
			rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/"+tt.username, map[string]any{
				"password":      "password1234",
				"consoleAccess": true,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Describe user.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users/"+tt.username, nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.username, parseResponse(t, rec)["username"])

			// List users.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			users, ok := parseResponse(t, rec)["users"].([]any)
			require.True(t, ok)
			assert.Len(t, users, 1)

			// Delete user.
			rec = doRequest(t, h, http.MethodDelete, "/v1/brokers/"+brokerID+"/users/"+tt.username, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify deletion.
			rec = doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users/"+tt.username, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestUpdateUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "broker-for-user-update", mq.EngineTypeActiveMQ)

	// Create user.
	rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/myuser", map[string]any{
		"password": "oldpassword1234",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update user (PUT for update).
	rec = doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID+"/users/myuser", map[string]any{
		"password": "newpassword1234",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestUpdateUser_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "test-broker-upd", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/admin", map[string]any{
		"password": "password1234",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Send invalid JSON body for update (nil marshals to a body of "null",
	// which json.Unmarshal into a struct rejects... use the raw HTTP path
	// instead so we control the literal bytes).
	rec = doRawJSONRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID+"/users/admin", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdateUser_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "broker-no-users", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPut, "/v1/brokers/"+brokerID+"/users/nonexistent", map[string]any{
		"password": "newpassword1234",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUserOps_BrokerNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "create_user_on_nonexistent_broker",
			method:     http.MethodPost,
			path:       "/v1/brokers/nonexistent/users/admin",
			body:       map[string]any{"password": "adminpassword1234"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "describe_user_not_found",
			method:     http.MethodGet,
			path:       "/v1/brokers/nonexistent/users/admin",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_user_not_found",
			method:     http.MethodDelete,
			path:       "/v1/brokers/nonexistent/users/admin",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_users_broker_not_found",
			method:     http.MethodGet,
			path:       "/v1/brokers/nonexistent/users",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDescribeUser_DoesNotReturnPassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "pwd-sec-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/secuser", map[string]any{
			"password": "SecurePassword1!",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doRequest(t, h, http.MethodGet,
		"/v1/brokers/"+brokerID+"/users/secuser", nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	body := descRec.Body.String()
	assert.NotContains(t, body, "SecurePassword1!", "password must NOT appear in DescribeUser response")
	assert.NotContains(t, body, "password", "password key must NOT appear in DescribeUser response")
}

func TestListUsers_DoesNotReturnPasswords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "list-pwd-broker", mq.EngineTypeActiveMQ)

	for _, name := range []string{"user1", "user2", "user3"} {
		rec := doRequest(t, h, http.MethodPost,
			"/v1/brokers/"+brokerID+"/users/"+name, map[string]any{
				"password": "Password123!",
			})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	body := listRec.Body.String()
	assert.NotContains(t, body, "Password123!", "passwords must NOT appear in ListUsers response")
}

func TestDescribeUser_GroupsEmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bid := createTestBroker(t, h, "groups-check-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/v1/brokers/%s/users/alice", bid),
		map[string]any{"password": "ValidPass12!!"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet,
		fmt.Sprintf("/v1/brokers/%s/users/alice", bid), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	groups, hasGroupsKey := resp["groups"]
	assert.True(t, hasGroupsKey, "DescribeUser must include 'groups' key even when empty")
	assert.IsType(t, []any{}, groups, "'groups' must be an array, not null")
	assert.Empty(t, groups, "'groups' must be [] when user has no groups")
}

func TestDescribeUser_GroupsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bid := createTestBroker(t, h, "roundtrip-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/v1/brokers/%s/users/bob", bid),
		map[string]any{
			"password": "ValidPass12!!",
			"groups":   []string{"admin", "ops"},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet,
		fmt.Sprintf("/v1/brokers/%s/users/bob", bid), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	groups := parseResponse(t, rec)["groups"].([]any)
	require.Len(t, groups, 2)
	assert.Contains(t, groups, "admin")
	assert.Contains(t, groups, "ops")
}

func TestListUsers_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bid := createTestBroker(t, h, "listusers-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers/"+bid+"/users", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	users, hasUsers := resp["users"]
	assert.True(t, hasUsers, "ListUsers must include 'users' key")
	assert.IsType(t, []any{}, users, "'users' must be an array")
	assert.Empty(t, users, "'users' must be [] when no users exist")
}

func TestDescribeUser_AllFieldsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "user-describe-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/myuser", map[string]any{
			"password":      "MySecurePass1!",
			"consoleAccess": true,
			"groups":        []string{"admins", "operators"},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doRequest(t, h, http.MethodGet,
		"/v1/brokers/"+brokerID+"/users/myuser", nil)
	require.Equal(t, http.StatusOK, desc.Code)

	out := parseResponse(t, desc)
	assert.Equal(t, brokerID, out["brokerId"])
	assert.Equal(t, "myuser", out["username"])
	assert.Equal(t, true, out["consoleAccess"])

	groups, ok := out["groups"].([]any)
	require.True(t, ok, "groups must be present")
	assert.Len(t, groups, 2)
}

func TestListUsers_SortedByUsername(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "sorted-users", mq.EngineTypeActiveMQ)

	for _, name := range []string{"zebra", "alpha", "middle"} {
		rec := doRequest(t, h, http.MethodPost,
			"/v1/brokers/"+brokerID+"/users/"+name, map[string]any{
				"password": "SortedUsers123!",
			})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	out := parseResponse(t, listRec)
	users, ok := out["users"].([]any)
	require.True(t, ok && len(users) == 3)

	names := make([]string, len(users))
	for i, u := range users {
		names[i] = u.(map[string]any)["username"].(string)
	}
	assert.Equal(t, []string{"alpha", "middle", "zebra"}, names,
		"ListUsers must return users sorted by username")
}

func TestCreateUser_AlreadyExists_Returns409(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "dup-user-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/dupuser", map[string]any{
			"password": "DupUserPassword1!",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/dupuser", map[string]any{
			"password": "DupUserPassword1!",
		})
	assert.Equal(t, http.StatusConflict, rec2.Code,
		"duplicate CreateUser must return 409 Conflict")
}

func TestDeleteUser_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "del-user-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodDelete,
		"/v1/brokers/"+brokerID+"/users/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateUser_UsernameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		username string
		name     string
		wantCode int
	}{
		{name: "valid username alphanumeric", username: "alice", wantCode: http.StatusOK},
		{name: "valid username with hyphen", username: "alice-b", wantCode: http.StatusOK},
		{name: "valid username with underscore", username: "alice_b", wantCode: http.StatusOK},
		{name: "valid username exactly 2 chars", username: "ab", wantCode: http.StatusOK},
		{name: "valid username exactly 100 chars", username: strings.Repeat("a", 100), wantCode: http.StatusOK},
		{name: "username too short (1 char) rejected", username: "a", wantCode: http.StatusBadRequest},
		{
			name: "username too long (101 chars) rejected", username: strings.Repeat("a", 101),
			wantCode: http.StatusBadRequest,
		},
		{name: "username starting with hyphen rejected", username: "-bad", wantCode: http.StatusBadRequest},
		{name: "username starting with underscore rejected", username: "_bad", wantCode: http.StatusBadRequest},
		{name: "username with @-sign rejected", username: "user@host", wantCode: http.StatusBadRequest},
		{name: "username with colon rejected", username: "user:name", wantCode: http.StatusBadRequest},
		{name: "username with comma rejected", username: "user,name", wantCode: http.StatusBadRequest},
		{name: "period allowed", username: "john.doe", wantCode: http.StatusOK},
		{name: "tilde allowed", username: "svc~user", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bid := createTestBroker(t, h, "test-broker", mq.EngineTypeActiveMQ)

			rec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/v1/brokers/%s/users/%s", bid, tt.username),
				map[string]any{"password": "ValidPass12!!"})
			assert.Equal(t, tt.wantCode, rec.Code, "username=%q: %s", tt.username, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "BadRequestException", parseResponse(t, rec)["__type"])
			}
		})
	}
}

// TestCreateUser_UsernameValidation_Backend covers characters that cannot
// appear in a URL path (e.g. space) and must be tested at the backend level.
func TestCreateUser_UsernameValidation_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		username string
		name     string
		wantErr  bool
	}{
		{name: "space in username rejected", username: "bad user", wantErr: true},
		{name: "slash in username rejected", username: "bad/user", wantErr: true},
		{name: "valid underscore username", username: "valid_user", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			br, err := b.CreateBrokerWithOptions(
				"bk", mq.DeploymentModeSingleInstance, mq.EngineTypeActiveMQ,
				"", "", false, false, nil, nil, nil, nil, nil,
			)
			require.NoError(t, err)

			err = b.CreateUser(br.BrokerID, tt.username, "ValidPass12!!", nil, false)
			if tt.wantErr {
				assert.Error(t, err, "username=%q should be rejected", tt.username)
			} else {
				assert.NoError(t, err, "username=%q should be accepted", tt.username)
			}
		})
	}
}

func TestCreateUser_ActiveMQMaxUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		count    int
		wantCode int
	}{
		{name: "creating 249th user succeeds", count: 249, wantCode: http.StatusOK},
		{name: "creating 250th user succeeds (at limit)", count: 250, wantCode: http.StatusOK},
		{name: "creating 251st user fails (over limit)", count: 251, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			h := mq.NewHandler(b)

			bid := createTestBroker(t, h, "big-broker", mq.EngineTypeActiveMQ)

			// Seed users up to tt.count-1 via backend to avoid HTTP overhead.
			for i := range tt.count - 1 {
				err := b.CreateUser(bid, fmt.Sprintf("user%04d", i), "ValidPass12!!", nil, false)
				require.NoError(t, err)
			}

			// The last create via HTTP is the one under test.
			username := fmt.Sprintf("user%04d", tt.count-1)
			rec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/v1/brokers/%s/users/%s", bid, username),
				map[string]any{"password": "ValidPass12!!"})
			assert.Equal(t, tt.wantCode, rec.Code, "count=%d: %s", tt.count, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "BadRequestException", parseResponse(t, rec)["__type"])
			}
		})
	}
}

func TestCreateUser_ActiveMQMaxGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		groups   []string
		wantCode int
	}{
		{name: "zero groups accepted", groups: nil, wantCode: http.StatusOK},
		{name: "20 groups accepted (at limit)", groups: makeGroups(20), wantCode: http.StatusOK},
		{name: "21 groups rejected (over limit)", groups: makeGroups(21), wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bid := createTestBroker(t, h, "groups-broker", mq.EngineTypeActiveMQ)

			body := map[string]any{"password": "ValidPass12!!"}
			if tt.groups != nil {
				body["groups"] = tt.groups
			}

			rec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/v1/brokers/%s/users/testuser", bid),
				body)
			assert.Equal(t, tt.wantCode, rec.Code, "groups=%d: %s", len(tt.groups), rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "BadRequestException", parseResponse(t, rec)["__type"])
			}
		})
	}
}

func TestUpdateUser_MaxGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		groups   []string
		wantCode int
	}{
		{name: "update to 20 groups accepted", groups: makeGroups(20), wantCode: http.StatusOK},
		{name: "update to 21 groups rejected", groups: makeGroups(21), wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bid := createTestBroker(t, h, "upd-groups-broker", mq.EngineTypeActiveMQ)

			// Create user first.
			rec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/v1/brokers/%s/users/editme", bid),
				map[string]any{"password": "ValidPass12!!"})
			require.Equal(t, http.StatusOK, rec.Code)

			// Now update with the test groups.
			rec = doRequest(t, h, http.MethodPut,
				fmt.Sprintf("/v1/brokers/%s/users/editme", bid),
				map[string]any{"groups": tt.groups})
			assert.Equal(t, tt.wantCode, rec.Code, "groups=%d: %s", len(tt.groups), rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "BadRequestException", parseResponse(t, rec)["__type"])
			}
		})
	}
}

func TestCreateUser_PasswordValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
		password   string
		wantStatus int
	}{
		{
			name: "colon_rejected", engineType: mq.EngineTypeActiveMQ,
			password: "Bad:Password123", wantStatus: http.StatusBadRequest,
		},
		{
			name: "equals_rejected", engineType: mq.EngineTypeActiveMQ,
			password: "Bad=Password123", wantStatus: http.StatusBadRequest,
		},
		{
			name: "comma_rejected", engineType: mq.EngineTypeActiveMQ,
			password: "Bad,Password123", wantStatus: http.StatusBadRequest,
		},
		{
			name: "short_password_rejected", engineType: mq.EngineTypeActiveMQ,
			password: "short", wantStatus: http.StatusBadRequest,
		},
		{
			name: "too_few_unique_chars_rejected", engineType: mq.EngineTypeActiveMQ,
			password: "aaaaaaaaaaaa", wantStatus: http.StatusBadRequest,
		},
		{
			name: "valid_password_accepted", engineType: mq.EngineTypeActiveMQ,
			password: "GoodPassword123", wantStatus: http.StatusOK,
		},
		{
			name: "exact_min_length_accepted", engineType: mq.EngineTypeActiveMQ,
			password: "Pass1234abcd", wantStatus: http.StatusOK,
		},
		{
			name: "rabbitmq_short_password_accepted", engineType: mq.EngineTypeRabbitMQ,
			password: "short", wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			brokerID := createTestBroker(t, h, "pwd-validation-"+tt.name, tt.engineType)

			rec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/tester", map[string]any{
				"password": tt.password,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "password=%q: %s", tt.password, rec.Body.String())
		})
	}
}

func TestCreateUser_PasswordValidation_Backend(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
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

func TestUpdateUser_PasswordValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "upd-pwd-test", mq.EngineTypeActiveMQ)

	// Create user with valid password first.
	rec := doRequest(t, h, http.MethodPost,
		"/v1/brokers/"+brokerID+"/users/upduser", map[string]any{
			"password": "ValidPassword1!",
		})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update with short password.
	rec = doRequest(t, h, http.MethodPut,
		"/v1/brokers/"+brokerID+"/users/upduser", map[string]any{
			"password": "tooshort",
		})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"UpdateUser with short ActiveMQ password must return 400")
}

func TestCreateUser_UsernameCharset_AllowsPeriodAndTilde(t *testing.T) {
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

			h := newTestHandler(t)
			brokerID := createTestBroker(t, h, "username-charset-"+tt.name, mq.EngineTypeActiveMQ)

			userPath := "/v1/brokers/" + brokerID + "/users/" + tt.username
			userRec := doRequest(t, h, http.MethodPost, userPath, map[string]any{
				"password": "ValidPassword123",
			})
			assert.Equal(t, tt.wantStatus, userRec.Code, "username=%q: %s", tt.username, userRec.Body.String())
		})
	}
}
