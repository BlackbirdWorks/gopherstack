package mq_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// TestUserSummary_NoFabricatedConsoleAccess proves ListUsers and
// DescribeBroker's Users list no longer carry a "consoleAccess" key.
// aws-sdk-go-v2/service/mq/types.UserSummary (used by both
// ListUsersOutput.Users and DescribeBrokerOutput.Users) only has
// username/pendingChange -- confirmed by deserializers.go's
// awsRestjson1_deserializeDocumentUserSummary, whose case list is exactly
// "pendingChange"/"username" with a default branch that silently discards
// anything else. A real typed SDK client has no field to observe the extra
// key landing or not (it is silently dropped either way), so this uses a
// raw-body absence assertion instead, per the fabricated-key exception.
func TestUserSummary_NoFabricatedConsoleAccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	brokerID := createTestBroker(t, h, "consoleaccess-broker", mq.EngineTypeActiveMQ)

	createRec := doRequest(t, h, http.MethodPost, "/v1/brokers/"+brokerID+"/users/extra", map[string]any{
		"username":      "extra",
		"password":      "supersecretpassword1",
		"consoleAccess": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	listRec := doRequest(t, h, http.MethodGet, "/v1/brokers/"+brokerID+"/users", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.NotContains(t, listRec.Body.String(), "consoleAccess",
		"ListUsers' UserSummary items must not carry a fabricated consoleAccess key")

	broker := describeTestBroker(t, h, brokerID)
	usersRaw, err := json.Marshal(broker["users"])
	require.NoError(t, err)
	assert.NotContains(t, string(usersRaw), "consoleAccess",
		"DescribeBrokerOutput.Users (UserSummary) must not carry a fabricated consoleAccess key")
}
