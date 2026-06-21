package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestParity_ForgotPasswordRejectsDisabledUser verifies that ForgotPassword
// returns NotAuthorizedException for disabled users. Real AWS rejects the
// request with "User is disabled"; the emulator previously granted the reset
// code, allowing a bypass of the disable gate.
func TestParity_ForgotPasswordRejectsDisabledUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := paSetupPoolAndClient(t, h, "fp-disabled-pool", "fp-disabled-client")

	// Create and confirm a user.
	paSignUpAndConfirm(t, h, clientID, poolID, "disableduser", "Pass1234!")

	// Disable the user.
	disableRec := doCognitoRequest(t, h, "AdminDisableUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "disableduser",
	})
	require.Equal(t, http.StatusOK, disableRec.Code, "AdminDisableUser failed")

	// ForgotPassword on a disabled user must be rejected.
	rec := doCognitoRequest(t, h, "ForgotPassword", map[string]any{
		"ClientId": clientID,
		"Username": "disableduser",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "NotAuthorizedException", errResp["__type"],
		"disabled user must produce NotAuthorizedException")
}

// TestParity_ForgotPasswordRejectsUnconfirmedUser verifies that ForgotPassword
// returns InvalidParameterException for unconfirmed (UNCONFIRMED status) users.
// Real AWS rejects with "Cannot reset password for the user as there is no
// registered/verified email or phone_number".
func TestParity_ForgotPasswordRejectsUnconfirmedUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, clientID := paSetupPoolAndClient(t, h, "fp-unconf-pool", "fp-unconf-client")

	// Sign up but do NOT confirm the user.
	signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "unconfirmeduser",
		"Password": "Pass1234!",
	})
	require.Equal(t, http.StatusOK, signupRec.Code, "SignUp failed")

	// ForgotPassword on an UNCONFIRMED user must be rejected.
	rec := doCognitoRequest(t, h, "ForgotPassword", map[string]any{
		"ClientId": clientID,
		"Username": "unconfirmeduser",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp["__type"],
		"unconfirmed user must produce InvalidParameterException")
}

// TestParity_ForgotPasswordAcceptsConfirmedEnabledUser verifies the happy path
// still works: a confirmed, enabled user receives a reset code.
func TestParity_ForgotPasswordAcceptsConfirmedEnabledUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := paSetupPoolAndClient(t, h, "fp-ok-pool", "fp-ok-client")

	paSignUpAndConfirm(t, h, clientID, poolID, "confirmeduser", "Pass1234!")

	rec := doCognitoRequest(t, h, "ForgotPassword", map[string]any{
		"ClientId": clientID,
		"Username": "confirmeduser",
	})
	assert.Equal(t, http.StatusOK, rec.Code, "confirmed+enabled user should get reset code")
}

func paSetupPoolAndClient(t *testing.T, h *cognitoidp.Handler, poolName, clientName string) (string, string) {
	t.Helper()

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": poolName})
	require.Equal(t, http.StatusOK, poolRec.Code)

	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	clientRec := doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": clientName,
	})
	require.Equal(t, http.StatusOK, clientRec.Code)

	var clientResp map[string]any
	require.NoError(t, json.Unmarshal(clientRec.Body.Bytes(), &clientResp))
	clientID := clientResp["UserPoolClient"].(map[string]any)["ClientId"].(string)

	return poolID, clientID
}

func paSignUpAndConfirm(t *testing.T, h *cognitoidp.Handler, clientID, poolID, username, password string) {
	t.Helper()

	signupRec := doCognitoRequest(t, h, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": username,
		"Password": password,
	})
	require.Equal(t, http.StatusOK, signupRec.Code, "SignUp failed for %s", username)

	confirmRec := doCognitoRequest(t, h, "AdminConfirmSignUp", map[string]any{
		"UserPoolId": poolID,
		"Username":   username,
	})
	require.Equal(t, http.StatusOK, confirmRec.Code, "AdminConfirmSignUp failed for %s", username)
}
