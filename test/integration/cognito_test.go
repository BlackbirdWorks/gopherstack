package integration_test

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitotypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CognitoIDP_FullFlow exercises the full Cognito IDP lifecycle:
// CreateUserPool → CreateUserPoolClient → AdminCreateUser → AdminSetUserPassword →
// AdminInitiateAuth → validate JWT → JWKS endpoint → SignUp → ConfirmSignUp → InitiateAuth.
func TestIntegration_CognitoIDP_FullFlow(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCognitoIDPClient(t)
	ctx := t.Context()

	// 1. Create user pool.
	poolOut, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("test-pool-" + t.Name()),
	})
	require.NoError(t, err, "CreateUserPool failed")
	require.NotNil(t, poolOut.UserPool)

	poolID := aws.ToString(poolOut.UserPool.Id)
	assert.NotEmpty(t, poolID, "pool ID should not be empty")

	// 2. Create app client.
	clientOut, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("test-client"),
	})
	require.NoError(t, err, "CreateUserPoolClient failed")
	require.NotNil(t, clientOut.UserPoolClient)

	appClientID := aws.ToString(clientOut.UserPoolClient.ClientId)
	assert.NotEmpty(t, appClientID, "client ID should not be empty")

	// 3. AdminCreateUser creates a user with FORCE_CHANGE_PASSWORD status.
	createUserOut, err := client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("admin-test-user"),
		UserAttributes: []cognitotypes.AttributeType{
			{Name: aws.String("email"), Value: aws.String("admin-test@example.com")},
		},
		TemporaryPassword: aws.String("TempPass123!"),
	})
	require.NoError(t, err, "AdminCreateUser failed")
	require.NotNil(t, createUserOut.User)
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", string(createUserOut.User.UserStatus))

	// 4. AdminSetUserPassword sets a permanent password, confirming the user.
	_, err = client.AdminSetUserPassword(ctx, &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("admin-test-user"),
		Password:   aws.String("PermPass456!"),
		Permanent:  true,
	})
	require.NoError(t, err, "AdminSetUserPassword failed")

	// 5. AdminGetUser verifies the user is confirmed.
	getUserOut, err := client.AdminGetUser(ctx, &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("admin-test-user"),
	})
	require.NoError(t, err, "AdminGetUser failed")
	assert.Equal(t, "CONFIRMED", string(getUserOut.UserStatus))

	// 6. AdminInitiateAuth authenticates the user and returns tokens.
	authOut, err := client.AdminInitiateAuth(ctx, &cognitoidpsdk.AdminInitiateAuthInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String(appClientID),
		AuthFlow:   cognitotypes.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{
			"USERNAME": "admin-test-user",
			"PASSWORD": "PermPass456!",
		},
	})
	require.NoError(t, err, "AdminInitiateAuth failed")
	require.NotNil(t, authOut.AuthenticationResult, "AuthenticationResult should not be nil")

	idToken := aws.ToString(authOut.AuthenticationResult.IdToken)
	accessToken := aws.ToString(authOut.AuthenticationResult.AccessToken)
	refreshToken := aws.ToString(authOut.AuthenticationResult.RefreshToken)
	assert.NotEmpty(t, idToken, "ID token should not be empty")
	assert.NotEmpty(t, accessToken, "access token should not be empty")
	assert.NotEmpty(t, refreshToken, "refresh token should not be empty")
	assert.Equal(t, int32(3600), authOut.AuthenticationResult.ExpiresIn)

	// 7. Fetch the JWKS and use it to verify the tokens.
	jwksURL := fmt.Sprintf("%s/%s/.well-known/jwks.json", endpoint, poolID)
	jwksResp, httpErr := http.Get(jwksURL)
	require.NoError(t, httpErr, "JWKS GET failed")
	defer jwksResp.Body.Close()
	require.Equal(t, http.StatusOK, jwksResp.StatusCode, "JWKS endpoint should return 200")

	jwksBody, readErr := io.ReadAll(jwksResp.Body)
	require.NoError(t, readErr, "reading JWKS body failed")

	var jwks struct {
		Keys []struct {
			Kty string `json:"kty"`
			N   string `json:"n"`
			E   string `json:"e"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
		} `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(jwksBody, &jwks), "JWKS should be valid JSON")
	require.Len(t, jwks.Keys, 1, "JWKS should have exactly one key")
	assert.Equal(t, "RSA", jwks.Keys[0].Kty)
	assert.Equal(t, "sig", jwks.Keys[0].Use)
	assert.Equal(t, "RS256", jwks.Keys[0].Alg)
	assert.NotEmpty(t, jwks.Keys[0].N)
	assert.NotEmpty(t, jwks.Keys[0].E)

	// 8. Verify the JWT signature using the JWKS public key.
	pubKey, parseErr := parseJWKPublicKey(jwks.Keys[0].N, jwks.Keys[0].E)
	require.NoError(t, parseErr, "should be able to parse JWK public key")

	parsedToken, tokenErr := jwt.Parse(idToken, func(_ *jwt.Token) (any, error) {
		return pubKey, nil
	}, jwt.WithValidMethods([]string{"RS256"}))
	require.NoError(t, tokenErr, "ID token should be valid and verifiable")
	assert.True(t, parsedToken.Valid, "ID token should be valid")

	claims, ok := parsedToken.Claims.(jwt.MapClaims)
	require.True(t, ok, "claims should be MapClaims")
	assert.Equal(t, "id", claims["token_use"])
	assert.Equal(t, "admin-test-user", claims["cognito:username"])
}

// TestIntegration_CognitoIDP_SignUpConfirmAuth exercises the self-service auth flow.
func TestIntegration_CognitoIDP_SignUpConfirmAuth(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCognitoIDPClient(t)
	ctx := t.Context()

	// Create pool and client.
	poolOut, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("signup-pool-" + t.Name()),
	})
	require.NoError(t, err)

	poolID := aws.ToString(poolOut.UserPool.Id)

	clientOut, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("signup-client"),
	})
	require.NoError(t, err)

	appClientID := aws.ToString(clientOut.UserPoolClient.ClientId)

	// SignUp creates a new user with UNCONFIRMED status.
	signupOut, err := client.SignUp(ctx, &cognitoidpsdk.SignUpInput{
		ClientId: aws.String(appClientID),
		Username: aws.String("newuser@example.com"),
		Password: aws.String("MyP@ssw0rd!"),
		UserAttributes: []cognitotypes.AttributeType{
			{Name: aws.String("email"), Value: aws.String("newuser@example.com")},
		},
	})
	require.NoError(t, err, "SignUp failed")
	assert.False(t, signupOut.UserConfirmed, "user should be unconfirmed after SignUp")
	assert.NotEmpty(t, aws.ToString(signupOut.UserSub), "UserSub should not be empty")

	// ConfirmSignUp confirms the user (any non-empty code is accepted by the mock).
	_, err = client.ConfirmSignUp(ctx, &cognitoidpsdk.ConfirmSignUpInput{
		ClientId:         aws.String(appClientID),
		Username:         aws.String("newuser@example.com"),
		ConfirmationCode: aws.String("123456"),
	})
	require.NoError(t, err, "ConfirmSignUp failed")

	// InitiateAuth authenticates the confirmed user.
	authOut, err := client.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(appClientID),
		AuthFlow: cognitotypes.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{
			"USERNAME": "newuser@example.com",
			"PASSWORD": "MyP@ssw0rd!",
		},
	})
	require.NoError(t, err, "InitiateAuth failed")
	require.NotNil(t, authOut.AuthenticationResult, "should get authentication result")
	assert.NotEmpty(t, aws.ToString(authOut.AuthenticationResult.IdToken))
	assert.NotEmpty(t, aws.ToString(authOut.AuthenticationResult.AccessToken))
	assert.NotEmpty(t, aws.ToString(authOut.AuthenticationResult.RefreshToken))
}

// TestIntegration_CognitoIDP_DescribePool exercises describe operations.
func TestIntegration_CognitoIDP_DescribePool(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCognitoIDPClient(t)
	ctx := t.Context()

	// Create a pool.
	poolName := "describe-pool-" + t.Name()
	poolOut, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String(poolName),
	})
	require.NoError(t, err)

	poolID := aws.ToString(poolOut.UserPool.Id)

	// DescribeUserPool returns the same pool.
	descOut, err := client.DescribeUserPool(ctx, &cognitoidpsdk.DescribeUserPoolInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err, "DescribeUserPool failed")
	assert.Equal(t, poolID, aws.ToString(descOut.UserPool.Id))
	assert.Equal(t, poolName, aws.ToString(descOut.UserPool.Name))

	// ListUserPools includes the new pool.
	listOut, err := client.ListUserPools(ctx, &cognitoidpsdk.ListUserPoolsInput{
		MaxResults: aws.Int32(60),
	})
	require.NoError(t, err, "ListUserPools failed")
	found := false

	for _, p := range listOut.UserPools {
		if aws.ToString(p.Id) == poolID {
			found = true

			break
		}
	}

	assert.True(t, found, "created pool should appear in ListUserPools")

	// Create and describe a client.
	clientOut, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("desc-client"),
	})
	require.NoError(t, err)

	appClientID := aws.ToString(clientOut.UserPoolClient.ClientId)

	descClientOut, err := client.DescribeUserPoolClient(ctx, &cognitoidpsdk.DescribeUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String(appClientID),
	})
	require.NoError(t, err, "DescribeUserPoolClient failed")
	assert.Equal(t, appClientID, aws.ToString(descClientOut.UserPoolClient.ClientId))
	assert.Equal(t, poolID, aws.ToString(descClientOut.UserPoolClient.UserPoolId))
}

// parseJWKPublicKey reconstructs an RSA public key from the base64url-encoded n and e JWK fields.
func parseJWKPublicKey(nB64, eB64 string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nB64)
	if err != nil {
		return nil, fmt.Errorf("decoding n: %w", err)
	}

	eBytes, err := base64.RawURLEncoding.DecodeString(eB64)
	if err != nil {
		return nil, fmt.Errorf("decoding e: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := int(new(big.Int).SetBytes(eBytes).Int64())

	return &rsa.PublicKey{N: n, E: e}, nil
}
