package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitoidptypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestCreateUserPool_DuplicateName_RealClient covers a fabricated-code bug:
// gopherstack rejected a second pool with a name already in use, raising a
// wire code of "UserPoolAlreadyExistsException" — a code that does not exist
// anywhere in cognitoidentityprovider@v1.67.4 (not types/errors.go, not any
// deserializer). Real AWS Cognito does not enforce unique pool names —
// CreateUserPool's own deserializer models no "already exists" exception at
// all — so a second pool with the same name must succeed with a distinct ID.
func TestCreateUserPool_DuplicateName_RealClient(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
	ctx := t.Context()

	first, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("dup-pool"),
	})
	require.NoError(t, err)

	second, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("dup-pool"),
	})
	require.NoError(t, err, "a second pool with the same name must succeed, not error")
	require.NotEqual(t, aws.ToString(first.UserPool.Id), aws.ToString(second.UserPool.Id))
}

// TestAdminCreateUser_DuplicateUsername_RealClient covers a fabricated-code
// bug: gopherstack raised a wire code of "UserAlreadyExistsException" for a
// duplicate username, which does not exist anywhere in
// cognitoidentityprovider@v1.67.4. AdminCreateUser's own deserializer models
// UsernameExistsException for this.
func TestAdminCreateUser_DuplicateUsername_RealClient(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("acu-pool")})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("dupuser"),
	})
	require.NoError(t, err)

	_, err = client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("dupuser"),
	})
	require.Error(t, err)

	var ue *cognitoidptypes.UsernameExistsException
	require.ErrorAs(t, err, &ue, "expected a real UsernameExistsException from the SDK deserializer")
}

// TestAdminGetDevice_UnknownUser_RealClient covers a wrong-code bug:
// AdminGetDevice/AdminListDevices raised UserNotFoundException for a missing
// user, but their own deserializers model ResourceNotFoundException (unlike
// AdminGetUser and similar ops, which do model UserNotFoundException).
func TestAdminGetDevice_UnknownUser_RealClient(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("agd-pool")})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.AdminGetDevice(ctx, &cognitoidpsdk.AdminGetDeviceInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("no-such-user"),
		DeviceKey:  aws.String("device-1"),
	})
	require.Error(t, err)

	var rnf *cognitoidptypes.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf, "expected a real ResourceNotFoundException from the SDK deserializer")
}

func TestAdminListDevices_UnknownUser_RealClient(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("ald-pool")})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.AdminListDevices(ctx, &cognitoidpsdk.AdminListDevicesInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("no-such-user"),
	})
	require.Error(t, err)

	var rnf *cognitoidptypes.ResourceNotFoundException
	require.ErrorAs(t, err, &rnf, "expected a real ResourceNotFoundException from the SDK deserializer")
}

// TestAddCustomAttributes_InvalidName_RealClient covers a wrong-code bug:
// AddCustomAttributes raised InvalidUserPoolConfigurationException for a
// custom attribute name missing the "custom:" prefix, but its own
// deserializer models InvalidParameterException — unlike InitiateAuth/
// AdminInitiateAuth, which genuinely do model
// InvalidUserPoolConfigurationException for auth-flow misconfiguration.
func TestAddCustomAttributes_InvalidName_RealClient(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("aca-pool")})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.AddCustomAttributes(ctx, &cognitoidpsdk.AddCustomAttributesInput{
		UserPoolId: aws.String(poolID),
		CustomAttributes: []cognitoidptypes.SchemaAttributeType{
			{Name: aws.String("not-prefixed"), AttributeDataType: cognitoidptypes.AttributeDataTypeString},
		},
	})
	require.Error(t, err)

	var ipe *cognitoidptypes.InvalidParameterException
	require.ErrorAs(t, err, &ipe, "expected a real InvalidParameterException from the SDK deserializer")
}

// TestCreateUserPoolDomain_Duplicate_RealClient covers a wrong-code bug:
// CreateUserPoolDomain raised GroupExistsException (CreateGroup's sentinel)
// for a domain already in use, but its own deserializer models
// InvalidParameterException — it has no dedicated "already exists" exception.
func TestCreateUserPoolDomain_Duplicate_RealClient(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("dom-pool")})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.CreateUserPoolDomain(ctx, &cognitoidpsdk.CreateUserPoolDomainInput{
		UserPoolId: aws.String(poolID),
		Domain:     aws.String("dup-domain-error-sweep"),
	})
	require.NoError(t, err)

	pool2, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("dom-pool-2")})
	require.NoError(t, err)

	_, err = client.CreateUserPoolDomain(ctx, &cognitoidpsdk.CreateUserPoolDomainInput{
		UserPoolId: aws.String(aws.ToString(pool2.UserPool.Id)),
		Domain:     aws.String("dup-domain-error-sweep"),
	})
	require.Error(t, err)

	var ipe *cognitoidptypes.InvalidParameterException
	require.ErrorAs(t, err, &ipe, "expected a real InvalidParameterException from the SDK deserializer")
}

// TestRevokeToken_WrongClient_RealClient covers a wrong-code bug: RevokeToken
// raised NotAuthorizedException for a token issued to a different client, but
// its own deserializer models UnauthorizedException, not the generic
// NotAuthorizedException most other ops use.
func TestRevokeToken_WrongClient_RealClient(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("rt-pool")})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	clientA, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("rt-client-a"),
	})
	require.NoError(t, err)
	clientAID := aws.ToString(clientA.UserPoolClient.ClientId)

	clientB, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("rt-client-b"),
	})
	require.NoError(t, err)
	clientBID := aws.ToString(clientB.UserPoolClient.ClientId)

	_, err = client.SignUp(ctx, &cognitoidpsdk.SignUpInput{
		ClientId: aws.String(clientAID),
		Username: aws.String("rt-user"),
		Password: aws.String("Passw0rd!"),
	})
	require.NoError(t, err)

	_, err = client.AdminConfirmSignUp(ctx, &cognitoidpsdk.AdminConfirmSignUpInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String("rt-user"),
	})
	require.NoError(t, err)

	authOut, err := client.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
		AuthFlow: cognitoidptypes.AuthFlowTypeUserPasswordAuth,
		ClientId: aws.String(clientAID),
		AuthParameters: map[string]string{
			"USERNAME": "rt-user",
			"PASSWORD": "Passw0rd!",
		},
	})
	require.NoError(t, err)
	refreshToken := aws.ToString(authOut.AuthenticationResult.RefreshToken)
	require.NotEmpty(t, refreshToken)

	_, err = client.RevokeToken(ctx, &cognitoidpsdk.RevokeTokenInput{
		ClientId: aws.String(clientBID),
		Token:    aws.String(refreshToken),
	})
	require.Error(t, err)

	var ue *cognitoidptypes.UnauthorizedException
	require.ErrorAs(t, err, &ue, "expected a real UnauthorizedException from the SDK deserializer")
}
