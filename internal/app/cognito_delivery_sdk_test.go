package app

import (
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

var cognitoCodePattern = regexp.MustCompile(`code is (\S+)`)

type cognitoSDKEnv struct {
	client *cognitoidpsdk.Client
	ses    *sesbackend.InMemoryBackend
	sns    *snsbackend.InMemoryBackend
}

func newCognitoSDKEnv(t *testing.T) *cognitoSDKEnv {
	t.Helper()

	ses, sns := sesbackend.NewInMemoryBackend(), snsbackend.NewInMemoryBackend()
	cognito := cognitoidpbackend.NewHandler(
		cognitoidpbackend.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost"), "us-east-1")
	wireCognitoMessageDelivery(cognito, sesbackend.NewHandler(ses), snsbackend.NewHandler(sns))

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(cognito))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	client := cognitoidpsdk.NewFromConfig(cfg, func(o *cognitoidpsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	return &cognitoSDKEnv{client: client, ses: ses, sns: sns}
}

func (e *cognitoSDKEnv) newPool(t *testing.T, in *cognitoidpsdk.CreateUserPoolInput) (string, string) {
	t.Helper()

	in.PoolName = aws.String("delivery")

	pool, err := e.client.CreateUserPool(t.Context(), in)
	require.NoError(t, err)

	app, err := e.client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: pool.UserPool.Id, ClientName: aws.String("app"),
	})
	require.NoError(t, err)

	return *pool.UserPool.Id, *app.UserPoolClient.ClientId
}

func TestCognitoSignUpDeliveryConfirmsViaSDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		attribute string
		value     string
		verified  types.VerifiedAttributeType
		sms       bool
	}{
		{
			name: "email via ses", attribute: "email", value: "bob@corp.example",
			verified: types.VerifiedAttributeTypeEmail,
		},
		{
			name: "sms via sns", attribute: "phone_number", value: "+15555550123",
			verified: types.VerifiedAttributeTypePhoneNumber, sms: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCognitoSDKEnv(t)
			poolID, clientID := env.newPool(t, &cognitoidpsdk.CreateUserPoolInput{
				AutoVerifiedAttributes: []types.VerifiedAttributeType{tt.verified},
			})

			out, err := env.client.SignUp(t.Context(), &cognitoidpsdk.SignUpInput{
				ClientId: aws.String(clientID), Username: aws.String("u1"), Password: aws.String("Pass1234!"),
				UserAttributes: []types.AttributeType{{Name: aws.String(tt.attribute), Value: aws.String(tt.value)}},
			})
			require.NoError(t, err)
			require.NotNil(t, out.CodeDeliveryDetails)
			assert.NotEqual(t, tt.value, aws.ToString(out.CodeDeliveryDetails.Destination))

			var body string

			if tt.sms {
				sms := env.sns.DrainSMSDeliveries()
				require.Len(t, sms, 1)
				assert.Equal(t, tt.value, sms[0].PhoneNumber)
				body = sms[0].Message
			} else {
				emails := env.ses.ListEmails()
				require.Len(t, emails, 1)
				assert.Equal(t, []string{tt.value}, emails[0].To)
				body = emails[0].BodyText
			}

			match := cognitoCodePattern.FindStringSubmatch(body)
			require.Len(t, match, 2)

			code := match[1]

			_, err = env.client.ConfirmSignUp(t.Context(), &cognitoidpsdk.ConfirmSignUpInput{
				ClientId: aws.String(clientID), Username: aws.String("u1"), ConfirmationCode: aws.String(code),
			})
			require.NoError(t, err)

			user, err := env.client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
				UserPoolId: aws.String(poolID), Username: aws.String("u1"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.UserStatusTypeConfirmed, user.UserStatus)
		})
	}
}

func TestCognitoDeveloperEmailRequiresVerifiedIdentity(t *testing.T) {
	t.Parallel()

	const identityARN = "arn:aws:ses:us-east-1:000000000000:identity/ops@corp.example"

	tests := []struct {
		name     string
		wantCode string
		verify   bool
	}{
		{name: "verified identity sends", verify: true},
		{name: "unverified identity rejected", wantCode: "InvalidEmailRoleAccessPolicyException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCognitoSDKEnv(t)
			if tt.verify {
				require.NoError(t, env.ses.VerifyEmailIdentity("ops@corp.example"))
			}

			_, clientID := env.newPool(t, &cognitoidpsdk.CreateUserPoolInput{
				AutoVerifiedAttributes: []types.VerifiedAttributeType{types.VerifiedAttributeTypeEmail},
				EmailConfiguration: &types.EmailConfigurationType{
					EmailSendingAccount: types.EmailSendingAccountTypeDeveloper,
					From:                aws.String("ops@corp.example"), SourceArn: aws.String(identityARN),
				},
			})

			_, err := env.client.SignUp(t.Context(), &cognitoidpsdk.SignUpInput{
				ClientId: aws.String(clientID), Username: aws.String("u1"), Password: aws.String("Pass1234!"),
				UserAttributes: []types.AttributeType{
					{Name: aws.String("email"), Value: aws.String("bob@corp.example")},
				},
			})

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantCode)

				return
			}

			require.NoError(t, err)
			assert.Len(t, env.ses.ListEmails(), 1)
		})
	}
}
