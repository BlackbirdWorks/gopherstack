package app

import (
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	cognitoidentitytypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentity/types"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	cognitoE2EAuthRole   = "arn:aws:iam::000000000000:role/CognitoAuth"
	cognitoE2EUnauthRole = "arn:aws:iam::000000000000:role/CognitoUnauth"
	cognitoE2EAdminRole  = "arn:aws:iam::000000000000:role/CognitoAdmin"
)

func cognitoTrust(poolID, amr string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Federated":"cognito-identity.amazonaws.com"},` +
		`"Action":"sts:AssumeRoleWithWebIdentity","Condition":{` +
		`"StringEquals":{"cognito-identity.amazonaws.com:aud":"` + poolID + `"},` +
		`"ForAnyValue:StringLike":{"cognito-identity.amazonaws.com:amr":"` + amr + `"}}}]}`
}

func TestInitializeServices_CognitoIdentityCredentialsAcceptedBySTS(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.faultStore = chaos.NewFaultStore()
	services, err := initializeServicesT(t,
		&service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()},
	)
	require.NoError(t, err)

	byName := serviceByName(services)
	iamH, ok := byName["IAM"].(*iambackend.Handler)
	require.True(t, ok)
	identityH, ok := byName["CognitoIdentity"].(*cognitoidentitybackend.Handler)
	require.True(t, ok)
	idpH, ok := byName["CognitoIDP"].(*cognitoidpbackend.Handler)
	require.True(t, ok)

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(byName["STS"]))
	require.NoError(t, registry.Register(identityH))

	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg := wiredSDKConfig(t, "test", "test")
	identityClient := cognitoidentitysdk.NewFromConfig(cfg, func(o *cognitoidentitysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	const username = "alice"

	userPool, err := idpH.Backend.CreateUserPool("e2e-users")
	require.NoError(t, err)
	client, err := idpH.Backend.CreateUserPoolClient(userPool.ID, "app")
	require.NoError(t, err)
	_, err = idpH.Backend.AdminCreateUser(userPool.ID, username, "Temp#Pass1", nil)
	require.NoError(t, err)
	require.NoError(t, idpH.Backend.AdminSetUserPassword(userPool.ID, username, "Perm#Pass1x", true))

	auth, err := idpH.Backend.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", username, "Perm#Pass1x")
	require.NoError(t, err)
	require.NotNil(t, auth.Tokens)

	provider := "cognito-idp.us-east-1.amazonaws.com/" + userPool.ID

	pool, err := identityClient.CreateIdentityPool(t.Context(), &cognitoidentitysdk.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("e2e-pool"),
		AllowUnauthenticatedIdentities: true,
		CognitoIdentityProviders: []cognitoidentitytypes.CognitoIdentityProvider{
			{ProviderName: aws.String(provider), ClientId: aws.String(client.ClientID)},
		},
	})
	require.NoError(t, err)

	poolID := aws.ToString(pool.IdentityPoolId)

	for role, doc := range map[string]string{
		"CognitoAuth":   cognitoTrust(poolID, "authenticated"),
		"CognitoUnauth": cognitoTrust(poolID, "unauthenticated"),
		"CognitoAdmin":  cognitoTrust(poolID, "authenticated"),
	} {
		_, err = iamH.Backend.CreateRole(role, "/", doc, "")
		require.NoError(t, err)
	}

	_, err = identityClient.SetIdentityPoolRoles(t.Context(), &cognitoidentitysdk.SetIdentityPoolRolesInput{
		IdentityPoolId: aws.String(poolID),
		Roles: map[string]string{
			"authenticated":   cognitoE2EAuthRole,
			"unauthenticated": cognitoE2EUnauthRole,
		},
		RoleMappings: map[string]cognitoidentitytypes.RoleMapping{
			provider + ":" + client.ClientID: {
				Type:                    cognitoidentitytypes.RoleMappingTypeRules,
				AmbiguousRoleResolution: cognitoidentitytypes.AmbiguousRoleResolutionTypeAuthenticatedRole,
				RulesConfiguration: &cognitoidentitytypes.RulesConfigurationType{
					Rules: []cognitoidentitytypes.MappingRule{{
						Claim:     aws.String("cognito:username"),
						MatchType: cognitoidentitytypes.MappingRuleMatchTypeEquals,
						Value:     aws.String(username),
						RoleARN:   aws.String(cognitoE2EAdminRole),
					}},
				},
			},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		logins  map[string]string
		name    string
		wantARN string
		wantErr string
	}{
		{
			name:    "unauthenticated",
			wantARN: "arn:aws:sts::000000000000:assumed-role/CognitoUnauth/CognitoIdentityCredentials",
		},
		{
			name:    "authenticated rule match",
			logins:  map[string]string{provider: auth.Tokens.IDToken},
			wantARN: "arn:aws:sts::000000000000:assumed-role/CognitoAdmin/CognitoIdentityCredentials",
		},
		{
			name:    "bad signature rejected",
			logins:  map[string]string{provider: auth.Tokens.IDToken + "x"},
			wantErr: "NotAuthorizedException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, idErr := identityClient.GetId(t.Context(), &cognitoidentitysdk.GetIdInput{
				IdentityPoolId: aws.String(poolID),
				Logins:         tt.logins,
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, idErr, tt.wantErr)

				return
			}

			require.NoError(t, idErr)

			creds, credErr := identityClient.GetCredentialsForIdentity(t.Context(),
				&cognitoidentitysdk.GetCredentialsForIdentityInput{IdentityId: id.IdentityId, Logins: tt.logins})
			require.NoError(t, credErr)

			stsCfg := wiredSDKConfig(t, "", "")
			stsCfg.Credentials = credentials.NewStaticCredentialsProvider(
				aws.ToString(creds.Credentials.AccessKeyId),
				aws.ToString(creds.Credentials.SecretKey),
				aws.ToString(creds.Credentials.SessionToken),
			)

			stsClient := stssdk.NewFromConfig(stsCfg, func(o *stssdk.Options) {
				o.BaseEndpoint = aws.String(srv.URL)
			})
			who, whoErr := stsClient.GetCallerIdentity(t.Context(), &stssdk.GetCallerIdentityInput{})
			require.NoError(t, whoErr)
			assert.Equal(t, tt.wantARN, aws.ToString(who.Arn))
		})
	}
}

func TestInitializeServices_CognitoIdentityCustomRoleAndPrincipalTags(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.faultStore = chaos.NewFaultStore()
	services, err := initializeServicesT(t,
		&service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()},
	)
	require.NoError(t, err)

	byName := serviceByName(services)
	iamH, ok := byName["IAM"].(*iambackend.Handler)
	require.True(t, ok)
	stsH, ok := byName["STS"].(*stsbackend.Handler)
	require.True(t, ok)
	identityH, ok := byName["CognitoIdentity"].(*cognitoidentitybackend.Handler)
	require.True(t, ok)
	idpH, ok := byName["CognitoIDP"].(*cognitoidpbackend.Handler)
	require.True(t, ok)

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(stsH))
	require.NoError(t, registry.Register(identityH))

	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	identityClient := cognitoidentitysdk.NewFromConfig(wiredSDKConfig(t, "test", "test"),
		func(o *cognitoidentitysdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	userPool, err := idpH.Backend.CreateUserPool("tags-users")
	require.NoError(t, err)
	client, err := idpH.Backend.CreateUserPoolClient(userPool.ID, "app")
	require.NoError(t, err)
	_, err = idpH.Backend.AdminCreateUser(userPool.ID, "bob", "Temp#Pass1", nil)
	require.NoError(t, err)
	require.NoError(t, idpH.Backend.AdminSetUserPassword(userPool.ID, "bob", "Perm#Pass1x", true))

	auth, err := idpH.Backend.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "bob", "Perm#Pass1x")
	require.NoError(t, err)

	provider := "cognito-idp.us-east-1.amazonaws.com/" + userPool.ID
	logins := map[string]string{provider: auth.Tokens.IDToken}

	pool, err := identityClient.CreateIdentityPool(t.Context(), &cognitoidentitysdk.CreateIdentityPoolInput{
		IdentityPoolName: aws.String("tags-pool"),
		CognitoIdentityProviders: []cognitoidentitytypes.CognitoIdentityProvider{
			{ProviderName: aws.String(provider), ClientId: aws.String(client.ClientID)},
		},
	})
	require.NoError(t, err)

	poolID := aws.ToString(pool.IdentityPoolId)

	for _, role := range []string{"CognitoAuth", "CognitoAdmin"} {
		_, err = iamH.Backend.CreateRole(role, "/", cognitoTrust(poolID, "authenticated"), "")
		require.NoError(t, err)
	}

	_, err = identityClient.SetIdentityPoolRoles(t.Context(), &cognitoidentitysdk.SetIdentityPoolRolesInput{
		IdentityPoolId: aws.String(poolID),
		Roles:          map[string]string{"authenticated": cognitoE2EAuthRole},
		RoleMappings: map[string]cognitoidentitytypes.RoleMapping{
			provider + ":" + client.ClientID: {
				Type:                    cognitoidentitytypes.RoleMappingTypeRules,
				AmbiguousRoleResolution: cognitoidentitytypes.AmbiguousRoleResolutionTypeAuthenticatedRole,
				RulesConfiguration: &cognitoidentitytypes.RulesConfigurationType{
					Rules: []cognitoidentitytypes.MappingRule{{
						Claim:     aws.String("cognito:username"),
						MatchType: cognitoidentitytypes.MappingRuleMatchTypeEquals,
						Value:     aws.String("bob"),
						RoleARN:   aws.String(cognitoE2EAdminRole),
					}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = identityClient.SetPrincipalTagAttributeMap(
		t.Context(),
		&cognitoidentitysdk.SetPrincipalTagAttributeMapInput{
			IdentityPoolId:       aws.String(poolID),
			IdentityProviderName: aws.String(provider),
			UseDefaults:          aws.Bool(true),
			PrincipalTags:        map[string]string{"who": "cognito:username"},
		},
	)
	require.NoError(t, err)

	id, err := identityClient.GetId(t.Context(), &cognitoidentitysdk.GetIdInput{
		IdentityPoolId: aws.String(poolID), Logins: logins,
	})
	require.NoError(t, err)

	tests := []struct {
		name       string
		customRole string
		wantARN    string
		wantErr    string
	}{
		{name: "mapped role", wantARN: "assumed-role/CognitoAdmin/"},
		{name: "authenticated role as custom", customRole: cognitoE2EAuthRole, wantARN: "assumed-role/CognitoAuth/"},
		{name: "unlisted custom role", customRole: cognitoE2EUnauthRole, wantErr: "NotAuthorizedException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &cognitoidentitysdk.GetCredentialsForIdentityInput{IdentityId: id.IdentityId, Logins: logins}
			if tt.customRole != "" {
				in.CustomRoleArn = aws.String(tt.customRole)
			}

			creds, credErr := identityClient.GetCredentialsForIdentity(t.Context(), in)
			if tt.wantErr != "" {
				require.ErrorContains(t, credErr, tt.wantErr)

				return
			}

			require.NoError(t, credErr)

			session := stsH.Backend.LookupSession(
				aws.ToString(creds.Credentials.AccessKeyId), aws.ToString(creds.Credentials.SessionToken))
			require.NotNil(t, session)
			assert.Contains(t, session.AssumedRoleArn, tt.wantARN)

			got := map[string]string{}
			for _, tag := range session.Tags {
				got[tag.Key] = tag.Value
			}

			assert.Equal(t, "bob", got["who"])
			assert.Equal(t, client.ClientID, got["clientID"])
			assert.NotEmpty(t, got["username"])
		})
	}
}
