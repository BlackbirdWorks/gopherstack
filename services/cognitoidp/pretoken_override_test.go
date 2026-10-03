package cognitoidp_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const (
	preTokenARN  = "arn:aws:lambda:us-east-1:000000000000:function:pre"
	preTokenUser = "pat"
	preTokenPass = "Pass1234!"
	roleA        = "arn:aws:iam::123456789012:role/role-a"
)

type preTokenEnv struct {
	client   *cognitoidpsdk.Client
	inv      *fakeInvoker
	clientID string
	poolID   string
}

func newPreTokenEnv(
	t *testing.T, version string, respond func(string, map[string]any) (map[string]any, error),
) *preTokenEnv {
	t.Helper()

	inv := &fakeInvoker{respond: respond}
	b := newTestBackend()
	b.SetLambdaTriggerInvoker(inv)
	client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(b, "us-east-1"))

	lambdaCfg := &types.LambdaConfigType{PreTokenGenerationConfig: &types.PreTokenGenerationVersionConfigType{
		LambdaArn: aws.String(preTokenARN), LambdaVersion: types.PreTokenGenerationLambdaVersionType(version),
	}}
	if version == "" {
		lambdaCfg = &types.LambdaConfigType{PreTokenGeneration: aws.String(preTokenARN)}
	}

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("pre-token"), LambdaConfig: lambdaCfg,
	})
	require.NoError(t, err)

	appClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: pool.UserPool.Id, ClientName: aws.String("c"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{
			types.ExplicitAuthFlowsTypeAllowUserPasswordAuth,
			types.ExplicitAuthFlowsTypeAllowAdminUserPasswordAuth,
			types.ExplicitAuthFlowsTypeAllowRefreshTokenAuth,
		},
	})
	require.NoError(t, err)

	env := &preTokenEnv{
		client: client, inv: inv, poolID: aws.ToString(pool.UserPool.Id),
		clientID: aws.ToString(appClient.UserPoolClient.ClientId),
	}

	_, err = client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String(preTokenUser),
		MessageAction: types.MessageActionTypeSuppress,
		UserAttributes: []types.AttributeType{
			{Name: aws.String("email"), Value: aws.String("pat@example.com")},
			{Name: aws.String("email_verified"), Value: aws.String("true")},
		},
	})
	require.NoError(t, err)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: pool.UserPool.Id, Username: aws.String(preTokenUser),
		Password: aws.String(preTokenPass), Permanent: true,
	})
	require.NoError(t, err)

	return env
}

func (e *preTokenEnv) addToGroup(t *testing.T, name, role string) {
	t.Helper()

	_, err := e.client.CreateGroup(t.Context(), &cognitoidpsdk.CreateGroupInput{
		UserPoolId: aws.String(e.poolID), GroupName: aws.String(name), RoleArn: aws.String(role),
	})
	require.NoError(t, err)

	_, err = e.client.AdminAddUserToGroup(t.Context(), &cognitoidpsdk.AdminAddUserToGroupInput{
		UserPoolId: aws.String(e.poolID), Username: aws.String(preTokenUser), GroupName: aws.String(name),
	})
	require.NoError(t, err)
}

func (e *preTokenEnv) signIn(t *testing.T) *types.AuthenticationResultType {
	t.Helper()

	out, err := e.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(e.clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": preTokenUser, "PASSWORD": preTokenPass},
	})
	require.NoError(t, err)

	return out.AuthenticationResult
}

func (e *preTokenEnv) refresh(t *testing.T, token string) *types.AuthenticationResultType {
	t.Helper()

	out, err := e.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(e.clientID), AuthFlow: types.AuthFlowTypeRefreshTokenAuth,
		AuthParameters: map[string]string{"REFRESH_TOKEN": token},
	})
	require.NoError(t, err)

	return out.AuthenticationResult
}

func v2Response(details map[string]any) func(string, map[string]any) (map[string]any, error) {
	return func(string, map[string]any) (map[string]any, error) {
		return map[string]any{"claimsAndScopeOverrideDetails": details}, nil
	}
}

func TestPreTokenGenerationVersionedOverrides(t *testing.T) {
	t.Parallel()

	v1Resp := func(string, map[string]any) (map[string]any, error) {
		return map[string]any{"claimsOverrideDetails": map[string]any{
			"claimsToAddOrOverride": map[string]any{"custom:role": "admin"},
			"claimsToSuppress":      []any{"email"},
		}}, nil
	}

	tests := []struct {
		respond      func(string, map[string]any) (map[string]any, error)
		check        func(t *testing.T, id, access map[string]any)
		name         string
		version      string
		group        string
		wantVersion  string
		wantRequest  bool
		wantScopeKey bool
	}{
		{
			name: "bare arn keeps v1 shape", version: "", wantVersion: "1", respond: v1Resp,
			check: func(t *testing.T, id, access map[string]any) {
				t.Helper()
				assert.Equal(t, "admin", id["custom:role"])
				assert.NotContains(t, id, "email")
				assert.Equal(t, "admin", access["custom:role"])
			},
		},
		{
			name: "v1_0 ignores claimsAndScopeOverrideDetails", version: "V1_0", wantVersion: "1",
			respond: v2Response(map[string]any{
				"idTokenGeneration":     map[string]any{"claimsToAddOrOverride": map[string]any{"x": "y"}},
				"accessTokenGeneration": map[string]any{"scopesToAdd": []any{"extra"}},
			}),
			check: func(t *testing.T, id, access map[string]any) {
				t.Helper()
				assert.NotContains(t, id, "x")
				assert.Equal(t, "aws.cognito.signin.user.admin", access["scope"])
			},
		},
		{
			name: "v2 adds and suppresses id and access claims", version: "V2_0", wantVersion: "2",
			wantRequest: true, wantScopeKey: true,
			respond: v2Response(map[string]any{
				"idTokenGeneration": map[string]any{
					"claimsToAddOrOverride": map[string]any{
						"tier": float64(3), "beta": true, "tags": []any{"a", float64(1), false},
						"meta": map[string]any{"k": map[string]any{"n": "v"}}, "family_name": "Doe",
						"address": map[string]any{"street": "x"}, "email_verified": []any{"nope"},
						"nilval": nil,
					},
					"claimsToSuppress": []any{"email"},
				},
				"accessTokenGeneration": map[string]any{
					"claimsToAddOrOverride": map[string]any{
						"team": "blue", "limits": map[string]any{"rps": float64(5)},
					},
					"claimsToSuppress": []any{"team", "auth_time"},
				},
			}),
			check: func(t *testing.T, id, access map[string]any) {
				t.Helper()
				assert.InDelta(t, 3, id["tier"], 0)
				assert.Equal(t, true, id["beta"])
				assert.Equal(t, []any{"a", float64(1), false}, id["tags"])
				assert.Equal(t, map[string]any{"k": map[string]any{"n": "v"}}, id["meta"])
				assert.Equal(t, "Doe", id["family_name"])
				assert.NotContains(t, id, "address", "address cannot hold an object")
				assert.NotEqual(t, []any{"nope"}, id["email_verified"], "email_verified cannot hold an array")
				assert.NotContains(t, id, "nilval")
				assert.NotContains(t, id, "email")
				assert.NotContains(t, id, "team", "id token edits are separate from access")
				assert.Equal(t, map[string]any{"rps": float64(5)}, access["limits"])
				assert.NotContains(t, access, "team", "suppress wins over add")
				assert.Contains(t, access, "auth_time", "auth_time is protected")
			},
		},
		{
			name: "v3 scopes add and suppress", version: "V3_0", wantVersion: "3",
			wantRequest: true, wantScopeKey: true,
			respond: v2Response(map[string]any{"accessTokenGeneration": map[string]any{
				"scopesToAdd":      []any{"api/extra", "bad scope", "openid"},
				"scopesToSuppress": []any{"aws.cognito.signin.user.admin"},
			}}),
			check: func(t *testing.T, _, access map[string]any) {
				t.Helper()
				assert.Equal(t, "api/extra openid", access["scope"])
			},
		},
		{
			name: "v2 suppressing every scope drops the claim", version: "V2_0", wantVersion: "2", wantRequest: true,
			respond: v2Response(map[string]any{"accessTokenGeneration": map[string]any{
				"scopesToSuppress": []any{"aws.cognito.signin.user.admin"},
			}}),
			check: func(t *testing.T, _, access map[string]any) {
				t.Helper()
				assert.NotContains(t, access, "scope")
			},
		},
		{
			name: "protected claims are untouchable", version: "V2_0", wantVersion: "2", wantRequest: true,
			respond: v2Response(map[string]any{
				"idTokenGeneration": map[string]any{
					"claimsToAddOrOverride": map[string]any{
						"sub": "forged", "token_use": "access", "cognito:username": "forged", "aud": "forged",
						"identities": "forged", "cognito:custom": "forged", "dev:attr": "forged", "nonce": "forged",
					},
					"claimsToSuppress": []any{"sub", "exp", "iss", "token_use", "aud", "cognito:username"},
				},
				"accessTokenGeneration": map[string]any{
					"claimsToAddOrOverride": map[string]any{
						"client_id": "forged", "username": "forged", "origin_jti": "forged", "scope": "forged",
						"version": "forged", "aud": "forged", "token_use": "id",
					},
					"claimsToSuppress": []any{"client_id", "username", "sub", "iat", "token_use"},
				},
			}),
			check: func(t *testing.T, id, access map[string]any) {
				t.Helper()
				assert.Equal(t, preTokenUser, id["cognito:username"])
				assert.Equal(t, "id", id["token_use"])

				for _, k := range []string{"sub", "exp", "iss", "aud"} {
					assert.Contains(t, id, k)
				}

				for _, k := range []string{"identities", "cognito:custom", "dev:attr", "nonce"} {
					assert.NotContains(t, id, k)
				}

				assert.Equal(t, "access", access["token_use"])
				assert.Equal(t, preTokenUser, access["username"])
				assert.NotEqual(t, "forged", access["client_id"])
				assert.NotEqual(t, "forged", access["scope"])
				assert.NotContains(t, access, "origin_jti")
				assert.NotContains(t, access, "version")
				assert.NotContains(t, access, "aud", "aud only accepted when it equals the client id")

				for _, k := range []string{"sub", "iat", "client_id"} {
					assert.Contains(t, access, k)
				}
			},
		},
		{
			name: "group overrides replace groups roles and preferred role", version: "V2_0", wantVersion: "2",
			wantRequest: true, group: "orig",
			respond: v2Response(map[string]any{"groupOverrideDetails": map[string]any{
				"groupsToOverride":   []any{"g1", "g2"},
				"iamRolesToOverride": []any{roleA},
				"preferredRole":      roleA,
			}}),
			check: func(t *testing.T, id, access map[string]any) {
				t.Helper()
				assert.Equal(t, []any{"g1", "g2"}, id["cognito:groups"])
				assert.Equal(t, []any{"g1", "g2"}, access["cognito:groups"])
				assert.Equal(t, []any{roleA}, id["cognito:roles"])
				assert.Equal(t, roleA, id["cognito:preferred_role"])
				assert.NotContains(t, access, "cognito:roles")
			},
		},
		{
			name: "empty groupOverrideDetails suppresses groups", version: "V3_0", wantVersion: "3",
			wantRequest: true, group: "orig",
			respond: v2Response(map[string]any{"groupOverrideDetails": map[string]any{}}),
			check: func(t *testing.T, id, access map[string]any) {
				t.Helper()
				assert.NotContains(t, id, "cognito:groups")
				assert.NotContains(t, access, "cognito:groups")
			},
		},
		{
			name: "claimsToSuppress cognito:groups removes group claims", version: "V2_0", wantVersion: "2",
			wantRequest: true, group: "orig",
			respond: v2Response(map[string]any{
				"idTokenGeneration":     map[string]any{"claimsToSuppress": []any{"cognito:groups"}},
				"accessTokenGeneration": map[string]any{"claimsToSuppress": []any{"cognito:groups"}},
			}),
			check: func(t *testing.T, id, access map[string]any) {
				t.Helper()
				assert.NotContains(t, id, "cognito:groups")
				assert.NotContains(t, id, "cognito:roles")
				assert.NotContains(t, access, "cognito:groups")
			},
		},
		{
			name: "absent groupOverrideDetails keeps real groups", version: "V2_0", wantVersion: "2",
			wantRequest: true, group: "orig", respond: v2Response(map[string]any{}),
			check: func(t *testing.T, id, access map[string]any) {
				t.Helper()
				assert.Equal(t, []any{"orig"}, id["cognito:groups"])
				assert.Equal(t, []any{"orig"}, access["cognito:groups"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newPreTokenEnv(t, tt.version, tt.respond)
			if tt.group != "" {
				env.addToGroup(t, tt.group, roleA)
			}

			signIn := env.signIn(t)
			ev := env.inv.lastCall().event
			assert.Equal(t, "TokenGeneration_Authentication", ev["triggerSource"])
			assert.Equal(t, tt.wantVersion, ev["version"])

			req, _ := ev["request"].(map[string]any)
			assert.Contains(t, req, "userAttributes")
			assert.Contains(t, req, "groupConfiguration")
			assert.Equal(t, tt.wantRequest, req["scopes"] != nil, "scopes is a version-2/3 request field")

			if tt.wantScopeKey {
				assert.Equal(t, []any{"aws.cognito.signin.user.admin"}, req["scopes"])
			}

			tt.check(t, decodeJWTPayload(t, aws.ToString(signIn.IdToken)),
				decodeJWTPayload(t, aws.ToString(signIn.AccessToken)))

			refreshed := env.refresh(t, aws.ToString(signIn.RefreshToken))
			assert.Equal(t, "TokenGeneration_RefreshTokens", env.inv.lastCall().event["triggerSource"])
			tt.check(t, decodeJWTPayload(t, aws.ToString(refreshed.IdToken)),
				decodeJWTPayload(t, aws.ToString(refreshed.AccessToken)))
		})
	}
}

func TestPreTokenGenerationMalformedResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		details any
		name    string
	}{
		{name: "container not an object", details: "oops"},
		{name: "id block not an object", details: map[string]any{"idTokenGeneration": []any{"x"}}},
		{name: "access block not an object", details: map[string]any{"accessTokenGeneration": "x"}},
		{name: "claims not an object", details: map[string]any{
			"idTokenGeneration": map[string]any{"claimsToAddOrOverride": []any{"x"}},
		}},
		{name: "suppress not a list", details: map[string]any{
			"idTokenGeneration": map[string]any{"claimsToSuppress": "email"},
		}},
		{name: "suppress holds non-strings", details: map[string]any{
			"accessTokenGeneration": map[string]any{"claimsToSuppress": []any{float64(1)}},
		}},
		{name: "scopes not a list", details: map[string]any{
			"accessTokenGeneration": map[string]any{"scopesToAdd": "openid"},
		}},
		{name: "group override not an object", details: map[string]any{"groupOverrideDetails": "g"}},
		{name: "group names not a list", details: map[string]any{
			"groupOverrideDetails": map[string]any{"groupsToOverride": "g"},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newPreTokenEnv(t, "V2_0", v2Response2(tt.details))

			_, err := env.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
				ClientId: aws.String(env.clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
				AuthParameters: map[string]string{"USERNAME": preTokenUser, "PASSWORD": preTokenPass},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "UnexpectedLambdaException")
		})
	}
}

func v2Response2(details any) func(string, map[string]any) (map[string]any, error) {
	return func(string, map[string]any) (map[string]any, error) {
		return map[string]any{"claimsAndScopeOverrideDetails": details}, nil
	}
}

func TestPreTokenGenerationNewPasswordChallenge(t *testing.T) {
	t.Parallel()

	env := newPreTokenEnv(t, "V3_0", v2Response(map[string]any{
		"idTokenGeneration":     map[string]any{"claimsToAddOrOverride": map[string]any{"stage": "new"}},
		"accessTokenGeneration": map[string]any{"scopesToAdd": []any{"api/x"}},
	}))

	const second = "ChangedPass1!"

	_, err := env.client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(env.poolID), Username: aws.String("temp"), TemporaryPassword: aws.String(preTokenPass),
		MessageAction: types.MessageActionTypeSuppress,
	})
	require.NoError(t, err)

	challenge, err := env.client.AdminInitiateAuth(t.Context(), &cognitoidpsdk.AdminInitiateAuthInput{
		UserPoolId: aws.String(env.poolID), ClientId: aws.String(env.clientID),
		AuthFlow:       types.AuthFlowTypeAdminUserPasswordAuth,
		AuthParameters: map[string]string{"USERNAME": "temp", "PASSWORD": preTokenPass},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypeNewPasswordRequired, challenge.ChallengeName)

	done, err := env.client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId: aws.String(env.clientID), ChallengeName: types.ChallengeNameTypeNewPasswordRequired,
		Session:            challenge.Session,
		ChallengeResponses: map[string]string{"USERNAME": "temp", "NEW_PASSWORD": second},
	})
	require.NoError(t, err)

	ev := env.inv.lastCall().event
	assert.Equal(t, "TokenGeneration_NewPasswordChallenge", ev["triggerSource"])
	assert.Equal(t, "3", ev["version"])

	assert.Equal(t, "new", decodeJWTPayload(t, aws.ToString(done.AuthenticationResult.IdToken))["stage"])

	scope, _ := decodeJWTPayload(t, aws.ToString(done.AuthenticationResult.AccessToken))["scope"].(string)
	assert.Contains(t, scope, "api/x")
}

func TestPreTokenGenerationHostedLogin(t *testing.T) {
	t.Parallel()

	resp := v2Response(map[string]any{
		"idTokenGeneration": map[string]any{
			"claimsToAddOrOverride": map[string]any{"tenant": "acme", "gone": "x"},
			"claimsToSuppress":      []any{"gone"},
		},
		"accessTokenGeneration": map[string]any{
			"claimsToAddOrOverride": map[string]any{"plan": float64(2)},
			"scopesToAdd":           []any{"api/extra"},
		},
	})
	legacy := func(string, map[string]any) (map[string]any, error) {
		return map[string]any{"claimsOverrideDetails": map[string]any{
			"claimsToAddOrOverride": map[string]any{"tenant": "acme"},
		}}, nil
	}

	tests := []struct {
		respond func(string, map[string]any) (map[string]any, error)
		cfg     map[string]any
		name    string
		version string
		v2      bool
	}{
		{name: "v2", version: "2", v2: true, respond: resp, cfg: map[string]any{
			"PreTokenGenerationConfig": map[string]any{"LambdaArn": preTokenARN, "LambdaVersion": "V2_0"},
		}},
		{name: "v3", version: "3", v2: true, respond: resp, cfg: map[string]any{
			"PreTokenGenerationConfig": map[string]any{"LambdaArn": preTokenARN, "LambdaVersion": "V3_0"},
		}},
		{name: "v1", version: "1", respond: legacy, cfg: map[string]any{"PreTokenGeneration": preTokenARN}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &fakeInvoker{respond: tt.respond}
			env := newOAuthEnvWith(t, cognitoidp.UserPoolOptions{LambdaConfig: tt.cfg}, inv)

			verifier, challenge := pkcePair()
			code := codeFrom(t, env.login(t, authorizeQuery(env.pubID, challenge)))

			resp, body := env.postForm(t, "/oauth2/token", url.Values{
				"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
				"client_id": {env.pubID}, "code_verifier": {verifier},
			}, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

			ev := inv.lastCall().event
			assert.Equal(t, "TokenGeneration_HostedAuth", ev["triggerSource"])
			assert.Equal(t, tt.version, ev["version"])

			tokens := jsonMap(t, body)
			check := func(tokens map[string]any) {
				id := env.verifyAgainstJWKS(t, tokens["id_token"].(string))
				access := env.verifyAgainstJWKS(t, tokens["access_token"].(string))

				assert.Equal(t, "acme", id["tenant"])

				if !tt.v2 {
					return
				}

				assert.NotContains(t, id, "gone")
				assert.InDelta(t, 2, access["plan"], 0)
				assert.Contains(t, access["scope"], "api/extra")
			}

			check(tokens)

			resp, body = env.postForm(t, "/oauth2/token", url.Values{
				"grant_type": {"refresh_token"}, "refresh_token": {tokens["refresh_token"].(string)},
				"client_id": {env.pubID},
			}, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
			assert.Equal(t, "TokenGeneration_RefreshTokens", inv.lastCall().event["triggerSource"])
			check(jsonMap(t, body))
		})
	}
}
