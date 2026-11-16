package bedrockagentcorecontrol_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

type roleRefs struct {
	fakeRefs
	roles map[string]bool
}

func (r roleRefs) RoleExists(arn string) bool { return r.roles[arn] }

func TestRoleReferencesChecked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		role string
		want string
	}{
		{name: "existing role", role: testRole},
		{name: "missing role", role: "arn:aws:iam::" + testAccount + ":role/ghost", want: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetReferences(roleRefs{roles: map[string]bool{testRole: true}})

			_, err := f.client.CreateGateway(t.Context(), &sdk.CreateGatewayInput{
				Name: aws.String("role-gw"), RoleArn: aws.String(tt.role),
				AuthorizerType: types.AuthorizerTypeCustomJwt, AuthorizerConfiguration: jwtAuthorizer(),
			})
			assert.Equal(t, tt.want, apiCode(err))
		})
	}
}

func jwtConfig(mutate func(*types.CustomJWTAuthorizerConfiguration)) types.AuthorizerConfiguration {
	cfg := types.CustomJWTAuthorizerConfiguration{
		DiscoveryUrl: aws.String("https://idp.example.com/.well-known/openid-configuration"),
	}
	mutate(&cfg)

	return &types.AuthorizerConfigurationMemberCustomJWTAuthorizer{Value: cfg}
}

func TestJWTAuthorizerNestedValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*types.CustomJWTAuthorizerConfiguration)
		name   string
		want   string
	}{
		{name: "plain", mutate: func(*types.CustomJWTAuthorizerConfiguration) {}},
		{name: "oauth server metadata url", mutate: func(c *types.CustomJWTAuthorizerConfiguration) {
			c.DiscoveryUrl = aws.String("https://idp.example.com/.well-known/oauth-authorization-server/tenant")
		}},
		{
			name: "bad discovery suffix",
			want: "ValidationException", mutate: func(c *types.CustomJWTAuthorizerConfiguration) {
				c.DiscoveryUrl = aws.String("https://idp.example.com/openid")
			},
		},
		{
			name: "mapping key outside scopes",
			want: "ValidationException", mutate: func(c *types.CustomJWTAuthorizerConfiguration) {
				c.AllowedScopes = []string{"read"}
				c.AdvertisedScopeMapping = map[string]string{"write": "w"}
			},
		},
		{name: "mapping inside scopes", mutate: func(c *types.CustomJWTAuthorizerConfiguration) {
			c.AllowedScopes = []string{"read"}
			c.AdvertisedScopeMapping = map[string]string{"read": "r"}
		}},
		{
			name: "empty audience list",
			want: "ValidationException", mutate: func(c *types.CustomJWTAuthorizerConfiguration) {
				c.AllowedAudience = []string{}
			},
		},
		{
			name: "scope with space",
			want: "ValidationException", mutate: func(c *types.CustomJWTAuthorizerConfiguration) {
				c.AllowedScopes = []string{"a b"}
			},
		},
		{
			name: "claim name pattern",
			want: "ValidationException", mutate: func(c *types.CustomJWTAuthorizerConfiguration) {
				c.CustomClaims = []types.CustomClaimValidationType{{
					InboundTokenClaimName:      aws.String("bad name"),
					InboundTokenClaimValueType: types.InboundTokenClaimValueTypeString,
					AuthorizingClaimMatchValue: &types.AuthorizingClaimMatchValueType{
						ClaimMatchOperator: types.ClaimMatchOperatorTypeEquals,
						ClaimMatchValue:    &types.ClaimMatchValueTypeMemberMatchValueString{Value: "x"},
					},
				}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			_, err := f.client.CreateGateway(t.Context(), &sdk.CreateGatewayInput{
				Name: aws.String("nested-gw"), RoleArn: aws.String(testRole),
				AuthorizerType:          types.AuthorizerTypeCustomJwt,
				AuthorizerConfiguration: jwtConfig(tt.mutate),
			})
			assert.Equal(t, tt.want, apiCode(err))
		})
	}
}

func TestTargetSchemaContentChecked(t *testing.T) {
	t.Parallel()

	lambdaS3 := func(uri string) types.TargetConfiguration {
		return &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberLambda{
			Value: types.McpLambdaTargetConfiguration{
				LambdaArn:  aws.String(lambdaARN),
				ToolSchema: &types.ToolSchemaMemberS3{Value: types.S3Configuration{Uri: aws.String(uri)}},
			},
		}}
	}
	openAPI := func(doc string) types.TargetConfiguration {
		return &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberOpenApiSchema{
			Value: &types.ApiSchemaConfigurationMemberInlinePayload{Value: doc},
		}}
	}

	tests := []struct {
		cfg  types.TargetConfiguration
		name string
		want string
	}{
		{name: "s3 schema ok", cfg: lambdaS3("s3://bkt/good.json")},
		{name: "s3 schema not json", cfg: lambdaS3("s3://bkt/bad.json"), want: "ValidationException"},
		{name: "s3 object missing", cfg: lambdaS3("s3://bkt/none.json"), want: "ValidationException"},
		{name: "openapi without servers", cfg: openAPI(`{"openapi":"3.0.1","paths":{}}`), want: "ValidationException"},
		{name: "openapi ok", cfg: openAPISpecTarget("https://api.example.com")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			base := fakeRefs{
				functions: map[string]bool{lambdaARN: true},
				objects: map[string][]byte{
					"bkt/good.json": []byte(`[{"name":"t","description":"d","inputSchema":{"type":"object"}}]`),
					"bkt/bad.json":  []byte(`not json`),
				},
			}
			m.backend.SetReferences(fakeInvoker{fakeRefs: base, calls: m.calls})

			gid := aws.ToString(createGateway(t, m.client, "Schema-Gw").GatewayId)

			_, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
				GatewayIdentifier: aws.String(gid), Name: aws.String("tgt"), TargetConfiguration: tt.cfg,
				CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
					{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
				},
			})
			assert.Equal(t, tt.want, apiCode(err))
		})
	}
}

type objectRefs struct {
	fakeRefs
	exists map[string]bool
}

func (o objectRefs) ObjectExists(bucket, key string) bool { return o.exists[bucket+"/"+key] }

func TestCodeRuntimePackageChecked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "package exists", key: "agent/code.zip"},
		{name: "package missing", key: "agent/none.zip", want: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			base := fakeRefs{objects: map[string][]byte{"code-bkt/agent/code.zip": []byte("zip")}}
			f.backend.SetReferences(objectRefs{
				fakeRefs: base,
				exists:   map[string]bool{"code-bkt/agent/code.zip": true},
			})

			out, err := f.client.CreateAgentRuntime(t.Context(), &sdk.CreateAgentRuntimeInput{
				AgentRuntimeName: aws.String("code_rt"), RoleArn: aws.String(testRole),
				NetworkConfiguration: &types.NetworkConfiguration{NetworkMode: types.NetworkModePublic},
				AgentRuntimeArtifact: &types.AgentRuntimeArtifactMemberCodeConfiguration{Value: types.CodeConfiguration{
					Runtime: types.AgentManagedRuntimeTypePython313, EntryPoint: []string{"main.py"},
					Code: &types.CodeMemberS3{Value: types.S3Location{
						Bucket: aws.String("code-bkt"), Prefix: aws.String(tt.key),
					}},
				}},
			})
			assert.Equal(t, tt.want, apiCode(err))

			if tt.want != "" {
				return
			}

			f.clock.advance()

			info, rerr := f.backend.DataPlane().Runtime(
				dataplane.Scope{Account: testAccount, Region: "us-east-1"}, aws.ToString(out.AgentRuntimeId), "DEFAULT")
			require.NoError(t, rerr)
			require.NotNil(t, info.Code)
			assert.Equal(t, "PYTHON_3_13", info.Code.Runtime)

			zipBytes, lerr := info.Code.Load()
			require.NoError(t, lerr)
			assert.Equal(t, "zip", string(zipBytes))
		})
	}
}
