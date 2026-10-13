package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_CreateDomainNameKeepsMembers(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	created, err := client.CreateDomainName(ctx, &apigwsdk.CreateDomainNameInput{
		DomainName:                          aws.String("api.example.com"),
		CertificateName:                     aws.String("cert"),
		RegionalCertificateName:             aws.String("regional-cert"),
		OwnershipVerificationCertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/ov"),
		Policy:                              aws.String(`{"Version":"2012-10-17"}`),
		RoutingMode:                         apigwtypes.RoutingModeRoutingRuleThenBasePathMapping,
		EndpointAccessMode:                  apigwtypes.EndpointAccessModeStrict,
		MutualTlsAuthentication: &apigwtypes.MutualTlsAuthenticationInput{
			TruststoreUri: aws.String("s3://bucket/trust.pem"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetDomainName(ctx, &apigwsdk.GetDomainNameInput{DomainName: created.DomainName})
	require.NoError(t, err)
	assert.Equal(t, "cert", aws.ToString(got.CertificateName))
	assert.Equal(t, "regional-cert", aws.ToString(got.RegionalCertificateName))
	assert.Equal(
		t,
		"arn:aws:acm:us-east-1:000000000000:certificate/ov",
		aws.ToString(got.OwnershipVerificationCertificateArn),
	)
	assert.NotEmpty(t, aws.ToString(got.Policy))
	assert.Equal(t, apigwtypes.RoutingModeRoutingRuleThenBasePathMapping, got.RoutingMode)
	assert.Equal(t, apigwtypes.EndpointAccessModeStrict, got.EndpointAccessMode)
	require.NotNil(t, got.MutualTlsAuthentication)
	assert.Equal(t, "s3://bucket/trust.pem", aws.ToString(got.MutualTlsAuthentication.TruststoreUri))
}

func TestSDK_DomainNameAccessAssociationTags(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)

	assoc, err := client.CreateDomainNameAccessAssociation(
		t.Context(),
		&apigwsdk.CreateDomainNameAccessAssociationInput{
			DomainNameArn:               aws.String("arn:aws:apigateway:us-east-1::/domainnames/api.example.com"),
			AccessAssociationSource:     aws.String("vpce-123"),
			AccessAssociationSourceType: apigwtypes.AccessAssociationSourceTypeVpce,
			Tags:                        map[string]string{"env": "dev"},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "dev"}, assoc.Tags)

	list, err := client.GetDomainNameAccessAssociations(t.Context(), &apigwsdk.GetDomainNameAccessAssociationsInput{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, map[string]string{"env": "dev"}, list.Items[0].Tags)
}

func TestSDK_CreateDeploymentStageMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		canary     *apigwtypes.DeploymentCanarySettings
		name       string
		wantCanary bool
	}{
		{name: "plain_redeploy_moves_stage"},
		{
			name:       "canary_keeps_primary",
			canary:     &apigwtypes.DeploymentCanarySettings{PercentTraffic: 25},
			wantCanary: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("a")})
			require.NoError(t, err)

			first, err := client.CreateDeployment(ctx, &apigwsdk.CreateDeploymentInput{
				RestApiId: api.Id, StageName: aws.String("prod"),
				CacheClusterEnabled: aws.Bool(true), CacheClusterSize: apigwtypes.CacheClusterSizeSize0Point5Gb,
				StageDescription: aws.String("live"),
			})
			require.NoError(t, err)

			_, err = client.UpdateStage(ctx, &apigwsdk.UpdateStageInput{
				RestApiId: api.Id, StageName: aws.String("prod"),
				PatchOperations: []apigwtypes.PatchOperation{
					{Op: apigwtypes.OpReplace, Path: aws.String("/variables/k"), Value: aws.String("v")},
				},
			})
			require.NoError(t, err)

			second, err := client.CreateDeployment(ctx, &apigwsdk.CreateDeploymentInput{
				RestApiId: api.Id, StageName: aws.String("prod"), CanarySettings: tt.canary,
			})
			require.NoError(t, err)

			stage, err := client.GetStage(
				ctx,
				&apigwsdk.GetStageInput{RestApiId: api.Id, StageName: aws.String("prod")},
			)
			require.NoError(t, err)
			assert.True(t, stage.CacheClusterEnabled)
			assert.Equal(t, apigwtypes.CacheClusterSizeSize0Point5Gb, stage.CacheClusterSize)
			assert.Equal(t, "live", aws.ToString(stage.Description), "redeploy keeps the stage's own settings")
			assert.Equal(t, "v", stage.Variables["k"])

			if tt.wantCanary {
				assert.Equal(t, aws.ToString(first.Id), aws.ToString(stage.DeploymentId))
				require.NotNil(t, stage.CanarySettings)
				assert.Equal(t, aws.ToString(second.Id), aws.ToString(stage.CanarySettings.DeploymentId))
				assert.InDelta(t, 25, stage.CanarySettings.PercentTraffic, 0.001)

				return
			}

			assert.Equal(t, aws.ToString(second.Id), aws.ToString(stage.DeploymentId))
		})
	}
}

func TestSDK_PutIntegrationKeepsMembers(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("a")})
	require.NoError(t, err)

	roots, err := client.GetResources(ctx, &apigwsdk.GetResourcesInput{RestApiId: api.Id})
	require.NoError(t, err)
	rootID := roots.Items[0].Id

	_, err = client.PutMethod(ctx, &apigwsdk.PutMethodInput{
		RestApiId: api.Id, ResourceId: rootID, HttpMethod: aws.String("GET"), AuthorizationType: aws.String("NONE"),
	})
	require.NoError(t, err)

	_, err = client.PutIntegration(ctx, &apigwsdk.PutIntegrationInput{
		RestApiId: api.Id, ResourceId: rootID, HttpMethod: aws.String("GET"),
		Type: apigwtypes.IntegrationTypeHttp, IntegrationHttpMethod: aws.String("GET"),
		Uri:                  aws.String("https://example.com"),
		TlsConfig:            &apigwtypes.TlsConfig{InsecureSkipVerification: true},
		IntegrationTarget:    aws.String("arn:aws:elasticloadbalancing:us-east-1:000000000000:listener/net/n/1/2"),
		ResponseTransferMode: apigwtypes.ResponseTransferModeStream,
	})
	require.NoError(t, err)

	got, err := client.GetIntegration(ctx, &apigwsdk.GetIntegrationInput{
		RestApiId: api.Id, ResourceId: rootID, HttpMethod: aws.String("GET"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.TlsConfig)
	assert.True(t, got.TlsConfig.InsecureSkipVerification)
	assert.NotEmpty(t, aws.ToString(got.IntegrationTarget))
	assert.Equal(t, apigwtypes.ResponseTransferModeStream, got.ResponseTransferMode)
}

func TestSDK_DocumentationVersionStageAndLocationStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status apigwtypes.LocationStatusType
		want   int
	}{
		{name: "documented", status: apigwtypes.LocationStatusTypeDocumented, want: 1},
		{name: "undocumented", status: apigwtypes.LocationStatusTypeUndocumented, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("a")})
			require.NoError(t, err)

			for name, props := range map[string]string{"documented": `{"info":{"description":"x"}}`, "undocumented": ""} {
				_, err = client.CreateDocumentationPart(ctx, &apigwsdk.CreateDocumentationPartInput{
					RestApiId: api.Id,
					Location: &apigwtypes.DocumentationPartLocation{
						Type:   apigwtypes.DocumentationPartTypeMethod,
						Path:   aws.String("/" + name),
						Method: aws.String("GET"),
					},
					Properties: aws.String(props),
				})
				require.NoError(t, err)
			}

			parts, err := client.GetDocumentationParts(ctx, &apigwsdk.GetDocumentationPartsInput{
				RestApiId: api.Id, LocationStatus: tt.status,
			})
			require.NoError(t, err)
			require.Len(t, parts.Items, tt.want)

			wantPath := "/undocumented"
			if tt.status == apigwtypes.LocationStatusTypeDocumented {
				wantPath = "/documented"
			}

			assert.Equal(t, wantPath, aws.ToString(parts.Items[0].Location.Path))

			_, err = client.CreateDeployment(
				ctx,
				&apigwsdk.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("prod")},
			)
			require.NoError(t, err)

			_, err = client.CreateDocumentationVersion(ctx, &apigwsdk.CreateDocumentationVersionInput{
				RestApiId: api.Id, DocumentationVersion: aws.String("v1"), StageName: aws.String("prod"),
			})
			require.NoError(t, err)

			stage, err := client.GetStage(
				ctx,
				&apigwsdk.GetStageInput{RestApiId: api.Id, StageName: aws.String("prod")},
			)
			require.NoError(t, err)
			assert.Equal(t, "v1", aws.ToString(stage.DocumentationVersion))

			_, err = client.CreateDocumentationVersion(ctx, &apigwsdk.CreateDocumentationVersionInput{
				RestApiId: api.Id, DocumentationVersion: aws.String("v2"), StageName: aws.String("missing"),
			})
			var nf *apigwtypes.NotFoundException
			require.ErrorAs(t, err, &nf)
		})
	}
}

func TestSDK_PatchAuthTypeAndSecurityPolicy(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	api, err := client.CreateRestApi(ctx, &apigwsdk.CreateRestApiInput{Name: aws.String("a")})
	require.NoError(t, err)

	auth, err := client.CreateAuthorizer(ctx, &apigwsdk.CreateAuthorizerInput{
		RestApiId: api.Id, Name: aws.String("auth"), Type: apigwtypes.AuthorizerTypeToken,
		AuthorizerUri:  aws.String("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn/invocations"),
		IdentitySource: aws.String("method.request.header.Authorization"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateAuthorizer(ctx, &apigwsdk.UpdateAuthorizerInput{
		RestApiId: api.Id, AuthorizerId: auth.Id,
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/authType"), Value: aws.String("custom")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "custom", aws.ToString(updated.AuthType))

	rest, err := client.UpdateRestApi(ctx, &apigwsdk.UpdateRestApiInput{
		RestApiId: api.Id,
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/securityPolicy"), Value: aws.String("TLS_1_2")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, apigwtypes.SecurityPolicyTls12, rest.SecurityPolicy)
}

func TestSDK_CreateRestApiKeepsSecurityPolicyAndVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policy   apigwtypes.SecurityPolicy
		version  string
		wantPol  apigwtypes.SecurityPolicy
		wantVers string
	}{
		{
			name:     "both",
			policy:   apigwtypes.SecurityPolicyTls12,
			version:  "v1.2",
			wantPol:  apigwtypes.SecurityPolicyTls12,
			wantVers: "v1.2",
		},
		{name: "neither"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			in := &apigwsdk.CreateRestApiInput{Name: aws.String("sp-" + tt.name), SecurityPolicy: tt.policy}
			if tt.version != "" {
				in.Version = aws.String(tt.version)
			}

			created, err := client.CreateRestApi(ctx, in)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPol, created.SecurityPolicy)
			assert.Equal(t, tt.wantVers, aws.ToString(created.Version))

			got, err := client.GetRestApi(ctx, &apigwsdk.GetRestApiInput{RestApiId: created.Id})
			require.NoError(t, err)
			assert.Equal(t, tt.wantPol, got.SecurityPolicy)
			assert.Equal(t, tt.wantVers, aws.ToString(got.Version))
		})
	}
}
