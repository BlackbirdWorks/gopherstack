package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestSlice8_CloudFront_RealClient covers cloudfront's highest-priority
// typed-client-uncovered op families (gopherstack-n3zi slice 8): cache/
// origin-request/response-headers policies, origin access identities/
// controls, key groups/public keys, field-level encryption, continuous
// deployment, functions, key value store, invalidations, streaming
// distribution config, and CopyDistribution/UpdateDistributionWithStagingConfig.
// Every Get*Config/Update/Delete drives the real ETag/IfMatch sequence a
// real client uses. Each subtest creates real state through the typed
// aws-sdk-go-v2 cloudfront client and asserts decoded response values.
func TestSlice8_CloudFront_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testSlice8CachePolicyRealClient, "cache_policy"},
		{testSlice8OriginRequestPolicyRealClient, "origin_request_policy"},
		{testSlice8ResponseHeadersPolicyRealClient, "response_headers_policy"},
		{testSlice8OAIRealClient, "oai"},
		{testSlice8OACRealClient, "oac"},
		{testSlice8PublicKeyKeyGroupRealClient, "public_key_key_group"},
		{testSlice8FieldLevelEncryptionRealClient, "field_level_encryption"},
		{testSlice8ContinuousDeploymentRealClient, "continuous_deployment"},
		{testSlice8FunctionRealClient, "function"},
		{testSlice8KeyValueStoreRealClient, "key_value_store"},
		{testSlice8InvalidationRealClient, "invalidation"},
		{testSlice8StreamingDistributionConfigRealClient, "streaming_distribution_config"},
		{testSlice8CopyAndStagingDistributionRealClient, "copy_and_staging_distribution"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newSlice8CloudFrontBackendAndClient(
	t *testing.T,
) (*cloudfront.InMemoryBackend, *cfsdk.Client) {
	t.Helper()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	return backend, client
}

func testSlice8CachePolicyRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateCachePolicy(ctx, &cfsdk.CreateCachePolicyInput{
		CachePolicyConfig: &types.CachePolicyConfig{
			Name:       aws.String("slice8-cachepolicy"),
			MinTTL:     aws.Int64(1),
			DefaultTTL: aws.Int64(10),
			MaxTTL:     aws.Int64(100),
			Comment:    aws.String("slice8"),
			ParametersInCacheKeyAndForwardedToOrigin: &types.ParametersInCacheKeyAndForwardedToOrigin{
				CookiesConfig: &types.CachePolicyCookiesConfig{
					CookieBehavior: types.CachePolicyCookieBehaviorNone,
				},
				HeadersConfig: &types.CachePolicyHeadersConfig{
					HeaderBehavior: types.CachePolicyHeaderBehaviorNone,
				},
				QueryStringsConfig: &types.CachePolicyQueryStringsConfig{
					QueryStringBehavior: types.CachePolicyQueryStringBehaviorNone,
				},
				EnableAcceptEncodingGzip: aws.Bool(false),
			},
		},
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.CachePolicy.Id)
	require.NotEmpty(t, id)

	getOut, err := client.GetCachePolicy(ctx, &cfsdk.GetCachePolicyInput{Id: aws.String(id)})
	require.NoError(t, err)
	assert.Equal(t, "slice8-cachepolicy", aws.ToString(getOut.CachePolicy.CachePolicyConfig.Name))

	cfgOut, err := client.GetCachePolicyConfig(
		ctx,
		&cfsdk.GetCachePolicyConfigInput{Id: aws.String(id)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(cfgOut.ETag))

	updateOut, err := client.UpdateCachePolicy(ctx, &cfsdk.UpdateCachePolicyInput{
		Id:      aws.String(id),
		IfMatch: cfgOut.ETag,
		CachePolicyConfig: &types.CachePolicyConfig{
			Name:                                     aws.String("slice8-cachepolicy"),
			MinTTL:                                   aws.Int64(2),
			DefaultTTL:                               aws.Int64(20),
			MaxTTL:                                   aws.Int64(200),
			Comment:                                  aws.String("slice8-updated"),
			ParametersInCacheKeyAndForwardedToOrigin: cfgOut.CachePolicyConfig.ParametersInCacheKeyAndForwardedToOrigin,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), aws.ToInt64(updateOut.CachePolicy.CachePolicyConfig.MinTTL))

	_, err = client.DeleteCachePolicy(
		ctx,
		&cfsdk.DeleteCachePolicyInput{Id: aws.String(id), IfMatch: updateOut.ETag},
	)
	require.NoError(t, err)
}

func testSlice8OriginRequestPolicyRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	cfg := &types.OriginRequestPolicyConfig{
		Name:    aws.String("slice8-originrequestpolicy"),
		Comment: aws.String("slice8"),
		CookiesConfig: &types.OriginRequestPolicyCookiesConfig{
			CookieBehavior: types.OriginRequestPolicyCookieBehaviorNone,
		},
		HeadersConfig: &types.OriginRequestPolicyHeadersConfig{
			HeaderBehavior: types.OriginRequestPolicyHeaderBehaviorNone,
		},
		QueryStringsConfig: &types.OriginRequestPolicyQueryStringsConfig{
			QueryStringBehavior: types.OriginRequestPolicyQueryStringBehaviorNone,
		},
	}

	createOut, err := client.CreateOriginRequestPolicy(ctx, &cfsdk.CreateOriginRequestPolicyInput{
		OriginRequestPolicyConfig: cfg,
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.OriginRequestPolicy.Id)
	require.NotEmpty(t, id)

	getOut, err := client.GetOriginRequestPolicy(
		ctx,
		&cfsdk.GetOriginRequestPolicyInput{Id: aws.String(id)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(getOut.ETag))

	cfgOut, err := client.GetOriginRequestPolicyConfig(
		ctx, &cfsdk.GetOriginRequestPolicyConfigInput{Id: aws.String(id)},
	)
	require.NoError(t, err)

	updateCfg := cfgOut.OriginRequestPolicyConfig
	updateCfg.Comment = aws.String("slice8-updated")

	updateOut, err := client.UpdateOriginRequestPolicy(ctx, &cfsdk.UpdateOriginRequestPolicyInput{
		Id:                        aws.String(id),
		IfMatch:                   cfgOut.ETag,
		OriginRequestPolicyConfig: updateCfg,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-updated",
		aws.ToString(updateOut.OriginRequestPolicy.OriginRequestPolicyConfig.Comment),
	)

	_, err = client.DeleteOriginRequestPolicy(
		ctx, &cfsdk.DeleteOriginRequestPolicyInput{Id: aws.String(id), IfMatch: updateOut.ETag},
	)
	require.NoError(t, err)
}

func testSlice8ResponseHeadersPolicyRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateResponseHeadersPolicy(
		ctx,
		&cfsdk.CreateResponseHeadersPolicyInput{
			ResponseHeadersPolicyConfig: &types.ResponseHeadersPolicyConfig{
				Name:    aws.String("slice8-responseheaderspolicy"),
				Comment: aws.String("slice8"),
			},
		},
	)
	require.NoError(t, err)
	id := aws.ToString(createOut.ResponseHeadersPolicy.Id)
	require.NotEmpty(t, id)

	getOut, err := client.GetResponseHeadersPolicy(
		ctx,
		&cfsdk.GetResponseHeadersPolicyInput{Id: aws.String(id)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(getOut.ETag))

	cfgOut, err := client.GetResponseHeadersPolicyConfig(
		ctx, &cfsdk.GetResponseHeadersPolicyConfigInput{Id: aws.String(id)},
	)
	require.NoError(t, err)

	updateCfg := cfgOut.ResponseHeadersPolicyConfig
	updateCfg.Comment = aws.String("slice8-updated")

	updateOut, err := client.UpdateResponseHeadersPolicy(
		ctx,
		&cfsdk.UpdateResponseHeadersPolicyInput{
			Id:                          aws.String(id),
			IfMatch:                     cfgOut.ETag,
			ResponseHeadersPolicyConfig: updateCfg,
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-updated",
		aws.ToString(updateOut.ResponseHeadersPolicy.ResponseHeadersPolicyConfig.Comment),
	)

	_, err = client.DeleteResponseHeadersPolicy(
		ctx, &cfsdk.DeleteResponseHeadersPolicyInput{Id: aws.String(id), IfMatch: updateOut.ETag},
	)
	require.NoError(t, err)
}

func testSlice8OAIRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateCloudFrontOriginAccessIdentity(
		ctx, &cfsdk.CreateCloudFrontOriginAccessIdentityInput{
			CloudFrontOriginAccessIdentityConfig: &types.CloudFrontOriginAccessIdentityConfig{
				CallerReference: aws.String("slice8-oai"),
				Comment:         aws.String("slice8"),
			},
		},
	)
	require.NoError(t, err)
	id := aws.ToString(createOut.CloudFrontOriginAccessIdentity.Id)
	require.NotEmpty(t, id)

	getOut, err := client.GetCloudFrontOriginAccessIdentity(
		ctx, &cfsdk.GetCloudFrontOriginAccessIdentityInput{Id: aws.String(id)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(getOut.ETag))

	cfgOut, err := client.GetCloudFrontOriginAccessIdentityConfig(
		ctx, &cfsdk.GetCloudFrontOriginAccessIdentityConfigInput{Id: aws.String(id)},
	)
	require.NoError(t, err)

	updateOut, err := client.UpdateCloudFrontOriginAccessIdentity(
		ctx, &cfsdk.UpdateCloudFrontOriginAccessIdentityInput{
			Id:      aws.String(id),
			IfMatch: cfgOut.ETag,
			CloudFrontOriginAccessIdentityConfig: &types.CloudFrontOriginAccessIdentityConfig{
				CallerReference: cfgOut.CloudFrontOriginAccessIdentityConfig.CallerReference,
				Comment:         aws.String("slice8-updated"),
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-updated",
		aws.ToString(
			updateOut.CloudFrontOriginAccessIdentity.CloudFrontOriginAccessIdentityConfig.Comment,
		),
	)

	_, err = client.DeleteCloudFrontOriginAccessIdentity(
		ctx,
		&cfsdk.DeleteCloudFrontOriginAccessIdentityInput{
			Id:      aws.String(id),
			IfMatch: updateOut.ETag,
		},
	)
	require.NoError(t, err)
}

func testSlice8OACRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateOriginAccessControl(ctx, &cfsdk.CreateOriginAccessControlInput{
		OriginAccessControlConfig: &types.OriginAccessControlConfig{
			Name:                          aws.String("slice8-oac"),
			OriginAccessControlOriginType: types.OriginAccessControlOriginTypesS3,
			SigningBehavior:               types.OriginAccessControlSigningBehaviorsAlways,
			SigningProtocol:               types.OriginAccessControlSigningProtocolsSigv4,
		},
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.OriginAccessControl.Id)
	require.NotEmpty(t, id)

	getOut, err := client.GetOriginAccessControl(
		ctx,
		&cfsdk.GetOriginAccessControlInput{Id: aws.String(id)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(getOut.ETag))

	cfgOut, err := client.GetOriginAccessControlConfig(
		ctx, &cfsdk.GetOriginAccessControlConfigInput{Id: aws.String(id)},
	)
	require.NoError(t, err)

	updateCfg := cfgOut.OriginAccessControlConfig
	updateCfg.Description = aws.String("slice8-updated")

	updateOut, err := client.UpdateOriginAccessControl(ctx, &cfsdk.UpdateOriginAccessControlInput{
		Id:                        aws.String(id),
		IfMatch:                   cfgOut.ETag,
		OriginAccessControlConfig: updateCfg,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-updated",
		aws.ToString(updateOut.OriginAccessControl.OriginAccessControlConfig.Description),
	)

	_, err = client.DeleteOriginAccessControl(
		ctx, &cfsdk.DeleteOriginAccessControlInput{Id: aws.String(id), IfMatch: updateOut.ETag},
	)
	require.NoError(t, err)
}

func testSlice8PublicKeyKeyGroupRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	pkOut, err := client.CreatePublicKey(ctx, &cfsdk.CreatePublicKeyInput{
		PublicKeyConfig: &types.PublicKeyConfig{
			CallerReference: aws.String("slice8-pk"),
			EncodedKey:      aws.String(testRSA2048PublicKeyPEM),
			Name:            aws.String("slice8-publickey"),
		},
	})
	require.NoError(t, err)
	pkID := aws.ToString(pkOut.PublicKey.Id)
	require.NotEmpty(t, pkID)

	pkGetOut, err := client.GetPublicKey(ctx, &cfsdk.GetPublicKeyInput{Id: aws.String(pkID)})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(pkGetOut.ETag))

	pkCfgOut, err := client.GetPublicKeyConfig(
		ctx,
		&cfsdk.GetPublicKeyConfigInput{Id: aws.String(pkID)},
	)
	require.NoError(t, err)

	// UpdatePublicKey's real doc comment states "the only value you can
	// change is the comment" (cloudfront@v1.67.4 api_op_UpdatePublicKey.go) --
	// Name/EncodedKey/CallerReference are resent unchanged from the Get.
	pkUpdateOut, err := client.UpdatePublicKey(ctx, &cfsdk.UpdatePublicKeyInput{
		Id:      aws.String(pkID),
		IfMatch: pkCfgOut.ETag,
		PublicKeyConfig: &types.PublicKeyConfig{
			CallerReference: pkCfgOut.PublicKeyConfig.CallerReference,
			EncodedKey:      pkCfgOut.PublicKeyConfig.EncodedKey,
			Name:            pkCfgOut.PublicKeyConfig.Name,
			Comment:         aws.String("slice8-updated"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-publickey", aws.ToString(pkUpdateOut.PublicKey.PublicKeyConfig.Name))
	assert.Equal(t, "slice8-updated", aws.ToString(pkUpdateOut.PublicKey.PublicKeyConfig.Comment))

	kgOut, err := client.CreateKeyGroup(ctx, &cfsdk.CreateKeyGroupInput{
		KeyGroupConfig: &types.KeyGroupConfig{
			Name:    aws.String("slice8-keygroup"),
			Items:   []string{pkID},
			Comment: aws.String("slice8"),
		},
	})
	require.NoError(t, err)
	kgID := aws.ToString(kgOut.KeyGroup.Id)
	require.NotEmpty(t, kgID)

	kgGetOut, err := client.GetKeyGroup(ctx, &cfsdk.GetKeyGroupInput{Id: aws.String(kgID)})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(kgGetOut.ETag))

	kgCfgOut, err := client.GetKeyGroupConfig(
		ctx,
		&cfsdk.GetKeyGroupConfigInput{Id: aws.String(kgID)},
	)
	require.NoError(t, err)

	kgUpdateOut, err := client.UpdateKeyGroup(ctx, &cfsdk.UpdateKeyGroupInput{
		Id:      aws.String(kgID),
		IfMatch: kgCfgOut.ETag,
		KeyGroupConfig: &types.KeyGroupConfig{
			Name:    aws.String("slice8-keygroup-renamed"),
			Items:   kgCfgOut.KeyGroupConfig.Items,
			Comment: aws.String("slice8-updated"),
		},
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-keygroup-renamed",
		aws.ToString(kgUpdateOut.KeyGroup.KeyGroupConfig.Name),
	)

	_, err = client.DeleteKeyGroup(
		ctx,
		&cfsdk.DeleteKeyGroupInput{Id: aws.String(kgID), IfMatch: kgUpdateOut.ETag},
	)
	require.NoError(t, err)

	_, err = client.DeletePublicKey(
		ctx,
		&cfsdk.DeletePublicKeyInput{Id: aws.String(pkID), IfMatch: pkUpdateOut.ETag},
	)
	require.NoError(t, err)
}

func testSlice8FieldLevelEncryptionRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	pkOut, err := client.CreatePublicKey(ctx, &cfsdk.CreatePublicKeyInput{
		PublicKeyConfig: &types.PublicKeyConfig{
			CallerReference: aws.String("slice8-fle-pk"),
			EncodedKey:      aws.String(testRSA2048PublicKeyPEM),
			Name:            aws.String("slice8-fle-publickey"),
		},
	})
	require.NoError(t, err)
	pkID := aws.ToString(pkOut.PublicKey.Id)

	profileOut, err := client.CreateFieldLevelEncryptionProfile(
		ctx,
		&cfsdk.CreateFieldLevelEncryptionProfileInput{
			FieldLevelEncryptionProfileConfig: &types.FieldLevelEncryptionProfileConfig{
				CallerReference: aws.String("slice8-fle-profile"),
				Name:            aws.String("slice8fleprofile"),
				EncryptionEntities: &types.EncryptionEntities{
					Quantity: aws.Int32(1),
					Items: []types.EncryptionEntity{
						{
							PublicKeyId: aws.String(pkID),
							ProviderId:  aws.String("slice8-provider"),
							FieldPatterns: &types.FieldPatterns{
								Quantity: aws.Int32(1),
								Items:    []string{"CreditCardNumber"},
							},
						},
					},
				},
			},
		},
	)
	require.NoError(t, err)
	profileID := aws.ToString(profileOut.FieldLevelEncryptionProfile.Id)
	require.NotEmpty(t, profileID)

	profileGetOut, err := client.GetFieldLevelEncryptionProfile(
		ctx, &cfsdk.GetFieldLevelEncryptionProfileInput{Id: aws.String(profileID)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(profileGetOut.ETag))

	profileCfgOut, err := client.GetFieldLevelEncryptionProfileConfig(
		ctx, &cfsdk.GetFieldLevelEncryptionProfileConfigInput{Id: aws.String(profileID)},
	)
	require.NoError(t, err)

	updatedProfileCfg := profileCfgOut.FieldLevelEncryptionProfileConfig
	updatedProfileCfg.Comment = aws.String("slice8-updated")

	profileUpdateOut, err := client.UpdateFieldLevelEncryptionProfile(
		ctx, &cfsdk.UpdateFieldLevelEncryptionProfileInput{
			Id:                                aws.String(profileID),
			IfMatch:                           profileCfgOut.ETag,
			FieldLevelEncryptionProfileConfig: updatedProfileCfg,
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-updated",
		aws.ToString(
			profileUpdateOut.FieldLevelEncryptionProfile.FieldLevelEncryptionProfileConfig.Comment,
		),
	)

	fleOut, err := client.CreateFieldLevelEncryptionConfig(
		ctx,
		&cfsdk.CreateFieldLevelEncryptionConfigInput{
			FieldLevelEncryptionConfig: &types.FieldLevelEncryptionConfig{
				CallerReference: aws.String("slice8-fle-config"),
				Comment:         aws.String("slice8"),
				QueryArgProfileConfig: &types.QueryArgProfileConfig{
					ForwardWhenQueryArgProfileIsUnknown: aws.Bool(true),
				},
			},
		},
	)
	require.NoError(t, err)
	fleID := aws.ToString(fleOut.FieldLevelEncryption.Id)
	require.NotEmpty(t, fleID)

	fleGetOut, err := client.GetFieldLevelEncryption(
		ctx,
		&cfsdk.GetFieldLevelEncryptionInput{Id: aws.String(fleID)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(fleGetOut.ETag))

	fleCfgOut, err := client.GetFieldLevelEncryptionConfig(
		ctx, &cfsdk.GetFieldLevelEncryptionConfigInput{Id: aws.String(fleID)},
	)
	require.NoError(t, err)

	updatedFleCfg := fleCfgOut.FieldLevelEncryptionConfig
	updatedFleCfg.Comment = aws.String("slice8-updated")

	fleUpdateOut, err := client.UpdateFieldLevelEncryptionConfig(
		ctx,
		&cfsdk.UpdateFieldLevelEncryptionConfigInput{
			Id:                         aws.String(fleID),
			IfMatch:                    fleCfgOut.ETag,
			FieldLevelEncryptionConfig: updatedFleCfg,
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-updated",
		aws.ToString(fleUpdateOut.FieldLevelEncryption.FieldLevelEncryptionConfig.Comment),
	)

	_, err = client.DeleteFieldLevelEncryptionConfig(
		ctx,
		&cfsdk.DeleteFieldLevelEncryptionConfigInput{
			Id:      aws.String(fleID),
			IfMatch: fleUpdateOut.ETag,
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteFieldLevelEncryptionProfile(
		ctx,
		&cfsdk.DeleteFieldLevelEncryptionProfileInput{
			Id:      aws.String(profileID),
			IfMatch: profileUpdateOut.ETag,
		},
	)
	require.NoError(t, err)
}

func testSlice8ContinuousDeploymentRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	staging, err := backend.CreateDistribution("slice8-cdp-staging", "staging", true, nil)
	require.NoError(t, err)

	createOut, err := client.CreateContinuousDeploymentPolicy(
		ctx,
		&cfsdk.CreateContinuousDeploymentPolicyInput{
			ContinuousDeploymentPolicyConfig: &types.ContinuousDeploymentPolicyConfig{
				Enabled: aws.Bool(true),
				StagingDistributionDnsNames: &types.StagingDistributionDnsNames{
					Quantity: aws.Int32(1),
					Items:    []string{staging.DomainName},
				},
				TrafficConfig: &types.TrafficConfig{
					Type: types.ContinuousDeploymentPolicyTypeSingleWeight,
					SingleWeightConfig: &types.ContinuousDeploymentSingleWeightConfig{
						Weight: aws.Float32(0.1),
					},
				},
			},
		},
	)
	require.NoError(t, err)
	id := aws.ToString(createOut.ContinuousDeploymentPolicy.Id)
	require.NotEmpty(t, id)

	getOut, err := client.GetContinuousDeploymentPolicy(
		ctx, &cfsdk.GetContinuousDeploymentPolicyInput{Id: aws.String(id)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(getOut.ETag))

	cfgOut, err := client.GetContinuousDeploymentPolicyConfig(
		ctx, &cfsdk.GetContinuousDeploymentPolicyConfigInput{Id: aws.String(id)},
	)
	require.NoError(t, err)

	updateCfg := cfgOut.ContinuousDeploymentPolicyConfig
	updateCfg.Enabled = aws.Bool(false)

	updateOut, err := client.UpdateContinuousDeploymentPolicy(
		ctx, &cfsdk.UpdateContinuousDeploymentPolicyInput{
			Id:                               aws.String(id),
			IfMatch:                          cfgOut.ETag,
			ContinuousDeploymentPolicyConfig: updateCfg,
		},
	)
	require.NoError(t, err)
	assert.False(
		t,
		aws.ToBool(updateOut.ContinuousDeploymentPolicy.ContinuousDeploymentPolicyConfig.Enabled),
	)

	_, err = client.DeleteContinuousDeploymentPolicy(
		ctx,
		&cfsdk.DeleteContinuousDeploymentPolicyInput{Id: aws.String(id), IfMatch: updateOut.ETag},
	)
	require.NoError(t, err)
}

func testSlice8FunctionRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	code := []byte(`function handler(event) { return event.request; }`)

	createOut, err := client.CreateFunction(ctx, &cfsdk.CreateFunctionInput{
		Name: aws.String("slice8function"),
		FunctionConfig: &types.FunctionConfig{
			Comment: aws.String("slice8"),
			Runtime: types.FunctionRuntimeCloudfrontJs20,
		},
		FunctionCode: code,
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createOut.FunctionSummary.Name))

	descOut, err := client.DescribeFunction(
		ctx,
		&cfsdk.DescribeFunctionInput{Name: aws.String("slice8function")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(descOut.ETag))

	getOut, err := client.GetFunction(
		ctx,
		&cfsdk.GetFunctionInput{Name: aws.String("slice8function")},
	)
	require.NoError(t, err)
	assert.Equal(t, code, getOut.FunctionCode)

	// TestFunction is a disclosed structural parity gap (PARITY.md:
	// gopherstack vendors no JavaScript engine, so a well-formed request
	// gets the real, declared TestFunctionFailed error rather than a
	// fabricated result) -- this exercises the op for real (If-Match/
	// EventObject validated) and asserts the documented failure mode.
	eventObject := `{"version":"1.0","context":{"eventType":"viewer-request"},` +
		`"viewer":{"ip":"1.2.3.4"},"request":{"method":"GET","uri":"/",` +
		`"headers":{},"cookies":{},"querystring":{}}}`

	_, err = client.TestFunction(ctx, &cfsdk.TestFunctionInput{
		Name:        aws.String("slice8function"),
		IfMatch:     descOut.ETag,
		EventObject: []byte(eventObject),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TestFunctionFailed")

	updateOut, err := client.UpdateFunction(ctx, &cfsdk.UpdateFunctionInput{
		Name:    aws.String("slice8function"),
		IfMatch: descOut.ETag,
		FunctionConfig: &types.FunctionConfig{
			Comment: aws.String("slice8-updated"),
			Runtime: types.FunctionRuntimeCloudfrontJs20,
		},
		FunctionCode: code,
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-updated",
		aws.ToString(updateOut.FunctionSummary.FunctionConfig.Comment),
	)

	// The pinned aws-sdk-go-v2 cloudfront@v1.67.4 client has its own upstream
	// bug in UpdateFunctionOutput's header binding (deserializers.go:
	// response.Header.Values("ETtag") -- a typo'd header name), so
	// updateOut.ETag is always nil for a real client regardless of server
	// behavior; re-fetch via DescribeFunction instead, matching what any
	// real caller of this exact client must do.
	postUpdateDescOut, err := client.DescribeFunction(
		ctx, &cfsdk.DescribeFunctionInput{Name: aws.String("slice8function")},
	)
	require.NoError(t, err)

	publishOut, err := client.PublishFunction(ctx, &cfsdk.PublishFunctionInput{
		Name:    aws.String("slice8function"),
		IfMatch: postUpdateDescOut.ETag,
	})
	require.NoError(t, err)
	assert.Equal(t, types.FunctionStageLive, publishOut.FunctionSummary.FunctionMetadata.Stage)

	finalDescOut, err := client.DescribeFunction(
		ctx,
		&cfsdk.DescribeFunctionInput{Name: aws.String("slice8function")},
	)
	require.NoError(t, err)

	_, err = client.DeleteFunction(ctx, &cfsdk.DeleteFunctionInput{
		Name:    aws.String("slice8function"),
		IfMatch: finalDescOut.ETag,
	})
	require.NoError(t, err)
}

func testSlice8KeyValueStoreRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKeyValueStore(ctx, &cfsdk.CreateKeyValueStoreInput{
		Name:    aws.String("slice8kvs"),
		Comment: aws.String("slice8"),
	})
	require.NoError(t, err)
	id := aws.ToString(createOut.KeyValueStore.Id)
	require.NotEmpty(t, id)

	descOut, err := client.DescribeKeyValueStore(
		ctx,
		&cfsdk.DescribeKeyValueStoreInput{Name: aws.String("slice8kvs")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(descOut.ETag))

	updateOut, err := client.UpdateKeyValueStore(ctx, &cfsdk.UpdateKeyValueStoreInput{
		Name:    aws.String("slice8kvs"),
		IfMatch: descOut.ETag,
		Comment: aws.String("slice8-updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-updated", aws.ToString(updateOut.KeyValueStore.Comment))

	_, err = client.DeleteKeyValueStore(ctx, &cfsdk.DeleteKeyValueStoreInput{
		Name:    aws.String("slice8kvs"),
		IfMatch: updateOut.ETag,
	})
	require.NoError(t, err)
}

func testSlice8InvalidationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	dist, err := backend.CreateDistribution("slice8-inv-dist", "slice8", true, nil)
	require.NoError(t, err)

	createOut, err := client.CreateInvalidation(ctx, &cfsdk.CreateInvalidationInput{
		DistributionId: aws.String(dist.ID),
		InvalidationBatch: &types.InvalidationBatch{
			CallerReference: aws.String("slice8-invalidation"),
			Paths: &types.Paths{
				Quantity: aws.Int32(1),
				Items:    []string{"/*"},
			},
		},
	})
	require.NoError(t, err)
	invalidationID := aws.ToString(createOut.Invalidation.Id)
	require.NotEmpty(t, invalidationID)

	getOut, err := client.GetInvalidation(ctx, &cfsdk.GetInvalidationInput{
		DistributionId: aws.String(dist.ID),
		Id:             aws.String(invalidationID),
	})
	require.NoError(t, err)
	assert.Equal(t, invalidationID, aws.ToString(getOut.Invalidation.Id))
}

func testSlice8StreamingDistributionConfigRealClient(t *testing.T) {
	t.Helper()

	_, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateStreamingDistribution(
		ctx,
		&cfsdk.CreateStreamingDistributionInput{
			StreamingDistributionConfig: &types.StreamingDistributionConfig{
				CallerReference: aws.String("slice8-sd"),
				Comment:         aws.String("slice8"),
				Enabled:         aws.Bool(true),
				S3Origin: &types.S3Origin{
					DomainName:           aws.String("slice8-bucket.s3.amazonaws.com"),
					OriginAccessIdentity: aws.String(""),
				},
				TrustedSigners: &types.TrustedSigners{
					Enabled:  aws.Bool(false),
					Quantity: aws.Int32(0),
				},
			},
		},
	)
	require.NoError(t, err)
	id := aws.ToString(createOut.StreamingDistribution.Id)
	require.NotEmpty(t, id)

	cfgOut, err := client.GetStreamingDistributionConfig(
		ctx, &cfsdk.GetStreamingDistributionConfigInput{Id: aws.String(id)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(cfgOut.ETag))
	assert.Equal(t, "slice8", aws.ToString(cfgOut.StreamingDistributionConfig.Comment))
}

func testSlice8CopyAndStagingDistributionRealClient(t *testing.T) {
	t.Helper()

	backend, client := newSlice8CloudFrontBackendAndClient(t)
	ctx := t.Context()

	primary, err := backend.CreateDistribution("slice8-copy-primary", "primary", true, nil)
	require.NoError(t, err)

	copyOut, err := client.CopyDistribution(ctx, &cfsdk.CopyDistributionInput{
		PrimaryDistributionId: aws.String(primary.ID),
		CallerReference:       aws.String("slice8-copy"),
		Staging:               aws.Bool(true),
	})
	require.NoError(t, err)
	stagingID := aws.ToString(copyOut.Distribution.Id)
	require.NotEmpty(t, stagingID)
	require.NotEqual(t, primary.ID, stagingID)

	getPrimary, err := client.GetDistribution(
		ctx,
		&cfsdk.GetDistributionInput{Id: aws.String(primary.ID)},
	)
	require.NoError(t, err)

	stagingOut, err := client.UpdateDistributionWithStagingConfig(
		ctx, &cfsdk.UpdateDistributionWithStagingConfigInput{
			Id:                    aws.String(primary.ID),
			StagingDistributionId: aws.String(stagingID),
			IfMatch:               getPrimary.ETag,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, primary.ID, aws.ToString(stagingOut.Distribution.Id))
}
