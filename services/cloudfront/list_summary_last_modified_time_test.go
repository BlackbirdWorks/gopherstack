package cloudfront_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSummaries_HaveLastModifiedTime proves LastModifiedTime -- required
// on types.CachePolicy/OriginRequestPolicy/ResponseHeadersPolicy/KeyGroup/
// FieldLevelEncryption/FieldLevelEncryptionProfile (cloudfront@v1.67.4
// types.go) -- decodes on both the List summary (CachePolicySummary etc, the
// over-wide-response census target) and the singular Get response for all
// six families, plus the same class of gap on PublicKey.CreatedTime and
// VpcOrigin.CreatedTime/LastModifiedTime/Status. None of these backend
// models tracked the relevant timestamp(s) at all before this fix, so every
// real client's List/Get response silently decoded a zero time.Time (or, for
// VpcOrigin, an empty Status too) regardless of when the resource was
// actually created or updated.
func TestListSummaries_HaveLastModifiedTime(t *testing.T) {
	t.Parallel()

	before := time.Now().Add(-time.Minute)

	t.Run("cache policy", func(t *testing.T) {
		t.Parallel()

		_, client := newRealClientBackendAndClient(t)
		ctx := t.Context()

		createOut, err := client.CreateCachePolicy(ctx, &cfsdk.CreateCachePolicyInput{
			CachePolicyConfig: &types.CachePolicyConfig{
				Name:       aws.String("lmt-cache-policy"),
				MinTTL:     aws.Int64(0),
				DefaultTTL: aws.Int64(1),
				MaxTTL:     aws.Int64(2),
				ParametersInCacheKeyAndForwardedToOrigin: &types.ParametersInCacheKeyAndForwardedToOrigin{
					EnableAcceptEncodingGzip: aws.Bool(false),
					CookiesConfig: &types.CachePolicyCookiesConfig{
						CookieBehavior: types.CachePolicyCookieBehaviorNone,
					},
					HeadersConfig: &types.CachePolicyHeadersConfig{
						HeaderBehavior: types.CachePolicyHeaderBehaviorNone,
					},
					QueryStringsConfig: &types.CachePolicyQueryStringsConfig{
						QueryStringBehavior: types.CachePolicyQueryStringBehaviorNone,
					},
				},
			},
		})
		require.NoError(t, err)
		assert.True(t, createOut.CachePolicy.LastModifiedTime.After(before))

		listOut, err := client.ListCachePolicies(ctx, &cfsdk.ListCachePoliciesInput{Type: types.CachePolicyTypeCustom})
		require.NoError(t, err)
		require.Len(t, listOut.CachePolicyList.Items, 1)
		assert.True(t, listOut.CachePolicyList.Items[0].CachePolicy.LastModifiedTime.After(before))
	})

	t.Run("origin request policy", func(t *testing.T) {
		t.Parallel()

		_, client := newRealClientBackendAndClient(t)
		ctx := t.Context()

		createOut, err := client.CreateOriginRequestPolicy(ctx, &cfsdk.CreateOriginRequestPolicyInput{
			OriginRequestPolicyConfig: &types.OriginRequestPolicyConfig{
				Name: aws.String("lmt-orp"),
				CookiesConfig: &types.OriginRequestPolicyCookiesConfig{
					CookieBehavior: types.OriginRequestPolicyCookieBehaviorNone,
				},
				HeadersConfig: &types.OriginRequestPolicyHeadersConfig{
					HeaderBehavior: types.OriginRequestPolicyHeaderBehaviorNone,
				},
				QueryStringsConfig: &types.OriginRequestPolicyQueryStringsConfig{
					QueryStringBehavior: types.OriginRequestPolicyQueryStringBehaviorNone,
				},
			},
		})
		require.NoError(t, err)
		assert.True(t, createOut.OriginRequestPolicy.LastModifiedTime.After(before))

		listOut, err := client.ListOriginRequestPolicies(
			ctx, &cfsdk.ListOriginRequestPoliciesInput{Type: types.OriginRequestPolicyTypeCustom},
		)
		require.NoError(t, err)
		require.Len(t, listOut.OriginRequestPolicyList.Items, 1)
		assert.True(t, listOut.OriginRequestPolicyList.Items[0].OriginRequestPolicy.LastModifiedTime.After(before))
	})

	t.Run("response headers policy", func(t *testing.T) {
		t.Parallel()

		_, client := newRealClientBackendAndClient(t)
		ctx := t.Context()

		createOut, err := client.CreateResponseHeadersPolicy(ctx, &cfsdk.CreateResponseHeadersPolicyInput{
			ResponseHeadersPolicyConfig: &types.ResponseHeadersPolicyConfig{
				Name: aws.String("lmt-rhp"),
			},
		})
		require.NoError(t, err)
		assert.True(t, createOut.ResponseHeadersPolicy.LastModifiedTime.After(before))

		listOut, err := client.ListResponseHeadersPolicies(
			ctx, &cfsdk.ListResponseHeadersPoliciesInput{Type: types.ResponseHeadersPolicyTypeCustom},
		)
		require.NoError(t, err)
		require.Len(t, listOut.ResponseHeadersPolicyList.Items, 1)
		assert.True(t, listOut.ResponseHeadersPolicyList.Items[0].ResponseHeadersPolicy.LastModifiedTime.After(before))
	})

	t.Run("key group", func(t *testing.T) {
		t.Parallel()

		_, client := newRealClientBackendAndClient(t)
		ctx := t.Context()

		pkOut, err := client.CreatePublicKey(ctx, &cfsdk.CreatePublicKeyInput{
			PublicKeyConfig: &types.PublicKeyConfig{
				CallerReference: aws.String("lmt-kg-pk"),
				EncodedKey:      aws.String(testRSA2048PublicKeyPEM),
				Name:            aws.String("lmt-kg-publickey"),
			},
		})
		require.NoError(t, err)

		createOut, err := client.CreateKeyGroup(ctx, &cfsdk.CreateKeyGroupInput{
			KeyGroupConfig: &types.KeyGroupConfig{
				Name:  aws.String("lmt-keygroup"),
				Items: []string{aws.ToString(pkOut.PublicKey.Id)},
			},
		})
		require.NoError(t, err)
		assert.True(t, createOut.KeyGroup.LastModifiedTime.After(before))

		listOut, err := client.ListKeyGroups(ctx, &cfsdk.ListKeyGroupsInput{})
		require.NoError(t, err)
		require.Len(t, listOut.KeyGroupList.Items, 1)
		assert.True(t, listOut.KeyGroupList.Items[0].KeyGroup.LastModifiedTime.After(before))
	})

	t.Run("field level encryption config", func(t *testing.T) {
		t.Parallel()

		_, client := newRealClientBackendAndClient(t)
		ctx := t.Context()

		createOut, err := client.CreateFieldLevelEncryptionConfig(ctx, &cfsdk.CreateFieldLevelEncryptionConfigInput{
			FieldLevelEncryptionConfig: &types.FieldLevelEncryptionConfig{
				CallerReference: aws.String("lmt-fle"),
				QueryArgProfileConfig: &types.QueryArgProfileConfig{
					ForwardWhenQueryArgProfileIsUnknown: aws.Bool(true),
				},
			},
		})
		require.NoError(t, err)
		assert.True(t, createOut.FieldLevelEncryption.LastModifiedTime.After(before))

		listOut, err := client.ListFieldLevelEncryptionConfigs(ctx, &cfsdk.ListFieldLevelEncryptionConfigsInput{})
		require.NoError(t, err)
		require.Len(t, listOut.FieldLevelEncryptionList.Items, 1)
		assert.True(t, listOut.FieldLevelEncryptionList.Items[0].LastModifiedTime.After(before))
	})

	t.Run("field level encryption profile", func(t *testing.T) {
		t.Parallel()

		_, client := newRealClientBackendAndClient(t)
		ctx := t.Context()

		pkOut, err := client.CreatePublicKey(ctx, &cfsdk.CreatePublicKeyInput{
			PublicKeyConfig: &types.PublicKeyConfig{
				CallerReference: aws.String("lmt-flep-pk"),
				EncodedKey:      aws.String(testRSA2048PublicKeyPEM),
				Name:            aws.String("lmt-flep-publickey"),
			},
		})
		require.NoError(t, err)

		createOut, err := client.CreateFieldLevelEncryptionProfile(
			ctx, &cfsdk.CreateFieldLevelEncryptionProfileInput{
				FieldLevelEncryptionProfileConfig: &types.FieldLevelEncryptionProfileConfig{
					Name:            aws.String("lmt-flep"),
					CallerReference: aws.String("lmt-flep-ref"),
					EncryptionEntities: &types.EncryptionEntities{
						Quantity: aws.Int32(1),
						Items: []types.EncryptionEntity{
							{
								PublicKeyId: pkOut.PublicKey.Id,
								ProviderId:  aws.String("lmt-provider"),
								FieldPatterns: &types.FieldPatterns{
									Quantity: aws.Int32(1),
									Items:    []string{"field1"},
								},
							},
						},
					},
				},
			},
		)
		require.NoError(t, err)
		assert.True(t, createOut.FieldLevelEncryptionProfile.LastModifiedTime.After(before))

		listOut, err := client.ListFieldLevelEncryptionProfiles(
			ctx, &cfsdk.ListFieldLevelEncryptionProfilesInput{},
		)
		require.NoError(t, err)
		require.Len(t, listOut.FieldLevelEncryptionProfileList.Items, 1)
		assert.True(t, listOut.FieldLevelEncryptionProfileList.Items[0].LastModifiedTime.After(before))
	})

	t.Run("public key", func(t *testing.T) {
		t.Parallel()

		_, client := newRealClientBackendAndClient(t)
		ctx := t.Context()

		createOut, err := client.CreatePublicKey(ctx, &cfsdk.CreatePublicKeyInput{
			PublicKeyConfig: &types.PublicKeyConfig{
				CallerReference: aws.String("lmt-pk"),
				EncodedKey:      aws.String(testRSA2048PublicKeyPEM),
				Name:            aws.String("lmt-publickey"),
			},
		})
		require.NoError(t, err)
		assert.True(t, createOut.PublicKey.CreatedTime.After(before))

		listOut, err := client.ListPublicKeys(ctx, &cfsdk.ListPublicKeysInput{})
		require.NoError(t, err)
		require.Len(t, listOut.PublicKeyList.Items, 1)
		assert.True(t, listOut.PublicKeyList.Items[0].CreatedTime.After(before))
	})

	t.Run("vpc origin", func(t *testing.T) {
		t.Parallel()

		_, client := newRealClientBackendAndClient(t)
		ctx := t.Context()

		createOut, err := client.CreateVpcOrigin(ctx, &cfsdk.CreateVpcOriginInput{
			VpcOriginEndpointConfig: &types.VpcOriginEndpointConfig{
				Name:                 aws.String("lmt-vpc-origin"),
				Arn:                  aws.String("arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-lmt"),
				HTTPPort:             aws.Int32(80),
				HTTPSPort:            aws.Int32(443),
				OriginProtocolPolicy: types.OriginProtocolPolicyHttpsOnly,
			},
		})
		require.NoError(t, err)
		assert.True(t, createOut.VpcOrigin.CreatedTime.After(before))
		assert.True(t, createOut.VpcOrigin.LastModifiedTime.After(before))
		assert.NotEmpty(t, aws.ToString(createOut.VpcOrigin.Status))

		listOut, err := client.ListVpcOrigins(ctx, &cfsdk.ListVpcOriginsInput{})
		require.NoError(t, err)
		require.Len(t, listOut.VpcOriginList.Items, 1)
		assert.True(t, listOut.VpcOriginList.Items[0].CreatedTime.After(before))
		assert.True(t, listOut.VpcOriginList.Items[0].LastModifiedTime.After(before))
		assert.NotEmpty(t, aws.ToString(listOut.VpcOriginList.Items[0].Status))
	})
}
