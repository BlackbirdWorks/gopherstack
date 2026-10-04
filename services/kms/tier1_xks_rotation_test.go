package kms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func xksInput(name, endpoint string) *kmssdk.CreateCustomKeyStoreInput {
	return &kmssdk.CreateCustomKeyStoreInput{
		CustomKeyStoreName:   aws.String(name),
		CustomKeyStoreType:   kmstypes.CustomKeyStoreTypeExternalKeyStore,
		XksProxyConnectivity: kmstypes.XksProxyConnectivityTypePublicEndpoint,
		XksProxyUriEndpoint:  aws.String(endpoint),
		XksProxyUriPath:      aws.String("/kms/xks/v1"),
		XksProxyAuthenticationCredential: &kmstypes.XksProxyAuthenticationCredentialType{
			AccessKeyId: aws.String("AKIAEXAMPLE0001"), RawSecretAccessKey: aws.String("secretsecretsecret"),
		},
	}
}

func TestCustomKeyStore_XksCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(in *kmssdk.CreateCustomKeyStoreInput)
		name    string
		errCode string
	}{
		{name: "ok", mutate: func(*kmssdk.CreateCustomKeyStoreInput) {}},
		{
			name:    "missing-endpoint",
			errCode: "XksProxyInvalidConfigurationException",
			mutate:  func(in *kmssdk.CreateCustomKeyStoreInput) { in.XksProxyUriEndpoint = nil },
		},
		{
			name:    "bad-path",
			errCode: "XksProxyInvalidConfigurationException",
			mutate:  func(in *kmssdk.CreateCustomKeyStoreInput) { in.XksProxyUriPath = aws.String("/nope") },
		},
		{
			name:    "http-endpoint",
			errCode: "XksProxyInvalidConfigurationException",
			mutate: func(in *kmssdk.CreateCustomKeyStoreInput) {
				in.XksProxyUriEndpoint = aws.String("http://x.example.com")
			},
		},
		{
			name:    "vpc-needs-service-name",
			errCode: "XksProxyInvalidConfigurationException",
			mutate: func(in *kmssdk.CreateCustomKeyStoreInput) {
				in.XksProxyConnectivity = kmstypes.XksProxyConnectivityTypeVpcEndpointService
			},
		},
		{
			name:    "no-credential",
			errCode: "XksProxyInvalidConfigurationException",
			mutate:  func(in *kmssdk.CreateCustomKeyStoreInput) { in.XksProxyAuthenticationCredential = nil },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKMSClient(t, newTestKMSHandler())
			in := xksInput("s", "https://xks.example.com")
			tt.mutate(in)

			_, err := client.CreateCustomKeyStore(t.Context(), in)
			if tt.errCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errCode)
		})
	}
}

func TestCustomKeyStore_XksUniqueness(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	_, err := client.CreateCustomKeyStore(ctx, xksInput("a", "https://xks.example.com"))
	require.NoError(t, err)

	_, err = client.CreateCustomKeyStore(ctx, xksInput("b", "https://xks.example.com"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "XksProxyUriInUseException")

	in := xksInput("c", "https://xks.example.com")
	in.XksProxyUriPath = aws.String("/other/kms/xks/v1")
	in.XksProxyConnectivity = kmstypes.XksProxyConnectivityTypeVpcEndpointService
	in.XksProxyVpcEndpointServiceName = aws.String("com.amazonaws.vpce.us-east-1.vpce-svc-1")
	_, err = client.CreateCustomKeyStore(ctx, in)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "XksProxyUriEndpointInUseException")
}

func TestCustomKeyStore_XksDescribeAndUpdate(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	in := xksInput("vpc", "https://vpc1.example.com")
	in.XksProxyConnectivity = kmstypes.XksProxyConnectivityTypeVpcEndpointService
	in.XksProxyVpcEndpointServiceName = aws.String("com.amazonaws.vpce.us-east-1.vpce-svc-1")
	in.XksProxyVpcEndpointServiceOwner = aws.String("111122223333")

	created, err := client.CreateCustomKeyStore(ctx, in)
	require.NoError(t, err)

	describe := func() *kmstypes.XksProxyConfigurationType {
		out, descErr := client.DescribeCustomKeyStores(ctx, &kmssdk.DescribeCustomKeyStoresInput{
			CustomKeyStoreId: created.CustomKeyStoreId,
		})
		require.NoError(t, descErr)
		require.Len(t, out.CustomKeyStores, 1)

		return out.CustomKeyStores[0].XksProxyConfiguration
	}

	cfg := describe()
	require.NotNil(t, cfg)
	assert.Equal(t, "111122223333", aws.ToString(cfg.VpcEndpointServiceOwner))
	assert.Equal(t, "com.amazonaws.vpce.us-east-1.vpce-svc-1", aws.ToString(cfg.VpcEndpointServiceName))
	assert.Equal(t, "AKIAEXAMPLE0001", aws.ToString(cfg.AccessKeyId))
	assert.Equal(t, kmstypes.XksProxyConnectivityTypeVpcEndpointService, cfg.Connectivity)

	_, err = client.UpdateCustomKeyStore(ctx, &kmssdk.UpdateCustomKeyStoreInput{
		CustomKeyStoreId:                created.CustomKeyStoreId,
		XksProxyVpcEndpointServiceOwner: aws.String("444455556666"),
		XksProxyUriPath:                 aws.String("/new/kms/xks/v1"),
	})
	require.NoError(t, err)

	cfg = describe()
	assert.Equal(t, "444455556666", aws.ToString(cfg.VpcEndpointServiceOwner))
	assert.Equal(t, "/new/kms/xks/v1", aws.ToString(cfg.UriPath))

	_, err = client.ConnectCustomKeyStore(
		ctx,
		&kmssdk.ConnectCustomKeyStoreInput{CustomKeyStoreId: created.CustomKeyStoreId},
	)
	require.NoError(t, err)

	_, err = client.UpdateCustomKeyStore(ctx, &kmssdk.UpdateCustomKeyStoreInput{
		CustomKeyStoreId:                created.CustomKeyStoreId,
		XksProxyVpcEndpointServiceOwner: aws.String("777788889999"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CustomKeyStoreInvalidStateException")

	_, err = client.UpdateCustomKeyStore(ctx, &kmssdk.UpdateCustomKeyStoreInput{
		CustomKeyStoreId: created.CustomKeyStoreId,
		XksProxyAuthenticationCredential: &kmstypes.XksProxyAuthenticationCredentialType{
			AccessKeyId: aws.String("AKIAROTATED0002"), RawSecretAccessKey: aws.String("rotatedsecretvalue"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "AKIAROTATED0002", aws.ToString(describe().AccessKeyId))
}

func TestListKeyRotations_IncludeKeyMaterial(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		include     kmstypes.IncludeKeyMaterial
		wantEntries int
		wantFirstNo bool
	}{
		{name: "default", include: "", wantEntries: 2},
		{name: "rotations-only", include: kmstypes.IncludeKeyMaterialRotationsOnly, wantEntries: 2},
		{name: "all", include: kmstypes.IncludeKeyMaterialAllKeyMaterial, wantEntries: 3, wantFirstNo: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKMSClient(t, newTestKMSHandler())
			ctx := t.Context()

			key, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
			require.NoError(t, err)

			for range 2 {
				_, err = client.RotateKeyOnDemand(ctx, &kmssdk.RotateKeyOnDemandInput{KeyId: key.KeyMetadata.KeyId})
				require.NoError(t, err)
			}

			out, err := client.ListKeyRotations(ctx, &kmssdk.ListKeyRotationsInput{
				KeyId: key.KeyMetadata.KeyId, IncludeKeyMaterial: tt.include,
			})
			require.NoError(t, err)
			require.Len(t, out.Rotations, tt.wantEntries)

			ids := map[string]bool{}
			current := 0

			for _, r := range out.Rotations {
				ids[aws.ToString(r.KeyMaterialId)] = true

				if r.KeyMaterialState == kmstypes.KeyMaterialStateCurrent {
					current++
				}
			}

			assert.Len(t, ids, tt.wantEntries, "key material ids must be distinct")
			assert.Equal(t, 1, current)

			if tt.wantFirstNo {
				assert.Nil(t, out.Rotations[0].RotationDate)
				assert.Empty(t, out.Rotations[0].RotationType)
			}
		})
	}

	t.Run("asymmetric-all-rejected", func(t *testing.T) {
		t.Parallel()

		client := newTestKMSClient(t, newTestKMSHandler())
		key, err := client.CreateKey(t.Context(), &kmssdk.CreateKeyInput{KeySpec: kmstypes.KeySpecRsa2048})
		require.NoError(t, err)

		_, err = client.ListKeyRotations(t.Context(), &kmssdk.ListKeyRotationsInput{
			KeyId: key.KeyMetadata.KeyId, IncludeKeyMaterial: kmstypes.IncludeKeyMaterialAllKeyMaterial,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "UnsupportedOperationException")
	})
}
