package iam_test

import (
	"testing"

	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestRealClient_AccountProperties(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	client := newTestIAMClient(t, h)
	ctx := t.Context()

	// A fresh account has no properties set -- no fabricated defaults.
	empty, err := client.GetAccountProperties(ctx, &iamsdk.GetAccountPropertiesInput{})
	require.NoError(t, err)
	assert.Empty(t, empty.Properties)

	_, err = client.PutAccountProperties(ctx, &iamsdk.PutAccountPropertiesInput{
		Properties: map[string]string{"RoleManager/Enabled": "true"},
	})
	require.NoError(t, err)

	got, err := client.GetAccountProperties(ctx, &iamsdk.GetAccountPropertiesInput{})
	require.NoError(t, err)
	assert.Equal(t, "true", got.Properties["RoleManager/Enabled"])

	// A second namespace's properties are unaffected.
	_, err = client.PutAccountProperties(ctx, &iamsdk.PutAccountPropertiesInput{
		Properties: map[string]string{"OtherNamespace/Setting": "x"},
	})
	require.NoError(t, err)

	got, err = client.GetAccountProperties(ctx, &iamsdk.GetAccountPropertiesInput{})
	require.NoError(t, err)
	assert.Equal(t, "true", got.Properties["RoleManager/Enabled"])
	assert.Equal(t, "x", got.Properties["OtherNamespace/Setting"])
}

func TestRealClient_PutAccountProperties_MixedNamespaceRejected(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	client := newTestIAMClient(t, h)
	ctx := t.Context()

	_, err := client.PutAccountProperties(ctx, &iamsdk.PutAccountPropertiesInput{
		Properties: map[string]string{
			"RoleManager/Enabled": "true",
			"OtherNamespace/X":    "y",
		},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidInput", apiErr.ErrorCode())
}

func TestRealClient_PutAccountProperties_MalformedKeyRejected(t *testing.T) {
	t.Parallel()

	backend := iam.NewInMemoryBackend()
	h := iam.NewHandler(backend)
	client := newTestIAMClient(t, h)
	ctx := t.Context()

	_, err := client.PutAccountProperties(ctx, &iamsdk.PutAccountPropertiesInput{
		Properties: map[string]string{"NoSlashHere": "true"},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidInput", apiErr.ErrorCode())
}
