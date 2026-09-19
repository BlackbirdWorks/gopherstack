package kms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateCustomKeyStore_PreservesOmittedName proves the zeroguard fix
// (cmd/zeroguard): UpdateCustomKeyStoreInput.NewCustomKeyStoreName is a
// plain string in the real SDK's request type. Before the fix it was
// decoded as string guarded by `!= ""`, so an omitted rename and an
// explicit empty-string rename were indistinguishable -- an explicit empty
// name was silently ignored rather than rejected, even though the field has
// no legitimate empty form (a key store must always have a name).
// CustomKeyStoreId is left a plain string: it is a required lookup
// identifier, never written back to state.
func TestUpdateCustomKeyStore_PreservesOmittedName(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	created, err := client.CreateCustomKeyStore(ctx, &kmssdk.CreateCustomKeyStoreInput{
		CustomKeyStoreName: aws.String("original-store"),
	})
	require.NoError(t, err)

	_, err = client.UpdateCustomKeyStore(ctx, &kmssdk.UpdateCustomKeyStoreInput{
		CustomKeyStoreId:      created.CustomKeyStoreId,
		NewCustomKeyStoreName: aws.String("renamed-store"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeCustomKeyStores(ctx, &kmssdk.DescribeCustomKeyStoresInput{
		CustomKeyStoreId: created.CustomKeyStoreId,
	})
	require.NoError(t, err)
	require.Len(t, desc.CustomKeyStores, 1)
	assert.Equal(t, "renamed-store", aws.ToString(desc.CustomKeyStores[0].CustomKeyStoreName))

	// Omitted NewCustomKeyStoreName must preserve the prior name.
	_, err = client.UpdateCustomKeyStore(ctx, &kmssdk.UpdateCustomKeyStoreInput{
		CustomKeyStoreId: created.CustomKeyStoreId,
	})
	require.NoError(t, err)

	desc, err = client.DescribeCustomKeyStores(ctx, &kmssdk.DescribeCustomKeyStoresInput{
		CustomKeyStoreId: created.CustomKeyStoreId,
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed-store", aws.ToString(desc.CustomKeyStores[0].CustomKeyStoreName),
		"omitted name must survive")

	// Explicit empty string has no legitimate meaning (a key store always
	// needs a name) and must be rejected, not silently ignored.
	_, err = client.UpdateCustomKeyStore(ctx, &kmssdk.UpdateCustomKeyStoreInput{
		CustomKeyStoreId:      created.CustomKeyStoreId,
		NewCustomKeyStoreName: aws.String(""),
	})
	require.Error(t, err)

	desc, err = client.DescribeCustomKeyStores(ctx, &kmssdk.DescribeCustomKeyStoresInput{
		CustomKeyStoreId: created.CustomKeyStoreId,
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed-store", aws.ToString(desc.CustomKeyStores[0].CustomKeyStoreName),
		"rejected rename must not mutate state")
}
