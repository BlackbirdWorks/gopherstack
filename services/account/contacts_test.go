package account_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/account"
)

func TestBackend_DeleteAlternateContact_NotFound(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.DeleteAlternateContact(account.ContactTypeBilling)
	require.Error(t, err)
}

func TestBackend_PutContactInformation_Get(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.GetContactInformation()
	require.Error(t, err)

	err = b.PutContactInformation(&account.ContactInformation{
		FullName: "Test Corp",
	})
	require.NoError(t, err)

	info, err := b.GetContactInformation()
	require.NoError(t, err)
	assert.Equal(t, "Test Corp", info.FullName)
}
