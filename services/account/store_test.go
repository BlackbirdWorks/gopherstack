package account_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/account"
)

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := account.NewInMemoryBackend("000000000000", "us-east-1")

	err := b.PutAlternateContact(&account.AlternateContact{
		AlternateContactType: account.ContactTypeBilling,
		Name:                 "Test",
		EmailAddress:         "test@example.com",
	})
	require.NoError(t, err)

	b.Reset()

	_, err = b.GetAlternateContact(account.ContactTypeBilling)
	require.Error(t, err)
}
