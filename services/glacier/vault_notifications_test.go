package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func TestNotificationsStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snsTopic string
		events   []string
	}{
		{
			name:     "set_and_get",
			snsTopic: "arn:aws:sns:us-east-1:000000000000:test-topic",
			events:   []string{"ArchiveRetrievalCompleted", "InventoryRetrievalCompleted"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			err = bk.SetVaultNotifications(testAccountID, testRegion, "vault", tt.snsTopic, tt.events)
			require.NoError(t, err)

			snsTopic, events, err := bk.GetVaultNotifications(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Equal(t, tt.snsTopic, snsTopic)
			assert.Equal(t, tt.events, events)

			err = bk.DeleteVaultNotifications(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			snsTopic, _, err = bk.GetVaultNotifications(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Empty(t, snsTopic)
		})
	}
}
