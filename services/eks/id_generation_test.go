package eks_test

import (
	"regexp"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// anywhereSubscriptionIDPattern locks in the fix for CreateEksAnywhereSubscription's ID,
// previously a name+time.Now().UnixNano() hash that collided under synctest.
var anywhereSubscriptionIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

func TestEKSBackend_AnywhereSubscriptionID_Unique(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
		defer b.Close()

		term := eks.SubscriptionTerm{Duration: 1, Unit: "MONTHS"}

		sub1, err := b.CreateEksAnywhereSubscription("same-name", term, false, 1, "CLUSTER", nil)
		require.NoError(t, err)

		sub2, err := b.CreateEksAnywhereSubscription("same-name", term, false, 1, "CLUSTER", nil)
		require.NoError(t, err)

		assert.NotEqual(t, sub1.ID, sub2.ID,
			"two subscriptions with the same name created back-to-back must get distinct IDs")
		assert.Regexp(t, anywhereSubscriptionIDPattern, sub1.ID)
		assert.Regexp(t, anywhereSubscriptionIDPattern, sub2.ID)
	})
}
