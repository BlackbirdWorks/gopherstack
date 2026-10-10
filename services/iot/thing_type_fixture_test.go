package iot_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func mustCreateThingTypes(t *testing.T, b *iot.InMemoryBackend, names ...string) {
	t.Helper()

	for _, n := range names {
		_, err := b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: n})
		require.NoError(t, err)
	}
}
