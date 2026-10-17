package mq_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestConfigurationIDShape(t *testing.T) {
	t.Parallel()

	tests := []struct{ name string }{{name: "activemq"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mq.NewInMemoryBackend("123456789012", "us-east-1")
			c, err := b.CreateConfiguration("cfg", "", mq.EngineTypeActiveMQ, "", "", nil)
			require.NoError(t, err)

			assert.Regexp(t, `^c-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, c.ID)
		})
	}
}
