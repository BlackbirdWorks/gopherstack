package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestResourceSharedWith(t *testing.T) {
	t.Parallel()

	const (
		receiver = "444455556666"
		res      = "arn:aws:cloudhsm:us-east-1:123456789012:backup/backup-abc"
	)

	tests := []struct {
		name     string
		account  string
		resource string
		want     bool
	}{
		{name: "receiver", account: receiver, resource: res, want: true},
		{name: "other account", account: "999999999999", resource: res},
		{name: "other resource", account: receiver, resource: res + "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("123456789012", "us-east-1")
			_, err := b.CreateResourceShare("s", true, nil, []string{receiver}, []string{res})
			require.NoError(t, err)

			assert.Equal(t, tt.want, b.ResourceSharedWith(tt.resource, tt.account))
		})
	}
}
