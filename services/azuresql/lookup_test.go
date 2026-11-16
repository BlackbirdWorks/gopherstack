package azuresql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

func TestServerIDByName(t *testing.T) {
	t.Parallel()

	b := newBackend(t, &fakeLauncher{})
	id := "/subscriptions/s/resourcegroups/rg/providers/microsoft.sql/servers/mysrv"
	mustEnsure(t, b, id, azuresql.Spec{})

	tests := []struct {
		name string
		find string
		ok   bool
	}{
		{"exact", "mysrv", true},
		{"case insensitive", "MySrv", true},
		{"missing", "other", false},
		{"suffix only", "srv", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := b.ServerIDByName(tt.find)
			assert.Equal(t, tt.ok, ok)

			if tt.ok {
				assert.Equal(t, id, got)
			}
		})
	}
}
