package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestBackend_GetExtension_ResolvesIdentifierForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pick func(ext *appconfig.Extension) string
		name string
	}{
		{name: "id", pick: func(e *appconfig.Extension) string { return e.ID }},
		{name: "name", pick: func(e *appconfig.Extension) string { return e.Name }},
		{name: "arn", pick: func(e *appconfig.Extension) string { return e.Arn }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
			ext, err := b.CreateExtension("ident-ext", "d", nil, nil, nil)
			require.NoError(t, err)

			got, err := b.GetExtension(tt.pick(ext), 0)
			require.NoError(t, err)
			assert.Equal(t, ext.ID, got.ID)

			require.NoError(t, b.DeleteExtension(tt.pick(ext), 0))
		})
	}
}
