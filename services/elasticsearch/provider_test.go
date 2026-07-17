package elasticsearch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestElasticsearchProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "default_init",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &elasticsearch.Provider{}
			assert.Equal(t, "Elasticsearch", p.Name())

			ctx := &service.AppContext{}
			handler, err := p.Init(ctx)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, handler)
		})
	}
}

// TestElasticsearchProvider_ErrNilAppContext verifies that Provider.Init
// returns ErrNilAppContext for a nil ctx.
func TestElasticsearchProvider_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &elasticsearch.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticsearch.ErrNilAppContext)
}
