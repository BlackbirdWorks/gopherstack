package azureblob_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestProvider_Init_NilAppContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_context_errors"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &azureblob.Provider{}
			_, err := p.Init(nil)

			require.Error(t, err, tt.name)
			assert.ErrorIs(t, err, azureblob.ErrNilAppContext, tt.name)
		})
	}
}

func TestProvider_Init_ReturnsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "normal_init"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &azureblob.Provider{}
			reg, err := p.Init(&service.AppContext{})

			require.NoError(t, err, tt.name)
			require.NotNil(t, reg, tt.name)
			assert.Equal(t, "AzureBlob", reg.Name(), tt.name)
		})
	}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &azureblob.Provider{}
	assert.Equal(t, "AzureBlob", p.Name())
}
