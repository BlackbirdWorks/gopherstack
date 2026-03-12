package kinesisanalytics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

func TestProvider_InitWithContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "initializes successfully with empty context",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &kinesisanalytics.Provider{}
			appCtx := &service.AppContext{}

			h, err := p.Init(appCtx)
			require.NoError(t, err)
			require.NotNil(t, h)

			handler, ok := h.(*kinesisanalytics.Handler)
			require.True(t, ok, "should return *Handler")
			assert.NotNil(t, handler.Backend)
		})
	}
}
