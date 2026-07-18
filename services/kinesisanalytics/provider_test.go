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

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "initializes with defaults",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &kinesisanalytics.Provider{}
			assert.Equal(t, "KinesisAnalytics", p.Name())
		})
	}
}

// TestProviderInit_NilContext verifies ErrNilAppContext is returned for nil input.
func TestProviderInit_NilContext(t *testing.T) {
	t.Parallel()

	p := &kinesisanalytics.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, kinesisanalytics.ErrNilAppContext)
}

// TestErrNilAppContextValue verifies the ErrNilAppContext is non-nil and descriptive.
func TestErrNilAppContextValue(t *testing.T) {
	t.Parallel()

	require.Error(t, kinesisanalytics.ErrNilAppContext)
	assert.Contains(t, kinesisanalytics.ErrNilAppContext.Error(), "nil")
}

// TestProviderInit_NonNilContext verifies Provider.Init succeeds with valid AppContext.
func TestProviderInit_NonNilContext(t *testing.T) {
	t.Parallel()

	p := &kinesisanalytics.Provider{}
	h, err := p.Init(&service.AppContext{})

	require.NoError(t, err)
	require.NotNil(t, h)
}
