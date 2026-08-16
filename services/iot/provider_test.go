package iot_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestRefinement1_ProviderInit_NilCtx verifies that ErrNilAppContext is returned.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nil_context_returns_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &iot.Provider{}
			_, err := p.Init(nil)
			require.ErrorIs(t, err, iot.ErrNilAppContext)
		})
	}
}

// TestRefinement1_ProviderName verifies Provider.Name returns "IoT".
func TestProviderName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "provider_name_is_IoT", want: "IoT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &iot.Provider{}
			assert.Equal(t, tt.want, p.Name())
		})
	}
}

// TestRefinement1_ProviderInit_Valid verifies Provider.Init succeeds with valid context.
func TestProviderInit_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "valid_context_succeeds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &iot.Provider{}
			svc, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
			require.NoError(t, err)
			assert.NotNil(t, svc)
		})
	}
}
