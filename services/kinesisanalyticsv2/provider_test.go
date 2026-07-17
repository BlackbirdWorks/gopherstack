package kinesisanalyticsv2_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &kinesisanalyticsv2.Provider{}
	assert.Equal(t, "KinesisAnalyticsV2", p.Name())
}

// TestProvider_Init verifies Init builds a working handler when supplied a
// valid AppContext.
func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &kinesisanalyticsv2.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "KinesisAnalyticsV2", svc.Name())
}

func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &kinesisanalyticsv2.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, kinesisanalyticsv2.ErrNilAppContext)
}
