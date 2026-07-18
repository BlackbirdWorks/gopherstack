package scheduler_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

func TestSchedulerProvider(t *testing.T) {
	t.Parallel()

	p := &scheduler.Provider{}
	assert.Equal(t, "Scheduler", p.Name())
}

func TestSchedulerProviderInit(t *testing.T) {
	t.Parallel()

	p := &scheduler.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Scheduler", svc.Name())
}

func TestSchedulerProviderInit_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &scheduler.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, scheduler.ErrNilAppContext)
}
