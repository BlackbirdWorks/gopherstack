package route53resolver_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestProviderNilContext(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, route53resolver.ErrNilAppContext)
}

func TestProviderValidContext(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// --- Endpoint validation ---

func TestProviderName(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	assert.Equal(t, "Route53Resolver", p.Name())
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Route53Resolver", svc.Name())
}
