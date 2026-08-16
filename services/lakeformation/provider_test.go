package lakeformation_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "provider name", want: "LakeFormation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &lakeformation.Provider{}
			assert.Equal(t, tt.want, p.Name())

			ctx := &service.AppContext{Logger: slog.Default()}
			svc, err := p.Init(ctx)
			require.NoError(t, err)
			assert.NotNil(t, svc)
		})
	}
}

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &lakeformation.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, lakeformation.ErrNilAppContext)
}

func TestProviderInit_Success(t *testing.T) {
	t.Parallel()

	p := &lakeformation.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}
