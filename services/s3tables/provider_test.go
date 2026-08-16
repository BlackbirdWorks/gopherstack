package s3tables_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

func TestProvider_InitNilContext(t *testing.T) {
	t.Parallel()

	p := &s3tables.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &s3tables.Provider{}
	assert.Equal(t, "S3tables", p.Name())
}

func TestProvider_InitDefault(t *testing.T) {
	t.Parallel()

	p := &s3tables.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	require.NotNil(t, svc)
}
