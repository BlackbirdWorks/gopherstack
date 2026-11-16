package ocimysql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnginePublishesXPort(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	db := e.mkEngineDB(t, "x", nil)

	require.NotNil(t, db.PortX)
	assert.Equal(t, *db.Port+10000, *db.PortX, "portX is the published X protocol port, not the configured 33060")
	require.NotEmpty(t, db.Endpoints)
	assert.Equal(t, *db.PortX, *db.Endpoints[0].PortX)

	l.mu.Lock()
	defer l.mu.Unlock()

	require.NotEmpty(t, l.specs)
	assert.True(t, l.specs[0].PublishX)
}
