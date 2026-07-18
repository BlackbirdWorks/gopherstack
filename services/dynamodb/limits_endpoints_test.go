package dynamodb_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestDescribeLimits(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, resp := invokeOp(t, handler, "DescribeLimits", map[string]any{})
	require.Equal(t, http.StatusOK, code)

	assert.NotNil(t, resp["AccountMaxReadCapacityUnits"])
	assert.NotNil(t, resp["AccountMaxWriteCapacityUnits"])
	assert.NotNil(t, resp["TableMaxReadCapacityUnits"])
	assert.NotNil(t, resp["TableMaxWriteCapacityUnits"])

	// Values should be positive numbers
	rcu, ok := resp["AccountMaxReadCapacityUnits"].(float64)
	require.True(t, ok)
	assert.Greater(t, rcu, float64(0))
}

func TestDescribeEndpoints(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, resp := invokeOp(t, handler, "DescribeEndpoints", map[string]any{})
	require.Equal(t, http.StatusOK, code)

	endpoints, ok := resp["Endpoints"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	assert.NotEmpty(t, ep["Address"])
	assert.NotNil(t, ep["CachePeriodInMinutes"])
}
