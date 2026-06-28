package lambda_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCapacityProviderTestBackend creates an InMemoryBackend suitable for unit
// testing capacity-provider function-version assignments. It uses nil allocators
// so no real HTTP servers are started, and closes the backend on cleanup.
func newCapacityProviderTestBackend(t *testing.T) *lambda.InMemoryBackend {
	t.Helper()

	bk := lambda.NewInMemoryBackend(
		nil,
		nil,
		lambda.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		bk.Close(ctx)
	})

	return bk
}

// TestListFunctionVersionsByCapacityProvider_SeededAssignments verifies that
// function-version ARNs seeded onto a capacity provider via the internal seeding
// helper are returned by ListFunctionVersionsByCapacityProvider in sorted order.
//
// AWS exposes no public assignment API in this emulator's surface, so seeding is
// the only way to populate these assignments.
func TestListFunctionVersionsByCapacityProvider_SeededAssignments(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)

	_, err := bk.CreateCapacityProvider(&lambda.CreateCapacityProviderInput{
		Name:                      "my-cp",
		TargetOnDemandConcurrency: 100,
	})
	require.NoError(t, err)

	const (
		v1 = "arn:aws:lambda:us-east-1:000000000000:function:fn:1"
		v2 = "arn:aws:lambda:us-east-1:000000000000:function:fn:2"
	)

	// Seed in reverse order to confirm deterministic sorted output.
	require.NoError(t, bk.SeedCapacityProviderFunctionVersions("my-cp", v2, v1))

	p, err := bk.ListFunctionVersionsByCapacityProvider("my-cp", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Next)
	assert.Equal(t, []string{v1, v2}, p.Data)
}

// TestListFunctionVersionsByCapacityProvider_Pagination verifies that the
// MaxItems/Marker pagination is honoured for seeded assignments.
func TestListFunctionVersionsByCapacityProvider_Pagination(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)

	_, err := bk.CreateCapacityProvider(&lambda.CreateCapacityProviderInput{Name: "cp"})
	require.NoError(t, err)

	const (
		v1 = "arn:aws:lambda:us-east-1:000000000000:function:fn:1"
		v2 = "arn:aws:lambda:us-east-1:000000000000:function:fn:2"
		v3 = "arn:aws:lambda:us-east-1:000000000000:function:fn:3"
	)
	require.NoError(t, bk.SeedCapacityProviderFunctionVersions("cp", v1, v2, v3))

	first, err := bk.ListFunctionVersionsByCapacityProvider("cp", "", 2)
	require.NoError(t, err)
	assert.Equal(t, []string{v1, v2}, first.Data)
	require.NotEmpty(t, first.Next)

	second, err := bk.ListFunctionVersionsByCapacityProvider("cp", first.Next, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{v3}, second.Data)
	assert.Empty(t, second.Next)
}

// TestListFunctionVersionsByCapacityProvider_NotFound verifies that listing or
// seeding versions for a missing capacity provider returns ErrFunctionNotFound,
// which the handler maps to ResourceNotFoundException.
func TestListFunctionVersionsByCapacityProvider_NotFound(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)

	_, err := bk.ListFunctionVersionsByCapacityProvider("missing", "", 0)
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)

	err = bk.SeedCapacityProviderFunctionVersions("missing", "arn:whatever")
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}
