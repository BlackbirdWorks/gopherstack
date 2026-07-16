package lambda_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
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

// --- CapacityProvider tests ---

func TestNewOps_CapacityProvider_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createBody string
		wantName   string
		wantStatus int
	}{
		{
			name:       "with_concurrency",
			createBody: `{"Name":"my-provider","TargetOnDemandConcurrency":100}`,
			wantStatus: http.StatusCreated,
			wantName:   "my-provider",
		},
		{
			name:       "without_concurrency",
			createBody: `{"Name":"basic-provider"}`,
			wantStatus: http.StatusCreated,
			wantName:   "basic-provider",
		},
		{
			name:       "missing_name",
			createBody: `{}`,
			wantStatus: http.StatusBadRequest,
			wantName:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers", tt.createBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				var out lambda.CreateCapacityProviderOutput
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.NotNil(t, out.CapacityProvider)
				assert.Equal(t, tt.wantName, out.CapacityProvider.Name)
				assert.NotEmpty(t, out.CapacityProvider.CapacityProviderArn)
			}
		})
	}
}

func TestNewOps_CapacityProvider_GetDeleteUpdateList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	// Create
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers",
		`{"Name":"test-cp","TargetOnDemandConcurrency":50}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create duplicate → conflict
	dupRec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers",
		`{"Name":"test-cp"}`)
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers/test-cp", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	// Get not found
	getNotFound := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers/nonexistent", "")
	assert.Equal(t, http.StatusNotFound, getNotFound.Code)

	// Update
	updateRec := callInMemoryHandler(t, h, http.MethodPut, "/2025-11-30/capacity-providers/test-cp",
		`{"TargetOnDemandConcurrency":200}`)
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut lambda.UpdateCapacityProviderOutput
	require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&updateOut))
	require.NotNil(t, updateOut.CapacityProvider)
	assert.Equal(t, 200, updateOut.CapacityProvider.TargetOnDemandConcurrency)

	// List
	listRec := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListCapacityProvidersOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.CapacityProviders, 1)

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete, "/2025-11-30/capacity-providers/test-cp", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// List after delete → empty
	listRec2 := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers", "")
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 lambda.ListCapacityProvidersOutput
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
	assert.Empty(t, listOut2.CapacityProviders)
}

// --- ListFunctionVersionsByCapacityProvider tests ---

func TestNewOps_ListFunctionVersionsByCapacityProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cpName     string
		wantStatus int
		setup      bool // create the CP before the list call
		wantEmpty  bool
	}{
		{
			name:       "exists_returns_empty_list",
			cpName:     "my-cp",
			setup:      true,
			wantStatus: http.StatusOK,
			wantEmpty:  true,
		},
		{
			name:       "not_found_returns_404",
			cpName:     "nonexistent-cp",
			setup:      false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			if tt.setup {
				rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers",
					`{"Name":"`+tt.cpName+`","TargetOnDemandConcurrency":100}`)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2025-11-30/capacity-providers/"+tt.cpName+"/function-versions", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantEmpty {
				var out struct {
					FunctionVersions []any `json:"FunctionVersions"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Empty(t, out.FunctionVersions)
			}
		})
	}
}

// TestNewOps_CapacityProvider_UpdateNotFound tests updating a nonexistent capacity provider.
func TestNewOps_CapacityProvider_UpdateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2025-11-30/capacity-providers/nonexistent", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
