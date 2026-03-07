package ecr_test

// This file verifies the Backend and Snapshottable interface contracts and
// tests the Handler's behaviour when its Backend does or does not implement
// Snapshottable.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// compile-time assertion: InMemoryBackend satisfies Backend.
var _ ecr.Backend = (*ecr.InMemoryBackend)(nil)

// compile-time assertion: InMemoryBackend satisfies Snapshottable.
var _ ecr.Snapshottable = (*ecr.InMemoryBackend)(nil)

// ---- stubBackend -------------------------------------------------------

// stubBackend is a minimal Backend implementation that does NOT implement
// Snapshottable, used to verify that Handler gracefully skips persistence
// for non-snapshottable backends.
type stubBackend struct {
	repos map[string]*ecr.Repository
}

func newStubBackend() *stubBackend {
	return &stubBackend{repos: make(map[string]*ecr.Repository)}
}

func (s *stubBackend) CreateRepository(name string) (*ecr.Repository, error) {
	if name == "" {
		return nil, ecr.ErrInvalidRepositoryName
	}

	if _, ok := s.repos[name]; ok {
		return nil, ecr.ErrRepositoryAlreadyExists
	}

	r := &ecr.Repository{RepositoryName: name, RegistryID: "000000000000"}
	s.repos[name] = r

	cp := *r

	return &cp, nil
}

func (s *stubBackend) DescribeRepositories(names []string) ([]ecr.Repository, error) {
	if len(names) == 0 {
		out := make([]ecr.Repository, 0, len(s.repos))
		for _, r := range s.repos {
			out = append(out, *r)
		}

		return out, nil
	}

	out := make([]ecr.Repository, 0, len(names))

	for _, n := range names {
		r, ok := s.repos[n]
		if !ok {
			return nil, ecr.ErrRepositoryNotFound
		}

		out = append(out, *r)
	}

	return out, nil
}

func (s *stubBackend) DeleteRepository(name string) (*ecr.Repository, error) {
	r, ok := s.repos[name]
	if !ok {
		return nil, ecr.ErrRepositoryNotFound
	}

	delete(s.repos, name)

	cp := *r

	return &cp, nil
}

func (s *stubBackend) ProxyEndpoint() string { return "stub:5000" }
func (s *stubBackend) SetEndpoint(_ string)  {}

// ---- tests --------------------------------------------------------------

// TestECR_Handler_AcceptsBackendInterface ensures Handler works with any
// Backend implementation, not just InMemoryBackend.
func TestECR_Handler_AcceptsBackendInterface(t *testing.T) {
	t.Parallel()

	h := ecr.NewHandler(newStubBackend(), nil)

	bodyBytes, err := json.Marshal(map[string]any{"repositoryName": "my-repo"})
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921.CreateRepository")
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	repo, ok := resp["repository"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-repo", repo["repositoryName"])
}

// TestECR_Handler_SnapshotNilForNonSnapshottable verifies that Snapshot returns
// nil (rather than panicking) when the backend does not implement Snapshottable.
func TestECR_Handler_SnapshotNilForNonSnapshottable(t *testing.T) {
	t.Parallel()

	h := ecr.NewHandler(newStubBackend(), nil)
	assert.Nil(t, h.Snapshot())
}

// TestECR_Handler_RestoreNoopForNonSnapshottable verifies that Restore is a
// no-op (no panic, no error) when the backend does not implement Snapshottable.
func TestECR_Handler_RestoreNoopForNonSnapshottable(t *testing.T) {
	t.Parallel()

	h := ecr.NewHandler(newStubBackend(), nil)
	require.NoError(t, h.Restore([]byte(`{"repos":{}}`)))
}

// TestECR_InMemoryBackend_ProxyEndpoint verifies the ProxyEndpoint accessor.
func TestECR_InMemoryBackend_ProxyEndpoint(t *testing.T) {
	t.Parallel()

	b := ecr.NewInMemoryBackend("000000000000", "us-east-1", "initial:5000")
	assert.Equal(t, "initial:5000", b.ProxyEndpoint())

	b.SetEndpoint("updated:9000")
	assert.Equal(t, "updated:9000", b.ProxyEndpoint())
}

// TestECR_GetAuthorizationToken_ProxyEndpointFromBackend checks that the
// proxyEndpoint in the auth token response comes from the Backend interface.
func TestECR_GetAuthorizationToken_ProxyEndpointFromBackend(t *testing.T) {
	t.Parallel()

	h := ecr.NewHandler(newStubBackend(), nil)

	bodyBytes, err := json.Marshal(map[string]any{})
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921.GetAuthorizationToken")
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	authData := resp["authorizationData"].([]any)
	entry := authData[0].(map[string]any)
	assert.Equal(t, "stub:5000", entry["proxyEndpoint"])
}
