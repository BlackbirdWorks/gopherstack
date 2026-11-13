package gcpstorage_test

import (
	"errors"
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

const (
	saA = "a@test-project.iam.gserviceaccount.com"
	saB = "b@test-project.iam.gserviceaccount.com"
)

func listHMAC(t *testing.T, e *sdkEnv, opts ...storage.HMACKeyOption) []*storage.HMACKey {
	t.Helper()

	it := e.client.ListHMACKeys(t.Context(), sdkProject, opts...)

	var out []*storage.HMACKey

	for {
		k, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, k)
	}
}

func TestHMACKeyLifecycle(t *testing.T) {
	t.Parallel()

	e := newSDK(t)

	key, err := e.client.CreateHMACKey(t.Context(), sdkProject, saA)
	require.NoError(t, err)
	assert.Len(t, key.AccessID, 61)
	assert.Equal(t, "GOOG1E", key.AccessID[:6])
	assert.Len(t, key.Secret, 40)
	assert.Equal(t, storage.Active, key.State)
	assert.Equal(t, saA, key.ServiceAccountEmail)
	assert.Equal(t, sdkProject, key.ProjectID)

	h := e.client.HMACKeyHandle(sdkProject, key.AccessID)

	got, err := h.Get(t.Context())
	require.NoError(t, err)
	assert.Empty(t, got.Secret, "the secret is returned only by create")
	assert.Equal(t, key.AccessID, got.AccessID)

	var gerr *googleapi.Error

	require.ErrorAs(t, h.Delete(t.Context()), &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code, "an ACTIVE key cannot be deleted")

	up, err := h.Update(t.Context(), storage.HMACKeyAttrsToUpdate{State: storage.Inactive})
	require.NoError(t, err)
	assert.Equal(t, storage.Inactive, up.State)
	assert.NotEqual(t, got.Etag, up.Etag)

	require.NoError(t, h.Delete(t.Context()))

	gone, err := h.Get(t.Context())
	require.NoError(t, err)
	assert.Equal(t, storage.Deleted, gone.State)

	assert.Empty(t, listHMAC(t, e))
	assert.Len(t, listHMAC(t, e, storage.ShowDeletedHMACKeys()), 1)

	require.ErrorAs(t, h.Delete(t.Context()), &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)

	_, err = h.Update(t.Context(), storage.HMACKeyAttrsToUpdate{State: storage.Active})
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code, "a deleted key cannot be revived")

	_, err = e.client.HMACKeyHandle(sdkProject, "GOOG1Enope").Get(t.Context())
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)
}

func TestHMACKeyLimitsAndFilters(t *testing.T) {
	t.Parallel()

	e := newSDK(t)

	for range 10 {
		_, err := e.client.CreateHMACKey(t.Context(), sdkProject, saA)
		require.NoError(t, err)
	}

	_, err := e.client.CreateHMACKey(t.Context(), sdkProject, saA)

	var gerr *googleapi.Error

	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)

	_, err = e.client.CreateHMACKey(t.Context(), sdkProject, saB)
	require.NoError(t, err)

	assert.Len(t, listHMAC(t, e), 11)
	assert.Len(t, listHMAC(t, e, storage.ForHMACKeyServiceAccountEmail(saB)), 1)
	assert.Empty(t, listHMAC(t, e, storage.ForHMACKeyServiceAccountEmail("c@x.iam.gserviceaccount.com")))
}
