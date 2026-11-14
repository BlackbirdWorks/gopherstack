package ocilogging_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocilogging"
)

func TestSavedSearchLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	created, err := e.mgmt.CreateLogSavedSearch(t.Context(), logging.CreateLogSavedSearchRequest{
		CompartmentId: new(testCompartment), Name: new("errors"), Query: new(`search "x" | where level = 'ERROR'`),
		Description: new("d"), FreeformTags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	assert.Equal(t, logging.LogSavedSearchLifecycleStateActive, created.LifecycleState)
	assert.NotEmpty(t, *created.Etag)

	got, err := e.mgmt.GetLogSavedSearch(t.Context(), logging.GetLogSavedSearchRequest{LogSavedSearchId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "errors", *got.Name)
	assert.Equal(t, map[string]string{"k": "v"}, got.FreeformTags)

	_, err = e.mgmt.UpdateLogSavedSearch(t.Context(), logging.UpdateLogSavedSearchRequest{
		LogSavedSearchId: created.Id, IfMatch: new("stale"),
		Name: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.mgmt.UpdateLogSavedSearch(t.Context(), logging.UpdateLogSavedSearchRequest{
		LogSavedSearchId: created.Id, IfMatch: created.Etag,
		Name: new("renamed"), Query: new("search \"y\""),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.Name)
	assert.Equal(t, "search \"y\"", *upd.Query)
	assert.Equal(t, "d", *upd.Description)

	list, err := e.mgmt.ListLogSavedSearches(t.Context(), logging.ListLogSavedSearchesRequest{
		CompartmentId: new(testCompartment), Name: new("renamed"),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	other := "ocid1.compartment.oc1..other"
	_, err = e.mgmt.ChangeLogSavedSearchCompartment(t.Context(), logging.ChangeLogSavedSearchCompartmentRequest{
		LogSavedSearchId: created.Id,
		CompartmentId:    &other,
	})
	require.NoError(t, err)

	list, err = e.mgmt.ListLogSavedSearches(
		t.Context(),
		logging.ListLogSavedSearchesRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	snap := e.backend.Snapshot(t.Context())
	restored := oci.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	_, _, err = restored.GetSavedSearch(*created.Id)
	require.NoError(t, err)

	_, err = e.mgmt.DeleteLogSavedSearch(t.Context(), logging.DeleteLogSavedSearchRequest{LogSavedSearchId: created.Id})
	require.NoError(t, err)

	_, err = e.mgmt.GetLogSavedSearch(t.Context(), logging.GetLogSavedSearchRequest{LogSavedSearchId: created.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestSavedSearchValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   logging.CreateLogSavedSearchDetails
		name string
	}{
		{
			name: "no query",
			in:   logging.CreateLogSavedSearchDetails{CompartmentId: new(testCompartment), Name: new("n")},
		},
		{
			name: "no name",
			in:   logging.CreateLogSavedSearchDetails{CompartmentId: new(testCompartment), Query: new("q")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			_, err := e.mgmt.CreateLogSavedSearch(
				t.Context(),
				logging.CreateLogSavedSearchRequest{CreateLogSavedSearchDetails: tt.in},
			)
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestSavedSearchRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	req := logging.CreateLogSavedSearchRequest{
		OpcRetryToken: new("tok"),
		CompartmentId: new(testCompartment), Name: new("n"), Query: new("q"),
	}

	a, err := e.mgmt.CreateLogSavedSearch(t.Context(), req)
	require.NoError(t, err)

	b, err := e.mgmt.CreateLogSavedSearch(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, *a.Id, *b.Id)
}
