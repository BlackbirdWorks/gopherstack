package gcpartifactregistry_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestLocationCatalogue(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	ctx := t.Context()

	it := e.client.ListLocations(ctx, &location.ListLocationsRequest{Name: "projects/" + project})
	ids := map[string]string{}

	for {
		l, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		ids[l.GetLocationId()] = l.GetDisplayName()
	}

	assert.Len(t, ids, 46)
	assert.Equal(t, "Iowa", ids["us-central1"])
	assert.Contains(t, ids, "europe")

	got, err := e.client.GetLocation(ctx, &location.GetLocationRequest{Name: loc})
	require.NoError(t, err)
	assert.Equal(t, loc, got.GetName())
	assert.Equal(t, "us-central1", got.GetLabels()["cloud.googleapis.com/region"])

	_, err = e.client.GetLocation(ctx, &location.GetLocationRequest{Name: "projects/" + project + "/locations/mars-1"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.ListLocations(ctx, &location.ListLocationsRequest{Name: "bad"}).Next()
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "list", Method: http.MethodGet, Path: "/v1/projects/p/locations?pageSize=2", Status: 200,
			Contains: []string{`"locations":[`, `"nextPageToken"`},
		},
		{
			Name: "get", Method: http.MethodGet, Path: "/v1/projects/p/locations/europe-west4", Status: 200,
			Contains: []string{`"locationId":"europe-west4"`, `"displayName":"Netherlands"`},
		},
	})
}
