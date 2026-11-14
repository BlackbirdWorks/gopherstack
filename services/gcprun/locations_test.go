package gcprun_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLocations(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	it := e.locs.ListLocations(t.Context(), &location.ListLocationsRequest{Name: "projects/test-project"})
	seen := map[string]bool{}

	for {
		l, err := it.Next()
		if err != nil {
			require.ErrorIs(t, err, iterator.Done)

			break
		}

		seen[l.GetLocationId()] = true

		assert.Equal(t, "projects/test-project/locations/"+l.GetLocationId(), l.GetName())
		assert.Equal(t, l.GetLocationId(), l.GetLabels()["cloud.googleapis.com/region"])
	}

	assert.True(t, seen["us-central1"])
	assert.True(t, seen["europe-west1"])

	got, err := e.locs.GetLocation(t.Context(), &location.GetLocationRequest{Name: parent})
	require.NoError(t, err)
	assert.Equal(t, "us-central1", got.GetLocationId())
	assert.Equal(t, "Iowa", got.GetDisplayName())

	tests := map[string]struct {
		get  string
		list string
		code codes.Code
	}{
		"unknown location": {get: "projects/p/locations/mars-1", code: codes.NotFound},
		"bad get name":     {get: "projects/p", code: codes.InvalidArgument},
		"bad list name":    {list: "locations/x", code: codes.InvalidArgument},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var gotErr error
			if tt.list != "" {
				_, gotErr = e.locs.ListLocations(t.Context(), &location.ListLocationsRequest{Name: tt.list}).Next()
			} else {
				_, gotErr = e.locs.GetLocation(t.Context(), &location.GetLocationRequest{Name: tt.get})
			}

			assert.Equal(t, tt.code, status.Code(gotErr))
		})
	}
}
