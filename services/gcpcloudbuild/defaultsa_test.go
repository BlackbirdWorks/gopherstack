package gcpcloudbuild_test

import (
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestGetDefaultServiceAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		resName string
		want    codes.Code
	}{
		{name: "global", resName: "projects/" + project + "/locations/global/defaultServiceAccount"},
		{name: "regional", resName: "projects/" + project + "/locations/us-central1/defaultServiceAccount"},
		{name: "wrong kind", resName: "projects/" + project + "/locations/global/builds", want: codes.InvalidArgument},
		{name: "no location", resName: "projects/" + project + "/defaultServiceAccount", want: codes.InvalidArgument},
		{name: "empty", resName: "", want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, nil)

			conn, err := grpc.NewClient(e.handler.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			got, err := cloudbuildpb.NewCloudBuildClient(conn).GetDefaultServiceAccount(
				t.Context(), &cloudbuildpb.GetDefaultServiceAccountRequest{Name: tt.resName})
			if tt.want != codes.OK {
				assert.Equal(t, tt.want, status.Code(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.resName, got.GetName())
			assert.Regexp(t,
				`^projects/`+project+`/serviceAccounts/\d{12}-compute@developer\.gserviceaccount\.com$`,
				got.GetServiceAccountEmail())
		})
	}
}
