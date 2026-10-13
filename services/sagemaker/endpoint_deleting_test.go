package sagemaker_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestDeleteEndpoint_DeletingStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		wantStatus string
		repeat     bool
	}{
		{name: "first delete", wantStatus: "Deleting"},
		{name: "repeat delete is a no-op", repeat: true, wantStatus: "Deleting"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			bk := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			bk.SetLifecycleDelay(time.Hour)
			t.Cleanup(func() { bk.Shutdown(ctx) })

			_, err := bk.CreateEndpointConfig(ctx, "ec", nil, nil)
			require.NoError(t, err)
			_, err = bk.CreateEndpoint(ctx, sagemaker.CreateEndpointOptions{Name: "ep", EndpointConfigName: "ec"})
			require.NoError(t, err)

			require.NoError(t, bk.DeleteEndpoint(ctx, "ep"))

			if tc.repeat {
				require.NoError(t, bk.DeleteEndpoint(ctx, "ep"))
			}

			ep, err := bk.DescribeEndpoint(ctx, "ep")
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, ep.EndpointStatus)
		})
	}
}
