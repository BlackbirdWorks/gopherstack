package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpotInstanceOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "request_spot", op: "request"},
		{name: "request_bad_image", op: "request_bad_image", wantErr: true},
		{name: "describe_all", op: "describe_all"},
		{name: "describe_by_id", op: "describe_by_id"},
		{name: "cancel", op: "cancel"},
		{name: "cancel_not_found", op: "cancel_not_found", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "request":
				req, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.05", nil)
				require.NoError(t, err)
				assert.NotEmpty(t, req.ID)
				assert.Equal(t, "active", req.State)
				assert.NotEmpty(t, req.InstanceID)

			case "request_bad_image":
				_, err := b.RequestSpotInstances("", "t2.micro", "", "0.05", nil)
				require.Error(t, err)

			case "describe_all":
				_, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.01", nil)
				require.NoError(t, err)
				reqs := b.DescribeSpotInstanceRequests(nil)
				assert.NotEmpty(t, reqs)

			case "describe_by_id":
				req, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.01", nil)
				require.NoError(t, err)
				reqs := b.DescribeSpotInstanceRequests([]string{req.ID})
				require.Len(t, reqs, 1)
				assert.Equal(t, req.ID, reqs[0].ID)

			case "cancel":
				req, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.01", nil)
				require.NoError(t, err)
				err = b.CancelSpotInstanceRequests([]string{req.ID})
				require.NoError(t, err)
				reqs := b.DescribeSpotInstanceRequests([]string{req.ID})
				require.Len(t, reqs, 1)
				assert.Equal(t, "cancelled", reqs[0].State)

			case "cancel_not_found":
				err := b.CancelSpotInstanceRequests([]string{"sir-nonexistent"})
				require.Error(t, err)
			}
		})
	}
}
