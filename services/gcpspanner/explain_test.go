package gcpspanner_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

func TestExplainUnimplemented(t *testing.T) {
	t.Parallel()

	plain := fmt.Errorf("%w", io.ErrClosedPipe)

	tests := []struct {
		err     error
		name    string
		wantMsg string
		want    codes.Code
	}{
		{
			status.Error(codes.Unimplemented, ""),
			"empty message gains a reason",
			"does not implement M",
			codes.Unimplemented,
		},
		{status.Error(codes.Unimplemented, "custom"), "message kept", "custom", codes.Unimplemented},
		{status.Error(codes.NotFound, ""), "other code kept", "", codes.NotFound},
		{plain, "non status kept", io.ErrClosedPipe.Error(), codes.Unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := gcpspanner.ExplainUnimplemented("M", tt.err)
			assert.Equal(t, tt.want, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantMsg)
		})
	}
}
