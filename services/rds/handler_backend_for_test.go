package rds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestHandler_BackendFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		wantRegion string
		wantHome   bool
	}{
		{name: "empty-is-home", region: "", wantRegion: "us-east-1", wantHome: true},
		{name: "home-region", region: "us-east-1", wantRegion: "us-east-1", wantHome: true},
		{name: "other-region", region: "eu-west-1", wantRegion: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := rds.NewHandler(rds.NewInMemoryBackend("000000000000", "us-east-1"))

			bk := h.BackendFor(tc.region)
			assert.Equal(t, tc.wantRegion, bk.Region())
			assert.Equal(t, tc.wantHome, bk == h.Backend)
			assert.Same(t, bk, h.BackendFor(tc.region))
		})
	}
}
