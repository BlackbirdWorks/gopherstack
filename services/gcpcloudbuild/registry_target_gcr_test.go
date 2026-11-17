package gcpcloudbuild_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

func TestRegistryTargetGCR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, image, want string
		ok                bool
	}{
		{name: "gcr", image: "gcr.io/proj/app:v1", want: "h:1/proj/gcr.io/app:v1", ok: true},
		{name: "eu", image: "eu.gcr.io/proj/a/b", want: "h:1/proj/eu.gcr.io/a/b:latest", ok: true},
		{name: "lookalike host", image: "gcr.io.evil.test/proj/app"},
		{name: "unknown prefix", image: "xx.gcr.io/proj/app"},
		{name: "query project", image: "gcr.io/p?x=1/app"},
		{name: "upper project", image: "gcr.io/Proj/app"},
		{name: "dots project", image: "gcr.io/../app"},
		{name: "no image", image: "gcr.io/proj"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := gcpcloudbuild.RegistryTarget("h:1", tt.image)
			if !tt.ok {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
