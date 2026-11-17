package gcpcompute

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFreeImageNameSkipsExistingImages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		taken  map[string]bool
		name   string
		want   string
		suffix []string
	}{
		{name: "free", taken: map[string]bool{}, suffix: []string{"aaaaaa"}, want: "web-aaaaaa"},
		{
			name: "collision retried", taken: map[string]bool{"web-aaaaaa": true},
			suffix: []string{"aaaaaa", "bbbbbb"}, want: "web-bbbbbb",
		},
		{
			name: "two collisions", taken: map[string]bool{"web-aaaaaa": true, "web-bbbbbb": true},
			suffix: []string{"aaaaaa", "bbbbbb", "cccccc"}, want: "web-cccccc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			i := 0
			got := freeImageName("web", func(n string) bool { return tt.taken[n] }, func() string {
				s := tt.suffix[i%len(tt.suffix)]
				i++

				return s
			})
			assert.Equal(t, tt.want, got)
		})
	}
}
