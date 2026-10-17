package mediaconvert

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateJobIDShape(t *testing.T) {
	t.Parallel()

	tests := []struct{ name string }{{name: "first"}, {name: "second"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Regexp(t, `^\d{13}-[a-z0-9]{6}$`, generateJobID())
		})
	}
}
