package neptune

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLookupNodeIDWithoutRecordedSpace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, space, id, want string
	}{
		{name: "no space", id: "x", want: "x"},
		{name: "unrecorded space uses namespace plus id", space: "person", id: "x", want: "personx"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &pgLoader{ids: &pgIDSpaces{}}
			assert.Equal(t, tt.want, l.lookupNodeID(tt.space, tt.id))
		})
	}
}
