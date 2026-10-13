package batch_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestNodeRangeProperty_JSONKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
	}{
		{name: "wire key", in: `{"targetNodes":"0:","container":{"image":"busybox"}}`},
		{name: "legacy snapshot key", in: `{"targetNodes":"0:","containerProperties":{"image":"busybox"}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var r batch.NodeRangeProperty
			require.NoError(t, json.Unmarshal([]byte(tt.in), &r))
			require.NotNil(t, r.ContainerProperties)
			assert.Equal(t, "busybox", r.ContainerProperties.Image)

			out, err := json.Marshal(r)
			require.NoError(t, err)
			assert.Contains(t, string(out), `"container":`)
			assert.NotContains(t, string(out), `"containerProperties"`)
		})
	}
}
