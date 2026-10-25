package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestResourceCreator_OpenSearchInstanceCountBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   any
		wantErr bool
	}{
		{name: "in range", count: float64(3)},
		{name: "above int32", count: float64(1 << 40), wantErr: true},
		{name: "below int32", count: float64(-(1 << 40)), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := cloudformation.NewResourceCreator(newAdditionalServiceBackends(t))
			props := map[string]any{
				"DomainName":    "bounds-os",
				"ClusterConfig": map[string]any{"InstanceCount": tt.count},
			}

			_, err := rc.Create(t.Context(), "D", "AWS::OpenSearch::Domain", props, nil, nil)
			if tt.wantErr {
				require.ErrorIs(t, err, cloudformation.ErrPropertyValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}
