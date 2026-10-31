package container_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestManagedLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		wantRes  string
	}{
		{name: "with resource", resource: "i-1", wantRes: "i-1"},
		{name: "service only", resource: "", wantRes: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := container.ManagedLabels("ec2", tt.resource)
			id := container.CurrentIdentity()

			assert.Equal(t, "true", got[container.LabelManaged])
			assert.Equal(t, id.Owner, got[container.LabelOwner])
			assert.Equal(t, "ec2", got[container.LabelService])
			assert.Equal(t, tt.wantRes, got[container.LabelResource])

			if tt.resource == "" {
				assert.NotContains(t, got, container.LabelResource)
			}
		})
	}
}
