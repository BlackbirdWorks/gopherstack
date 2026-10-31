package batch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestRunContainerStampsIdentityLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		job  string
		spec ContainerSpec
	}{
		{
			name: "with job label",
			spec: ContainerSpec{Image: "x", Labels: map[string]string{"gopherstack.batch.job": "j-1"}},
			job:  "j-1",
		},
		{name: "no spec labels", spec: ContainerSpec{Image: "x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fd := &fakeDocker{}
			r := &dockerContainerRunner{cli: fd}

			_, err := r.RunContainer(t.Context(), tt.spec, RunHooks{})
			require.NoError(t, err)

			l := fd.cfg.Labels
			assert.Equal(t, "true", l[container.LabelManaged])
			assert.Equal(t, container.CurrentIdentity().Owner, l[container.LabelOwner])
			assert.NotEmpty(t, l[container.LabelPID])
			assert.Equal(t, "batch", l[container.LabelService])
			assert.NotContains(t, l, container.LabelResource, "lost jobs fail, so they are never adopted")
			assert.Equal(t, tt.job, l["gopherstack.batch.job"])
		})
	}
}
