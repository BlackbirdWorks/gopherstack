package app

import (
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFormationValidatePropertiesFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "default_on", want: true},
		{name: "explicit_false", args: []string{"--cloudformation-validate-properties=false"}},
		{name: "negated", args: []string{"--no-cloudformation-validate-properties"}},
		{name: "explicit_true", args: []string{"--cloudformation-validate-properties"}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var root rootCLI

			parser, err := kong.New(&root, kong.Name("gopherstack"), kong.Writers(nil, nil))
			require.NoError(t, err)
			_, err = parser.Parse(tt.args)
			require.NoError(t, err)

			assert.Equal(t, tt.want, root.Serve.GetCloudFormationValidateProperties())
		})
	}
}
