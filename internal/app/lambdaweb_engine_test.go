package app

import (
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLambdaWebEngineDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		args []string
	}{
		{name: "default_docker", want: "docker"},
		{name: "flag_stub", args: []string{"--lambdaweb-engine=stub"}, want: "stub"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var root rootCLI

			parser, err := kong.New(&root, kong.Name("gopherstack"), kong.Writers(nil, nil))
			require.NoError(t, err)
			_, err = parser.Parse(tt.args)
			require.NoError(t, err)

			assert.Equal(t, tt.want, root.Serve.GetLambdaWebSettings().Engine)
		})
	}
}
