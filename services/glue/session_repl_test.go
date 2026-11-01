package glue_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionReplScriptCommitted(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("session_repl.pyrepl")
	require.NoError(t, err)
	require.NotEmpty(t, data)

	for _, marker := range []string{`"req"`, `"resp"`, "ready.json", "def run(", "def main("} {
		assert.Contains(t, string(data), marker)
	}
}
