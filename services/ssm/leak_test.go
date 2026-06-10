package ssm_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestSendCommand_PrunesExpiredCommands verifies that sending a command
// opportunistically prunes commands (and their invocations) that have aged out,
// so the commands/commandInvocations maps stay bounded even when the background
// janitor never runs.
func TestSendCommand_PrunesExpiredCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		preExpire int
	}{
		{name: "one expired", preExpire: 1},
		{name: "several expired", preExpire: 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.Background()

			// Send all commands first (AWS-RunShellScript is a built-in document),
			// recording their IDs so they can be expired together afterward.
			ids := make([]string, 0, tc.preExpire)
			for range tc.preExpire {
				out, err := b.SendCommand(ctx, &ssm.SendCommandInput{
					DocumentName: "AWS-RunShellScript",
					InstanceIDs:  []string{"i-1111"},
				})
				require.NoError(t, err)
				ids = append(ids, out.Command.CommandID)
			}

			require.Equal(t, tc.preExpire, b.CommandCount())
			require.Equal(t, tc.preExpire, b.CommandInvocationCount())

			// Now force every command into the past.
			for _, id := range ids {
				b.SetCommandExpiresAfter(id, 1)
			}

			// A fresh send should evict every expired command/invocation and add
			// exactly one live command.
			_, err := b.SendCommand(ctx, &ssm.SendCommandInput{
				DocumentName: "AWS-RunShellScript",
				InstanceIDs:  []string{"i-2222"},
			})
			require.NoError(t, err)

			require.Equal(t, 1, b.CommandCount(),
				"expired commands must be pruned on SendCommand")
			require.Equal(t, 1, b.CommandInvocationCount(),
				"expired command invocations must be pruned on SendCommand")
		})
	}
}
