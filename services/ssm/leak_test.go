package ssm_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestJanitor_PrunesExpiredCommands verifies that the janitor sweep removes
// commands (and their invocations) whose ExpiresAfter timestamp has passed.
// Expired-command cleanup is the janitor's job; SendCommand no longer does an
// O(n) on-write sweep.
func TestJanitor_PrunesExpiredCommands(t *testing.T) {
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

			// Force every command into the past.
			for _, id := range ids {
				b.SetCommandExpiresAfter(id, 1)
			}

			// SendCommand must NOT prune on the write path; expired commands
			// still present after a fresh send.
			_, err := b.SendCommand(ctx, &ssm.SendCommandInput{
				DocumentName: "AWS-RunShellScript",
				InstanceIDs:  []string{"i-2222"},
			})
			require.NoError(t, err)

			require.Equal(t, tc.preExpire+1, b.CommandCount(),
				"SendCommand must not prune expired commands — that is the janitor's job")

			// The janitor sweep removes expired entries and leaves only the live one.
			j := ssm.NewJanitor(b, 0)
			j.SweepOnce(ctx)

			require.Equal(t, 1, b.CommandCount(),
				"janitor must prune all expired commands")
			require.Equal(t, 1, b.CommandInvocationCount(),
				"janitor must prune expired command invocations")
		})
	}
}
