package ssm_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_HistoryCap verifies that PutParameter caps history to
// MaxHistoryCap entries, evicting the oldest entries on overflow.
func TestInMemoryBackend_HistoryCap(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	// Insert MaxHistoryCap + 10 versions.
	total := ssm.MaxHistoryCap + 10
	for i := range total {
		_, err := b.PutParameter(&ssm.PutParameterInput{
			Name:      "/capped/param",
			Type:      "String",
			Value:     "v",
			Overwrite: i > 0,
		})
		require.NoError(t, err)
	}

	assert.Equal(t, ssm.MaxHistoryCap, b.HistoryLen("/capped/param"))

	// The history returned should have MaxHistoryCap entries (newest first).
	out, err := b.GetParameterHistory(&ssm.GetParameterHistoryInput{Name: "/capped/param"})
	require.NoError(t, err)

	// GetParameterHistory caps at 50 by default, so just verify the newest version is present.
	assert.NotEmpty(t, out.Parameters)
	assert.Equal(t, int64(total), out.Parameters[0].Version)
}

// TestInMemoryBackend_DeleteCleansHistory verifies that deleting a parameter
// also removes its history and tags entries.
func TestInMemoryBackend_DeleteCleansHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "single_delete"},
		{name: "multi_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			paramName := "/delete/test-" + tt.name

			_, err := b.PutParameter(&ssm.PutParameterInput{
				Name:  paramName,
				Type:  "String",
				Value: "hello",
			})
			require.NoError(t, err)
			_, err = b.PutParameter(&ssm.PutParameterInput{
				Name:      paramName,
				Type:      "String",
				Value:     "world",
				Overwrite: true,
			})
			require.NoError(t, err)

			// Add a tag so we can verify it gets cleaned up on delete.
			err = b.AddTagsToResource(&ssm.AddTagsToResourceInput{
				ResourceType: "Parameter",
				ResourceID:   paramName,
				Tags:         []ssm.Tag{{Key: "env", Value: "test"}},
			})
			require.NoError(t, err)

			assert.Equal(t, 2, b.HistoryLen(paramName))
			assert.True(t, b.HasTagEntry(paramName), "tag entry should exist before delete")

			if tt.name == "single_delete" {
				_, err = b.DeleteParameter(&ssm.DeleteParameterInput{Name: paramName})
			} else {
				_, err = b.DeleteParameters(&ssm.DeleteParametersInput{Names: []string{paramName}})
			}

			require.NoError(t, err)
			assert.Equal(t, 0, b.HistoryLen(paramName))
			assert.False(t, b.HasTagEntry(paramName), "tag entry should be removed after delete")

			// Re-create the parameter and confirm no stale tags bleed through.
			_, err = b.PutParameter(&ssm.PutParameterInput{
				Name:  paramName,
				Type:  "String",
				Value: "fresh",
			})
			require.NoError(t, err)

			tagsOut, err := b.ListTagsForResource(&ssm.ListTagsForResourceInput{
				ResourceType: "Parameter",
				ResourceID:   paramName,
			})
			require.NoError(t, err)
			assert.Empty(t, tagsOut.TagList, "no stale tags should appear on recreated parameter")
		})
	}
}

// TestJanitor_SweepsExpiredCommands verifies that the janitor removes commands
// whose ExpiresAfter is in the past together with their invocations.
func TestJanitor_SweepsExpiredCommands(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	// AWS-RunShellScript is pre-registered as a default document.
	out1, err := b.SendCommand(&ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript",
		InstanceIDs:  []string{"i-1111"},
	})
	require.NoError(t, err)

	out2, err := b.SendCommand(&ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript",
		InstanceIDs:  []string{"i-2222"},
	})
	require.NoError(t, err)

	// Force the first command into the past.
	b.SetCommandExpiresAfter(out1.Command.CommandID, float64(time.Now().Add(-1*time.Second).Unix()))

	assert.Equal(t, 2, b.CommandCount())
	assert.Equal(t, 2, b.CommandInvocationCount())

	// Run the janitor once with a very short interval so it fires quickly.
	j := ssm.NewJanitor(b, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	go j.Run(ctx)

	// Wait until the expired command is gone.
	require.Eventually(t, func() bool {
		return b.CommandCount() == 1
	}, 2*time.Second, 20*time.Millisecond)

	assert.Equal(t, 1, b.CommandInvocationCount())

	// The non-expired command must still exist.
	listOut, err := b.ListCommands(&ssm.ListCommandsInput{CommandID: out2.Command.CommandID})
	require.NoError(t, err)
	require.Len(t, listOut.Commands, 1)
}

// TestHandler_WithJanitor_StartWorker verifies that StartWorker can be called
// on a handler that has a janitor attached without error.
func TestHandler_WithJanitor_StartWorker(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	h := ssm.NewHandler(b).WithJanitor(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err)
}
