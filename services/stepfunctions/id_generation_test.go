package stepfunctions_test

import (
	"context"
	"regexp"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// syncExecutionNamePattern locks in the fix for StartSyncExecution's
// auto-generated name, which used to collide under synctest.
var syncExecutionNamePattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

func TestStepFunctionsBackend_SyncExecutionName_Unique(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := stepfunctions.NewInMemoryBackend()

		sm, err := b.CreateStateMachine(
			context.Background(), "sync-id-sm", minimalDefinition, validRoleARN, "EXPRESS",
		)
		require.NoError(t, err)

		res1, err := b.StartSyncExecution(sm.StateMachineArn, "", "{}")
		require.NoError(t, err)

		res2, err := b.StartSyncExecution(sm.StateMachineArn, "", "{}")
		require.NoError(t, err)

		assert.NotEqual(t, res1.Name, res2.Name,
			"two sync executions started back-to-back must get distinct names")
		assert.Regexp(t, syncExecutionNamePattern, res1.Name)
		assert.Regexp(t, syncExecutionNamePattern, res2.Name)
	})
}
