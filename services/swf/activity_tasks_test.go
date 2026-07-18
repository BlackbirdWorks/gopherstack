package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestCountPendingTasks_ActuallyCount(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.EnqueueActivityTaskInternal("dom", "list1", "act-1", "MyAct", "1.0", "", "wf-1", "run-1")
	b.EnqueueActivityTaskInternal("dom", "list1", "act-2", "MyAct", "1.0", "", "wf-1", "run-1")

	assert.Equal(t, 2, b.CountPendingActivityTasks("dom", "list1"))
	assert.Equal(t, 0, b.CountPendingActivityTasks("dom", "other-list"))
}
