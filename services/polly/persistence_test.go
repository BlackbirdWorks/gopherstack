package polly_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

// Test_SnapshotRestore_FullState exercises a Snapshot->Restore round trip
// across both store.Table-backed resource families the Phase 3.3 conversion
// touched (lexicons, tasks).
func Test_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := polly.NewInMemoryBackendWithConfig("111122223333", "us-west-2")

	require.NoError(t, original.PutLexicon(
		"greeting", `<lexicon alphabet="ipa" xml:lang="en-US"><lexeme></lexeme></lexicon>`,
	))
	require.NoError(t, original.PutLexicon("farewell", `<lexicon alphabet="ipa" xml:lang="en-GB"></lexicon>`))

	task, err := original.StartSpeechSynthesisTask(
		polly.SynthesisOptions{Text: "hello world", VoiceID: "Joanna"},
		"my-bucket", "audio/", "arn:aws:sns:us-west-2:111122223333:notifications",
	)
	require.NoError(t, err)

	// Advance the task once (scheduled -> inProgress) before snapshotting, so
	// the round trip is proven to carry a non-default TaskStatus, not just
	// the freshly-created default.
	_, err = original.GetSpeechSynthesisTask(task.TaskID)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := polly.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	lexicons := fresh.ListLexicons()
	require.Len(t, lexicons, 2)
	assert.Equal(t, "farewell", lexicons[0].Name)
	assert.Equal(t, "greeting", lexicons[1].Name)
	assert.Contains(t, lexicons[0].ARN, "111122223333")
	assert.Contains(t, lexicons[0].ARN, "us-west-2")

	// One more advance from the persisted "inProgress" status must land on
	// "completed" (text carries no "[fail]" marker), not restart from
	// "scheduled" -- proving TaskStatus itself round-tripped through the
	// snapshot rather than resetting.
	restoredTask, err := fresh.GetSpeechSynthesisTask(task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, task.TaskID, restoredTask.TaskID)
	assert.Equal(t, "completed", restoredTask.TaskStatus)
	assert.Equal(t, "my-bucket", restoredTask.OutputS3BucketName)
	assert.Equal(t, "audio/", restoredTask.OutputS3KeyPrefix)
	assert.Equal(t, "arn:aws:sns:us-west-2:111122223333:notifications", restoredTask.SNSTopicArn)
}

// Test_RestoreVersionMismatch verifies that a snapshot whose version doesn't
// match the current backend -- including a version-less snapshot, which
// decodes with Version == 0 -- is discarded cleanly rather than partially
// decoded: every table resets to empty, and Restore itself returns no error.
func Test_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
	}{
		{name: "explicit_mismatch", data: `{"version":999,"tables":{}}`},
		{name: "old_or_absent_version_decodes_as_zero", data: `{"tables":{}}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			b := polly.NewInMemoryBackend()
			require.NoError(t, b.PutLexicon("seed", `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`))
			task, err := b.StartSpeechSynthesisTask(
				polly.SynthesisOptions{Text: "hi", VoiceID: "Joanna"}, "bucket", "", "",
			)
			require.NoError(t, err)

			require.NoError(t, b.Restore(t.Context(), []byte(test.data)))

			assert.Empty(t, b.ListLexicons())

			tasks, _, listErr := b.ListSpeechSynthesisTasks("", "", 0)
			require.NoError(t, listErr)
			assert.Empty(t, tasks)

			_, err = b.GetSpeechSynthesisTask(task.TaskID)
			require.ErrorIs(t, err, polly.ErrTaskNotFound)
		})
	}
}

// Test_RestoreInvalidData verifies that malformed JSON is reported as an
// error rather than silently discarded or partially applied.
func Test_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := polly.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// Test_HandlerSnapshotRestore verifies the dead-wiring fix: Handler now
// implements persistence.Persistable by delegating to the backend, which it
// did not before Phase 3.3 (Polly had no persistence at all -- see
// persistence.go's file doc comment).
func Test_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	var _ persistence.Persistable = (*polly.Handler)(nil)

	original := polly.NewHandler(polly.NewInMemoryBackend())
	require.NoError(t, original.Backend.PutLexicon(
		"seed", `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`,
	))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := polly.NewHandler(polly.NewInMemoryBackend())
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Len(t, fresh.Backend.ListLexicons(), 1)
}
