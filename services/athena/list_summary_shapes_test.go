package athena_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestListNotebookSessions_NarrowSummary proves the gopherstack
// list-summary-shapes fix (2026-09-18): ListNotebookSessions leaked the
// full SessionSummary shape (Description/EngineVersion/NotebookVersion/
// Status), where the real types.NotebookSessionSummary is CreationTime and
// SessionId only. Drives the real aws-sdk-go-v2 client end to end, plus a
// raw-body assertion that the removed leak keys are absent.
func TestListNotebookSessions_NarrowSummary(t *testing.T) {
	t.Parallel()

	backend := athena.NewInMemoryBackend(realClientAthenaRegion, realClientAthenaAccountID)
	handler := athena.NewHandler(backend)
	client := newTestAthenaClient(t, handler)
	ctx := t.Context()

	createdNotebook, err := client.CreateNotebook(ctx, &athenasdk.CreateNotebookInput{
		WorkGroup: aws.String("primary"),
		Name:      aws.String("list-summary-nb"),
	})
	require.NoError(t, err)
	notebookID := aws.ToString(createdNotebook.NotebookId)

	started, err := client.StartSession(ctx, &athenasdk.StartSessionInput{
		WorkGroup:       aws.String("primary"),
		NotebookVersion: aws.String("v1"),
		EngineConfiguration: &athenatypes.EngineConfiguration{
			MaxConcurrentDpus: aws.Int32(5),
			AdditionalConfigs: map[string]string{"NotebookId": notebookID},
		},
	})
	require.NoError(t, err)
	sessionID := aws.ToString(started.SessionId)

	listed, err := client.ListNotebookSessions(ctx, &athenasdk.ListNotebookSessionsInput{
		NotebookId: aws.String(notebookID),
	})
	require.NoError(t, err)
	require.Len(t, listed.NotebookSessionsList, 1)
	assert.Equal(t, sessionID, aws.ToString(listed.NotebookSessionsList[0].SessionId))
	assert.NotZero(t, aws.ToTime(listed.NotebookSessionsList[0].CreationTime))

	rec := doRequest(t, handler, "ListNotebookSessions", `{"NotebookId":"`+notebookID+`"}`)
	body := rec.Body.String()
	assert.NotContains(t, body, "Description")
	assert.NotContains(t, body, "EngineVersion")
	assert.NotContains(t, body, "NotebookVersion")
	assert.NotContains(t, body, `"Status"`)
	assert.Contains(t, body, "SessionId")
	assert.Contains(t, body, "CreationTime")
}
