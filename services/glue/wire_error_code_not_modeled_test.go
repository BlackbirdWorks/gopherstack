package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// These four ops (glue@v1.152.0 deserializers.go) do not model
// EntityNotFoundException at all -- only InvalidInputException among the
// codes that could describe an unresolvable identifier -- unlike their
// sibling Get/Update/Create ops in the same families, which do model
// EntityNotFoundException.
func TestDeleteFormType_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteFormType(t.Context(), &gluesdk.DeleteFormTypeInput{
		Identifier: aws.String("Missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "DeleteFormType has no EntityNotFoundException case")
}

func TestDeleteGlossary_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteGlossary(t.Context(), &gluesdk.DeleteGlossaryInput{
		Identifier: aws.String("missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "DeleteGlossary has no EntityNotFoundException case")
}

func TestDeleteGlossaryTerm_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteGlossaryTerm(t.Context(), &gluesdk.DeleteGlossaryTermInput{
		Identifier: aws.String("missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "DeleteGlossaryTerm has no EntityNotFoundException case")
}

func TestListGlossaryTerms_GlossaryNotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.ListGlossaryTerms(t.Context(), &gluesdk.ListGlossaryTermsInput{
		GlossaryIdentifier: aws.String("missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "ListGlossaryTerms has no EntityNotFoundException case")
}

// StopMaterializedViewRefreshTaskRun's error switch (glue@v1.152.0
// deserializers.go) has no EntityNotFoundException case either -- it models
// MaterializedViewRefreshTaskNotRunningException, which is exactly the
// "nothing running to stop" condition this call site hits.
func TestStopMaterializedViewRefreshTaskRun_NoRun(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.StopMaterializedViewRefreshTaskRun(t.Context(), &gluesdk.StopMaterializedViewRefreshTaskRunInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tbl1"),
	})
	require.Error(t, err)

	var nre *types.MaterializedViewRefreshTaskNotRunningException
	require.ErrorAs(t, err, &nre, "StopMaterializedViewRefreshTaskRun has no EntityNotFoundException case")
}

// DeleteJob's own doc comment (glue@v1.152.0 api_op_DeleteJob.go) states "If
// the job definition is not found, no exception is thrown" -- confirmed by
// its error switch also having no EntityNotFoundException case.
func TestDeleteJob_NotFound_Idempotent(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteJob(t.Context(), &gluesdk.DeleteJobInput{
		JobName: aws.String("missing"),
	})
	require.NoError(t, err, "DeleteJob on an unknown JobName must not error")
}

// DeleteTrigger's own doc comment states "If the trigger is not found, no
// exception is thrown", matching its error switch having no
// EntityNotFoundException case.
func TestDeleteTrigger_NotFound_Idempotent(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteTrigger(t.Context(), &gluesdk.DeleteTriggerInput{
		Name: aws.String("missing"),
	})
	require.NoError(t, err, "DeleteTrigger on an unknown Name must not error")
}

func TestDeleteSession_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteSession(t.Context(), &gluesdk.DeleteSessionInput{
		Id: aws.String("missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "DeleteSession has no EntityNotFoundException case")
}

func TestStopSession_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.StopSession(t.Context(), &gluesdk.StopSessionInput{
		Id: aws.String("missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "StopSession has no EntityNotFoundException case")
}

func TestDeleteWorkflow_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteWorkflow(t.Context(), &gluesdk.DeleteWorkflowInput{
		Name: aws.String("missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "DeleteWorkflow has no EntityNotFoundException case")
}

func TestDeleteAsset_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteAsset(t.Context(), &gluesdk.DeleteAssetInput{
		Identifier: aws.String("missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "DeleteAsset has no EntityNotFoundException case")
}

func TestDeleteAssetType_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DeleteAssetType(t.Context(), &gluesdk.DeleteAssetTypeInput{
		Identifier: aws.String("missing"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "DeleteAssetType has no EntityNotFoundException case")
}

func TestDescribeConnectionType_NotFound(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.DescribeConnectionType(t.Context(), &gluesdk.DescribeConnectionTypeInput{
		ConnectionType: aws.String("CUSTOM_CONN"),
	})
	require.Error(t, err)

	var ie *types.InvalidInputException
	require.ErrorAs(t, err, &ie, "DescribeConnectionType has no EntityNotFoundException case")
}
