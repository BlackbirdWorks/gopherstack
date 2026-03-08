package integration_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_SSM_GetParameterHistory_InitialVersion(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "test-param-" + uuid.NewString()

	// Create initial parameter
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("version-1"),
		Type:  types.ParameterTypeString,
	})
	require.NoError(t, err)

	// Get history for initial version
	historyResp, err := client.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
		Name: aws.String(paramName),
	})
	require.NoError(t, err)
	require.NotNil(t, historyResp.Parameters)
	require.Len(t, historyResp.Parameters, 1)
	assert.Equal(t, paramName, *historyResp.Parameters[0].Name)
	assert.Equal(t, "version-1", *historyResp.Parameters[0].Value)
}

func TestIntegration_SSM_GetParameterHistory_MultipleVersions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "test-param-multi-" + uuid.NewString()

	// Create and update parameter multiple times
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("version-1"),
		Type:  types.ParameterTypeString,
	})
	require.NoError(t, err)

	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String(paramName),
		Value:     aws.String("version-2"),
		Type:      types.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String(paramName),
		Value:     aws.String("version-3"),
		Type:      types.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err)

	// Get history - should have all 3 versions
	historyResp, err := client.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
		Name: aws.String(paramName),
	})
	require.NoError(t, err)
	require.NotNil(t, historyResp.Parameters)
	require.Len(t, historyResp.Parameters, 3)

	// Verify reverse order (newest first)
	assert.Equal(t, int64(3), historyResp.Parameters[0].Version)
	assert.Equal(t, "version-3", *historyResp.Parameters[0].Value)

	assert.Equal(t, int64(2), historyResp.Parameters[1].Version)
	assert.Equal(t, "version-2", *historyResp.Parameters[1].Value)

	assert.Equal(t, int64(1), historyResp.Parameters[2].Version)
	assert.Equal(t, "version-1", *historyResp.Parameters[2].Value)
}

func TestIntegration_SSM_GetParameterHistory_WithMaxResults(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "test-param-maxresults-" + uuid.NewString()

	// Create multiple versions (5 versions)
	for i := 1; i <= 5; i++ {
		overwrite := i > 1
		_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
			Name:      aws.String(paramName),
			Value:     aws.String("version-" + strconv.Itoa(i)),
			Type:      types.ParameterTypeString,
			Overwrite: aws.Bool(overwrite),
		})
		require.NoError(t, err)
	}

	// Get history with MaxResults limit
	maxResults := int32(2)
	historyResp, err := client.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
		Name:       aws.String(paramName),
		MaxResults: aws.Int32(maxResults),
	})
	require.NoError(t, err)
	require.NotNil(t, historyResp.Parameters)
	require.Len(t, historyResp.Parameters, 2)

	// Should return the latest 2 versions
	assert.Equal(t, int64(5), historyResp.Parameters[0].Version)
	assert.Equal(t, "version-5", *historyResp.Parameters[0].Value)

	assert.Equal(t, int64(4), historyResp.Parameters[1].Version)
	assert.Equal(t, "version-4", *historyResp.Parameters[1].Value)
}

func TestIntegration_SSM_GetParameterHistory_ParameterNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "nonexistent-param-" + uuid.NewString()

	// Try to get history for non-existent parameter
	_, err := client.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
		Name: aws.String(paramName),
	})
	require.Error(t, err)
	// The error should be a ParameterNotFound error
	assert.Contains(t, err.Error(), "ParameterNotFound")
}

func TestIntegration_SSM_SecureString_PutAndGetEncrypted(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "secure-param-" + uuid.NewString()

	// Create a SecureString parameter
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("super-secret-value"),
		Type:  types.ParameterTypeSecureString,
	})
	require.NoError(t, err)

	// Get without decryption - should be encrypted
	getResp, err := client.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           aws.String(paramName),
		WithDecryption: aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ParameterTypeSecureString, getResp.Parameter.Type)
	assert.NotEqual(t, "super-secret-value", *getResp.Parameter.Value)
	assert.NotEmpty(t, *getResp.Parameter.Value) // Should be encrypted
}

func TestIntegration_SSM_SecureString_GetWithDecryption(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	paramName := "secure-param-" + uuid.NewString()

	// Create a SecureString parameter
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(paramName),
		Value: aws.String("super-secret-value"),
		Type:  types.ParameterTypeSecureString,
	})
	require.NoError(t, err)

	// Get with decryption - should be decrypted
	getResp, err := client.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           aws.String(paramName),
		WithDecryption: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ParameterTypeSecureString, getResp.Parameter.Type)
	assert.Equal(t, "super-secret-value", *getResp.Parameter.Value)
}

func TestIntegration_SSM_SecureString_GetMultipleParameters(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	// Create mixed parameter types
	secureParam := "secure-" + uuid.NewString()
	stringParam := "string-" + uuid.NewString()

	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(secureParam),
		Value: aws.String("secure-value"),
		Type:  types.ParameterTypeSecureString,
	})
	require.NoError(t, err)

	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String(stringParam),
		Value: aws.String("plain-value"),
		Type:  types.ParameterTypeString,
	})
	require.NoError(t, err)

	// Get both with decryption
	getResp, err := client.GetParameters(ctx, &ssm.GetParametersInput{
		Names:          []string{secureParam, stringParam},
		WithDecryption: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, getResp.Parameters, 2)

	// Verify both are decrypted/plain
	for _, param := range getResp.Parameters {
		switch *param.Name {
		case secureParam:
			assert.Equal(t, "secure-value", *param.Value)
		case stringParam:
			assert.Equal(t, "plain-value", *param.Value)
		}
	}
}

func TestIntegration_SSM_Document_CreateAndGet(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	docName := "test-doc-" + uuid.NewString()
	content := `{"schemaVersion":"2.2","description":"Test","mainSteps":[]}`

	createResp, err := client.CreateDocument(ctx, &ssm.CreateDocumentInput{
		Name:         aws.String(docName),
		Content:      aws.String(content),
		DocumentType: types.DocumentTypeCommand,
	})
	require.NoError(t, err)
	require.NotNil(t, createResp.DocumentDescription)
	assert.Equal(t, docName, *createResp.DocumentDescription.Name)
	assert.Equal(t, types.DocumentStatusActive, createResp.DocumentDescription.Status)

	getResp, err := client.GetDocument(ctx, &ssm.GetDocumentInput{
		Name: aws.String(docName),
	})
	require.NoError(t, err)
	assert.Equal(t, docName, *getResp.Name)
	assert.Equal(t, content, *getResp.Content)
}

func TestIntegration_SSM_Document_DescribeDocument(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	docName := "test-desc-doc-" + uuid.NewString()

	_, err := client.CreateDocument(ctx, &ssm.CreateDocumentInput{
		Name:         aws.String(docName),
		Content:      aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		DocumentType: types.DocumentTypeCommand,
	})
	require.NoError(t, err)

	descResp, err := client.DescribeDocument(ctx, &ssm.DescribeDocumentInput{
		Name: aws.String(docName),
	})
	require.NoError(t, err)
	require.NotNil(t, descResp.Document)
	assert.Equal(t, docName, *descResp.Document.Name)
	assert.Equal(t, types.DocumentTypeCommand, descResp.Document.DocumentType)
}

func TestIntegration_SSM_Document_ListDocuments(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	prefix := "list-test-doc-" + uuid.NewString()

	for i := range 3 {
		_, err := client.CreateDocument(ctx, &ssm.CreateDocumentInput{
			Name:         aws.String(prefix + "-" + strconv.Itoa(i)),
			Content:      aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
			DocumentType: types.DocumentTypeCommand,
		})
		require.NoError(t, err)
	}

	listResp, err := client.ListDocuments(ctx, &ssm.ListDocumentsInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listResp.DocumentIdentifiers), 3)
}

func TestIntegration_SSM_Document_UpdateDocument(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	docName := "test-update-doc-" + uuid.NewString()

	_, err := client.CreateDocument(ctx, &ssm.CreateDocumentInput{
		Name:         aws.String(docName),
		Content:      aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		DocumentType: types.DocumentTypeCommand,
	})
	require.NoError(t, err)

	updateResp, err := client.UpdateDocument(ctx, &ssm.UpdateDocumentInput{
		Name:            aws.String(docName),
		Content:         aws.String(`{"schemaVersion":"2.2","description":"updated","mainSteps":[]}`),
		DocumentVersion: aws.String("1"),
	})
	require.NoError(t, err)
	require.NotNil(t, updateResp.DocumentDescription)
	assert.Equal(t, "2", *updateResp.DocumentDescription.LatestVersion)
}

func TestIntegration_SSM_Document_DeleteDocument(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	docName := "test-del-doc-" + uuid.NewString()

	_, err := client.CreateDocument(ctx, &ssm.CreateDocumentInput{
		Name:         aws.String(docName),
		Content:      aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		DocumentType: types.DocumentTypeCommand,
	})
	require.NoError(t, err)

	_, err = client.DeleteDocument(ctx, &ssm.DeleteDocumentInput{
		Name: aws.String(docName),
	})
	require.NoError(t, err)

	_, err = client.GetDocument(ctx, &ssm.GetDocumentInput{Name: aws.String(docName)})
	require.Error(t, err)
}

func TestIntegration_SSM_Document_Permissions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	docName := "test-perm-doc-" + uuid.NewString()

	_, err := client.CreateDocument(ctx, &ssm.CreateDocumentInput{
		Name:         aws.String(docName),
		Content:      aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		DocumentType: types.DocumentTypeCommand,
	})
	require.NoError(t, err)

	_, err = client.ModifyDocumentPermission(ctx, &ssm.ModifyDocumentPermissionInput{
		Name:            aws.String(docName),
		PermissionType:  types.DocumentPermissionTypeShare,
		AccountIdsToAdd: []string{"111111111111"},
	})
	require.NoError(t, err)

	descPermResp, err := client.DescribeDocumentPermission(ctx, &ssm.DescribeDocumentPermissionInput{
		Name:           aws.String(docName),
		PermissionType: types.DocumentPermissionTypeShare,
	})
	require.NoError(t, err)
	assert.Contains(t, descPermResp.AccountIds, "111111111111")
}

func TestIntegration_SSM_Document_ListDocumentVersions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	docName := "test-ver-doc-" + uuid.NewString()

	_, err := client.CreateDocument(ctx, &ssm.CreateDocumentInput{
		Name:         aws.String(docName),
		Content:      aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		DocumentType: types.DocumentTypeCommand,
	})
	require.NoError(t, err)

	_, err = client.UpdateDocument(ctx, &ssm.UpdateDocumentInput{
		Name:            aws.String(docName),
		Content:         aws.String(`{"schemaVersion":"2.2","description":"v2","mainSteps":[]}`),
		DocumentVersion: aws.String("1"),
	})
	require.NoError(t, err)

	versResp, err := client.ListDocumentVersions(ctx, &ssm.ListDocumentVersionsInput{
		Name: aws.String(docName),
	})
	require.NoError(t, err)
	require.Len(t, versResp.DocumentVersions, 2)
	assert.Equal(t, "1", *versResp.DocumentVersions[0].DocumentVersion)
	assert.Equal(t, "2", *versResp.DocumentVersions[1].DocumentVersion)
}

func TestIntegration_SSM_Document_DefaultDocuments(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	tests := []struct {
		name string
	}{
		{name: "AWS-RunShellScript"},
		{name: "AWS-RunPowerShellScript"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			getResp, err := client.GetDocument(ctx, &ssm.GetDocumentInput{
				Name: aws.String(tt.name),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.name, *getResp.Name)
			assert.NotEmpty(t, *getResp.Content)
		})
	}
}

func TestIntegration_SSM_Command_SendAndList(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	sendResp, err := client.SendCommand(ctx, &ssm.SendCommandInput{
		DocumentName: aws.String("AWS-RunShellScript"),
		InstanceIds:  []string{"i-test01"},
		Comment:      aws.String("integration test"),
	})
	require.NoError(t, err)
	require.NotNil(t, sendResp.Command)
	assert.NotEmpty(t, *sendResp.Command.CommandId)
	assert.Equal(t, "AWS-RunShellScript", *sendResp.Command.DocumentName)

	listResp, err := client.ListCommands(ctx, &ssm.ListCommandsInput{
		CommandId: sendResp.Command.CommandId,
	})
	require.NoError(t, err)
	require.Len(t, listResp.Commands, 1)
	assert.Equal(t, *sendResp.Command.CommandId, *listResp.Commands[0].CommandId)
}

func TestIntegration_SSM_Command_GetCommandInvocation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	sendResp, err := client.SendCommand(ctx, &ssm.SendCommandInput{
		DocumentName: aws.String("AWS-RunShellScript"),
		InstanceIds:  []string{"i-invtest"},
	})
	require.NoError(t, err)

	invResp, err := client.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{
		CommandId:  sendResp.Command.CommandId,
		InstanceId: aws.String("i-invtest"),
	})
	require.NoError(t, err)
	assert.Equal(t, *sendResp.Command.CommandId, *invResp.CommandId)
	assert.Equal(t, "i-invtest", *invResp.InstanceId)
}

func TestIntegration_SSM_Command_ListCommandInvocations(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSSMClient(t)
	ctx := t.Context()

	sendResp, err := client.SendCommand(ctx, &ssm.SendCommandInput{
		DocumentName: aws.String("AWS-RunShellScript"),
		InstanceIds:  []string{"i-inv01", "i-inv02"},
	})
	require.NoError(t, err)

	listInvResp, err := client.ListCommandInvocations(ctx, &ssm.ListCommandInvocationsInput{
		CommandId: sendResp.Command.CommandId,
	})
	require.NoError(t, err)
	require.Len(t, listInvResp.CommandInvocations, 2)

	instanceIDs := []string{
		*listInvResp.CommandInvocations[0].InstanceId,
		*listInvResp.CommandInvocations[1].InstanceId,
	}
	assert.Contains(t, instanceIDs, "i-inv01")
	assert.Contains(t, instanceIDs, "i-inv02")
}
