package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestListCommands_FiltersByDocumentName proves ListCommandsInput.Filters is
// honoured for the documented DocumentName key (api_op_ListCommands.go
// types.CommandFilter) -- previously not even parsed into this backend's
// ListCommandsInput, so every command was always returned regardless of the
// filter.
func TestListCommands_FiltersByDocumentName(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	for _, docName := range []string{"doc-a", "doc-b"} {
		_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
			Name:    aws.String(docName),
			Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		})
		require.NoError(t, err)

		_, err = client.SendCommand(ctx, &ssmsdk.SendCommandInput{
			DocumentName: aws.String(docName),
			InstanceIds:  []string{"i-cmdfilter"},
		})
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		wantDoc     string
		filters     []ssmtypes.CommandFilter
		wantDocsLen int
	}{
		{
			name:        "no filter returns every command",
			filters:     nil,
			wantDocsLen: 2,
		},
		{
			name: "DocumentName filter narrows to matching command",
			filters: []ssmtypes.CommandFilter{
				{Key: ssmtypes.CommandFilterKeyDocumentName, Value: aws.String("doc-a")},
			},
			wantDocsLen: 1,
			wantDoc:     "doc-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.ListCommands(ctx, &ssmsdk.ListCommandsInput{Filters: tt.filters})
			require.NoError(t, err)
			require.Len(t, out.Commands, tt.wantDocsLen)

			if tt.wantDoc != "" {
				assert.Equal(t, tt.wantDoc, aws.ToString(out.Commands[0].DocumentName))
			}
		})
	}
}

// TestListCommandInvocations_FiltersByDocumentName proves
// ListCommandInvocationsInput.Filters is honoured the same way ListCommands'
// is -- both share the CommandFilter shape and were both silently dropped.
func TestListCommandInvocations_FiltersByDocumentName(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	for _, docName := range []string{"doc-x", "doc-y"} {
		_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
			Name:    aws.String(docName),
			Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		})
		require.NoError(t, err)

		_, err = client.SendCommand(ctx, &ssmsdk.SendCommandInput{
			DocumentName: aws.String(docName),
			InstanceIds:  []string{"i-invfilter"},
		})
		require.NoError(t, err)
	}

	out, err := client.ListCommandInvocations(ctx, &ssmsdk.ListCommandInvocationsInput{
		Filters: []ssmtypes.CommandFilter{
			{Key: ssmtypes.CommandFilterKeyDocumentName, Value: aws.String("doc-y")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.CommandInvocations, 1)
	assert.Equal(t, "doc-y", aws.ToString(out.CommandInvocations[0].DocumentName))

	all, err := client.ListCommandInvocations(ctx, &ssmsdk.ListCommandInvocationsInput{})
	require.NoError(t, err)
	assert.Len(t, all.CommandInvocations, 2, "no filter must still return every invocation")
}
