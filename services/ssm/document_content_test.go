package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestCreateDocument_ContentValidation(t *testing.T) {
	t.Parallel()

	const (
		cmd  = ssmtypes.DocumentTypeCommand
		auto = ssmtypes.DocumentTypeAutomation
		jsn  = ssmtypes.DocumentFormatJson
		yml  = ssmtypes.DocumentFormatYaml
	)

	tests := []struct {
		name    string
		content string
		typ     ssmtypes.DocumentType
		format  ssmtypes.DocumentFormat
		wantErr bool
	}{
		{name: "json object", content: `{"schemaVersion":"2.2","mainSteps":[]}`, typ: cmd, format: jsn},
		{name: "yaml object", content: "schemaVersion: '2.2'\nmainSteps: []\n", typ: cmd, format: yml},
		{name: "not json", content: "notjson", typ: cmd, format: jsn, wantErr: true},
		{name: "json array", content: `[1]`, typ: auto, format: jsn, wantErr: true},
		{name: "bad yaml", content: "a: [", typ: cmd, format: yml, wantErr: true},
		{
			name: "calendar text skipped", content: "BEGIN:VCALENDAR",
			typ: ssmtypes.DocumentTypeChangeCalendar, format: ssmtypes.DocumentFormatText,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))

			_, err := client.CreateDocument(t.Context(), &ssmsdk.CreateDocumentInput{
				Name: aws.String("d"), Content: aws.String(tt.content), DocumentType: tt.typ, DocumentFormat: tt.format,
			})
			if tt.wantErr {
				var invalid *ssmtypes.InvalidDocumentContent
				require.ErrorAs(t, err, &invalid)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUpdateDocument_DuplicateContent(t *testing.T) {
	t.Parallel()

	client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
	v1 := aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`)
	v2 := aws.String(`{"schemaVersion":"2.2","mainSteps":[],"description":"x"}`)

	_, err := client.CreateDocument(t.Context(), &ssmsdk.CreateDocumentInput{
		Name: aws.String("d"), Content: v1, DocumentType: ssmtypes.DocumentTypeCommand,
	})
	require.NoError(t, err)

	tests := []struct {
		content *string
		name    string
		wantErr bool
	}{
		{name: "same content", content: v1, wantErr: true},
		{name: "new content", content: v2},
		{name: "same as latest again", content: v2, wantErr: true},
	}

	for _, tt := range tests {
		_, err = client.UpdateDocument(t.Context(), &ssmsdk.UpdateDocumentInput{
			Name: aws.String("d"), Content: tt.content, DocumentVersion: aws.String("$LATEST"),
		})
		if tt.wantErr {
			var dup *ssmtypes.DuplicateDocumentContent
			require.ErrorAs(t, err, &dup, tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
	}
}

// terraform-provider-aws's document-delete waiter treats an InvalidDocument
// error as "gone" only when its message contains "does not exist".
func TestDescribeDocument_MissingMessageDoesNotExist(t *testing.T) {
	t.Parallel()

	client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))

	_, err := client.DescribeDocument(t.Context(), &ssmsdk.DescribeDocumentInput{Name: aws.String("nope")})

	var invalid *ssmtypes.InvalidDocument
	require.ErrorAs(t, err, &invalid)
	require.Contains(t, aws.ToString(invalid.Message), "does not exist")
}
