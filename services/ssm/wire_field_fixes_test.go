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

// TestDescribeParameters_ARN_RealClient covers a layer-3 bug (gopherstack-g8k9):
// ARN is already tracked per parameter (parameters.go's PutParameter sets
// param.ARN, and GetParameter/GetParametersByPath already emit it via the
// shared Parameter type), but DescribeParameters built its own ParameterMetadata
// items and never copied p.ARN across. Real field name and presence confirmed
// against ssm@v1.73.4 deserializers.go's
// awsAwsjson11_deserializeDocumentParameterMetadata (case "ARN":). Pre-fix, a
// real client's DescribeParameters always showed a nil ARN for every
// parameter regardless of what GetParameter returned for the same name.
func TestDescribeParameters_ARN_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.PutParameter(ctx, &ssmsdk.PutParameterInput{
		Name:  aws.String("/wire-fixes/arn-param"),
		Value: aws.String("v1"),
		Type:  "String",
	})
	require.NoError(t, err)

	got, err := client.GetParameter(ctx, &ssmsdk.GetParameterInput{
		Name: aws.String("/wire-fixes/arn-param"),
	})
	require.NoError(t, err)
	wantARN := aws.ToString(got.Parameter.ARN)
	require.NotEmpty(t, wantARN)

	out, err := client.DescribeParameters(ctx, &ssmsdk.DescribeParametersInput{})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 1)
	require.NotNil(
		t,
		out.Parameters[0].ARN,
		"ParameterMetadata.ARN must round-trip from the same value GetParameter returns; pre-fix it was always nil",
	)
	assert.Equal(t, wantARN, aws.ToString(out.Parameters[0].ARN))
}

// TestGetDocument_CreatedDateStatusInfoRequires_RealClient covers a layer-3 bug
// (gopherstack-g8k9): CreatedDate, StatusInformation, and Requires are all
// already tracked on the internal Document/DocumentVersion structs and already
// emitted correctly by DescribeDocument's DocumentDescription (documents.go's
// asDocumentDescription), but GetDocumentOutput never carried any of the
// three despite serving the exact same underlying document. Real field
// presence confirmed against ssm@v1.73.4 deserializers.go's
// awsAwsjson11_deserializeOpDocumentGetDocumentOutput (cases "CreatedDate",
// "StatusInformation", "Requires"). Pre-fix, a real client's GetDocument
// always showed a zero CreatedDate and nil StatusInformation/Requires.
func TestGetDocument_CreatedDateStatusInfoRequires_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("wire-fixes-doc"),
		Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
	})
	require.NoError(t, err)

	described, err := client.DescribeDocument(ctx, &ssmsdk.DescribeDocumentInput{
		Name: aws.String("wire-fixes-doc"),
	})
	require.NoError(t, err)
	wantCreatedDate := aws.ToTime(described.Document.CreatedDate)

	got, err := client.GetDocument(ctx, &ssmsdk.GetDocumentInput{
		Name: aws.String("wire-fixes-doc"),
	})
	require.NoError(t, err)
	require.NotNil(
		t,
		got.CreatedDate,
		"GetDocument.CreatedDate must round-trip from the same document DescribeDocument reads; pre-fix it was always nil",
	)
	assert.Equal(t, wantCreatedDate, aws.ToTime(got.CreatedDate))
}

// TestListDocuments_Tags_RealClient covers a layer-3 bug (gopherstack-g8k9):
// CreateDocument's Tags input is already stored into the backend's generic
// miscResourceTags store (documents.go's CreateDocument, readable back via
// ListTagsForResource(ResourceType=Document)), but ListDocuments' DocumentIdentifier
// never carried a Tags field at all, and neither did DocumentDescription
// (CreateDocument/UpdateDocument/DescribeDocument's shared response shape).
// Real field presence confirmed against ssm@v1.73.4 deserializers.go's
// awsAwsjson11_deserializeDocumentDocumentIdentifier and
// _DocumentDescription (both have case "Tags":). Pre-fix, a real client's
// ListDocuments always showed a nil Tags slice for every document regardless
// of what was supplied at CreateDocument.
func TestListDocuments_Tags_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("wire-fixes-tagged-doc"),
		Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
		Tags: []ssmtypes.Tag{
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	out, err := client.ListDocuments(ctx, &ssmsdk.ListDocumentsInput{
		Filters: []ssmtypes.DocumentKeyValuesFilter{
			{Key: aws.String("Name"), Values: []string{"wire-fixes-tagged-doc"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.DocumentIdentifiers, 1)

	entry := out.DocumentIdentifiers[0]
	require.NotEmpty(
		t,
		entry.Tags,
		"DocumentIdentifier.Tags must round-trip from CreateDocument's Tags input; pre-fix it was always empty",
	)
	assert.Equal(t, "team", aws.ToString(entry.Tags[0].Key))
	assert.Equal(t, "platform", aws.ToString(entry.Tags[0].Value))
}

func TestSSMListOps_NarrowSummaryParity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		test func(t *testing.T, client *ssmsdk.Client)
		name string
	}{
		{
			name: "list_association_versions_narrow_shape",
			test: func(t *testing.T, client *ssmsdk.Client) {
				t.Helper()
				ctx := t.Context()
				docName := "assoc-narrow-doc"
				_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
					Name:    aws.String(docName),
					Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
				})
				require.NoError(t, err)

				createOut, err := client.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
					Name:            aws.String(docName),
					AssociationName: aws.String("my-assoc"),
				})
				require.NoError(t, err)
				require.NotNil(t, createOut.AssociationDescription)
				assocID := createOut.AssociationDescription.AssociationId

				listOut, err := client.ListAssociationVersions(ctx, &ssmsdk.ListAssociationVersionsInput{
					AssociationId: assocID,
				})
				require.NoError(t, err)
				require.Len(t, listOut.AssociationVersions, 1)

				v := listOut.AssociationVersions[0]
				assert.Equal(t, aws.ToString(assocID), aws.ToString(v.AssociationId))
				assert.Equal(t, "my-assoc", aws.ToString(v.AssociationName))
				assert.Equal(t, docName, aws.ToString(v.Name))
			},
		},
		{
			name: "list_document_versions_narrow_shape",
			test: func(t *testing.T, client *ssmsdk.Client) {
				t.Helper()
				ctx := t.Context()
				docName := "doc-versions-narrow-doc"
				_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
					Name:    aws.String(docName),
					Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
				})
				require.NoError(t, err)

				listOut, err := client.ListDocumentVersions(ctx, &ssmsdk.ListDocumentVersionsInput{
					Name: aws.String(docName),
				})
				require.NoError(t, err)
				require.Len(t, listOut.DocumentVersions, 1)

				v := listOut.DocumentVersions[0]
				assert.Equal(t, docName, aws.ToString(v.Name))
				assert.Equal(t, "1", aws.ToString(v.DocumentVersion))
				assert.True(t, v.IsDefaultVersion)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			client := newTestSSMClient(t, ssm.NewHandler(backend))
			tt.test(t, client)
		})
	}
}
