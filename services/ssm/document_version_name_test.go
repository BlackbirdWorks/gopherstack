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

func TestDocumentVersionName_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
	ctx := t.Context()
	content := aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`)

	created, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name: aws.String("vn-doc"), Content: content, VersionName: aws.String("release-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "release-1", aws.ToString(created.DocumentDescription.VersionName))

	_, err = client.UpdateDocument(ctx, &ssmsdk.UpdateDocumentInput{
		Name: aws.String("vn-doc"), Content: content, VersionName: aws.String("release-2"),
	})
	require.NoError(t, err)

	_, err = client.UpdateDocument(ctx, &ssmsdk.UpdateDocumentInput{
		Name: aws.String("vn-doc"), Content: content, VersionName: aws.String("release-1"),
	})

	var dup *ssmtypes.DuplicateDocumentVersionName

	require.ErrorAs(t, err, &dup)

	listed, err := client.ListDocumentVersions(ctx, &ssmsdk.ListDocumentVersionsInput{Name: aws.String("vn-doc")})
	require.NoError(t, err)
	require.Len(t, listed.DocumentVersions, 2)
	assert.Equal(t, "release-1", aws.ToString(listed.DocumentVersions[0].VersionName))
	assert.Equal(t, "release-2", aws.ToString(listed.DocumentVersions[1].VersionName))

	tests := []struct {
		name        string
		version     *string
		versionName *string
		wantVersion string
		wantErr     bool
	}{
		{"by name", nil, aws.String("release-1"), "1", false},
		{"by other name", nil, aws.String("release-2"), "2", false},
		{"name and matching version", aws.String("1"), aws.String("release-1"), "1", false},
		{"name and mismatched version", aws.String("2"), aws.String("release-1"), "", true},
		{"unknown name", nil, aws.String("nope"), "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, getErr := client.GetDocument(ctx, &ssmsdk.GetDocumentInput{
				Name: aws.String("vn-doc"), DocumentVersion: tt.version, VersionName: tt.versionName,
			})
			desc, descErr := client.DescribeDocument(ctx, &ssmsdk.DescribeDocumentInput{
				Name: aws.String("vn-doc"), DocumentVersion: tt.version, VersionName: tt.versionName,
			})

			if tt.wantErr {
				var invalid *ssmtypes.InvalidDocumentVersion

				require.Error(t, getErr)
				require.ErrorAs(t, getErr, &invalid)
				require.Error(t, descErr)

				return
			}

			require.NoError(t, getErr)
			require.NoError(t, descErr)
			assert.Equal(t, tt.wantVersion, aws.ToString(got.DocumentVersion))
			assert.Equal(t, aws.ToString(tt.versionName), aws.ToString(got.VersionName))
			assert.Equal(t, tt.wantVersion, aws.ToString(desc.Document.DocumentVersion))
			assert.Equal(t, aws.ToString(tt.versionName), aws.ToString(desc.Document.VersionName))
		})
	}
}

func TestDocumentVersionName_DeleteByName(t *testing.T) {
	t.Parallel()

	client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))
	ctx := t.Context()
	content := aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`)

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name: aws.String("vn-del"), Content: content, VersionName: aws.String("release-1"),
	})
	require.NoError(t, err)

	_, err = client.UpdateDocument(ctx, &ssmsdk.UpdateDocumentInput{
		Name: aws.String("vn-del"), Content: content, VersionName: aws.String("release-2"),
	})
	require.NoError(t, err)

	_, err = client.DeleteDocument(ctx, &ssmsdk.DeleteDocumentInput{
		Name: aws.String("vn-del"), VersionName: aws.String("release-2"),
	})
	require.NoError(t, err)

	listed, err := client.ListDocumentVersions(ctx, &ssmsdk.ListDocumentVersionsInput{Name: aws.String("vn-del")})
	require.NoError(t, err)
	require.Len(t, listed.DocumentVersions, 1)
	assert.Equal(t, "release-1", aws.ToString(listed.DocumentVersions[0].VersionName))
}
