package athena_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

var errNoSuchKey = errors.New("NoSuchKey")

type fakeS3Reader struct {
	objects map[string]string
}

func (f *fakeS3Reader) PutObject(context.Context, *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error) {
	return &sdk_s3.PutObjectOutput{}, nil
}

func (f *fakeS3Reader) GetObject(_ context.Context, in *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error) {
	body, ok := f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]
	if !ok {
		return nil, errNoSuchKey
	}

	return &sdk_s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(body))}, nil
}

func TestImportNotebook_S3Location_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		uri     string
		wantErr string
		wired   bool
	}{
		{name: "loads object", uri: "s3://nb/dir/a.ipynb", wired: true},
		{name: "missing object", uri: "s3://nb/dir/none.ipynb", wired: true, wantErr: "cannot read"},
		{name: "bad uri", uri: "nb/dir/a.ipynb", wired: true, wantErr: "s3://bucket/key"},
		{name: "s3 not wired", uri: "s3://nb/dir/a.ipynb", wantErr: "not available"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := athena.NewInMemoryBackend("123456789012", config.DefaultRegion)
			if tt.wired {
				backend.SetS3Backend(&fakeS3Reader{objects: map[string]string{"nb/dir/a.ipynb": `{"cells":[]}`}})
			}

			client := newTestAthenaClient(t, athena.NewHandler(backend))
			ctx := t.Context()

			out, err := client.ImportNotebook(ctx, &athenasdk.ImportNotebookInput{
				WorkGroup:             aws.String("primary"),
				Name:                  aws.String("nb1"),
				Type:                  "IPYNB",
				NotebookS3LocationUri: aws.String(tt.uri),
			})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)

			exp, err := client.ExportNotebook(ctx, &athenasdk.ExportNotebookInput{NotebookId: out.NotebookId})
			require.NoError(t, err)
			assert.JSONEq(t, `{"cells":[]}`, aws.ToString(exp.Payload))
		})
	}
}
