package neptune

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gatedS3Reader struct {
	files   map[string]string
	release chan struct{}
}

func (g *gatedS3Reader) ListObjectsV2(
	_ context.Context, in *sdk_s3.ListObjectsV2Input,
) (*sdk_s3.ListObjectsV2Output, error) {
	out := &sdk_s3.ListObjectsV2Output{}
	for k := range g.files {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
	}

	return out, nil
}

func (g *gatedS3Reader) GetObject(_ context.Context, in *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error) {
	<-g.release

	return &sdk_s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte(g.files[*in.Key])))}, nil
}

func TestLoaderStatusDuringRunningLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		files  map[string]string
		name   string
		format string
	}{
		{
			name: "opencypher", format: "opencypher",
			files: map[string]string{"d/n.csv": ":ID,:LABEL\na,x\n", "d/r.csv": ":START_ID,:END_ID,:TYPE\na,a,rel\n"},
		},
		{name: "csv", format: "csv", files: map[string]string{"d/v.csv": "~id,~label\nv1,p\n"}},
		{name: "ntriples", format: "ntriples", files: map[string]string{"d/a.nt": "<urn:s> <urn:p> <urn:o> .\n"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gate := &gatedS3Reader{files: tt.files, release: make(chan struct{})}
			rs := newRDFService(func() S3Reader { return gate })
			ls := rs.loader

			job, apiErr := rs.newLoadJob(t.Context(), loadRequest{Source: "s3://lake/d/", Format: tt.format})
			require.Empty(t, apiErr)
			require.Empty(t, ls.admit(job, false))
			ls.dispatch(t.Context(), rs)

			snapshot := func() *loadJob {
				ls.mu.RLock("test")
				defer ls.mu.RUnlock()

				cp, _ := ls.snapshotJob(job.id)

				return cp
			}

			require.Equal(t, loadInProgress, snapshot().status)

			for range 2000 {
				snapshot()
			}

			close(gate.release)
			ls.wg.Wait()

			assert.True(t, snapshot().finished)
		})
	}
}
