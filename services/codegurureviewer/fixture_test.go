package codegurureviewer_test

import (
	"archive/zip"
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	codecommit "github.com/blackbirdworks/gopherstack/services/codecommit"
	cg "github.com/blackbirdworks/gopherstack/services/codegurureviewer"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	testAccount = "000000000000"
	testRegion  = "us-east-1"
	window      = time.Minute
)

type siblings struct{ cc, s3 service.Registerable }

func (s siblings) GetCodeCommitHandler() service.Registerable { return s.cc }
func (s siblings) GetS3Handler() service.Registerable         { return s.s3 }

type fixture struct {
	backend *cg.InMemoryBackend
	handler *cg.Handler
	cc      *codecommit.InMemoryBackend
	s3      *s3backend.S3Handler
	client  *sdk.Client
	clock   *atomic.Int64
}

func (f *fixture) now() time.Time { return time.Unix(f.clock.Load(), 0).UTC() }

func (f *fixture) settle() { f.clock.Add(int64(2 * window / time.Second)) }

func newFixture(t *testing.T) *fixture {
	t.Helper()

	f := &fixture{clock: &atomic.Int64{}}
	f.clock.Store(time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC).Unix())

	f.cc = codecommit.NewInMemoryBackend(testAccount, testRegion)
	f.s3 = s3backend.NewHandler(s3backend.NewInMemoryBackend(nil))

	f.backend = cg.NewInMemoryBackend(testAccount, testRegion)
	f.backend.SetClock(f.now)
	f.backend.SetLifecycleDelay(window)
	f.backend.SetAppConfig(siblings{cc: codecommit.NewHandler(f.cc), s3: f.s3})
	f.handler = cg.NewHandler(f.backend)

	url := sdktest.Serve(t, f.handler)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testAccount, "test", "")),
	)
	require.NoError(t, err)

	f.client = sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) })

	return f
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func (f *fixture) commitRepo(t *testing.T, name string, files map[string]string) {
	t.Helper()

	_, err := f.cc.CreateRepository(name, "", "", nil)
	require.NoError(t, err)

	for path, content := range files {
		_, _, err = f.cc.PutFile(name, "main", path, []byte(content), codecommit.PutFileMetadata{})
		require.NoError(t, err)
	}
}

func (f *fixture) bucketWithZip(t *testing.T, bucket, key string, files map[string]string) {
	t.Helper()

	_, err := f.s3.Backend.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for path, content := range files {
		w, werr := zw.Create(path)
		require.NoError(t, werr)
		_, werr = w.Write([]byte(content))
		require.NoError(t, werr)
	}

	require.NoError(t, zw.Close())

	_, err = f.s3.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(buf.Bytes()),
	})
	require.NoError(t, err)
}

func (f *fixture) associateCodeCommit(t *testing.T, name string) string {
	t.Helper()

	out, err := f.client.AssociateRepository(t.Context(), &sdk.AssociateRepositoryInput{
		Repository: &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String(name)}},
	})
	require.NoError(t, err)

	f.settle()

	return aws.ToString(out.RepositoryAssociation.AssociationArn)
}

func (f *fixture) review(t *testing.T, name, assoc, branch string) *sdk.CreateCodeReviewOutput {
	t.Helper()

	out, err := f.client.CreateCodeReview(t.Context(), &sdk.CreateCodeReviewInput{
		Name: aws.String(name), RepositoryAssociationArn: aws.String(assoc),
		Type: &types.CodeReviewType{RepositoryAnalysis: &types.RepositoryAnalysis{
			RepositoryHead: &types.RepositoryHeadSourceCodeType{BranchName: aws.String(branch)},
		}},
	})
	require.NoError(t, err)

	return out
}

const badGo = `package main

import "os"

func load() error {
	f, err := os.Open("x")
	if err == nil {
		return err
	}
	_ = f
	q := fmt.Sprintf("SELECT * FROM users WHERE id = %s", id)
	_ = q
	return nil
}
`

const badPy = `# TODO one
# TODO two
# FIXME three
key = "AKIAABCDEFGHIJKLMNOP"
`
