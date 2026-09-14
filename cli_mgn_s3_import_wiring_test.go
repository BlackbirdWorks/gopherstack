package main

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	mgnbackend "github.com/blackbirdworks/gopherstack/services/mgn"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// TestInitializeServices_MGNS3ImportWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than calling
// wireMGNS3 directly, so that deleting the wiring call from
// wireStorageAndSecretsIntegrations -- not just breaking the helper function
// itself -- is what this test is sensitive to.
//
// Regression test for gopherstack-i6oz: without InMemoryBackend.SetS3Backend
// wired, StartImport always fails the ImportTask (errImportSourceUnreadable:
// "no S3 backend configured") before ever reading the object, so no AWS-wire
// path can create a SourceServer. This asserts the real production
// composition root actually binds the MGN backend's S3Accessor seam to the
// real S3 backend: a bucket/object created through the real S3 backend, a
// real StartImport call, and a real SourceServer appearing in
// DescribeSourceServers.
func TestInitializeServices_MGNS3ImportWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	portAlloc, err := portalloc.New(19100, 19200)
	require.NoError(t, err)

	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
		PortAlloc:  portAlloc,
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	mgnH, ok := byName["MGN"].(*mgnbackend.Handler)
	require.True(t, ok, "MGN handler must be registered")
	require.NotNil(t, mgnH.Backend)

	t.Cleanup(mgnH.Backend.Close)
	mgnH.Backend.InitializeService()

	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	require.True(t, ok, "S3 handler must be registered")

	s3Bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
	require.True(t, ok, "S3 backend must be an InMemoryBackend")

	ctx := t.Context()

	bucketName := "mgn-s3-import-wiring-bucket"
	_, err = s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	key := "inventory/servers.csv"
	csvBody := "mgn:server:hostname,mgn:server:user-provided-id\nweb-1.example.com,mgn-wiring-server\n"
	_, err = s3Bk.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(csvBody)),
	})
	require.NoError(t, err)

	_, err = mgnH.Backend.StartImport(&mgnbackend.S3BucketSource{S3Bucket: bucketName, S3Key: key}, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, describeErr := mgnH.Backend.DescribeSourceServers(mgnbackend.DescribeSourceServersFilters{}, "", 0)

		return describeErr == nil && len(out.Data) == 1
	}, 5*time.Second, 20*time.Millisecond,
		"a server imported via the real cli.go composition root's S3 wiring (wireMGNS3) must "+
			"actually be read from the S3 object and created as a real SourceServer, not fail "+
			"with \"no S3 backend configured\"")
}
