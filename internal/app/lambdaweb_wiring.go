package app

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	lambdawebbackend "github.com/blackbirdworks/gopherstack/services/lambdaweb"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// lambdaWebCode serves web function deployment packages from versioned S3 buckets.
type lambdaWebCode struct {
	backend s3backend.StorageBackend
}

func (c lambdaWebCode) ResolveObject(ctx context.Context, bucket, key, versionID string) (string, error) {
	ver, err := c.backend.GetBucketVersioning(ctx, &awss3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		if errors.Is(err, s3backend.ErrNoSuchBucket) {
			return "", lambdawebbackend.ErrBucketNotFound
		}

		return "", err
	}

	if ver.Status != s3types.BucketVersioningStatusEnabled {
		return "", lambdawebbackend.ErrBucketNotVersioned
	}

	in := &awss3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		in.VersionId = aws.String(versionID)
	}

	head, err := c.backend.HeadObject(ctx, in)
	if err != nil {
		if errors.Is(err, s3backend.ErrNoSuchKey) || errors.Is(err, s3backend.ErrNoSuchBucket) {
			return "", lambdawebbackend.ErrObjectNotFound
		}

		return "", err
	}

	return aws.ToString(head.VersionId), nil
}

func (c lambdaWebCode) GetObject(ctx context.Context, bucket, key, versionID string) ([]byte, error) {
	in := &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if versionID != "" {
		in.VersionId = aws.String(versionID)
	}

	out, err := c.backend.GetObject(ctx, in)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	return io.ReadAll(out.Body)
}

// lambdaWebCredentials adapts the Lambda STS role-credential issuer to lambdaweb.CredentialIssuer.
type lambdaWebCredentials struct {
	inner *lambdaRoleCredentials
}

func (a lambdaWebCredentials) IssueCredentials(roleARN, sessionName string) (lambdawebbackend.Credentials, error) {
	c, err := a.inner.IssueFunctionCredentials(roleARN, sessionName)
	if err != nil {
		return lambdawebbackend.Credentials{}, err
	}

	return lambdawebbackend.Credentials{
		AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken,
	}, nil
}

// wireLambdaWeb gives Lambda Web Functions its S3 code store, CloudWatch Logs sink, execution-role
// credentials, emulator endpoint and the SigV4 secret IamAuth endpoints verify against.
func wireLambdaWeb(cli *CLI, byName map[string]service.Registerable) {
	h, ok := byName["LambdaWeb"].(*lambdawebbackend.Handler)
	if !ok {
		return
	}

	bk, ok := h.Backend.(*lambdawebbackend.InMemoryBackend)
	if !ok {
		return
	}

	if s3H, s3Ok := byName["S3"].(*s3backend.S3Handler); s3Ok {
		bk.SetCodeStore(lambdaWebCode{backend: s3H.Backend})
	}

	if logsH, logsOk := byName["CloudWatchLogs"].(*cwlogsbackend.Handler); logsOk {
		if logsBk, bkOk := logsH.Backend.(*cwlogsbackend.InMemoryBackend); bkOk {
			bk.SetLogSink(codebuildLogs{backend: logsBk})
		}
	}

	bk.SetCredentialIssuer(lambdaWebCredentials{inner: &lambdaRoleCredentials{byName: byName}})

	if cli != nil {
		bk.SetSigV4Secret(cli.SigV4Secret)

		if cli.Port != "" {
			bk.SetEndpointURL(lambdaEndpointURL(cli.Lambda.DockerHost, cli.Port))
		}
	}
}
