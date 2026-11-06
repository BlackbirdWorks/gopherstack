package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codecommitbackend "github.com/blackbirdworks/gopherstack/services/codecommit"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// wireCodeBuildEngine gives docker-engine CodeBuild access to S3, CloudWatch Logs, CodeCommit, SSM and Secrets Manager.
func wireCodeBuildEngine(byName map[string]service.Registerable) {
	cbH, ok := byName["CodeBuild"].(*codebuildbackend.Handler)
	if !ok || !cbH.Backend.EngineEnabled() {
		return
	}

	var svc codebuildbackend.EngineServices

	if s3H, s3Ok := byName["S3"].(*s3backend.S3Handler); s3Ok {
		svc.Store = codebuildS3{backend: s3H.Backend}
	}

	if h, hOk := byName["CloudWatchLogs"].(*cwlogsbackend.Handler); hOk {
		if bk, bkOk := h.Backend.(*cwlogsbackend.InMemoryBackend); bkOk {
			svc.Logs = codebuildLogs{backend: bk}
		}
	}

	if h, hOk := byName["CodeCommit"].(*codecommitbackend.Handler); hOk {
		svc.Repos = codebuildRepos{handler: h}
	}

	ssmH, _ := byName["SSM"].(*ssmbackend.Handler)
	smH, _ := byName["SecretsManager"].(*secretsmanagerbackend.Handler)

	if ssmH != nil || smH != nil {
		svc.Values = codebuildValues{ssm: ssmH, sm: smH}
	}

	cbH.SetEngineServices(svc)
}

// codebuildCredentials issues service-role credentials from STS.
type codebuildCredentials struct {
	byName map[string]service.Registerable
}

func (c *codebuildCredentials) IssueBuildCredentials(
	_ context.Context, roleArn, sessionName string,
) (codebuildbackend.BuildCredentials, error) {
	stsH, ok := c.byName["STS"].(*stsbackend.Handler)
	if !ok {
		return codebuildbackend.BuildCredentials{}, errCodeBuildBackendMissing
	}

	stsBk, ok := stsH.Backend.(*stsbackend.InMemoryBackend)
	if !ok {
		return codebuildbackend.BuildCredentials{}, errCodeBuildBackendMissing
	}

	out, err := stsBk.AssumeRoleForService(codebuildbackend.ServicePrincipal(), roleArn, sessionName)
	if err != nil {
		return codebuildbackend.BuildCredentials{}, err
	}

	creds := out.AssumeRoleResult.Credentials

	exp, err := time.Parse(time.RFC3339, creds.Expiration)
	if err != nil {
		return codebuildbackend.BuildCredentials{}, err
	}

	return codebuildbackend.BuildCredentials{
		AccessKeyID: creds.AccessKeyID, SecretAccessKey: creds.SecretAccessKey,
		SessionToken: creds.SessionToken, Expires: exp,
	}, nil
}

// wireCodeBuildRuntimeEnv gives docker-engine builds the emulator endpoint and service-role credentials.
func wireCodeBuildRuntimeEnv(cli *CLI, byName map[string]service.Registerable) {
	cbH, ok := byName["CodeBuild"].(*codebuildbackend.Handler)
	if !ok || !cbH.Backend.EngineEnabled() || cli == nil || cli.Port == "" {
		return
	}

	cbH.Backend.SetEngineRuntime(
		lambdaEndpointURL(cli.Lambda.DockerHost, cli.Port), &codebuildCredentials{byName: byName},
	)
}

var errCodeBuildBackendMissing = errors.New("backend is not available")

type codebuildS3 struct{ backend s3backend.StorageBackend }

func (s codebuildS3) GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := s.backend.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	return io.ReadAll(out.Body)
}

func (s codebuildS3) PutObjectBytes(ctx context.Context, bucket, key string, data []byte) error {
	_, err := s.backend.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})

	return err
}

func (s codebuildS3) ListObjectKeys(ctx context.Context, bucket, prefix string) ([]string, error) {
	var (
		keys  []string
		token *string
	)

	for {
		out, err := s.backend.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, nil
		}

		token = out.NextContinuationToken
	}
}

type codebuildLogs struct {
	backend *cwlogsbackend.InMemoryBackend
}

func (l codebuildLogs) EnsureLogGroupAndStream(_ context.Context, region, group, stream string) error {
	return (&cwLogsAdapter{backend: l.backend, region: region}).EnsureLogGroupAndStream(group, stream)
}

func (l codebuildLogs) PutLogLines(_ context.Context, region, group, stream string, lines []string) error {
	return (&cwLogsAdapter{backend: l.backend, region: region}).PutLogLines(group, stream, lines)
}

type codebuildRepos struct{ handler *codecommitbackend.Handler }

func (r codebuildRepos) RepoFiles(_ context.Context, region, repo, version string) (map[string][]byte, error) {
	files, err := r.handler.RegionHandler(region).Backend.GetFolderFiles(repo, version, "")
	if err != nil {
		return nil, err
	}

	out := make(map[string][]byte, len(files))
	for _, f := range files {
		out[f.FilePath] = f.FileContent
	}

	return out, nil
}

type codebuildValues struct {
	ssm *ssmbackend.Handler
	sm  *secretsmanagerbackend.Handler
}

func (v codebuildValues) Parameter(ctx context.Context, region, name string) (string, error) {
	if v.ssm == nil {
		return "", errCodeBuildBackendMissing
	}

	out, err := v.ssm.Backend.GetParameter(ssmbackend.WithRegion(ctx, region), &ssmbackend.GetParameterInput{
		Name: name, WithDecryption: true,
	})
	if err != nil {
		return "", err
	}

	return out.Parameter.Value, nil
}

func (v codebuildValues) SecretValue(ctx context.Context, region, id, stage, versionID string) (string, error) {
	if v.sm == nil {
		return "", errCodeBuildBackendMissing
	}

	out, err := v.sm.Backend.GetSecretValue(secretsmanagerbackend.WithRegion(ctx, region),
		&secretsmanagerbackend.GetSecretValueInput{SecretID: id, VersionStage: stage, VersionID: versionID})
	if err != nil {
		return "", err
	}

	return out.SecretString, nil
}
