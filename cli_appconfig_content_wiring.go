package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appconfigbackend "github.com/blackbirdworks/gopherstack/services/appconfig"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

var errUnsupportedConfigLocation = errors.New("unsupported configuration profile location")

// wireAppConfigContent serves non-hosted configuration profiles from SSM, S3 and Secrets Manager.
func wireAppConfigContent(appconfigReg, ssmReg, s3Reg, smReg service.Registerable) {
	acH, ok := appconfigReg.(*appconfigbackend.Handler)
	if !ok {
		return
	}

	r := &appConfigContentReader{}

	if ssmH, ssmOk := ssmReg.(*ssmbackend.Handler); ssmOk {
		r.ssm, _ = ssmH.Backend.(*ssmbackend.InMemoryBackend)
	}

	if s3H, s3Ok := s3Reg.(*s3backend.S3Handler); s3Ok {
		r.s3, _ = s3H.Backend.(*s3backend.InMemoryBackend)
	}

	if smH, smOk := smReg.(*secretsmanagerbackend.Handler); smOk {
		r.sm, _ = smH.Backend.(*secretsmanagerbackend.InMemoryBackend)
	}

	acH.SetContentReaderResolver(func(region string) appconfigbackend.ConfigurationContentReader {
		return &appConfigRegionReader{reader: r, region: region}
	})
}

type appConfigContentReader struct {
	ssm *ssmbackend.InMemoryBackend
	s3  *s3backend.InMemoryBackend
	sm  *secretsmanagerbackend.InMemoryBackend
}

type appConfigRegionReader struct {
	reader *appConfigContentReader
	region string
}

func (a *appConfigRegionReader) ReadConfiguration(
	ctx context.Context, locationURI, _, version string,
) ([]byte, string, error) {
	scheme, ref, ok := strings.Cut(locationURI, "://")
	if !ok || ref == "" {
		return nil, "", fmt.Errorf("%w: %q", errUnsupportedConfigLocation, locationURI)
	}

	r := a.reader

	switch {
	case scheme == "ssm-parameter" && r.ssm != nil:
		return r.readSSMParameter(ssmbackend.WithRegion(ctx, a.region), ref, version)
	case scheme == "ssm-document" && r.ssm != nil:
		return r.readSSMDocument(ssmbackend.WithRegion(ctx, a.region), ref, version)
	case scheme == "s3" && r.s3 != nil:
		return r.readS3Object(ctx, ref, version)
	case scheme == svcSecretsManager && r.sm != nil:
		return r.readSecret(secretsmanagerbackend.WithRegion(ctx, a.region), ref, version)
	default:
		return nil, "", fmt.Errorf("%w: %q", errUnsupportedConfigLocation, locationURI)
	}
}

func arnResource(ref, marker string) string {
	if !strings.HasPrefix(ref, "arn:") {
		return ref
	}

	_, after, ok := strings.Cut(ref, marker)
	if !ok {
		return ref
	}

	return after
}

func (r *appConfigContentReader) readSSMParameter(ctx context.Context, ref, version string) ([]byte, string, error) {
	name := arnResource(ref, ":parameter")
	if version != "" {
		name += ":" + version
	}

	out, err := r.ssm.GetParameter(ctx, &ssmbackend.GetParameterInput{Name: name, WithDecryption: true})
	if err != nil {
		return nil, "", err
	}

	return []byte(out.Parameter.Value), "", nil
}

func (r *appConfigContentReader) readSSMDocument(ctx context.Context, ref, version string) ([]byte, string, error) {
	out, err := r.ssm.GetDocument(ctx, &ssmbackend.GetDocumentInput{
		Name: arnResource(ref, ":document/"), DocumentVersion: version,
	})
	if err != nil {
		return nil, "", err
	}

	contentType := ""

	switch out.DocumentFormat {
	case "JSON":
		contentType = "application/json"
	case "YAML":
		contentType = "application/x-yaml"
	case "TEXT":
		contentType = "text/plain"
	}

	return []byte(out.Content), contentType, nil
}

func (r *appConfigContentReader) readS3Object(ctx context.Context, ref, version string) ([]byte, string, error) {
	bucket, key, ok := strings.Cut(ref, "/")
	if !ok || bucket == "" || key == "" {
		return nil, "", fmt.Errorf("%w: s3://%s", errUnsupportedConfigLocation, ref)
	}

	in := &s3sdk.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if version != "" {
		in.VersionId = aws.String(version)
	}

	out, err := r.s3.GetObject(ctx, in)
	if err != nil {
		return nil, "", err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}

	return data, aws.ToString(out.ContentType), nil
}

func (r *appConfigContentReader) readSecret(ctx context.Context, ref, version string) ([]byte, string, error) {
	out, err := r.sm.GetSecretValue(ctx, &secretsmanagerbackend.GetSecretValueInput{SecretID: ref, VersionID: version})
	if err != nil {
		return nil, "", err
	}

	if out.SecretString != "" {
		return []byte(out.SecretString), "", nil
	}

	return out.SecretBinary, "", nil
}
