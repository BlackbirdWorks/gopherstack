package codepipeline

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	actionCategoryDeploy = "Deploy"

	configKeyS3Bucket   = "S3Bucket"
	configKeyS3Key      = "S3ObjectKey"
	configKeyBucketName = "BucketName"
	configKeyObjectKey  = "ObjectKey"
	configKeyExtract    = "Extract"
	configKeyPrimarySrc = "PrimarySource"

	// failureConfigurationError and failureJobFailed are FailureType enum values.
	failureConfigurationError = "ConfigurationError"
	failureJobFailed          = "JobFailed"

	artifactNameMaxLen  = 20
	artifactSuffixLen   = 7
	maxExtractedBytes   = 512 << 20
	artifactContentType = "application/zip"
)

var errArtifactNotZip = errors.New("artifact is not a valid zip archive")

func failActionf(ae *ActionExecution, code, format string, args ...any) {
	ae.Status = statusFailed
	ae.ErrorCode = code
	ae.ErrorMessage = fmt.Sprintf(format, args...)
}

// artifactLocation derives the artifact store bucket/key for an artifact of one execution.
// It is a pure function of its inputs so it survives snapshot restore.
func artifactLocation(p *Pipeline, action Action, execID, artifactName string) (string, string, bool) {
	store := p.Declaration.ArtifactStore

	if s, ok := p.Declaration.ArtifactStores[action.Region]; ok && action.Region != "" {
		store = s
	}

	if store.Location == "" {
		return "", "", false
	}

	name := artifactName
	if len(name) > artifactNameMaxLen {
		name = name[:artifactNameMaxLen]
	}

	sum := sha256.Sum256([]byte(execID + "/" + artifactName))
	suffix := base32.StdEncoding.EncodeToString(sum[:])[:artifactSuffixLen]

	return store.Location, p.Declaration.Name + "/" + name + "/" + suffix, true
}

func (b *InMemoryBackend) putArtifact(
	ctx context.Context, p *Pipeline, action Action, execID, name string, data []byte,
) error {
	bucket, key, ok := artifactLocation(p, action, execID, name)
	if !ok {
		return fmt.Errorf("%w: pipeline has no artifact store location", ErrValidation)
	}

	_, err := b.artifactS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(artifactContentType),
	})
	if err != nil {
		return fmt.Errorf("write artifact %q to s3://%s/%s: %w", name, bucket, key, err)
	}

	return nil
}

func (b *InMemoryBackend) getArtifact(
	ctx context.Context, p *Pipeline, action Action, execID, name string,
) ([]byte, error) {
	bucket, key, ok := artifactLocation(p, action, execID, name)
	if !ok {
		return nil, fmt.Errorf("%w: pipeline has no artifact store location", ErrValidation)
	}

	data, _, err := b.readObject(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("read artifact %q: %w", name, err)
	}

	return data, nil
}

func (b *InMemoryBackend) readObject(ctx context.Context, bucket, key string) ([]byte, *s3.GetObjectOutput, error) {
	out, err := b.artifactS3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, nil, err
	}

	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, err
	}

	return data, out, nil
}

// runS3SourceAction reads the configured object and stages it as the action's output artifacts.
func (b *InMemoryBackend) runS3SourceAction(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	bucket, key := action.Configuration[configKeyS3Bucket], action.Configuration[configKeyS3Key]
	if bucket == "" || key == "" {
		return
	}

	data, out, err := b.readObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			failActionf(ae, failureConfigurationError,
				"The object with key '%s' in bucket '%s' does not exist", key, bucket)
		} else {
			failActionf(ae, failureJobFailed, "Unable to read s3://%s/%s: %v", bucket, key, err)
		}

		return
	}

	for _, art := range action.OutputArtifacts {
		if err = b.putArtifact(ctx, p, action, execID, art.Name, data); err != nil {
			failActionf(ae, failureJobFailed, "%v", err)

			return
		}
	}

	ae.OutputVariables = map[string]string{"BucketName": bucket, "ObjectKey": key}

	if out.ETag != nil {
		ae.OutputVariables["ETag"] = strings.Trim(aws.ToString(out.ETag), `"`)
	}

	if out.VersionId != nil {
		ae.OutputVariables["VersionId"] = aws.ToString(out.VersionId)
	}
}

// runS3DeployAction writes the input artifact to the target bucket, extracting it when Extract=true.
func (b *InMemoryBackend) runS3DeployAction(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	bucket := action.Configuration[configKeyBucketName]
	if bucket == "" {
		return
	}

	if len(action.InputArtifacts) == 0 {
		failActionf(ae, failureConfigurationError, "Deploy action %q has no input artifact", action.Name)

		return
	}

	data, err := b.getArtifact(ctx, p, action, execID, action.InputArtifacts[0].Name)
	if err != nil {
		failActionf(ae, failureJobFailed, "%v", err)

		return
	}

	prefix := action.Configuration[configKeyObjectKey]

	switch {
	case strings.EqualFold(action.Configuration[configKeyExtract], "true"):
		err = b.extractTo(ctx, bucket, prefix, data)
	case prefix == "":
		failActionf(ae, failureConfigurationError, "ObjectKey is required when Extract is false")

		return
	default:
		err = b.putObject(ctx, bucket, prefix, data)
	}

	if err != nil {
		failActionf(ae, failureJobFailed, "Deploy to s3://%s failed: %v", bucket, err)
	}
}

func (b *InMemoryBackend) putObject(ctx context.Context, bucket, key string, data []byte) error {
	_, err := b.artifactS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})

	return err
}

func (b *InMemoryBackend) extractTo(ctx context.Context, bucket, prefix string, data []byte) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return errArtifactNotZip
	}

	prefix = strings.Trim(prefix, "/")
	remaining := int64(maxExtractedBytes)

	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}

		if !fs.ValidPath(f.Name) {
			return fmt.Errorf("%w: unsafe entry name %q", ErrValidation, f.Name)
		}

		rc, openErr := f.Open()
		if openErr != nil {
			return openErr
		}

		body, readErr := io.ReadAll(io.LimitReader(rc, remaining+1))
		_ = rc.Close()

		if readErr != nil {
			return readErr
		}

		if int64(len(body)) > remaining {
			return fmt.Errorf("%w: extracted artifact exceeds %d bytes", ErrValidation, maxExtractedBytes)
		}

		remaining -= int64(len(body))

		key := f.Name
		if prefix != "" {
			key = prefix + "/" + key
		}

		if err = b.putObject(ctx, bucket, key, body); err != nil {
			return err
		}
	}

	return nil
}

// primaryInputArtifact returns the name of the action's primary input artifact, or "".
func primaryInputArtifact(action Action) string {
	if want := action.Configuration[configKeyPrimarySrc]; want != "" {
		return want
	}

	if len(action.InputArtifacts) > 0 {
		return action.InputArtifacts[0].Name
	}

	return ""
}

// passThroughArtifacts stages the primary input artifact under each output artifact name.
// The emulated build does not execute a buildspec, so its output is its input.
func (b *InMemoryBackend) passThroughArtifacts(
	ctx context.Context, p *Pipeline, execID string, action Action,
) error {
	in := primaryInputArtifact(action)
	if in == "" || len(action.OutputArtifacts) == 0 || b.artifactS3 == nil {
		return nil
	}

	data, err := b.getArtifact(ctx, p, action, execID, in)
	if errors.Is(err, awserr.ErrNotFound) {
		return nil
	}

	if err != nil {
		return err
	}

	for _, out := range action.OutputArtifacts {
		if err = b.putArtifact(ctx, p, action, execID, out.Name, data); err != nil {
			return err
		}
	}

	return nil
}
