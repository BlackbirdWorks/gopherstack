package glacier

// This file writes a completed Select job's real S3 output-location objects,
// replacing gopherstack's earlier GetJobOutput-only delivery (see select.go's
// package doc). Real S3 Glacier Select never serves Select results via
// GetJobOutput -- GetJobOutput's own documented response shapes cover only
// archive content and vault inventory (aws-sdk-go-v2/service/glacier doc
// comment on GetJobOutput; api-job-output-get.md in
// awsdocs/amazon-glacier-developer-guide, "the output will be either the
// content of an archive or a vault inventory" -- Select is absent). Instead,
// per glacier-select.md's "S3 Glacier Select Output" section (same repo),
// results land under OutputLocation.S3.Prefix/<jobID>/:
//
//	<prefix>/<jobID>/job.txt              -- written once, a static
//	                                          Describe-Job-shaped snapshot
//	<prefix>/<jobID>/results/<part>       -- the query result (gopherstack
//	                                          always emits exactly one part)
//	<prefix>/<jobID>/result_manifest.txt  -- lists the result part key(s)
//	<prefix>/<jobID>/errors/<part>        -- on query execution failure
//	<prefix>/<jobID>/error_manifest.txt   -- lists the error part key(s)
//
// The exact manifest file schema is not publicly documented beyond "allows
// programmatic retrieval" of the listed parts; gopherstack emits a small JSON
// object carrying that property rather than guessing at an undocumented
// format.

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// S3Accessor is the subset of S3 operations a Select job needs to write its real
// OutputLocation output. Satisfied by the in-process S3 backend, wired in cli.go's
// wireGlacierS3 alongside the other cross-service S3 write-back integrations
// (DynamoDB, MGN, SageMaker).
type S3Accessor interface {
	PutObject(ctx context.Context, in *s3sdk.PutObjectInput) (*s3sdk.PutObjectOutput, error)
}

// SetS3Backend wires the S3 backend used to deliver completed Select jobs' real
// OutputLocation output.
func (b *InMemoryBackend) SetS3Backend(s3 S3Accessor) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.s3 = s3
}

// s3Backend returns the wired S3 accessor, or nil when none is configured.
func (b *InMemoryBackend) s3Backend() S3Accessor {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.s3
}

// materializeSelectOutput writes a completed Select job's real S3 output exactly
// once, if an S3 backend is wired and the job's archive data is still available.
// Safe to call for any job/action/state -- it is a fast no-op unless the job is a
// completed, not-yet-written Select job. Best-effort: a PutObject failure (e.g. the
// OutputLocation bucket does not exist in the wired S3 backend) is logged and
// swallowed rather than surfaced, since GetJobOutput's real archive/inventory
// delivery paths are unaffected either way and this is supplementary delivery, not
// the source of truth for job success/failure.
func (b *InMemoryBackend) materializeSelectOutput(ctx context.Context, accountID, region, vaultName, jobID string) {
	s3 := b.s3Backend()
	if s3 == nil {
		return
	}

	vArn := vaultARN(accountID, region, vaultName)

	b.mu.Lock()

	j, ok := b.jobs.Get(jobKey(vArn, jobID))
	if !ok || j.Action != jobTypeSelect || !j.Completed || j.SelectOutputWritten ||
		j.OutputLocation == nil || j.OutputLocation.S3 == nil {
		b.mu.Unlock()

		return
	}

	j.SelectOutputWritten = true
	archiveData, hasData := b.archiveData[j.ArchiveID]
	snapshot := toDescribeJobResponse(cloneJob(j))
	ol := j.OutputLocation
	sp := j.SelectParameters

	b.mu.Unlock()

	if !hasData {
		return
	}

	writeSelectS3Output(ctx, s3, ol, jobID, snapshot, archiveData, sp)
}

// selectOutputBaseKey returns the S3 key prefix under which a Select job's
// OutputLocation objects are written: the caller-supplied
// OutputLocation.S3.Prefix, followed by the job's own unique prefix (its job ID),
// per glacier-select.md's "S3 Glacier Select creates a unique prefix referring to
// the job ID" under the supplied prefix.
func selectOutputBaseKey(ol *outputLocationDTO, jobID string) string {
	prefix := ol.S3.Prefix
	if prefix != "" && !hasTrailingSlash(prefix) {
		prefix += "/"
	}

	return prefix + jobID + "/"
}

// hasTrailingSlash reports whether s ends in "/".
func hasTrailingSlash(s string) bool {
	return len(s) > 0 && s[len(s)-1] == '/'
}

// writeSelectS3Output writes job.txt plus either results/result_manifest.txt (on
// success) or errors/error_manifest.txt (on query execution failure) to the
// job's OutputLocation bucket.
func writeSelectS3Output(
	ctx context.Context,
	s3 S3Accessor,
	ol *outputLocationDTO,
	jobID string,
	jobSnapshot describeJobResponse,
	archiveData []byte,
	sp *selectParametersDTO,
) {
	bucket := ol.S3.BucketName
	base := selectOutputBaseKey(ol, jobID)

	putSelectOutputObject(ctx, s3, bucket, base+"job.txt", marshalSelectOutputJSON(jobSnapshot))

	result, err := executeSelect(archiveData, sp)
	if err != nil {
		putSelectOutputObject(ctx, s3, bucket, base+"errors/1", []byte(err.Error()))
		putSelectOutputObject(ctx, s3, bucket, base+"error_manifest.txt",
			marshalSelectOutputJSON(map[string]any{"errors": []string{base + "errors/1"}}))

		return
	}

	putSelectOutputObject(ctx, s3, bucket, base+"results/1", result)
	putSelectOutputObject(ctx, s3, bucket, base+"result_manifest.txt",
		marshalSelectOutputJSON(map[string]any{"results": []string{base + "results/1"}}))
}

// marshalSelectOutputJSON marshals v to JSON, falling back to an empty object on
// the (unreachable in practice, since callers pass only structs/maps of strings)
// marshal error -- a malformed manifest object is still safer than a panic.
func marshalSelectOutputJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}

	return data
}

// putSelectOutputObject writes a single Select output object, logging (rather than
// propagating) any failure -- see materializeSelectOutput's doc comment.
func putSelectOutputObject(ctx context.Context, s3 S3Accessor, bucket, key string, data []byte) {
	_, err := s3.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "glacier: select job S3 output-location write failed",
			"bucket", bucket, "key", key, "error", err)
	}
}
