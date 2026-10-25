package sagemaker

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	transformLogGroup       = "/aws/sagemaker/TransformJobs"
	splitLine               = "Line"
	splitNone               = "None"
	strategyMulti           = "MultiRecord"
	defaultMaxPayloadMB     = 6
	defaultInvocationsRetry = 3
	defaultInvocationsSecs  = 600
	bytesPerMB              = 1 << 20
	transformVariant        = "AllTraffic"
)

var (
	errTransformFilters = errors.New(
		"DataProcessing InputFilter, OutputFilter and JoinSource are not supported by the container engine",
	)
	errTransformSplit = errors.New("SplitType RecordIO and TFRecord are not supported by the container engine")
)

// startTransformRun launches the container-backed run of a transform job. The caller holds b.mu.
func (b *InMemoryBackend) startTransformRun(eng *engineState, region string, tj *TransformJob, model *Model) {
	ctx, cancel := b.jobContextLocked(region)
	key := engineKey("transform", region, tj.TransformJobName)
	eng.track(key, cancel)

	job := *cloneTransformJob(tj)
	m := *cloneModel(model)

	b.goEngine(func() {
		defer cancel()
		defer eng.untrack(key)

		stream := fmt.Sprintf("%s/%s-%d", job.TransformJobName, trainingHost, time.Now().Unix())
		lt := eng.openLog(ctx, region, transformLogGroup, stream)
		out := b.executeTransform(ctx, eng, region, &job, &m, lt)

		lt.flush()
		b.finishTransform(region, job.TransformJobName, out)
	})
}

func (b *InMemoryBackend) executeTransform(
	ctx context.Context, eng *engineState, region string, tj *TransformJob, model *Model, lt *logTarget,
) jobOutcome {
	fail := func(reason string) jobOutcome {
		lt.logf("%s", reason)

		return jobOutcome{status: statusFailed, failureReason: reason}
	}

	if err := eng.unavailable(); err != nil {
		return fail(err.Error())
	}

	if dp := tj.DataProcessing; dp != nil && (dp.InputFilter != "" || dp.OutputFilter != "" ||
		(dp.JoinSource != "" && dp.JoinSource != splitNone)) {
		return fail("ClientError: " + errTransformFilters.Error())
	}

	if st := tj.TransformInput.SplitType; st != "" && st != splitNone && st != splitLine {
		return fail("ClientError: " + errTransformSplit.Error())
	}

	root, err := eng.newStageDir("transform-" + sanitizeName(tj.TransformJobName))
	if err != nil {
		return fail(err.Error())
	}

	defer os.RemoveAll(root)

	src := tj.TransformInput.DataSource.S3DataSource
	inDir := filepath.Join(root, "in")

	if err = stageS3(ctx, eng.svc().Store, src.S3Uri, src.S3DataType, inDir); err != nil {
		if ctx.Err() != nil {
			return jobOutcome{status: pipelineStatusStopped}
		}

		return fail("ClientError: " + err.Error())
	}

	md, err := eng.startModel(ctx, modelStart{
		model: *model, region: region, label: tj.TransformJobName, variant: transformVariant, env: tj.Environment,
		logGroup: transformLogGroup, logStreamBase: tj.TransformJobName + "/model",
	})
	if err != nil {
		if ctx.Err() != nil {
			return jobOutcome{status: pipelineStatusStopped}
		}

		return fail("ClientError: " + failReason(err))
	}

	defer md.stop()

	results, ferr := transformFiles(ctx, md, inDir, tj, lt)
	if ctx.Err() != nil {
		return jobOutcome{status: pipelineStatusStopped}
	}

	if ferr != nil {
		return fail(ferr.Error())
	}

	store := eng.svc().Store

	for rel, data := range results {
		if err = putS3URI(ctx, store, joinS3(tj.TransformOutput.S3OutputPath, rel+".out"), data); err != nil {
			return fail("ClientError: " + err.Error())
		}
	}

	return jobOutcome{status: algorithmStatusCompleted}
}

// transformFiles sends every staged input file through the model and returns the assembled output per file.
func transformFiles(
	ctx context.Context, md *modelDeployment, inDir string, tj *TransformJob, lt *logTarget,
) (map[string][]byte, error) {
	results := make(map[string][]byte)

	err := filepath.WalkDir(inDir, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil || !d.Type().IsRegular() {
			return werr
		}

		rel, _ := filepath.Rel(inDir, p)

		data, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}

		out, terr := transformFile(ctx, md, data, tj, lt)
		if terr != nil {
			return terr
		}

		results[filepath.ToSlash(rel)] = out

		return nil
	})
	return results, err
}

func transformFile(ctx context.Context, md *modelDeployment, data []byte, tj *TransformJob, lt *logTarget) ([]byte, error) {
	if tj.TransformInput.CompressionType == "Gzip" || isGzip(data) && tj.TransformInput.CompressionType == "" {
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("ClientError: read gzip input: %w", err)
		}

		if data, err = io.ReadAll(io.LimitReader(zr, maxStagedBytes)); err != nil {
			return nil, fmt.Errorf("ClientError: read gzip input: %w", err)
		}
	}

	var out bytes.Buffer

	for _, payload := range splitRecords(data, tj) {
		resp, err := transformInvoke(ctx, md, payload, tj, lt)
		if err != nil {
			return nil, err
		}

		out.Write(resp)

		if tj.TransformOutput.AssembleWith == splitLine && !bytes.HasSuffix(resp, []byte("\n")) {
			out.WriteByte('\n')
		}
	}

	return out.Bytes(), nil
}

// splitRecords cuts an input file into request payloads per SplitType and BatchStrategy.
func splitRecords(data []byte, tj *TransformJob) [][]byte {
	if tj.TransformInput.SplitType != splitLine {
		return [][]byte{data}
	}

	lines := bytes.Split(bytes.TrimRight(data, "\n"), []byte("\n"))
	if tj.BatchStrategy != strategyMulti {
		return lines
	}

	limit := int(tj.MaxPayloadInMB) * bytesPerMB
	if limit <= 0 {
		limit = defaultMaxPayloadMB * bytesPerMB
	}

	var (
		batches [][]byte
		cur     []byte
	)

	for _, l := range lines {
		if len(cur) > 0 && len(cur)+len(l)+1 > limit {
			batches = append(batches, cur)
			cur = nil
		}

		if len(cur) > 0 {
			cur = append(cur, '\n')
		}

		cur = append(cur, l...)
	}

	return append(batches, cur)
}

func transformInvoke(
	ctx context.Context, md *modelDeployment, payload []byte, tj *TransformJob, lt *logTarget,
) ([]byte, error) {
	retries, timeout := int32(defaultInvocationsRetry), time.Duration(defaultInvocationsSecs)*time.Second
	if mc := tj.ModelClientConfig; mc != nil {
		if mc.InvocationsMaxRetries > 0 {
			retries = mc.InvocationsMaxRetries
		}

		if mc.InvocationsTimeoutInSeconds > 0 {
			timeout = time.Duration(mc.InvocationsTimeoutInSeconds) * time.Second
		}
	}

	inv := invocation{
		contentType: tj.TransformInput.ContentType, accept: tj.TransformOutput.Accept, body: payload, timeout: timeout,
	}

	var last error

	for attempt := int32(0); attempt <= retries; attempt++ {
		resp, err := md.invoke(ctx, inv, "")
		if err != nil {
			last = fmt.Errorf("AlgorithmError: %w", err)
			lt.logf("%v", last)

			continue
		}

		body, rerr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
		_ = resp.Body.Close()

		switch {
		case rerr != nil:
			last = fmt.Errorf("AlgorithmError: read model response: %w", rerr)
		case resp.StatusCode/100 == 2: //nolint:mnd // HTTP status class
			return body, nil
		case resp.StatusCode/100 == 4: //nolint:mnd // HTTP status class
			return nil, fmt.Errorf("ClientError: model returned %d: %s", resp.StatusCode, snippet(body))
		default:
			last = fmt.Errorf("AlgorithmError: model returned %d: %s", resp.StatusCode, snippet(body))
		}

		lt.logf("%v", last)

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, last
}

func snippet(b []byte) string {
	if len(b) > errBodySnippet {
		b = b[:errBodySnippet]
	}

	return strings.TrimSpace(string(b))
}

func (b *InMemoryBackend) finishTransform(region, name string, out jobOutcome) {
	b.mu.Lock("finishTransform")
	defer b.mu.Unlock()

	tj, ok := b.transformJobsStore(region).Get(name)
	if !ok || (tj.TransformJobStatus != trainingJobStatusInProgress && tj.TransformJobStatus != pipelineStatusStopping) {
		return
	}

	now := time.Now()
	tj.TransformJobStatus = out.status
	tj.TransformEndTime = &now
	tj.LastModifiedTime = now
	tj.FailureReason = out.failureReason
}
