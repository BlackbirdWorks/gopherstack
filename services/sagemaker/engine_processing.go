package sagemaker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const processingLogGroup = "/aws/sagemaker/ProcessingJobs"

var (
	errProcessingInput  = errors.New("only S3Input processing inputs are supported")
	errProcessingOutput = errors.New("only S3Output processing outputs are supported")
	errNoProcessingImg  = errors.New("AppSpecification.ImageUri is required")
)

// startProcessingRun launches the container-backed run of a processing job. The caller holds b.mu.
func (b *InMemoryBackend) startProcessingRun(eng *engineState, region string, pj *ProcessingJob) {
	ctx, cancel := b.jobContextLocked(region)
	key := engineKey("processing", region, pj.ProcessingJobName)
	eng.track(key, cancel)

	job := *cloneProcessingJob(pj)
	if pj.StoppingCondition != nil {
		sc := *pj.StoppingCondition
		job.StoppingCondition = &sc
	}

	b.goEngine(func() {
		defer cancel()
		defer eng.untrack(key)

		stream := fmt.Sprintf("%s/%s-%d", job.ProcessingJobName, trainingHost, time.Now().Unix())
		lt := eng.openLog(ctx, region, processingLogGroup, stream)
		out := b.executeProcessing(ctx, eng, &job, lt)

		lt.flush()
		b.finishProcessing(region, job.ProcessingJobName, out)
	})
}

func (b *InMemoryBackend) executeProcessing(
	ctx context.Context, eng *engineState, pj *ProcessingJob, lt *logTarget,
) jobOutcome {
	fail := func(reason string) jobOutcome {
		lt.logf("%s", reason)

		return jobOutcome{status: statusFailed, failureReason: reason}
	}

	if err := eng.unavailable(); err != nil {
		return fail(err.Error())
	}

	if pj.AppSpecification.ImageURI == "" {
		return fail("ClientError: " + errNoProcessingImg.Error())
	}

	root, err := eng.newStageDir("processing-" + sanitizeName(pj.ProcessingJobName))
	if err != nil {
		return fail(err.Error())
	}

	defer os.RemoveAll(root)

	mounts, outputs, err := stageProcessing(ctx, eng, root, pj)
	if err != nil {
		if ctx.Err() != nil {
			return jobOutcome{status: pipelineStatusStopped}
		}

		return fail("ClientError: " + err.Error())
	}

	runCtx := ctx
	cancel := context.CancelFunc(func() {})

	if sc := pj.StoppingCondition; sc != nil {
		runCtx, cancel = withMaxRuntime(ctx, sc.MaxRuntimeInSeconds)
	}

	defer cancel()

	code, err := eng.runToExit(runCtx, processingSpec(pj, mounts), lt)

	switch {
	case ctx.Err() != nil, errors.Is(runCtx.Err(), context.DeadlineExceeded):
		return jobOutcome{status: pipelineStatusStopped}
	case err != nil:
		return fail("AlgorithmError: " + err.Error())
	case code != 0:
		return fail(fmt.Sprintf("AlgorithmError: See job logs for more information, exit code: %d", code))
	}

	for uri, dir := range outputs {
		if err = uploadDir(ctx, eng.svc().Store, dir, uri); err != nil {
			return fail("ClientError: " + err.Error())
		}
	}

	return jobOutcome{status: algorithmStatusCompleted}
}

// stageProcessing downloads inputs and prepares output directories; it returns the container bind
// mounts and a map of output S3 URI to the host directory holding its files.
func stageProcessing(
	ctx context.Context, eng *engineState, root string, pj *ProcessingJob,
) ([]string, map[string]string, error) {
	var mounts []string

	store := eng.svc().Store

	for _, in := range pj.ProcessingInputs {
		if in.S3Input == nil {
			return nil, nil, fmt.Errorf("input %s: %w", in.InputName, errProcessingInput)
		}

		dir := filepath.Join(root, "in", sanitizeName(in.InputName))
		if err := stageS3(ctx, store, in.S3Input.S3Uri, in.S3Input.S3DataType, dir); err != nil {
			return nil, nil, fmt.Errorf("input %s: %w", in.InputName, err)
		}

		mounts = append(mounts, dir+":"+in.S3Input.LocalPath)
	}

	outputs := make(map[string]string, len(pj.ProcessingOutputConfig.Outputs))

	for _, out := range pj.ProcessingOutputConfig.Outputs {
		if out.S3Output == nil {
			return nil, nil, fmt.Errorf("output %s: %w", out.OutputName, errProcessingOutput)
		}

		dir := filepath.Join(root, "out", sanitizeName(out.OutputName))
		if err := mkSharedDir(dir); err != nil {
			return nil, nil, err
		}

		outputs[out.S3Output.S3Uri] = dir
		mounts = append(mounts, dir+":"+out.S3Output.LocalPath)
	}

	cfgDir := filepath.Join(root, "config")
	if err := writeProcessingConfig(cfgDir, pj); err != nil {
		return nil, nil, err
	}

	return append(mounts, cfgDir+":/opt/ml/config"), outputs, nil
}

// processingJobConfig is /opt/ml/config/processingjobconfig.json.
type processingJobConfig struct {
	StoppingCondition      *ProcessingStoppingCondition `json:"StoppingCondition"`
	Environment            map[string]string            `json:"Environment"`
	AppSpecification       ProcessingAppSpec            `json:"AppSpecification"`
	ProcessingOutputConfig ProcessingOutputConfig       `json:"ProcessingOutputConfig"`
	ProcessingResources    ProcessingResources          `json:"ProcessingResources"`
	ProcessingJobArn       string                       `json:"ProcessingJobArn"`
	ProcessingJobName      string                       `json:"ProcessingJobName"`
	RoleArn                string                       `json:"RoleArn"`
	ProcessingInputs       []ProcessingInput            `json:"ProcessingInputs"`
}

func writeProcessingConfig(dir string, pj *ProcessingJob) error {
	job := processingJobConfig{
		ProcessingJobArn: pj.ProcessingJobArn, ProcessingJobName: pj.ProcessingJobName,
		AppSpecification: pj.AppSpecification, Environment: pj.Environment, ProcessingInputs: pj.ProcessingInputs,
		ProcessingOutputConfig: pj.ProcessingOutputConfig, ProcessingResources: pj.ProcessingResources,
		RoleArn: pj.RoleArn, StoppingCondition: pj.StoppingCondition,
	}

	if err := writeJSONFile(dir, "processingjobconfig.json", job); err != nil {
		return err
	}

	return writeJSONFile(
		dir,
		"resourceconfig.json",
		newResourceConfig(pj.ProcessingResources.ClusterConfig.InstanceType),
	)
}

func processingSpec(pj *ProcessingJob, mounts []string) container.Spec {
	return container.Spec{
		Image:      pj.AppSpecification.ImageURI,
		Name:       "gopherstack-sagemaker-proc-" + sanitizeName(pj.ProcessingJobName),
		Env:        envList(pj.Environment),
		Mounts:     mounts,
		Entrypoint: pj.AppSpecification.ContainerEntrypoint,
		Cmd:        pj.AppSpecification.ContainerArguments,
	}
}

func (b *InMemoryBackend) finishProcessing(region, name string, out jobOutcome) {
	b.mu.Lock("finishProcessing")
	defer b.mu.Unlock()

	pj, ok := b.processingJobsStore(region).Get(name)
	if !ok ||
		(pj.ProcessingJobStatus != trainingJobStatusInProgress && pj.ProcessingJobStatus != pipelineStatusStopping) {
		return
	}

	now := time.Now()
	pj.ProcessingJobStatus = out.status
	pj.ProcessingEndTime = &now
	pj.LastModifiedTime = now
	pj.FailureReason = out.failureReason
}
