package sagemaker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	trainingLogGroup   = "/aws/sagemaker/TrainingJobs"
	trainingHost       = "algo-1"
	failureFileMaxSize = 1024
	secondaryFailed    = "Failed"
	secondaryMaxRun    = "MaxRuntimeExceeded"
	statusFailed       = "Failed"
	inputModeFile      = "File"
)

var (
	errChannelSource   = errors.New("only S3 data sources are supported")
	errNoTrainingImage = errors.New(
		"no training image: set AlgorithmSpecification.TrainingImage or a known AlgorithmName",
	)
	errBadOutputPath    = errors.New("OutputDataConfig.S3OutputPath is not a valid S3 URI")
	errAlgorithmNoImage = errors.New("algorithm has no TrainingSpecification.TrainingImage")
)

type jobOutcome struct {
	artifacts     *ModelArtifacts
	status        string
	secondary     string
	message       string
	failureReason string
}

// startTrainingRun launches the container-backed run of a freshly created training job. The caller holds b.mu.
func (b *InMemoryBackend) startTrainingRun(eng *engineState, region string, tj *TrainingJob) {
	ctx, cancel := b.jobContextLocked(region)
	key := engineKey("training", region, tj.TrainingJobName)
	eng.track(key, cancel)

	job := *cloneTrainingJob(tj)

	b.goEngine(func() {
		defer cancel()
		defer eng.untrack(key)

		stream := fmt.Sprintf("%s/%s-%d", job.TrainingJobName, trainingHost, time.Now().Unix())
		lt := eng.openLog(ctx, region, trainingLogGroup, stream)
		out := b.executeTraining(ctx, eng, region, &job, lt)

		lt.flush()
		b.finishTraining(region, job.TrainingJobName, out)
	})
}

func (b *InMemoryBackend) setTrainingStage(region, name, status, msg string) {
	b.mu.Lock("setTrainingStage")
	defer b.mu.Unlock()

	tj, ok := b.trainingJobsStore(region).Get(name)
	if !ok || tj.TrainingJobStatus != trainingJobStatusInProgress {
		return
	}

	advanceTrainingSecondary(tj, SecondaryStatusTransition{Status: status, StatusMessage: msg}, time.Now())
}

func (b *InMemoryBackend) trainingImage(region string, spec AlgorithmSpecification) (string, error) {
	if spec.TrainingImage != "" {
		return spec.TrainingImage, nil
	}

	if spec.AlgorithmName == "" {
		return "", errNoTrainingImage
	}

	b.mu.RLock("trainingImage")
	defer b.mu.RUnlock()

	alg, ok := b.algorithmsStoreRO(region).Get(spec.AlgorithmName)
	if !ok {
		return "", fmt.Errorf("%w: algorithm %s not found", errNoTrainingImage, spec.AlgorithmName)
	}

	var ts struct {
		TrainingImage string `json:"TrainingImage"`
	}

	if err := json.Unmarshal(alg.TrainingSpecification, &ts); err != nil || ts.TrainingImage == "" {
		return "", fmt.Errorf("%w: %s", errAlgorithmNoImage, spec.AlgorithmName)
	}

	return ts.TrainingImage, nil
}

func (b *InMemoryBackend) executeTraining(
	ctx context.Context, eng *engineState, region string, tj *TrainingJob, lt *logTarget,
) jobOutcome {
	fail := func(reason string) jobOutcome {
		lt.logf("%s", reason)

		return jobOutcome{status: statusFailed, secondary: secondaryFailed, message: reason, failureReason: reason}
	}

	if err := eng.unavailable(); err != nil {
		return fail(err.Error())
	}

	image, err := b.trainingImage(region, tj.AlgorithmSpecification)
	if err != nil {
		return fail("ClientError: " + err.Error())
	}

	root, err := eng.newStageDir("training-" + sanitizeName(tj.TrainingJobName))
	if err != nil {
		return fail(err.Error())
	}

	defer os.RemoveAll(root)

	b.setTrainingStage(region, tj.TrainingJobName, "Downloading", "Downloading input data")

	if err = stageTraining(ctx, eng, root, tj); err != nil {
		if ctx.Err() != nil {
			return stoppedOutcome()
		}

		return fail("ClientError: " + err.Error())
	}

	b.setTrainingStage(
		region,
		tj.TrainingJobName,
		"Training",
		"Training image download completed. Training in progress.",
	)

	runCtx, cancel := withMaxRuntime(ctx, tj.StoppingCondition.MaxRuntimeInSeconds)
	defer cancel()

	code, err := eng.runToExit(runCtx, trainingSpec(root, image, tj), lt)

	switch {
	case ctx.Err() != nil:
		return stoppedOutcome()
	case errors.Is(runCtx.Err(), context.DeadlineExceeded):
		return jobOutcome{
			status: pipelineStatusStopped, secondary: secondaryMaxRun,
			message: "Training job stopped: maximum runtime exceeded",
		}
	case err != nil:
		return fail("AlgorithmError: " + err.Error())
	case code != 0:
		return fail(algorithmFailure(root, code))
	}

	b.setTrainingStage(region, tj.TrainingJobName, "Uploading", "Uploading generated training model")

	artifacts, err := uploadTrainingOutput(ctx, eng, root, tj)
	if err != nil {
		return fail("ClientError: " + err.Error())
	}

	return jobOutcome{
		status: algorithmStatusCompleted, secondary: algorithmStatusCompleted,
		message: "Training job completed", artifacts: artifacts,
	}
}

func stoppedOutcome() jobOutcome {
	return jobOutcome{
		status: pipelineStatusStopped, secondary: pipelineStatusStopped, message: "Training job is stopped",
	}
}

func withMaxRuntime(ctx context.Context, seconds int32) (context.Context, context.CancelFunc) {
	if seconds <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
}

func stageTraining(ctx context.Context, eng *engineState, root string, tj *TrainingJob) error {
	for _, d := range []string{"input/config", "input/data", "model", "output/data"} {
		if err := mkSharedDir(filepath.Join(root, filepath.FromSlash(d))); err != nil {
			return err
		}
	}

	store := eng.svc().Store
	inputCfg := make(map[string]map[string]string, len(tj.InputDataConfig))

	for _, ch := range tj.InputDataConfig {
		src := ch.DataSource.S3DataSource
		if src == nil {
			return fmt.Errorf("channel %s: %w", ch.ChannelName, errChannelSource)
		}

		dir := filepath.Join(root, "input", "data", ch.ChannelName)
		if err := stageS3(ctx, store, src.S3Uri, src.S3DataType, dir); err != nil {
			return fmt.Errorf("channel %s: %w", ch.ChannelName, err)
		}

		inputCfg[ch.ChannelName] = channelConfig(ch, src)
	}

	cfgDir := filepath.Join(root, "input", "config")
	hyper := tj.HyperParameters

	if hyper == nil {
		hyper = map[string]string{}
	}

	res := newResourceConfig(tj.ResourceConfig.InstanceType)

	for name, v := range map[string]any{
		"hyperparameters.json": hyper, "resourceconfig.json": res, "inputdataconfig.json": inputCfg,
	} {
		if err := writeJSONFile(cfgDir, name, v); err != nil {
			return err
		}
	}

	return nil
}

func channelConfig(ch Channel, src *S3DataSource) map[string]string {
	cfg := map[string]string{
		"TrainingInputMode": inputModeFile, "S3DistributionType": "FullyReplicated", "RecordWrapperType": "None",
	}

	if src.S3DataDistributionType != "" {
		cfg["S3DistributionType"] = src.S3DataDistributionType
	}

	if ch.RecordWrapperType != "" {
		cfg["RecordWrapperType"] = ch.RecordWrapperType
	}

	if ch.ContentType != "" {
		cfg["ContentType"] = ch.ContentType
	}

	return cfg
}

func trainingSpec(root, image string, tj *TrainingJob) container.Spec {
	env := envList(tj.Environment, map[string]string{
		"TRAINING_JOB_NAME": tj.TrainingJobName, "TRAINING_JOB_ARN": tj.TrainingJobArn,
	})

	spec := container.Spec{
		Image: image,
		Name:  "gopherstack-sagemaker-train-" + sanitizeName(tj.TrainingJobName),
		Env:   env,
		Mounts: []string{
			filepath.Join(root, "input") + ":/opt/ml/input",
			filepath.Join(root, "model") + ":/opt/ml/model",
			filepath.Join(root, "output") + ":/opt/ml/output",
		},
		Cmd: []string{"train"},
	}

	as := tj.AlgorithmSpecification
	if len(as.ContainerEntrypoint) > 0 {
		spec.Entrypoint = as.ContainerEntrypoint
		spec.Cmd = as.ContainerArguments
	} else if len(as.ContainerArguments) > 0 {
		spec.Cmd = as.ContainerArguments
	}

	return spec
}

func algorithmFailure(root string, code int) string {
	data, _ := os.ReadFile(filepath.Join(root, "output", "failure"))
	msg := strings.TrimSpace(string(data))

	if len(msg) > failureFileMaxSize {
		msg = msg[:failureFileMaxSize]
	}

	if msg == "" {
		msg = "See job logs for more information"
	}

	return fmt.Sprintf("AlgorithmError: %s, exit code: %d", msg, code)
}

func uploadTrainingOutput(
	ctx context.Context,
	eng *engineState,
	root string,
	tj *TrainingJob,
) (*ModelArtifacts, error) {
	base := strings.TrimSuffix(tj.OutputDataConfig.S3OutputPath, "/")
	if _, _, err := parseS3URI(base); err != nil {
		return nil, fmt.Errorf("%w: %w", errBadOutputPath, err)
	}

	store := eng.svc().Store
	prefix := base + "/" + tj.TrainingJobName + "/output"

	model, err := tarGzDir(filepath.Join(root, "model"))
	if err != nil {
		return nil, err
	}

	if err = putS3URI(ctx, store, prefix+"/model.tar.gz", model); err != nil {
		return nil, err
	}

	data, err := tarGzDir(filepath.Join(root, "output", "data"))
	if err != nil {
		return nil, err
	}

	if err = putS3URI(ctx, store, prefix+"/output.tar.gz", data); err != nil {
		return nil, err
	}

	return &ModelArtifacts{S3ModelArtifacts: prefix + "/model.tar.gz"}, nil
}

func (b *InMemoryBackend) finishTraining(region, name string, out jobOutcome) {
	b.mu.Lock("finishTraining")
	defer b.mu.Unlock()

	tj, ok := b.trainingJobsStore(region).Get(name)
	if !ok || (tj.TrainingJobStatus != trainingJobStatusInProgress && tj.TrainingJobStatus != pipelineStatusStopping) {
		return
	}

	now := time.Now()
	advanceTrainingSecondary(tj, SecondaryStatusTransition{Status: out.secondary, StatusMessage: out.message}, now)
	tj.SecondaryStatusTransitions[len(tj.SecondaryStatusTransitions)-1].EndTime = &now
	tj.TrainingJobStatus = out.status
	tj.TrainingEndTime = &now
	tj.FailureReason = out.failureReason

	secs := max(int32(now.Sub(tj.CreationTime).Seconds()), 1)
	tj.BillableTimeInSeconds = secs
	tj.TrainingTimeInSeconds = secs

	if out.artifacts != nil {
		tj.ModelArtifacts = out.artifacts
	}
}

// resourceConfig is /opt/ml/input/config/resourceconfig.json for a single-instance job.
type resourceConfig struct {
	CurrentHost         string          `json:"current_host"`
	NetworkInterface    string          `json:"network_interface_name"`
	CurrentInstanceType string          `json:"current_instance_type"`
	CurrentGroupName    string          `json:"current_group_name,omitempty"`
	Hosts               []string        `json:"hosts"`
	InstanceGroups      []instanceGroup `json:"instance_groups,omitempty"`
}

type instanceGroup struct {
	Name         string   `json:"instance_group_name"`
	InstanceType string   `json:"instance_type"`
	Hosts        []string `json:"hosts"`
}

func newResourceConfig(instanceType string) resourceConfig {
	hosts := []string{trainingHost}

	return resourceConfig{
		CurrentHost: trainingHost, Hosts: hosts, NetworkInterface: "eth0", CurrentInstanceType: instanceType,
		CurrentGroupName: "homogeneousCluster",
		InstanceGroups:   []instanceGroup{{Name: "homogeneousCluster", InstanceType: instanceType, Hosts: hosts}},
	}
}
