package sagemaker_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func untar(t *testing.T, data []byte) map[string]string {
	t.Helper()

	zr, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)

	out := map[string]string{}
	tr := tar.NewReader(zr)

	for {
		hdr, nerr := tr.Next()
		if nerr == io.EOF {
			return out
		}

		require.NoError(t, nerr)

		b, rerr := io.ReadAll(tr)
		require.NoError(t, rerr)

		out[hdr.Name] = string(b)
	}
}

func waitTraining(t *testing.T, b *sagemaker.InMemoryBackend, name string) *sagemaker.TrainingJob {
	t.Helper()

	var tj *sagemaker.TrainingJob

	require.Eventually(t, func() bool {
		var err error

		tj, err = b.DescribeTrainingJob(t.Context(), name)

		return err == nil && tj.TrainingJobStatus != "InProgress" && tj.TrainingJobStatus != "Stopping"
	}, 10*time.Second, 5*time.Millisecond)

	return tj
}

func trainingOptions(name string) sagemaker.TrainingJobOptions {
	return sagemaker.TrainingJobOptions{
		TrainingJobName:        name,
		RoleArn:                "arn:aws:iam::000000000000:role/r",
		AlgorithmSpecification: sagemaker.AlgorithmSpecification{TrainingImage: "train-img", TrainingInputMode: "File"},
		HyperParameters:        map[string]string{"epochs": "3"},
		Environment:            map[string]string{"MODE": "test"},
		InputDataConfig: []sagemaker.Channel{{
			ChannelName: "train", ContentType: "text/csv",
			DataSource: sagemaker.ChannelDataSource{S3DataSource: &sagemaker.S3DataSource{S3Uri: "s3://data/train/", S3DataType: "S3Prefix"}},
		}},
		OutputDataConfig: sagemaker.OutputDataConfig{S3OutputPath: "s3://out/models"},
		ResourceConfig:   sagemaker.ResourceConfig{InstanceType: "ml.m5.large", InstanceCount: 1, VolumeSizeInGB: 1},
	}
}

func TestEngineTraining_Outcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		job        func(spec container.Spec, stop <-chan struct{}) int
		maxRuntime int32
		wantStatus string
		wantSecond string
		wantReason string
		wantModel  bool
	}{
		{
			name: "completes_and_uploads_model",
			job: func(spec container.Spec, _ <-chan struct{}) int {
				in := mountSource(spec, "/opt/ml/input")
				data := readFileOrEmpty(filepath.Join(in, "data", "train", "a.csv"))
				hp := readFileOrEmpty(filepath.Join(in, "config", "hyperparameters.json"))
				_ = os.WriteFile(filepath.Join(mountSource(spec, "/opt/ml/model"), "model.txt"), []byte("trained:"+data+hp), 0o644)
				_ = os.WriteFile(filepath.Join(mountSource(spec, "/opt/ml/output"), "data", "metrics.txt"), []byte("ok"), 0o644)

				return 0
			},
			wantStatus: "Completed", wantSecond: "Completed", wantModel: true,
		},
		{
			name: "failure_file_becomes_failure_reason",
			job: func(spec container.Spec, _ <-chan struct{}) int {
				_ = os.WriteFile(filepath.Join(mountSource(spec, "/opt/ml/output"), "failure"), []byte("bad hyperparameter"), 0o644)

				return 2
			},
			wantStatus: "Failed", wantSecond: "Failed", wantReason: "AlgorithmError: bad hyperparameter, exit code: 2",
		},
		{
			name: "max_runtime_exceeded",
			job: func(_ container.Spec, stop <-chan struct{}) int {
				<-stop

				return 137
			},
			maxRuntime: 1, wantStatus: "Stopped", wantSecond: "MaxRuntimeExceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newEngineRig(t, sagemaker.EngineConfig{})
			rig.rt.job = tt.job
			rig.rt.logs["train-img"] = "epoch 1\nepoch 2\n"
			requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "data", "train/a.csv", []byte("1,2")))

			opts := trainingOptions("job1")
			opts.StoppingCondition.MaxRuntimeInSeconds = tt.maxRuntime

			_, err := rig.b.CreateTrainingJobFull(t.Context(), opts)
			require.NoError(t, err)

			tj := waitTraining(t, rig.b, "job1")
			assert.Equal(t, tt.wantStatus, tj.TrainingJobStatus, tj.FailureReason)
			assert.Equal(t, tt.wantSecond, tj.SecondaryStatus)
			assert.Equal(t, tt.wantReason, tj.FailureReason)

			var seen []string
			for _, tr := range tj.SecondaryStatusTransitions {
				seen = append(seen, tr.Status)
			}

			assert.Contains(t, seen, "Downloading")
			assert.Contains(t, seen, "Training")

			spec, ok := rig.rt.lastSpec("train-img")
			require.True(t, ok)
			assert.Equal(t, []string{"train"}, spec.Cmd)
			assert.Equal(t, "test", envValue(spec, "MODE"))
			assert.Equal(t, "job1", envValue(spec, "TRAINING_JOB_NAME"))

			model, found := rig.s3.get("out", "models/job1/output/model.tar.gz")
			assert.Equal(t, tt.wantModel, found)

			if tt.wantModel {
				files := untar(t, model)
				assert.Equal(t, `trained:1,2{"epochs":"3"}`, files["model.txt"])
				assert.Equal(t, "s3://out/models/job1/output/model.tar.gz", tj.ModelArtifacts.S3ModelArtifacts)

				out, outFound := rig.s3.get("out", "models/job1/output/output.tar.gz")
				require.True(t, outFound)
				assert.Equal(t, "ok", untar(t, out)["metrics.txt"])
			}

			require.Eventually(t, func() bool {
				return strings.Contains(strings.Join(rig.logs.groupLines("/aws/sagemaker/TrainingJobs"), "\n"), "epoch 2")
			}, 5*time.Second, 5*time.Millisecond)
		})
	}
}

func TestEngineTraining_ConfigFilesAndStop(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	running := make(chan container.Spec, 1)
	rig.rt.job = func(spec container.Spec, stop <-chan struct{}) int {
		running <- spec

		<-stop

		return 137
	}
	requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "data", "train/a.csv", []byte("1,2")))

	_, err := rig.b.CreateTrainingJobFull(t.Context(), trainingOptions("job1"))
	require.NoError(t, err)

	spec := <-running
	cfg := filepath.Join(mountSource(spec, "/opt/ml/input"), "config")

	var res map[string]any
	require.NoError(t, json.Unmarshal([]byte(readFileOrEmpty(filepath.Join(cfg, "resourceconfig.json"))), &res))
	assert.Equal(t, "algo-1", res["current_host"])

	var chans map[string]map[string]string
	require.NoError(t, json.Unmarshal([]byte(readFileOrEmpty(filepath.Join(cfg, "inputdataconfig.json"))), &chans))
	assert.Equal(t, "text/csv", chans["train"]["ContentType"])
	assert.Equal(t, "File", chans["train"]["TrainingInputMode"])

	require.NoError(t, rig.b.StopTrainingJobFSM(t.Context(), "job1"))

	tj := waitTraining(t, rig.b, "job1")
	assert.Equal(t, "Stopped", tj.TrainingJobStatus)
	assert.Equal(t, "Stopped", tj.SecondaryStatus)
}

func TestEngineTraining_InputErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(*sagemaker.TrainingJobOptions)
		cfg        sagemaker.EngineConfig
		wantReason string
	}{
		{
			name: "missing_input_object",
			mutate: func(o *sagemaker.TrainingJobOptions) {
				o.InputDataConfig[0].DataSource.S3DataSource.S3Uri = "s3://data/none/x.csv"
			},
			wantReason: "ClientError",
		},
		{
			name:       "no_image",
			mutate:     func(o *sagemaker.TrainingJobOptions) { o.AlgorithmSpecification.TrainingImage = "" },
			wantReason: "no training image",
		},
		{
			name:       "no_runtime",
			cfg:        sagemaker.EngineConfig{Reason: "docker not installed"},
			wantReason: "docker not installed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newEngineRig(t, tt.cfg)
			rig.rt.job = func(container.Spec, <-chan struct{}) int { return 0 }
			requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "data", "train/a.csv", []byte("1")))

			opts := trainingOptions("job1")
			if tt.mutate != nil {
				tt.mutate(&opts)
			}

			_, err := rig.b.CreateTrainingJobFull(t.Context(), opts)
			require.NoError(t, err)

			tj := waitTraining(t, rig.b, "job1")
			assert.Equal(t, "Failed", tj.TrainingJobStatus)
			assert.Contains(t, tj.FailureReason, tt.wantReason)
		})
	}
}

func waitProcessing(t *testing.T, b *sagemaker.InMemoryBackend, name string) *sagemaker.ProcessingJob {
	t.Helper()

	var pj *sagemaker.ProcessingJob

	require.Eventually(t, func() bool {
		var err error

		pj, err = b.DescribeProcessingJob(t.Context(), name)

		return err == nil && pj.ProcessingJobStatus != "InProgress" && pj.ProcessingJobStatus != "Stopping"
	}, 10*time.Second, 5*time.Millisecond)

	return pj
}

func processingJob(name string) sagemaker.ProcessingJob {
	return sagemaker.ProcessingJob{
		ProcessingJobName: name,
		RoleArn:           "arn:aws:iam::000000000000:role/r",
		AppSpecification: sagemaker.ProcessingAppSpec{
			ImageURI: "proc-img", ContainerEntrypoint: []string{"python3", "/code/run.py"}, ContainerArguments: []string{"--mode", "x"},
		},
		Environment: map[string]string{"A": "b"},
		ProcessingInputs: []sagemaker.ProcessingInput{{
			InputName: "raw", S3Input: &sagemaker.ProcessingS3Input{S3Uri: "s3://data/raw/", LocalPath: "/opt/ml/processing/input"},
		}},
		ProcessingOutputConfig: sagemaker.ProcessingOutputConfig{Outputs: []sagemaker.ProcessingOutput{{
			OutputName: "clean", S3Output: &sagemaker.ProcessingS3Output{S3Uri: "s3://data/clean", LocalPath: "/opt/ml/processing/output"},
		}}},
		ProcessingResources: sagemaker.ProcessingResources{ClusterConfig: sagemaker.ProcessingClusterConfig{
			InstanceType: "ml.m5.large", InstanceCount: 1, VolumeSizeInGB: 1,
		}},
	}
}

func TestEngineProcessing_Outcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		job        func(spec container.Spec, stop <-chan struct{}) int
		mutate     func(*sagemaker.ProcessingJob)
		wantStatus string
		wantReason string
		wantOutput bool
	}{
		{
			name: "inputs_in_outputs_out",
			job: func(spec container.Spec, _ <-chan struct{}) int {
				in := readFileOrEmpty(filepath.Join(mountSource(spec, "/opt/ml/processing/input"), "in.txt"))
				_ = os.WriteFile(filepath.Join(mountSource(spec, "/opt/ml/processing/output"), "out.txt"), []byte(strings.ToUpper(in)), 0o644)

				return 0
			},
			wantStatus: "Completed", wantOutput: true,
		},
		{
			name:       "non_zero_exit_fails",
			job:        func(container.Spec, <-chan struct{}) int { return 3 },
			wantStatus: "Failed", wantReason: "exit code: 3",
		},
		{
			name:       "dataset_definition_unsupported",
			job:        func(container.Spec, <-chan struct{}) int { return 0 },
			mutate:     func(p *sagemaker.ProcessingJob) { p.ProcessingInputs[0].S3Input = nil },
			wantStatus: "Failed", wantReason: "only S3Input",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newEngineRig(t, sagemaker.EngineConfig{})
			cfgSeen := make(chan string, 1)
			rig.rt.job = func(spec container.Spec, stop <-chan struct{}) int {
				cfgSeen <- readFileOrEmpty(filepath.Join(mountSource(spec, "/opt/ml/config"), "processingjobconfig.json"))

				return tt.job(spec, stop)
			}
			requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "data", "raw/in.txt", []byte("hello")))

			job := processingJob("pj1")
			if tt.mutate != nil {
				tt.mutate(&job)
			}

			_, err := rig.b.CreateProcessingJob(t.Context(), job)
			require.NoError(t, err)

			pj := waitProcessing(t, rig.b, "pj1")
			assert.Equal(t, tt.wantStatus, pj.ProcessingJobStatus, pj.FailureReason)
			assert.Contains(t, pj.FailureReason, tt.wantReason)

			out, found := rig.s3.get("data", "clean/out.txt")
			assert.Equal(t, tt.wantOutput, found)

			if tt.wantOutput {
				assert.Equal(t, "HELLO", string(out))

				spec, ok := rig.rt.lastSpec("proc-img")
				require.True(t, ok)
				assert.Equal(t, []string{"python3", "/code/run.py"}, spec.Entrypoint)
				assert.Equal(t, []string{"--mode", "x"}, spec.Cmd)
				assert.Equal(t, "b", envValue(spec, "A"))
				assert.Contains(t, <-cfgSeen, "pj1")
			}
		})
	}
}

func TestEngineProcessing_Stop(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	started := make(chan struct{})
	rig.rt.job = func(_ container.Spec, stop <-chan struct{}) int {
		close(started)
		<-stop

		return 137
	}
	requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "data", "raw/in.txt", []byte("hello")))

	_, err := rig.b.CreateProcessingJob(t.Context(), processingJob("pj1"))
	require.NoError(t, err)

	<-started
	require.NoError(t, rig.b.StopProcessingJob(t.Context(), "pj1"))
	assert.Equal(t, "Stopped", waitProcessing(t, rig.b, "pj1").ProcessingJobStatus)
}

// upperServer answers /invocations with the upper-cased request and counts the requests.
type upperServer struct {
	bodies []string
	mu     sync.Mutex
}

func (u *upperServer) handler(container.Spec) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/invocations", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)

		u.mu.Lock()
		u.bodies = append(u.bodies, string(b))
		u.mu.Unlock()

		if strings.Contains(string(b), "bad") {
			http.Error(w, "bad record", http.StatusBadRequest)

			return
		}

		_, _ = w.Write([]byte(strings.ToUpper(string(b))))
	})

	return mux
}

func waitTransform(t *testing.T, b *sagemaker.InMemoryBackend, name string) *sagemaker.TransformJob {
	t.Helper()

	var tj *sagemaker.TransformJob

	require.Eventually(t, func() bool {
		var err error

		tj, err = b.DescribeTransformJob(t.Context(), name)

		return err == nil && tj.TransformJobStatus != "InProgress" && tj.TransformJobStatus != "Stopping"
	}, 10*time.Second, 5*time.Millisecond)

	return tj
}

func TestEngineTransform_Outcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      map[string]string
		mutate     func(*sagemaker.TransformJobOptions)
		wantStatus string
		wantReason string
		wantOut    map[string]string
		wantReqs   int
	}{
		{
			name:       "whole_file",
			input:      map[string]string{"in/a.csv": "r1\nr2\n"},
			wantStatus: "Completed", wantReqs: 1,
			wantOut: map[string]string{"out/a.csv.out": "R1\nR2\n"},
		},
		{
			name:  "line_single_record_assemble_line",
			input: map[string]string{"in/a.csv": "r1\nr2\n", "in/b.csv": "r3\n"},
			mutate: func(o *sagemaker.TransformJobOptions) {
				o.TransformInput.SplitType = "Line"
				o.BatchStrategy = "SingleRecord"
				o.TransformOutput.AssembleWith = "Line"
			},
			wantStatus: "Completed", wantReqs: 3,
			wantOut: map[string]string{"out/a.csv.out": "R1\nR2\n", "out/b.csv.out": "R3\n"},
		},
		{
			name:  "line_multi_record_batches",
			input: map[string]string{"in/a.csv": "r1\nr2\nr3\n"},
			mutate: func(o *sagemaker.TransformJobOptions) {
				o.TransformInput.SplitType = "Line"
				o.BatchStrategy = "MultiRecord"
				o.TransformOutput.AssembleWith = "Line"
			},
			wantStatus: "Completed", wantReqs: 1,
			wantOut: map[string]string{"out/a.csv.out": "R1\nR2\nR3\n"},
		},
		{
			name:  "client_error_fails_job",
			input: map[string]string{"in/a.csv": "ok\nbad\n"},
			mutate: func(o *sagemaker.TransformJobOptions) {
				o.TransformInput.SplitType = "Line"
				o.BatchStrategy = "SingleRecord"
			},
			wantStatus: "Failed", wantReason: "ClientError: model returned 400", wantReqs: 2,
		},
		{
			name:  "filters_unsupported",
			input: map[string]string{"in/a.csv": "r1\n"},
			mutate: func(o *sagemaker.TransformJobOptions) {
				o.DataProcessing = &sagemaker.TransformDataProcessing{InputFilter: "$[1:]"}
			},
			wantStatus: "Failed", wantReason: "not supported",
		},
		{
			name:  "recordio_unsupported",
			input: map[string]string{"in/a.csv": "r1\n"},
			mutate: func(o *sagemaker.TransformJobOptions) {
				o.TransformInput.SplitType = "RecordIO"
			},
			wantStatus: "Failed", wantReason: "RecordIO",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newEngineRig(t, sagemaker.EngineConfig{})
			srv := &upperServer{}
			rig.rt.serve = srv.handler
			rig.rt.logs["tx-img"] = "model up\n"

			for k, v := range tt.input {
				requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "data", k, []byte(v)))
			}

			_, err := rig.b.CreateModel(t.Context(), "m1", "arn:aws:iam::000000000000:role/r", &sagemaker.ContainerDefinition{Image: "tx-img"}, nil, nil)
			require.NoError(t, err)

			opts := sagemaker.TransformJobOptions{
				TransformJobName: "tj1", ModelName: "m1",
				TransformInput: sagemaker.TransformInput{
					ContentType: "text/csv",
					DataSource:  sagemaker.TransformDataSource{S3DataSource: sagemaker.TransformS3DataSource{S3Uri: "s3://data/in/", S3DataType: "S3Prefix"}},
				},
				TransformOutput:    sagemaker.TransformOutput{S3OutputPath: "s3://data/out", Accept: "text/csv"},
				TransformResources: sagemaker.TransformResources{InstanceType: "ml.m5.large", InstanceCount: 1},
			}
			if tt.mutate != nil {
				tt.mutate(&opts)
			}

			_, err = rig.b.CreateTransformJob(t.Context(), opts)
			require.NoError(t, err)

			tj := waitTransform(t, rig.b, "tj1")
			assert.Equal(t, tt.wantStatus, tj.TransformJobStatus, tj.FailureReason)
			assert.Contains(t, tj.FailureReason, tt.wantReason)

			for key, want := range tt.wantOut {
				got, ok := rig.s3.get("data", key)
				require.True(t, ok, key)
				assert.Equal(t, want, string(got))
			}

			srv.mu.Lock()
			assert.Len(t, srv.bodies, tt.wantReqs)
			srv.mu.Unlock()

			if tt.wantStatus == "Completed" {
				require.Eventually(t, func() bool {
					return strings.Contains(strings.Join(rig.logs.groupLines("/aws/sagemaker/TransformJobs"), "\n"), "model up")
				}, 5*time.Second, 5*time.Millisecond)
			}
		})
	}
}

func TestEngineTransform_StopInterruptsJob(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	gate := make(chan struct{})
	reached := make(chan struct{}, 1)
	rig.rt.serve = func(container.Spec) http.Handler {
		mux := http.NewServeMux()
		mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		mux.HandleFunc("/invocations", func(_ http.ResponseWriter, r *http.Request) {
			reached <- struct{}{}

			select {
			case <-gate:
			case <-r.Context().Done():
			}
		})

		return mux
	}
	t.Cleanup(func() { close(gate) })

	requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "data", "in/a.csv", []byte("r1\n")))

	_, err := rig.b.CreateModel(context.Background(), "m1", "arn:aws:iam::000000000000:role/r", &sagemaker.ContainerDefinition{Image: "tx-img"}, nil, nil)
	require.NoError(t, err)

	_, err = rig.b.CreateTransformJob(t.Context(), sagemaker.TransformJobOptions{
		TransformJobName: "tj1", ModelName: "m1",
		TransformInput: sagemaker.TransformInput{
			DataSource: sagemaker.TransformDataSource{S3DataSource: sagemaker.TransformS3DataSource{S3Uri: "s3://data/in/"}},
		},
		TransformOutput: sagemaker.TransformOutput{S3OutputPath: "s3://data/out"},
	})
	require.NoError(t, err)

	<-reached
	require.NoError(t, rig.b.StopTransformJob(t.Context(), "tj1"))
	assert.Equal(t, "Stopped", waitTransform(t, rig.b, "tj1").TransformJobStatus)
}
