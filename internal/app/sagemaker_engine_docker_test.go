package app

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	smrtypes "github.com/aws/aws-sdk-go-v2/service/sagemakerruntime/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
)

const (
	smDockerOptIn = "GOPHERSTACK_TEST_DOCKER"
	smBaseImage   = "python:3.12-slim"
	smRole        = "arn:aws:iam::000000000000:role/sm"
	smBucket      = "sm-e2e"
)

const smAppPy = `import http.server, json, os, sys, glob

mode = sys.argv[1] if len(sys.argv) > 1 else "process"

if mode == "serve":
    factor = open("/opt/ml/model/factor.txt").read().strip()

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            print("served", self.path, flush=True)

        def do_GET(self):
            self.send_response(200 if self.path == "/ping" else 404)
            self.end_headers()

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
            if "explode" in body:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"model exploded")
                return
            out = (body.upper() + "*" + factor).encode()
            self.send_response(200)
            self.send_header("Content-Type", self.headers.get("Accept") or "text/plain")
            attrs = self.headers.get("X-Amzn-SageMaker-Custom-Attributes", "")
            self.send_header("X-Amzn-SageMaker-Custom-Attributes", attrs)
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)

    http.server.ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()

elif mode == "train":
    hp = json.load(open("/opt/ml/input/config/hyperparameters.json"))
    print("training with", hp, flush=True)
    if hp.get("fail") == "true":
        open("/opt/ml/output/failure", "w").write("deliberate failure")
        sys.exit(1)
    total = 0
    for f in glob.glob("/opt/ml/input/data/train/*"):
        total += sum(int(x) for x in open(f).read().split())
    open("/opt/ml/model/model.txt", "w").write("sum=%d epochs=%s" % (total, hp["epochs"]))
    os.makedirs("/opt/ml/output/data", exist_ok=True)
    open("/opt/ml/output/data/metrics.txt", "w").write("done")

else:
    print("processing with env", os.environ.get("GREETING"), flush=True)
    for f in glob.glob("/opt/ml/processing/input/*"):
        data = open(f).read().upper()
        open("/opt/ml/processing/output/" + os.path.basename(f), "w").write(data)
`

func smBuildImage(t *testing.T) string {
	t.Helper()

	if os.Getenv(smDockerOptIn) != "1" {
		t.Skipf("set %s=1 to run against a real container runtime", smDockerOptIn)
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	inspect := exec.CommandContext(t.Context(), "docker", "image", "inspect", smBaseImage)
	if out, err := inspect.CombinedOutput(); err != nil {
		pull := exec.CommandContext(t.Context(), "docker", "pull", "-q", smBaseImage)
		if pullOut, perr := pull.CombinedOutput(); perr != nil {
			t.Skipf("cannot get %s: %v %s %s", smBaseImage, perr, out, pullOut)
		}
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "app.py"), []byte(smAppPy), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Dockerfile"),
		[]byte("FROM "+smBaseImage+"\nCOPY app.py /app.py\nENTRYPOINT [\"python3\", \"/app.py\"]\n"), 0o644))

	tag := fmt.Sprintf("gopherstack-sm-e2e:%d", os.Getpid())
	build := exec.CommandContext(t.Context(), "docker", "build", "-q", "-t", tag, dir)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("docker build: %v\n%s", err, out)
	}

	t.Cleanup(func() {
		_ = exec.CommandContext(context.Background(), "docker", "rmi", "-f", tag).Run()
	})

	return tag
}

type smClients struct {
	sm   *sagemaker.Client
	rt   *sagemakerruntime.Client
	s3   *s3.Client
	logs *cloudwatchlogs.Client
}

func smStack(t *testing.T) smClients {
	t.Helper()

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1", SageMakerEngine: "docker"}
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if h, ok := serviceByName(services)["SageMaker"].(*sagemakerbackend.Handler); ok {
			h.Shutdown(context.Background())
		}
	})

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	addr := srv.Listener.Addr().String()
	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(srv.URL),
		awscfg.WithHTTPClient(&http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer

				return d.DialContext(ctx, network, addr)
			},
		}}),
	)
	require.NoError(t, err)

	return smClients{
		sm: sagemaker.NewFromConfig(cfg), rt: sagemakerruntime.NewFromConfig(cfg),
		s3:   s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }),
		logs: cloudwatchlogs.NewFromConfig(cfg),
	}
}

func smTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)

	for name, body := range files {
		require.NoError(
			t,
			tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}),
		)
		_, err := tw.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func (c smClients) put(t *testing.T, key string, body []byte) {
	t.Helper()

	_, err := c.s3.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(smBucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	require.NoError(t, err)
}

func (c smClients) get(t *testing.T, key string) []byte {
	t.Helper()

	out, err := c.s3.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(smBucket), Key: aws.String(key)})
	require.NoError(t, err)

	defer out.Body.Close()

	b, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return b
}

func (c smClients) endpointStatus(t *testing.T, name string) *sagemaker.DescribeEndpointOutput {
	t.Helper()

	var out *sagemaker.DescribeEndpointOutput

	require.Eventually(t, func() bool {
		var err error

		out, err = c.sm.DescribeEndpoint(t.Context(), &sagemaker.DescribeEndpointInput{EndpointName: aws.String(name)})

		return err == nil && out.EndpointStatus != smtypes.EndpointStatusCreating &&
			out.EndpointStatus != smtypes.EndpointStatusUpdating
	}, 3*time.Minute, 250*time.Millisecond)

	return out
}

func (c smClients) createEndpoint(t *testing.T, name, model string, async *smtypes.AsyncInferenceConfig) {
	t.Helper()

	_, err := c.sm.CreateEndpointConfig(t.Context(), &sagemaker.CreateEndpointConfigInput{
		EndpointConfigName:   aws.String(name + "-cfg"),
		AsyncInferenceConfig: async,
		ProductionVariants: []smtypes.ProductionVariant{{
			VariantName: aws.String("AllTraffic"), ModelName: aws.String(model),
			InstanceType: smtypes.ProductionVariantInstanceTypeMlM5Large, InitialInstanceCount: aws.Int32(1),
		}},
	})
	require.NoError(t, err)

	_, err = c.sm.CreateEndpoint(t.Context(), &sagemaker.CreateEndpointInput{
		EndpointName: aws.String(name), EndpointConfigName: aws.String(name + "-cfg"),
	})
	require.NoError(t, err)
}

func TestSageMakerEngine_Docker_EndToEnd(t *testing.T) {
	t.Parallel()

	image := smBuildImage(t)
	c := smStack(t)
	ctx := t.Context()

	_, bucketErr := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(smBucket)})
	require.NoError(t, bucketErr)

	c.put(t, "model/model.tar.gz", smTarGz(t, map[string]string{"factor.txt": "7"}))
	c.put(t, "train/data.txt", []byte("1 2 3\n4 5"))
	c.put(t, "raw/a.txt", []byte("hello processing"))
	c.put(t, "batch/in/a.csv", []byte("r1\nr2\n"))

	_, modelErr := c.sm.CreateModel(ctx, &sagemaker.CreateModelInput{
		ModelName: aws.String("m1"), ExecutionRoleArn: aws.String(smRole),
		PrimaryContainer: &smtypes.ContainerDefinition{
			Image: aws.String(image), ModelDataUrl: aws.String("s3://" + smBucket + "/model/model.tar.gz"),
		},
	})
	require.NoError(t, modelErr)

	t.Run("endpoint", func(t *testing.T) {
		t.Parallel()

		c.createEndpoint(t, "ep1", "m1", nil)
		assert.Equal(t, smtypes.EndpointStatusInService, c.endpointStatus(t, "ep1").EndpointStatus)

		out, err := c.rt.InvokeEndpoint(ctx, &sagemakerruntime.InvokeEndpointInput{
			EndpointName: aws.String("ep1"), Body: []byte("abc"), ContentType: aws.String("text/plain"),
			Accept: aws.String("text/csv"), CustomAttributes: aws.String("k=v"),
		})
		require.NoError(t, err)
		assert.Equal(t, "ABC*7", string(out.Body))
		assert.Equal(t, "text/csv", aws.ToString(out.ContentType))
		assert.Equal(t, "k=v", aws.ToString(out.CustomAttributes))
		assert.Equal(t, "AllTraffic", aws.ToString(out.InvokedProductionVariant))

		_, err = c.rt.InvokeEndpoint(ctx, &sagemakerruntime.InvokeEndpointInput{
			EndpointName: aws.String("ep1"), Body: []byte("explode"),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ModelError")

		stream, err := c.rt.InvokeEndpointWithResponseStream(
			ctx,
			&sagemakerruntime.InvokeEndpointWithResponseStreamInput{
				EndpointName: aws.String("ep1"), Body: []byte("xyz"),
			},
		)
		require.NoError(t, err)

		var got strings.Builder

		for ev := range stream.GetStream().Events() {
			if part, ok := ev.(*smrtypes.ResponseStreamMemberPayloadPart); ok {
				got.Write(part.Value.Bytes)
			}
		}

		require.NoError(t, stream.GetStream().Close())
		assert.Equal(t, "XYZ*7", got.String())

		require.Eventually(t, func() bool {
			res, lerr := c.logs.FilterLogEvents(ctx, &cloudwatchlogs.FilterLogEventsInput{
				LogGroupName: aws.String("/aws/sagemaker/Endpoints/ep1"),
			})

			return lerr == nil && len(res.Events) > 0
		}, 20*time.Second, 250*time.Millisecond, "container logs reach /aws/sagemaker/Endpoints/ep1")

		_, err = c.sm.DeleteEndpoint(ctx, &sagemaker.DeleteEndpointInput{EndpointName: aws.String("ep1")})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			_, derr := c.sm.DescribeEndpoint(ctx, &sagemaker.DescribeEndpointInput{EndpointName: aws.String("ep1")})

			return derr != nil
		}, 60*time.Second, 250*time.Millisecond)
	})

	t.Run("async_endpoint", func(t *testing.T) {
		t.Parallel()

		c.createEndpoint(t, "ep2", "m1", &smtypes.AsyncInferenceConfig{
			OutputConfig: &smtypes.AsyncInferenceOutputConfig{
				S3OutputPath: aws.String("s3://" + smBucket + "/async-out"),
			},
		})
		require.Equal(t, smtypes.EndpointStatusInService, c.endpointStatus(t, "ep2").EndpointStatus)

		out, err := c.rt.InvokeEndpointAsync(ctx, &sagemakerruntime.InvokeEndpointAsyncInput{
			EndpointName: aws.String("ep2"), InputLocation: aws.String("s3://" + smBucket + "/raw/a.txt"),
			InferenceId: aws.String("inf-1"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s3://"+smBucket+"/async-out/inf-1.out", aws.ToString(out.OutputLocation))

		require.Eventually(t, func() bool {
			_, gerr := c.s3.GetObject(
				ctx,
				&s3.GetObjectInput{Bucket: aws.String(smBucket), Key: aws.String("async-out/inf-1.out")},
			)

			return gerr == nil
		}, 30*time.Second, 250*time.Millisecond)
		assert.Equal(t, "HELLO PROCESSING*7", string(c.get(t, "async-out/inf-1.out")))
	})

	t.Run("training", func(t *testing.T) {
		t.Parallel()

		create := func(name, fail string) {
			_, cerr := c.sm.CreateTrainingJob(ctx, &sagemaker.CreateTrainingJobInput{
				TrainingJobName: aws.String(name), RoleArn: aws.String(smRole),
				AlgorithmSpecification: &smtypes.AlgorithmSpecification{
					TrainingImage: aws.String(image), TrainingInputMode: smtypes.TrainingInputModeFile,
				},
				HyperParameters: map[string]string{"epochs": "4", "fail": fail},
				InputDataConfig: []smtypes.Channel{{
					ChannelName: aws.String("train"),
					DataSource: &smtypes.DataSource{S3DataSource: &smtypes.S3DataSource{
						S3Uri: aws.String("s3://" + smBucket + "/train/"), S3DataType: smtypes.S3DataTypeS3Prefix,
					}},
				}},
				OutputDataConfig: &smtypes.OutputDataConfig{S3OutputPath: aws.String("s3://" + smBucket + "/models")},
				ResourceConfig: &smtypes.ResourceConfig{
					InstanceType:   smtypes.TrainingInstanceTypeMlM5Large,
					InstanceCount:  aws.Int32(1),
					VolumeSizeInGB: aws.Int32(1),
				},
				StoppingCondition: &smtypes.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(120)},
			})
			require.NoError(t, cerr)
		}
		wait := func(name string) *sagemaker.DescribeTrainingJobOutput {
			var out *sagemaker.DescribeTrainingJobOutput

			require.Eventually(t, func() bool {
				var derr error

				out, derr = c.sm.DescribeTrainingJob(
					ctx,
					&sagemaker.DescribeTrainingJobInput{TrainingJobName: aws.String(name)},
				)

				return derr == nil && out.TrainingJobStatus != smtypes.TrainingJobStatusInProgress
			}, 3*time.Minute, 250*time.Millisecond)

			return out
		}

		create("tj-ok", "false")
		ok := wait("tj-ok")
		require.Equal(t, smtypes.TrainingJobStatusCompleted, ok.TrainingJobStatus, aws.ToString(ok.FailureReason))
		assert.Equal(
			t,
			"s3://"+smBucket+"/models/tj-ok/output/model.tar.gz",
			aws.ToString(ok.ModelArtifacts.S3ModelArtifacts),
		)

		zr, err := gzip.NewReader(bytes.NewReader(c.get(t, "models/tj-ok/output/model.tar.gz")))
		require.NoError(t, err)

		hdr, err := tar.NewReader(zr).Next()
		require.NoError(t, err)
		assert.Equal(t, "model.txt", hdr.Name)

		tr := tar.NewReader(bytes.NewReader(mustGunzip(t, c.get(t, "models/tj-ok/output/model.tar.gz"))))
		_, _ = tr.Next()
		body, _ := io.ReadAll(tr)
		assert.Equal(t, "sum=15 epochs=4", string(body))

		create("tj-bad", "true")
		bad := wait("tj-bad")
		assert.Equal(t, smtypes.TrainingJobStatusFailed, bad.TrainingJobStatus)
		assert.Equal(t, "AlgorithmError: deliberate failure, exit code: 1", aws.ToString(bad.FailureReason))

		require.Eventually(t, func() bool {
			res, lerr := c.logs.FilterLogEvents(ctx, &cloudwatchlogs.FilterLogEventsInput{
				LogGroupName: aws.String("/aws/sagemaker/TrainingJobs"), FilterPattern: aws.String("training with"),
			})

			return lerr == nil && len(res.Events) >= 2
		}, 20*time.Second, 250*time.Millisecond)
	})

	t.Run("processing", func(t *testing.T) {
		t.Parallel()

		_, err := c.sm.CreateProcessingJob(ctx, &sagemaker.CreateProcessingJobInput{
			ProcessingJobName: aws.String("pj1"), RoleArn: aws.String(smRole),
			AppSpecification: &smtypes.AppSpecification{
				ImageUri:           aws.String(image),
				ContainerArguments: []string{"process"},
			},
			Environment: map[string]string{"GREETING": "hi"},
			ProcessingInputs: []smtypes.ProcessingInput{{
				InputName: aws.String("raw"),
				S3Input: &smtypes.ProcessingS3Input{
					S3Uri: aws.String("s3://" + smBucket + "/raw/"), LocalPath: aws.String("/opt/ml/processing/input"),
					S3DataType: smtypes.ProcessingS3DataTypeS3Prefix, S3InputMode: smtypes.ProcessingS3InputModeFile,
				},
			}},
			ProcessingOutputConfig: &smtypes.ProcessingOutputConfig{Outputs: []smtypes.ProcessingOutput{{
				OutputName: aws.String("clean"),
				S3Output: &smtypes.ProcessingS3Output{
					S3Uri: aws.String(
						"s3://" + smBucket + "/clean",
					), LocalPath: aws.String("/opt/ml/processing/output"),
					S3UploadMode: smtypes.ProcessingS3UploadModeEndOfJob,
				},
			}}},
			ProcessingResources: &smtypes.ProcessingResources{ClusterConfig: &smtypes.ProcessingClusterConfig{
				InstanceCount: aws.Int32(
					1,
				), InstanceType: smtypes.ProcessingInstanceTypeMlM5Large, VolumeSizeInGB: aws.Int32(1),
			}},
		})
		require.NoError(t, err)

		var out *sagemaker.DescribeProcessingJobOutput

		require.Eventually(t, func() bool {
			var derr error

			out, derr = c.sm.DescribeProcessingJob(
				ctx,
				&sagemaker.DescribeProcessingJobInput{ProcessingJobName: aws.String("pj1")},
			)

			return derr == nil && out.ProcessingJobStatus != smtypes.ProcessingJobStatusInProgress
		}, 3*time.Minute, 250*time.Millisecond)
		require.Equal(t, smtypes.ProcessingJobStatusCompleted, out.ProcessingJobStatus, aws.ToString(out.FailureReason))
		assert.Equal(t, "HELLO PROCESSING", string(c.get(t, "clean/a.txt")))
	})

	t.Run("transform", func(t *testing.T) {
		t.Parallel()

		_, err := c.sm.CreateTransformJob(ctx, &sagemaker.CreateTransformJobInput{
			TransformJobName: aws.String(
				"xf1",
			), ModelName: aws.String("m1"), BatchStrategy: smtypes.BatchStrategySingleRecord,
			TransformInput: &smtypes.TransformInput{
				ContentType: aws.String("text/csv"), SplitType: smtypes.SplitTypeLine,
				DataSource: &smtypes.TransformDataSource{S3DataSource: &smtypes.TransformS3DataSource{
					S3Uri: aws.String("s3://" + smBucket + "/batch/in/"), S3DataType: smtypes.S3DataTypeS3Prefix,
				}},
			},
			TransformOutput: &smtypes.TransformOutput{
				S3OutputPath: aws.String("s3://" + smBucket + "/batch/out"),
				AssembleWith: smtypes.AssemblyTypeLine,
			},
			TransformResources: &smtypes.TransformResources{
				InstanceType:  smtypes.TransformInstanceTypeMlM5Large,
				InstanceCount: aws.Int32(1),
			},
		})
		require.NoError(t, err)

		var out *sagemaker.DescribeTransformJobOutput

		require.Eventually(t, func() bool {
			var derr error

			out, derr = c.sm.DescribeTransformJob(
				ctx,
				&sagemaker.DescribeTransformJobInput{TransformJobName: aws.String("xf1")},
			)

			return derr == nil && out.TransformJobStatus != smtypes.TransformJobStatusInProgress
		}, 3*time.Minute, 250*time.Millisecond)
		require.Equal(t, smtypes.TransformJobStatusCompleted, out.TransformJobStatus, aws.ToString(out.FailureReason))
		assert.Equal(t, "R1*7\nR2*7\n", string(c.get(t, "batch/out/a.csv.out")))
	})
}

func mustGunzip(t *testing.T, data []byte) []byte {
	t.Helper()

	zr, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)

	b, err := io.ReadAll(zr)
	require.NoError(t, err)

	return b
}
