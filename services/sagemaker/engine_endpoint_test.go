package sagemaker_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
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

func tarGz(t *testing.T, files map[string]string) []byte {
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

// seenRequest is what a model container received on /invocations.
type seenRequest struct {
	header http.Header
	body   string
}

// modelServer answers /ping and /invocations like a SageMaker inference container: the reply is
// "<image>|<request body>|<contents of /opt/ml/model/weights.txt>".
type modelServer struct {
	status func(image string) int
	seen   []seenRequest
	mu     sync.Mutex
}

func (m *modelServer) handler(spec container.Spec) http.Handler {
	mux := http.NewServeMux()
	modelDir := mountSource(spec, "/opt/ml/model")

	mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		if m.status != nil && m.status(spec.Image) != http.StatusOK {
			w.WriteHeader(m.status(spec.Image))

			return
		}

		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/invocations", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		m.mu.Lock()
		m.seen = append(m.seen, seenRequest{header: r.Header.Clone(), body: string(body)})
		m.mu.Unlock()

		if strings.Contains(string(body), "explode") {
			http.Error(w, "model blew up", http.StatusInternalServerError)

			return
		}

		weights := readFileOrEmpty(filepath.Join(modelDir, "weights.txt"))
		w.Header().Set("Content-Type", r.Header.Get("Accept"))
		w.Header().Set("X-Amzn-Sagemaker-Custom-Attributes", "from-container")
		_, _ = w.Write([]byte(spec.Image + "|" + string(body) + "|" + weights))
	})

	return mux
}

func (m *modelServer) last(t *testing.T) seenRequest {
	t.Helper()

	m.mu.Lock()
	defer m.mu.Unlock()

	require.NotEmpty(t, m.seen)

	return m.seen[len(m.seen)-1]
}

type endpointFixture struct {
	models   map[string]sagemaker.ContainerDefinition
	variants []sagemaker.ProductionVariant
}

func (f endpointFixture) create(t *testing.T, rig *engineRig) {
	t.Helper()

	ctx := t.Context()

	for mName, def := range f.models {
		d := def
		_, err := rig.b.CreateModel(ctx, mName, "arn:aws:iam::000000000000:role/r", &d, nil, nil)
		require.NoError(t, err)
	}

	_, err := rig.b.CreateEndpointConfig(ctx, "ep-cfg", f.variants, nil)
	require.NoError(t, err)

	_, err = rig.b.CreateEndpointFSM(ctx, sagemaker.CreateEndpointOptions{Name: "ep", EndpointConfigName: "ep-cfg"})
	require.NoError(t, err)
}

func waitEndpoint(t *testing.T, b *sagemaker.InMemoryBackend, name string) *sagemaker.Endpoint {
	t.Helper()

	var ep *sagemaker.Endpoint

	require.Eventually(t, func() bool {
		var err error

		ep, err = b.DescribeEndpoint(t.Context(), name)

		return err == nil && ep.EndpointStatus != "Creating" && ep.EndpointStatus != "Updating"
	}, 10*time.Second, 5*time.Millisecond)

	return ep
}

func singleVariant(model string) []sagemaker.ProductionVariant {
	return []sagemaker.ProductionVariant{{
		VariantName: "AllTraffic", ModelName: model, InstanceType: "ml.m5.large",
		InitialInstanceCount: 1, InitialVariantWeight: 1,
	}}
}

func TestEngineEndpoint_InvokeProxiesToContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantBody   string
		wantCustom string
		wantCT     string
		req        sagemaker.InvokeRequest
	}{
		{
			name: "csv_with_attributes",
			req: sagemaker.InvokeRequest{
				Body:             []byte("1,2,3"),
				ContentType:      "text/csv",
				Accept:           "application/json",
				CustomAttributes: "trace=1",
			},
			wantBody:   "img-a|1,2,3|w1",
			wantCustom: "from-container",
			wantCT:     "application/json",
		},
		{
			name:       "defaults_content_type",
			req:        sagemaker.InvokeRequest{Body: []byte("x")},
			wantBody:   "img-a|x|w1",
			wantCustom: "from-container",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newEngineRig(t, sagemaker.EngineConfig{})
			srv := &modelServer{}
			rig.rt.serve = srv.handler

			requireNoErr(
				t,
				rig.s3.PutObjectBytes(
					t.Context(),
					"models",
					"m/model.tar.gz",
					tarGz(t, map[string]string{"weights.txt": "w1"}),
				),
			)

			endpointFixture{
				variants: singleVariant("m1"),
				models: map[string]sagemaker.ContainerDefinition{"m1": {
					Image: "img-a", ModelDataURL: "s3://models/m/model.tar.gz", Environment: map[string]string{"K": "V"},
				}},
			}.create(t, rig)

			ep := waitEndpoint(t, rig.b, "ep")
			require.Equal(t, "InService", ep.EndpointStatus, ep.FailureReason)

			spec, ok := rig.rt.lastSpec("img-a")
			require.True(t, ok)
			assert.Equal(t, []string{"serve"}, spec.Cmd)
			assert.Equal(t, "V", envValue(spec, "K"))

			tt.req.EndpointName = "ep"
			res, err := rig.b.InvokeEndpoint(t.Context(), tt.req)
			require.NoError(t, err)

			assert.Equal(t, tt.wantBody, string(res.Body))
			assert.Equal(t, "AllTraffic", res.InvokedVariant)
			assert.Equal(t, tt.wantCustom, res.CustomAttributes)

			seen := srv.last(t)
			wantCT := tt.req.ContentType
			if wantCT == "" {
				wantCT = "application/octet-stream"
			}

			assert.Equal(t, wantCT, seen.header.Get("Content-Type"))
			assert.Equal(t, tt.req.Accept, seen.header.Get("Accept"))
			assert.Equal(t, tt.req.CustomAttributes, seen.header.Get("X-Amzn-Sagemaker-Custom-Attributes"))
		})
	}
}

func TestEngineEndpoint_ModelErrorSurfaces(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	rig.rt.serve = (&modelServer{}).handler

	endpointFixture{
		variants: singleVariant("m1"),
		models:   map[string]sagemaker.ContainerDefinition{"m1": {Image: "img-a"}},
	}.create(t, rig)
	require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

	_, err := rig.b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("explode")})

	var me *sagemaker.ModelError
	require.ErrorAs(t, err, &me)
	assert.Equal(t, 500, me.OriginalStatusCode)
	assert.Equal(t, "model blew up", me.OriginalMessage)
	assert.Contains(t, me.LogStreamArn, "/aws/sagemaker/Endpoints/ep")
}

func TestEngineEndpoint_FailureReasons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status     func(string) int
		name       string
		wantReason string
		cfg        sagemaker.EngineConfig
	}{
		{
			name: "ping_never_healthy",
			cfg: sagemaker.EngineConfig{
				StartupTimeout: 150 * time.Millisecond,
				PingInterval:   10 * time.Millisecond,
			},
			status:     func(string) int { return http.StatusServiceUnavailable },
			wantReason: "did not pass the ping health check",
		},
		{
			name:       "no_container_runtime",
			cfg:        sagemaker.EngineConfig{Reason: "docker daemon not reachable"},
			wantReason: "docker daemon not reachable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newEngineRig(t, tt.cfg)
			rig.rt.serve = (&modelServer{status: tt.status}).handler

			endpointFixture{
				variants: singleVariant("m1"),
				models:   map[string]sagemaker.ContainerDefinition{"m1": {Image: "img-a"}},
			}.create(t, rig)

			ep := waitEndpoint(t, rig.b, "ep")
			assert.Equal(t, "Failed", ep.EndpointStatus)
			assert.Contains(t, ep.FailureReason, tt.wantReason)

			_, err := rig.b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("x")})
			require.Error(t, err)
		})
	}
}

func TestEngineEndpoint_StubModeNeverFabricates(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(func() { b.Shutdown(context.Background()) })

	_, err := b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("x")})
	require.ErrorIs(t, err, sagemaker.ErrEngineDisabled)

	_, err = b.SubmitAsyncInference(t.Context(), sagemaker.AsyncRequest{EndpointName: "ep", Body: []byte("x")})
	require.ErrorIs(t, err, sagemaker.ErrEngineDisabled)
}

func TestEngineEndpoint_VariantsAndPipelines(t *testing.T) {
	t.Parallel()

	t.Run("target_variant", func(t *testing.T) {
		t.Parallel()

		rig := newEngineRig(t, sagemaker.EngineConfig{})
		rig.rt.serve = (&modelServer{}).handler

		endpointFixture{
			variants: []sagemaker.ProductionVariant{
				{VariantName: "blue", ModelName: "m-blue", InitialInstanceCount: 1, InitialVariantWeight: 1},
				{VariantName: "green", ModelName: "m-green", InitialInstanceCount: 1, InitialVariantWeight: 0},
			},
			models: map[string]sagemaker.ContainerDefinition{
				"m-blue":  {Image: "img-blue"},
				"m-green": {Image: "img-green"},
			},
		}.create(t, rig)
		require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

		for variant, wantImage := range map[string]string{"": "img-blue", "green": "img-green", "blue": "img-blue"} {
			res, err := rig.b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{
				EndpointName: "ep", TargetVariant: variant, Body: []byte("p"),
			})
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(string(res.Body), wantImage+"|"), "variant %q -> %s", variant, res.Body)
		}

		_, err := rig.b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{EndpointName: "ep", TargetVariant: "nope"})
		require.ErrorIs(t, err, sagemaker.ErrEndpointNotRunning)
	})

	t.Run("serial_pipeline", func(t *testing.T) {
		t.Parallel()

		rig := newEngineRig(t, sagemaker.EngineConfig{})
		rig.rt.serve = (&modelServer{}).handler

		ctx := t.Context()
		_, err := rig.b.CreateModel(
			ctx,
			"pipe",
			"arn:aws:iam::000000000000:role/r",
			nil,
			[]sagemaker.ContainerDefinition{
				{Image: "stage-1", ContainerHostname: "first"}, {Image: "stage-2", ContainerHostname: "second"},
			},
			nil,
		)
		require.NoError(t, err)

		_, err = rig.b.CreateEndpointConfig(ctx, "cfg", singleVariant("pipe"), nil)
		require.NoError(t, err)
		_, err = rig.b.CreateEndpointFSM(ctx, sagemaker.CreateEndpointOptions{Name: "ep", EndpointConfigName: "cfg"})
		require.NoError(t, err)
		require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

		res, err := rig.b.InvokeEndpoint(ctx, sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("in")})
		require.NoError(t, err)
		assert.Equal(t, "stage-2|stage-1|in||", string(res.Body))
	})
}

func TestEngineEndpoint_UpdateSwapsAndDeleteStops(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	rig.rt.serve = (&modelServer{}).handler

	endpointFixture{
		variants: singleVariant("m1"),
		models:   map[string]sagemaker.ContainerDefinition{"m1": {Image: "img-v1"}},
	}.create(t, rig)
	require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

	ctx := t.Context()
	_, err := rig.b.CreateModel(
		ctx,
		"m2",
		"arn:aws:iam::000000000000:role/r",
		&sagemaker.ContainerDefinition{Image: "img-v2"},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = rig.b.CreateEndpointConfig(ctx, "cfg2", singleVariant("m2"), nil)
	require.NoError(t, err)

	_, err = rig.b.UpdateEndpointFSM(ctx, "ep", sagemaker.UpdateEndpointOptions{EndpointConfigName: "cfg2"})
	require.NoError(t, err)

	ep := waitEndpoint(t, rig.b, "ep")
	require.Equal(t, "InService", ep.EndpointStatus)
	assert.Equal(t, "cfg2", ep.EndpointConfigName)

	res, err := rig.b.InvokeEndpoint(ctx, sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("p")})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(res.Body), "img-v2|"))

	require.Eventually(t, func() bool { return rig.rt.removedCount() == 1 }, 5*time.Second, 5*time.Millisecond,
		"the old (blue) container is removed after the swap")

	require.NoError(t, rig.b.DeleteEndpoint(ctx, "ep"))
	require.Eventually(t, func() bool {
		_, derr := rig.b.DescribeEndpoint(ctx, "ep")

		return derr != nil
	}, 5*time.Second, 5*time.Millisecond)
	assert.Equal(t, 2, rig.rt.removedCount())
}

func TestEngineEndpoint_FailedUpdateKeepsServing(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(
		t,
		sagemaker.EngineConfig{StartupTimeout: 150 * time.Millisecond, PingInterval: 10 * time.Millisecond},
	)
	rig.rt.serve = (&modelServer{status: func(image string) int {
		if image == "img-bad" {
			return http.StatusInternalServerError
		}

		return http.StatusOK
	}}).handler

	endpointFixture{
		variants: singleVariant("m1"),
		models:   map[string]sagemaker.ContainerDefinition{"m1": {Image: "img-good"}},
	}.create(t, rig)
	require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

	ctx := t.Context()
	_, err := rig.b.CreateModel(
		ctx,
		"m2",
		"arn:aws:iam::000000000000:role/r",
		&sagemaker.ContainerDefinition{Image: "img-bad"},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = rig.b.CreateEndpointConfig(ctx, "cfg2", singleVariant("m2"), nil)
	require.NoError(t, err)
	_, err = rig.b.UpdateEndpointFSM(ctx, "ep", sagemaker.UpdateEndpointOptions{EndpointConfigName: "cfg2"})
	require.NoError(t, err)

	ep := waitEndpoint(t, rig.b, "ep")
	assert.Equal(t, "InService", ep.EndpointStatus)
	assert.Equal(t, "ep-cfg", ep.EndpointConfigName)
	assert.Contains(t, ep.FailureReason, "ping health check")

	res, err := rig.b.InvokeEndpoint(ctx, sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("p")})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(res.Body), "img-good|"))
}

func TestEngineEndpoint_ModelDataAndLogs(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	rig.rt.serve = (&modelServer{}).handler
	rig.rt.logs["img-a"] = "listening on 8080\nready\n"

	requireNoErr(
		t,
		rig.s3.PutObjectBytes(
			t.Context(),
			"models",
			"m/model.tar.gz",
			tarGz(t, map[string]string{"nested/dir/f.txt": "x"}),
		),
	)

	endpointFixture{
		variants: singleVariant("m1"),
		models: map[string]sagemaker.ContainerDefinition{"m1": {
			Image: "img-a", ModelDataURL: "s3://models/m/model.tar.gz",
		}},
	}.create(t, rig)
	require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

	spec, ok := rig.rt.lastSpec("img-a")
	require.True(t, ok)

	data, err := os.ReadFile(filepath.Join(mountSource(spec, "/opt/ml/model"), "nested", "dir", "f.txt"))
	require.NoError(t, err)
	assert.Equal(t, "x", string(data))

	require.Eventually(t, func() bool {
		return strings.Join(rig.logs.groupLines("/aws/sagemaker/Endpoints/ep"), "\n") == "listening on 8080\nready"
	}, 5*time.Second, 5*time.Millisecond)

	_, err = rig.b.CreateEndpointFSM(
		t.Context(),
		sagemaker.CreateEndpointOptions{Name: "ep2", EndpointConfigName: "ep-cfg"},
	)
	require.NoError(t, err)
	assert.Equal(t, "InService", waitEndpoint(t, rig.b, "ep2").EndpointStatus)
}

func TestEngineEndpoint_InvokeUnavailableAfterDelete(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	rig.rt.serve = (&modelServer{}).handler

	endpointFixture{
		variants: singleVariant("m1"),
		models:   map[string]sagemaker.ContainerDefinition{"m1": {Image: "img-a"}},
	}.create(t, rig)
	require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

	require.NoError(t, rig.b.DeleteEndpoint(t.Context(), "ep"))
	require.Eventually(t, func() bool {
		_, err := rig.b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("x")})

		return err != nil
	}, 5*time.Second, 5*time.Millisecond)

	_, err := rig.b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("x")})
	assert.True(
		t,
		errors.Is(err, sagemaker.ErrEndpointNotFound) || errors.Is(err, sagemaker.ErrEndpointNotRunning),
		"%v",
		err,
	)
}

func TestEngineEndpoint_RejectsUnsafeModelArchive(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	rig.rt.serve = (&modelServer{}).handler
	requireNoErr(
		t,
		rig.s3.PutObjectBytes(
			t.Context(),
			"models",
			"m/model.tar.gz",
			tarGz(t, map[string]string{"../escape.txt": "x"}),
		),
	)

	endpointFixture{
		variants: singleVariant("m1"),
		models: map[string]sagemaker.ContainerDefinition{
			"m1": {Image: "img-a", ModelDataURL: "s3://models/m/model.tar.gz"},
		},
	}.create(t, rig)

	ep := waitEndpoint(t, rig.b, "ep")
	assert.Equal(t, "Failed", ep.EndpointStatus)
	assert.Contains(t, ep.FailureReason, "escapes the target directory")
}

func TestEngineEndpoint_MissingModelDataFails(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	rig.rt.serve = (&modelServer{}).handler

	endpointFixture{
		variants: singleVariant("m1"),
		models: map[string]sagemaker.ContainerDefinition{
			"m1": {Image: "img-a", ModelDataURL: "s3://models/none/model.tar.gz"},
		},
	}.create(t, rig)

	ep := waitEndpoint(t, rig.b, "ep")
	assert.Equal(t, "Failed", ep.EndpointStatus)
	assert.Contains(t, ep.FailureReason, "Unable to load model data")
}
