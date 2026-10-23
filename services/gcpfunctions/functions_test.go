package gcpfunctions_test

import (
	"errors"
	"net/http"
	"testing"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestFunction_LifecycleAndInvoke(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "hello", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	assert.Equal(t, parent+"/functions/hello", fn.GetName())
	assert.Equal(t, functionspb.Function_ACTIVE, fn.GetState())
	assert.Equal(t, functionspb.Environment_GEN_2, fn.GetEnvironment())
	assert.Contains(t, fn.GetUrl(), ".localhost:")
	assert.Equal(t, fn.GetUrl(), fn.GetServiceConfig().GetUri())
	assert.Equal(t, int32(60), fn.GetServiceConfig().GetTimeoutSeconds())
	assert.Equal(t, "256M", fn.GetServiceConfig().GetAvailableMemory())
	assert.Equal(t, functionspb.ServiceConfig_ALLOW_ALL, fn.GetServiceConfig().GetIngressSettings())
	assert.NotEmpty(t, fn.GetServiceConfig().GetRevision())
	assert.NotNil(t, fn.GetBuildConfig().GetSourceProvenance().GetResolvedStorageSource())
	assert.Equal(t, 1, e.rt.running(), "container starts during deploy")

	code, body := e.call(t, http.MethodGet, hostOf(fn), "/some/path", "")
	require.Equal(t, http.StatusOK, code)

	got := decode(t, body)
	assert.Equal(t, "/some/path", got.Path)
	assert.Equal(t, "hello", got.Env["FUNCTION_TARGET"])
	assert.Equal(t, "http", got.Env["FUNCTION_SIGNATURE_TYPE"])
	assert.Equal(t, "hi", got.Env["GREETING"])
	assert.Equal(t, "hello", got.Env["K_SERVICE"])
	assert.Equal(t, 1, e.rt.running())

	specs := e.rt.specs()
	require.Len(t, specs, 1)
	assert.Equal(t, "node:22", specs[0].Image)
	require.Len(t, specs[0].Mounts, 1)
	assert.Contains(t, specs[0].Mounts[0], ":/src:ro")

	listed, err := e.client.ListFunctions(t.Context(), &functionspb.ListFunctionsRequest{
		Parent: "projects/test-project/locations/-",
	}).Next()
	require.NoError(t, err)
	assert.Equal(t, fn.GetName(), listed.GetName())

	del, err := e.client.DeleteFunction(t.Context(), &functionspb.DeleteFunctionRequest{Name: fn.GetName()})
	require.NoError(t, err)
	require.NoError(t, del.Wait(t.Context()))
	assert.Equal(t, 0, e.rt.running())

	_, err = e.client.GetFunction(t.Context(), &functionspb.GetFunctionRequest{Name: fn.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	code, _ = e.call(t, http.MethodGet, hostOf(fn), "/", "")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestFunction_CreateErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	src := e.upload(t, zipOf(t, nodeFilesFn()))

	tests := []struct {
		mutate func(fn *functionspb.Function)
		name   string
		id     string
		parent string
		code   codes.Code
	}{
		{name: "bad id", id: "Bad_ID", code: codes.InvalidArgument},
		{name: "bad parent", id: "ok", parent: "projects/p", code: codes.InvalidArgument},
		{name: "all locations parent", id: "ok", parent: "projects/p/locations/-", code: codes.InvalidArgument},
		{
			name: "unknown runtime", id: "ok", code: codes.InvalidArgument,
			mutate: func(fn *functionspb.Function) { fn.BuildConfig.Runtime = "cobol" },
		},
		{
			name: "no entry point", id: "ok", code: codes.InvalidArgument,
			mutate: func(fn *functionspb.Function) { fn.BuildConfig.EntryPoint = "" },
		},
		{
			name: "no source", id: "ok", code: codes.InvalidArgument,
			mutate: func(fn *functionspb.Function) { fn.BuildConfig.Source = nil },
		},
		{name: "repo source", id: "ok", code: codes.InvalidArgument, mutate: func(fn *functionspb.Function) {
			fn.BuildConfig.Source = &functionspb.Source{Source: &functionspb.Source_GitUri{GitUri: "https://x"}}
		}},
		{
			name: "foreign bucket without object store",
			id:   "ok",
			code: codes.FailedPrecondition,
			mutate: func(fn *functionspb.Function) {
				fn.BuildConfig.Source = &functionspb.Source{Source: &functionspb.Source_StorageSource{
					StorageSource: &functionspb.StorageSource{Bucket: "b", Object: "o"},
				}}
			},
		},
		{
			name: "reserved env", id: "ok", code: codes.InvalidArgument,
			mutate: func(fn *functionspb.Function) { fn.ServiceConfig.EnvironmentVariables["PORT"] = "1" },
		},
		{name: "secrets", id: "ok", code: codes.InvalidArgument, mutate: func(fn *functionspb.Function) {
			fn.ServiceConfig.SecretEnvironmentVariables = []*functionspb.SecretEnvVar{{Key: "K"}}
		}},
		{
			name: "bad timeout", id: "ok", code: codes.InvalidArgument,
			mutate: func(fn *functionspb.Function) { fn.ServiceConfig.TimeoutSeconds = 99999 },
		},
		{
			name: "bad memory", id: "ok", code: codes.InvalidArgument,
			mutate: func(fn *functionspb.Function) { fn.ServiceConfig.AvailableMemory = "lots" },
		},
		{name: "unsupported event type", id: "ok", code: codes.InvalidArgument, mutate: func(fn *functionspb.Function) {
			fn.EventTrigger = &functionspb.EventTrigger{EventType: "google.cloud.storage.object.v1.finalized"}
		}},
		{name: "missing topic", id: "ok", code: codes.InvalidArgument, mutate: func(fn *functionspb.Function) {
			fn.EventTrigger = &functionspb.EventTrigger{
				EventType:   "google.cloud.pubsub.topic.v1.messagePublished",
				PubsubTopic: "projects/test-project/topics/nope",
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn := fnSpec(src)
			if tt.mutate != nil {
				tt.mutate(fn)
			}

			p := parent
			if tt.parent != "" {
				p = tt.parent
			}

			_, err := e.client.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
				Parent: p, FunctionId: tt.id, Function: fn,
			})
			assert.Equal(t, tt.code, status.Code(err), "%v", err)
		})
	}
}

func TestFunction_AlreadyExistsAndNotFound(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	src := e.upload(t, zipOf(t, nodeFilesFn()))
	e.createFunction(t, "dup", fnSpec(src))

	_, err := e.client.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
		Parent: parent, FunctionId: "dup", Function: fnSpec(src),
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	tests := []struct {
		call func() error
		name string
	}{
		{name: "get", call: func() error {
			_, e2 := e.client.GetFunction(
				t.Context(),
				&functionspb.GetFunctionRequest{Name: parent + "/functions/none"},
			)

			return e2
		}},
		{name: "delete", call: func() error {
			_, e2 := e.client.DeleteFunction(
				t.Context(),
				&functionspb.DeleteFunctionRequest{Name: parent + "/functions/none"},
			)

			return e2
		}},
		{name: "update", call: func() error {
			_, e2 := e.client.UpdateFunction(t.Context(), &functionspb.UpdateFunctionRequest{
				Function: &functionspb.Function{Name: parent + "/functions/none"},
			})

			return e2
		}},
		{name: "download url", call: func() error {
			_, e2 := e.client.GenerateDownloadUrl(t.Context(), &functionspb.GenerateDownloadUrlRequest{
				Name: parent + "/functions/none",
			})

			return e2
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, codes.NotFound, status.Code(tt.call()))
		})
	}
}

func TestFunction_Update(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "upd", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	_, body := e.call(t, http.MethodGet, hostOf(fn), "/", "")
	assert.Equal(t, "hi", decode(t, body).Env["GREETING"])

	next := fnSpec(nil)
	next.Name = fn.GetName()
	next.Description = "updated"
	next.ServiceConfig.EnvironmentVariables = map[string]string{"GREETING": "hello again"}

	op, err := e.client.UpdateFunction(t.Context(), &functionspb.UpdateFunctionRequest{
		Function:   next,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description", "service_config.environment_variables"}},
	})
	require.NoError(t, err)

	updated, err := op.Wait(t.Context())
	require.NoError(t, err)

	assert.Equal(t, "updated", updated.GetDescription())
	assert.NotEqual(t, fn.GetServiceConfig().GetRevision(), updated.GetServiceConfig().GetRevision())
	assert.Equal(t, "nodejs22", updated.GetBuildConfig().GetRuntime(), "unmasked build_config is kept")
	assert.Equal(t, 1, e.rt.running(), "old container is replaced")

	_, body = e.call(t, http.MethodGet, hostOf(updated), "/", "")
	assert.Equal(t, "hello again", decode(t, body).Env["GREETING"])

	_, err = e.client.UpdateFunction(t.Context(), &functionspb.UpdateFunctionRequest{
		Function:   next,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestFunction_UpdateSource(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "src", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	next := fnSpec(e.upload(t, zipOf(t, map[string]string{"index.js": "v2"})))
	next.Name = fn.GetName()

	op, err := e.client.UpdateFunction(t.Context(), &functionspb.UpdateFunctionRequest{
		Function:   next,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"build_config.source"}},
	})
	require.NoError(t, err)

	updated, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.NotEqual(t,
		fn.GetBuildConfig().GetSource().GetStorageSource().GetObject(),
		updated.GetBuildConfig().GetSource().GetStorageSource().GetObject())
}

func TestFunction_NoRuntimeServes503(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	fn := e.createFunction(t, "norun", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))
	assert.Equal(t, functionspb.Function_ACTIVE, fn.GetState())

	code, body := e.call(t, http.MethodGet, hostOf(fn), "/", "")
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Contains(t, body, "container runtime is unavailable")
}

func TestFunction_InvalidArchiveFailsDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		files   map[string]string
		name    string
		runtime string
		raw     []byte
	}{
		{name: "not a zip", raw: []byte("plain text"), runtime: "nodejs22"},
		{name: "python without main", files: map[string]string{"x.py": ""}, runtime: "python312"},
		{name: "go without sources", files: map[string]string{"README": ""}, runtime: "go123"},
		{name: "node without entry", files: map[string]string{"README": ""}, runtime: "nodejs22"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)

			data := tt.raw
			if data == nil {
				data = zipOf(t, tt.files)
			}

			spec := fnSpec(e.upload(t, data))
			spec.BuildConfig.Runtime = tt.runtime

			op, err := e.client.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
				Parent: parent, FunctionId: "bad", Function: spec,
			})
			require.NoError(t, err)

			_, err = op.Wait(t.Context())
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))

			got, err := e.client.GetFunction(
				t.Context(),
				&functionspb.GetFunctionRequest{Name: parent + "/functions/bad"},
			)
			require.NoError(t, err)
			assert.Equal(t, functionspb.Function_FAILED, got.GetState())
			require.NotEmpty(t, got.GetStateMessages())

			code, _ := e.call(t, http.MethodGet, hostOf(got), "/", "")
			assert.Equal(t, http.StatusServiceUnavailable, code)
		})
	}
}

func TestFunction_StartFailureFailsDeploy(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.rt.failRun = assert.AnError

	op, err := e.client.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
		Parent: parent, FunctionId: "boom", Function: fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))),
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	got, err := e.client.GetFunction(t.Context(), &functionspb.GetFunctionRequest{Name: parent + "/functions/boom"})
	require.NoError(t, err)
	assert.Equal(t, functionspb.Function_FAILED, got.GetState())

	code, _ := e.call(t, http.MethodGet, hostOf(got), "/", "")
	assert.Equal(t, http.StatusServiceUnavailable, code)
}

func TestFunction_Ingress(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "ing", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	tests := []struct {
		name string
		host string
		want int
	}{
		{"unknown function", "nope-abc-us-central1.localhost", http.StatusNotFound},
		{"not a localhost host", "example.com", http.StatusNotFound},
		{"function host", hostOf(fn), http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, _ := e.call(t, http.MethodGet, tt.host, "/", "")
			assert.Equal(t, tt.want, code)
		})
	}
}

func TestSource_UploadDownload(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	data := zipOf(t, nodeFilesFn())
	fn := e.createFunction(t, "dl", fnSpec(e.upload(t, data)))

	resp, err := e.client.GenerateDownloadUrl(t.Context(), &functionspb.GenerateDownloadUrlRequest{Name: fn.GetName()})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, resp.GetDownloadUrl(), nil)
	require.NoError(t, err)

	res, err := e.http.Do(req)
	require.NoError(t, err)

	defer res.Body.Close()

	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, "application/zip", res.Header.Get("Content-Type"))
	assert.Equal(t, int64(len(data)), res.ContentLength)

	up, err := e.client.GenerateUploadUrl(t.Context(), &functionspb.GenerateUploadUrlRequest{Parent: parent})
	require.NoError(t, err)

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{
			"wrong method",
			http.MethodGet,
			"/_gcf/upload/" + up.GetStorageSource().GetBucket() + "/" + up.GetStorageSource().GetObject(),
			http.StatusMethodNotAllowed,
		},
		{"unissued slot", http.MethodPut, "/_gcf/upload/bucket/other.zip", http.StatusForbidden},
		{
			"download unknown",
			http.MethodGet,
			"/_gcf/download/projects/p/locations/l/functions/x.zip",
			http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, _ := e.call(t, tt.method, "localhost", tt.path, "x")
			assert.Equal(t, tt.want, code)
		})
	}
}

func TestSource_ObjectReader(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.handler.Backend.SetObjectReader(fakeObjects{"my-bucket/src.zip": zipOf(t, nodeFilesFn())})

	spec := fnSpec(&functionspb.StorageSource{Bucket: "my-bucket", Object: "src.zip", Generation: 7})
	fn := e.createFunction(t, "gcs", spec)

	got := fn.GetBuildConfig().GetSourceProvenance().GetResolvedStorageSource()
	assert.Equal(t, "my-bucket", got.GetBucket())
	assert.Equal(t, int64(7), got.GetGeneration())

	spec = fnSpec(&functionspb.StorageSource{Bucket: "my-bucket", Object: "missing.zip"})
	_, err := e.client.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
		Parent: parent, FunctionId: "gcs2", Function: spec,
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestListRuntimes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)

	tests := []struct {
		name    string
		filter  string
		code    codes.Code
		wantAny bool
	}{
		{"all", "", codes.OK, true},
		{"gen2", "environment=GEN_2", codes.OK, true},
		{"gen1", "environment=GEN_1", codes.OK, false},
		{"bad filter", "stage=GA", codes.InvalidArgument, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.client.ListRuntimes(
				t.Context(),
				&functionspb.ListRuntimesRequest{Parent: parent, Filter: tt.filter},
			)
			require.Equal(t, tt.code, status.Code(err))

			if tt.code != codes.OK {
				return
			}

			assert.Equal(t, tt.wantAny, len(resp.GetRuntimes()) > 0)

			for _, r := range resp.GetRuntimes() {
				assert.Equal(t, functionspb.ListRuntimesResponse_GA, r.GetStage())
			}
		})
	}
}

func TestListFunctions_Pagination(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	src := e.upload(t, zipOf(t, nodeFilesFn()))

	for _, id := range []string{"a", "b", "c"} {
		e.createFunction(t, id, fnSpec(src))
	}

	it := e.client.ListFunctions(t.Context(), &functionspb.ListFunctionsRequest{Parent: parent, PageSize: 2})

	var names []string

	for {
		f, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		names = append(names, f.GetName())
	}

	assert.Equal(t, []string{parent + "/functions/a", parent + "/functions/b", parent + "/functions/c"}, names)

	_, err := e.client.ListFunctions(t.Context(), &functionspb.ListFunctionsRequest{Parent: parent, Filter: "x"}).Next()
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
