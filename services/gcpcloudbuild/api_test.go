package gcpcloudbuild_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestCreateBuildValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build *cloudbuildpb.Build
		req   func(*cloudbuildpb.CreateBuildRequest)
		name  string
	}{
		{name: "no steps", build: &cloudbuildpb.Build{}},
		{name: "step without name", build: buildOf(step(""))},
		{name: "duplicate ids", build: buildOf(step("a", withID("x")), step("b", withID("x")))},
		{
			name:  "forward wait",
			build: buildOf(step("a", waitFor("later")), step("b", withID("later"))),
		},
		{
			name: "script and args",
			build: buildOf(
				step("a", withArgs("x"), func(s *cloudbuildpb.BuildStep) { s.Script = "y" }),
			),
		},
		{name: "bad substitution key", build: func() *cloudbuildpb.Build {
			b := buildOf(step("a"))
			b.Substitutions = map[string]string{"lower": "x"}

			return b
		}()},
		{name: "undefined substitution", build: buildOf(step("a", withArgs("$_MISSING")))},
		{name: "bad artifact location", build: func() *cloudbuildpb.Build {
			b := buildOf(step("a"))
			b.Artifacts = &cloudbuildpb.Artifacts{
				Objects: &cloudbuildpb.Artifacts_ArtifactObjects{Location: "nope"},
			}

			return b
		}()},
		{
			name: "undefined secret env",
			build: buildOf(
				step("a", func(s *cloudbuildpb.BuildStep) { s.SecretEnv = []string{"X"} }),
			),
		},
		{name: "missing worker pool", build: func() *cloudbuildpb.Build {
			b := buildOf(step("a"))
			b.Options = &cloudbuildpb.BuildOptions{
				Pool: &cloudbuildpb.BuildOptions_PoolOption{
					Name: "projects/p/locations/l/workerPools/none",
				},
			}

			return b
		}()},
		{
			name:  "bad parent",
			build: buildOf(step("a")),
			req:   func(r *cloudbuildpb.CreateBuildRequest) { r.Parent = "organizations/x" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, nil)
			req := &cloudbuildpb.CreateBuildRequest{Parent: parent, Build: tt.build}

			if tt.req != nil {
				tt.req(req)
			}

			_, err := e.client.CreateBuild(t.Context(), req)
			assert.Equal(t, codes.InvalidArgument, status.Code(err), err)
			assert.Empty(t, e.pub.statuses(), "rejected builds publish nothing")
		})
	}
}

func TestListBuilds(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"ok": exits(0), "bad": exits(1)})

	okBuild := buildOf(step("ok"))
	okBuild.Tags = []string{"release"}

	good := e.run(t, okBuild)
	bad := e.run(t, buildOf(step("bad")))

	list := func(filter string, size int32) ([]*cloudbuildpb.Build, error) {
		it := e.client.ListBuilds(
			t.Context(),
			&cloudbuildpb.ListBuildsRequest{Parent: parent, Filter: filter, PageSize: size},
		)

		var out []*cloudbuildpb.Build

		for {
			b, err := it.Next()
			if err != nil {
				if err.Error() == "no more items in iterator" {
					return out, nil
				}

				return nil, err
			}

			out = append(out, b)
		}
	}

	tests := []struct {
		name    string
		filter  string
		want    []string
		size    int32
		wantErr bool
	}{
		{name: "all newest first", want: []string{bad.GetId(), good.GetId()}},
		{name: "paged", size: 1, want: []string{bad.GetId(), good.GetId()}},
		{name: "status", filter: `status = "SUCCESS"`, want: []string{good.GetId()}},
		{name: "not status", filter: `status != "SUCCESS"`, want: []string{bad.GetId()}},
		{
			name:   "tags and status",
			filter: `tags = "release" AND status="SUCCESS"`,
			want:   []string{good.GetId()},
		},
		{name: "build id", filter: `build_id = "` + bad.GetId() + `"`, want: []string{bad.GetId()}},
		{
			name:   "created after",
			filter: `create_time > "2000-01-01T00:00:00Z"`,
			want:   []string{bad.GetId(), good.GetId()},
		},
		{name: "created before", filter: `create_time < "2000-01-01T00:00:00Z"`, want: []string{}},
		{name: "unknown status", filter: `status = "NOPE"`, wantErr: true},
		{name: "unknown field", filter: `colour = "red"`, wantErr: true},
		{name: "garbage", filter: `???`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := list(tt.filter, tt.size)
			if tt.wantErr {
				assert.Equal(t, codes.InvalidArgument, status.Code(err), err)

				return
			}

			require.NoError(t, err)

			ids := make([]string, len(got))
			for i, b := range got {
				ids[i] = b.GetId()
			}

			assert.Equal(t, tt.want, ids)
		})
	}
}

func TestRetryAndGetErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})

	b := buildOf(step("img", withArgs("$_WHO")))
	b.Substitutions = map[string]string{"_WHO": "me"}
	first := e.run(t, b)

	op, err := e.client.RetryBuild(
		t.Context(),
		&cloudbuildpb.RetryBuildRequest{Name: first.GetName()},
	)
	require.NoError(t, err)

	second, err := op.Wait(t.Context())
	require.NoError(t, err)
	e.backend.Wait()

	assert.NotEqual(t, first.GetId(), second.GetId())
	assert.Equal(t, []string{"me"}, second.GetSteps()[0].GetArgs())
	assert.Equal(t, cloudbuildpb.Build_SUCCESS, e.get(t, second.GetName()).GetStatus())

	_, err = e.client.GetBuild(
		t.Context(),
		&cloudbuildpb.GetBuildRequest{Name: parent + "/builds/none"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.GetBuild(
		t.Context(),
		&cloudbuildpb.GetBuildRequest{
			Name: "projects/" + project + "/locations/us-east1/builds/" + first.GetId(),
		},
	)
	assert.Equal(t, codes.NotFound, status.Code(err), "a build is only visible in its own location")

	_, err = e.client.RetryBuild(
		t.Context(),
		&cloudbuildpb.RetryBuildRequest{Name: parent + "/builds/none"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.ApproveBuild(t.Context(), &cloudbuildpb.ApproveBuildRequest{
		Name: first.GetName(),
		ApprovalResult: &cloudbuildpb.ApprovalResult{
			Decision: cloudbuildpb.ApprovalResult_APPROVED,
		},
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = e.client.ApproveBuild(
		t.Context(),
		&cloudbuildpb.ApproveBuildRequest{Name: first.GetName()},
	)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRegionalBuild(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})
	regional := "projects/" + project + "/locations/europe-west1"

	op, err := e.client.CreateBuild(
		t.Context(),
		&cloudbuildpb.CreateBuildRequest{Parent: regional, Build: buildOf(step("img"))},
	)
	require.NoError(t, err)
	assert.Contains(t, op.Name(), regional+"/operations/")

	got, err := op.Wait(t.Context())
	require.NoError(t, err)
	e.backend.Wait()

	assert.Contains(t, got.GetName(), regional+"/builds/")
	assert.Contains(t, got.GetLogUrl(), "region=europe-west1")

	it := e.client.ListBuilds(t.Context(), &cloudbuildpb.ListBuildsRequest{Parent: parent})
	_, err = it.Next()
	assert.Error(t, err, "global listing does not include regional builds")
}

func TestBuildTriggers(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})

	trigger := &cloudbuildpb.BuildTrigger{
		Name:          "deploy",
		Description:   "d",
		Tags:          []string{"ci"},
		Substitutions: map[string]string{"_ENV": "prod"},
		BuildTemplate: &cloudbuildpb.BuildTrigger_Build{
			Build: buildOf(
				step("img", withArgs("$_ENV", "$BRANCH_NAME", "$SHORT_SHA", "$TRIGGER_NAME")),
			),
		},
	}

	created, err := e.client.CreateBuildTrigger(
		t.Context(),
		&cloudbuildpb.CreateBuildTriggerRequest{Parent: parent, Trigger: trigger},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, created.GetId())
	assert.Equal(t, parent+"/triggers/"+created.GetId(), created.GetResourceName())
	assert.NotNil(t, created.GetCreateTime())

	_, err = e.client.CreateBuildTrigger(
		t.Context(),
		&cloudbuildpb.CreateBuildTriggerRequest{Parent: parent, Trigger: trigger},
	)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	got, err := e.client.GetBuildTrigger(
		t.Context(),
		&cloudbuildpb.GetBuildTriggerRequest{Name: created.GetResourceName()},
	)
	require.NoError(t, err)
	assert.Equal(t, "deploy", got.GetName())

	legacy, err := e.client.GetBuildTrigger(
		t.Context(),
		&cloudbuildpb.GetBuildTriggerRequest{ProjectId: project, TriggerId: created.GetId()},
	)
	require.NoError(t, err)
	assert.Equal(t, created.GetId(), legacy.GetId())

	run, err := e.client.RunBuildTrigger(t.Context(), &cloudbuildpb.RunBuildTriggerRequest{
		Name: created.GetResourceName(),
		Source: &cloudbuildpb.RepoSource{
			RepoName: "repo", Revision: &cloudbuildpb.RepoSource_BranchName{BranchName: "main"},
		},
	})
	require.NoError(t, err)

	built, err := run.Wait(t.Context())
	require.NoError(t, err)
	e.backend.Wait()

	assert.Equal(t, created.GetId(), built.GetBuildTriggerId())
	assert.Equal(
		t,
		[]string{"prod", "main", "", "deploy"},
		e.get(t, built.GetName()).GetSteps()[0].GetArgs(),
	)
	assert.Contains(t, built.GetTags(), "trigger-"+created.GetId())

	updated, err := e.client.UpdateBuildTrigger(
		t.Context(),
		&cloudbuildpb.UpdateBuildTriggerRequest{
			ProjectId: project, TriggerId: created.GetId(),
			Trigger:    &cloudbuildpb.BuildTrigger{Description: "changed"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "changed", updated.GetDescription())
	assert.Equal(t, "deploy", updated.GetName())

	_, err = e.client.UpdateBuildTrigger(t.Context(), &cloudbuildpb.UpdateBuildTriggerRequest{
		ProjectId: project, TriggerId: created.GetId(),
		Trigger:    &cloudbuildpb.BuildTrigger{},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"id"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	it := e.client.ListBuildTriggers(
		t.Context(),
		&cloudbuildpb.ListBuildTriggersRequest{Parent: parent},
	)
	listed, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, created.GetId(), listed.GetId())

	require.NoError(
		t,
		e.client.DeleteBuildTrigger(
			t.Context(),
			&cloudbuildpb.DeleteBuildTriggerRequest{Name: created.GetResourceName()},
		),
	)

	_, err = e.client.GetBuildTrigger(
		t.Context(),
		&cloudbuildpb.GetBuildTriggerRequest{Name: created.GetResourceName()},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTriggerValidationAndFileTriggers(t *testing.T) {
	t.Parallel()

	e := newEnv(t, nil)

	tests := []struct {
		trigger *cloudbuildpb.BuildTrigger
		name    string
		want    codes.Code
	}{
		{
			name:    "no template",
			trigger: &cloudbuildpb.BuildTrigger{Name: "a"},
			want:    codes.InvalidArgument,
		},
		{
			name: "bad name",
			trigger: &cloudbuildpb.BuildTrigger{
				Name:          "a b",
				BuildTemplate: &cloudbuildpb.BuildTrigger_Filename{Filename: "cloudbuild.yaml"},
			},
			want: codes.InvalidArgument,
		},
		{name: "two event configs", trigger: &cloudbuildpb.BuildTrigger{
			Name:            "two",
			BuildTemplate:   &cloudbuildpb.BuildTrigger_Filename{Filename: "cloudbuild.yaml"},
			TriggerTemplate: &cloudbuildpb.RepoSource{RepoName: "r"},
			PubsubConfig:    &cloudbuildpb.PubsubConfig{Topic: "projects/p/topics/t"},
		}, want: codes.InvalidArgument},
		{name: "bad substitution", trigger: &cloudbuildpb.BuildTrigger{
			Name:          "subs",
			BuildTemplate: &cloudbuildpb.BuildTrigger_Filename{Filename: "x.yaml"},
			Substitutions: map[string]string{"bad": "x"},
		}, want: codes.InvalidArgument},
		{name: "filename trigger", trigger: &cloudbuildpb.BuildTrigger{
			Name:          "file",
			BuildTemplate: &cloudbuildpb.BuildTrigger_Filename{Filename: "cloudbuild.yaml"},
			TriggerTemplate: &cloudbuildpb.RepoSource{
				RepoName: "r",
				Revision: &cloudbuildpb.RepoSource_BranchName{BranchName: "main"},
			},
		}, want: codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreateBuildTrigger(
				t.Context(),
				&cloudbuildpb.CreateBuildTriggerRequest{Parent: parent, Trigger: tt.trigger},
			)
			assert.Equal(t, tt.want, status.Code(err), err)
		})
	}

	it := e.client.ListBuildTriggers(
		t.Context(),
		&cloudbuildpb.ListBuildTriggersRequest{Parent: parent},
	)
	for {
		trig, err := it.Next()
		if err != nil {
			break
		}

		if trig.GetName() != "file" {
			continue
		}

		_, err = e.client.RunBuildTrigger(
			t.Context(),
			&cloudbuildpb.RunBuildTriggerRequest{Name: trig.GetResourceName()},
		)
		assert.Equal(
			t,
			codes.FailedPrecondition,
			status.Code(err),
			"repository build configs cannot run",
		)
	}
}

func TestWebhookTrigger(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})

	created, err := e.client.CreateBuildTrigger(
		t.Context(),
		&cloudbuildpb.CreateBuildTriggerRequest{
			Parent: parent,
			Trigger: &cloudbuildpb.BuildTrigger{
				Name:          "hook",
				BuildTemplate: &cloudbuildpb.BuildTrigger_Build{Build: buildOf(step("img"))},
				WebhookConfig: &cloudbuildpb.WebhookConfig{
					AuthMethod: &cloudbuildpb.WebhookConfig_Secret{
						Secret: "projects/p/secrets/hook/versions/1",
					},
				},
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, cloudbuildpb.WebhookConfig_OK, created.GetWebhookConfig().GetState())

	_, err = e.client.ReceiveTriggerWebhook(
		t.Context(),
		&cloudbuildpb.ReceiveTriggerWebhookRequest{
			Name:   created.GetResourceName(),
			Secret: "wrong",
		},
	)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))

	_, err = e.client.ReceiveTriggerWebhook(
		t.Context(),
		&cloudbuildpb.ReceiveTriggerWebhookRequest{
			Name:   created.GetResourceName(),
			Secret: "whsec",
		},
	)
	require.NoError(t, err)
	e.backend.Wait()

	it := e.client.ListBuilds(t.Context(), &cloudbuildpb.ListBuildsRequest{Parent: parent})
	b, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, created.GetId(), b.GetBuildTriggerId())
	assert.Equal(t, cloudbuildpb.Build_SUCCESS, b.GetStatus())
}

func TestWorkerPools(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})
	regional := "projects/" + project + "/locations/us-central1"

	pool := &cloudbuildpb.WorkerPool{
		DisplayName: "pool",
		Config: &cloudbuildpb.WorkerPool_PrivatePoolV1Config{
			PrivatePoolV1Config: &cloudbuildpb.PrivatePoolV1Config{
				WorkerConfig: &cloudbuildpb.PrivatePoolV1Config_WorkerConfig{
					MachineType: "e2-medium",
					DiskSizeGb:  100,
				},
			},
		},
	}

	op, err := e.client.CreateWorkerPool(
		t.Context(),
		&cloudbuildpb.CreateWorkerPoolRequest{
			Parent:       regional,
			WorkerPool:   pool,
			WorkerPoolId: "p1",
		},
	)
	require.NoError(t, err)

	created, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, regional+"/workerPools/p1", created.GetName())
	assert.Equal(t, cloudbuildpb.WorkerPool_RUNNING, created.GetState())
	assert.NotEmpty(t, created.GetUid())
	assert.NotEmpty(t, created.GetEtag())

	_, err = e.client.CreateWorkerPool(
		t.Context(),
		&cloudbuildpb.CreateWorkerPoolRequest{
			Parent:       regional,
			WorkerPool:   pool,
			WorkerPoolId: "p1",
		},
	)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.CreateWorkerPool(
		t.Context(),
		&cloudbuildpb.CreateWorkerPoolRequest{
			Parent:       regional,
			WorkerPool:   pool,
			WorkerPoolId: "Bad_ID",
		},
	)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	b := buildOf(step("img"))
	b.Options = &cloudbuildpb.BuildOptions{
		Pool: &cloudbuildpb.BuildOptions_PoolOption{Name: created.GetName()},
	}
	built := e.run(t, b)
	assert.Equal(t, cloudbuildpb.Build_SUCCESS, built.GetStatus())

	upd, err := e.client.UpdateWorkerPool(t.Context(), &cloudbuildpb.UpdateWorkerPoolRequest{
		WorkerPool: &cloudbuildpb.WorkerPool{Name: created.GetName(), DisplayName: "renamed"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	require.NoError(t, err)

	updated, err := upd.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "renamed", updated.GetDisplayName())
	assert.NotEqual(t, created.GetEtag(), updated.GetEtag())

	it := e.client.ListWorkerPools(
		t.Context(),
		&cloudbuildpb.ListWorkerPoolsRequest{Parent: regional},
	)
	listed, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "renamed", listed.GetDisplayName())

	stale, err := e.client.DeleteWorkerPool(
		t.Context(),
		&cloudbuildpb.DeleteWorkerPoolRequest{Name: created.GetName(), Etag: "stale"},
	)
	_ = stale
	assert.Equal(t, codes.Aborted, status.Code(err))

	del, err := e.client.DeleteWorkerPool(
		t.Context(),
		&cloudbuildpb.DeleteWorkerPoolRequest{Name: created.GetName(), Etag: updated.GetEtag()},
	)
	require.NoError(t, err)
	require.NoError(t, del.Wait(t.Context()))

	_, err = e.client.GetWorkerPool(
		t.Context(),
		&cloudbuildpb.GetWorkerPoolRequest{Name: created.GetName()},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	missing, err := e.client.DeleteWorkerPool(
		t.Context(),
		&cloudbuildpb.DeleteWorkerPoolRequest{Name: created.GetName(), AllowMissing: true},
	)
	require.NoError(t, err)
	require.NoError(t, missing.Wait(t.Context()))
}

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name:   "create",
			Method: http.MethodPost,
			Path:   "/v1/projects/p/builds",
			Body:   `{"steps":[{"name":"img","args":["a"]}],"substitutions":{"_X":"1"}}`,
			Status: 200,
			Contains: []string{
				`"name":"operations/build/p/`,
				`"@type":"type.googleapis.com/google.devtools.cloudbuild.v1.BuildOperationMetadata"`,
				`"status":"QUEUED"`,
			},
		},
		{
			Name: "regional create", Method: http.MethodPost, Path: "/v1/projects/p/locations/us-east1/builds",
			Body: `{"steps":[{"name":"img"}]}`, Status: 200,
			Contains: []string{`"name":"projects/p/locations/us-east1/operations/`},
		},
		{
			Name: "invalid", Method: http.MethodPost, Path: "/v1/projects/p/builds", Body: `{}`, Status: 400,
			Contains: []string{`"status":"INVALID_ARGUMENT"`},
		},
		{
			Name: "list", Method: http.MethodGet, Path: "/v1/projects/p/builds?pageSize=5", Status: 200,
			Contains: []string{`"builds":[`, `"projectId":"p"`},
		},
		{
			Name:     "list filtered",
			Method:   http.MethodGet,
			Path:     "/v1/projects/p/locations/us-east1/builds?filter=status%3D%22CANCELLED%22",
			Status:   200,
			Contains: []string{`{}`},
		},
		{
			Name: "missing", Method: http.MethodGet, Path: "/v1/projects/p/builds/none", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "trigger", Method: http.MethodPost, Path: "/v1/projects/p/triggers",
			Body: `{"name":"t1","build":{"steps":[{"name":"img"}]}}`, Status: 200,
			Contains: []string{
				`"resourceName":"projects/p/locations/global/triggers/`,
				`"name":"t1"`,
			},
		},
		{
			Name: "triggers", Method: http.MethodGet, Path: "/v1/projects/p/triggers", Status: 200,
			Contains: []string{`"triggers":[`},
		},
		{
			Name:     "pool",
			Method:   http.MethodPost,
			Path:     "/v1/projects/p/locations/us-central1/workerPools?workerPoolId=wp1",
			Body:     `{"privatePoolV1Config":{"workerConfig":{"machineType":"e2-medium"}}}`,
			Status:   200,
			Contains: []string{`"done":true`, `"state":"RUNNING"`},
		},
		{
			Name:   "pools",
			Method: http.MethodGet,
			Path:   "/v1/projects/p/locations/us-central1/workerPools",
			Status: 200,
			Contains: []string{
				`"workerPools":[`,
				`"name":"projects/p/locations/us-central1/workerPools/wp1"`,
			},
		},
		{
			Name: "unknown operation", Method: http.MethodGet, Path: "/v1/operations/build/p/none", Status: 404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
	})

	e.backend.Wait()
}
