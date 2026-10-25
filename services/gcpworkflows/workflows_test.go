package gcpworkflows_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const hello = `
main:
  params: [args]
  steps:
    - greet:
        return: ${"hello " + args.name}
`

func TestCreateWorkflowShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	got := e.createWorkflow(t, "shape_wf", hello, func(w *workflowspb.Workflow) {
		w.Description = "greets"
		w.Labels = map[string]string{"team": "a"}
		w.ServiceAccount = "runner@test-project.iam.gserviceaccount.com"
		w.CallLogLevel = workflowspb.Workflow_LOG_ERRORS_ONLY
		w.UserEnvVars = map[string]string{"MODE": "test"}
	})

	assert.Equal(t, parent+"/workflows/shape_wf", got.GetName())
	assert.Equal(t, workflowspb.Workflow_ACTIVE, got.GetState())
	assert.Regexp(t, `^000001-[0-9a-f]{3}$`, got.GetRevisionId())
	assert.Equal(
		t,
		"projects/test-project/serviceAccounts/runner@test-project.iam.gserviceaccount.com",
		got.GetServiceAccount(),
	)
	assert.Equal(t, hello, got.GetSourceContents())
	assert.Equal(t, "greets", got.GetDescription())
	assert.Equal(t, map[string]string{"team": "a"}, got.GetLabels())
	assert.Equal(t, workflowspb.Workflow_LOG_ERRORS_ONLY, got.GetCallLogLevel())
	assert.Equal(t, map[string]string{"MODE": "test"}, got.GetUserEnvVars())
	assert.True(t, got.GetCreateTime().IsValid())
	assert.True(t, got.GetRevisionCreateTime().IsValid())

	read, err := e.wf.GetWorkflow(t.Context(), &workflowspb.GetWorkflowRequest{Name: got.GetName()})
	require.NoError(t, err)
	sameProto(t, got, read)

	op, err := e.wf.DeleteWorkflow(t.Context(), &workflowspb.DeleteWorkflowRequest{Name: got.GetName()})
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	_, err = e.wf.GetWorkflow(t.Context(), &workflowspb.GetWorkflowRequest{Name: got.GetName()})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestDefaultServiceAccount(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	got := e.createWorkflow(t, "default_sa", hello)

	assert.Regexp(
		t,
		`^projects/test-project/serviceAccounts/\d{12}-compute@developer\.gserviceaccount\.com$`,
		got.GetServiceAccount(),
	)
}

func TestCreateWorkflowErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*workflowspb.Workflow)
		name   string
		parent string
		id     string
		want   codes.Code
	}{
		{name: "bad parent", parent: "projects/p", id: "good", want: codes.InvalidArgument},
		{name: "unknown location", parent: "projects/p/locations/moon", id: "good", want: codes.InvalidArgument},
		{name: "id starts with digit", parent: parent, id: "1bad", want: codes.InvalidArgument},
		{name: "id ends with hyphen", parent: parent, id: "bad-", want: codes.InvalidArgument},
		{
			name:   "id too long",
			parent: parent,
			id: "a" + string(
				make([]byte, 0),
			) + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			want: codes.InvalidArgument,
		},
		{
			name:   "no source",
			parent: parent,
			id:     "nosrc",
			mutate: func(w *workflowspb.Workflow) { w.SourceCode = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "bad label",
			parent: parent,
			id:     "lbl",
			mutate: func(w *workflowspb.Workflow) { w.Labels = map[string]string{"A": "b"} },
			want:   codes.InvalidArgument,
		},
		{
			name:   "reserved env var",
			parent: parent,
			id:     "env",
			mutate: func(w *workflowspb.Workflow) { w.UserEnvVars = map[string]string{"GOOGLE_X": "1"} },
			want:   codes.InvalidArgument,
		},
		{
			name:   "bad service account",
			parent: parent,
			id:     "sa",
			mutate: func(w *workflowspb.Workflow) { w.ServiceAccount = "a b" },
			want:   codes.InvalidArgument,
		},
		{
			name:   "bad crypto key",
			parent: parent,
			id:     "key",
			mutate: func(w *workflowspb.Workflow) { w.CryptoKeyName = "nope" },
			want:   codes.InvalidArgument,
		},
	}

	e := newEnv(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: hello}}
			if tt.mutate != nil {
				tt.mutate(w)
			}

			_, err := e.wf.CreateWorkflow(t.Context(), &workflowspb.CreateWorkflowRequest{
				Parent: tt.parent, WorkflowId: tt.id, Workflow: w,
			})
			assert.Equal(t, tt.want, code(err), "%v", err)
		})
	}
}

func TestDuplicateWorkflow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createWorkflow(t, "dup", hello)

	_, err := e.wf.CreateWorkflow(t.Context(), &workflowspb.CreateWorkflowRequest{
		Parent: parent, WorkflowId: "dup",
		Workflow: &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: hello}},
	})
	assert.Equal(t, codes.AlreadyExists, code(err))
}

func TestDeployErrorsFailTheOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   string
	}{
		{"bad yaml", "main: [", "failed to build"},
		{"no main", "other:\n  steps:\n    - a:\n        return: 1\n", "main subworkflow is missing"},
		{"bad expression", "main:\n  steps:\n    - a:\n        return: '${1 +}'\n", "invalid expression"},
		{"unknown function", "main:\n  steps:\n    - a:\n        call: ghost\n", "not found"},
		{"unknown step field", "main:\n  steps:\n    - a:\n        bogus: 1\n", "unknown step field"},
		{
			"unknown connector",
			"main:\n  steps:\n    - a:\n        call: googleapis.nothing.v1.things.get\n",
			"not found",
		},
	}

	e := newEnv(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, err := e.wf.CreateWorkflow(t.Context(), &workflowspb.CreateWorkflowRequest{
				Parent:     parent,
				WorkflowId: "bad",
				Workflow: &workflowspb.Workflow{
					SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: tt.source},
				},
			})
			require.NoError(t, err)

			_, err = op.Wait(t.Context())
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, code(err))
			assert.Contains(t, err.Error(), "failed to build")
			assert.Contains(t, err.Error(), tt.want)
		})
	}

	_, err := e.wf.GetWorkflow(t.Context(), &workflowspb.GetWorkflowRequest{Name: parent + "/workflows/bad"})
	assert.Equal(t, codes.NotFound, code(err), "a workflow that failed to build is not stored")
}

func TestRevisions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	first := e.createWorkflow(t, "revs", hello)

	update := func(mask string, w *workflowspb.Workflow) *workflowspb.Workflow {
		w.Name = first.GetName()

		op, err := e.wf.UpdateWorkflow(t.Context(), &workflowspb.UpdateWorkflowRequest{
			Workflow: w, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{mask}},
		})
		require.NoError(t, err)

		got, err := op.Wait(t.Context())
		require.NoError(t, err)

		return got
	}

	labeled := update("labels", &workflowspb.Workflow{Labels: map[string]string{"k": "v"}})
	assert.Equal(t, first.GetRevisionId(), labeled.GetRevisionId(), "labels do not create a revision")
	assert.Equal(t, map[string]string{"k": "v"}, labeled.GetLabels())
	assert.True(
		t,
		labeled.GetUpdateTime().AsTime().Equal(first.GetUpdateTime().AsTime()) ||
			labeled.GetUpdateTime().AsTime().After(first.GetUpdateTime().AsTime()),
	)

	const v2 = "main:\n  steps:\n    - r:\n        return: 2\n"

	second := update(
		"source_contents",
		&workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: v2}},
	)
	assert.Regexp(t, `^000002-[0-9a-f]{3}$`, second.GetRevisionId())
	assert.Equal(t, v2, second.GetSourceContents())

	third := update(
		"service_account",
		&workflowspb.Workflow{ServiceAccount: "other@test-project.iam.gserviceaccount.com"},
	)
	assert.Regexp(t, `^000003-`, third.GetRevisionId())

	it := e.wf.ListWorkflowRevisions(
		t.Context(),
		&workflowspb.ListWorkflowRevisionsRequest{Name: first.GetName(), PageSize: 2},
	)

	var ids []string

	for {
		w, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)
		ids = append(ids, w.GetRevisionId())
	}

	assert.Equal(
		t,
		[]string{third.GetRevisionId(), second.GetRevisionId(), first.GetRevisionId()},
		ids,
		"newest first, across pages",
	)

	old, err := e.wf.GetWorkflow(
		t.Context(),
		&workflowspb.GetWorkflowRequest{Name: first.GetName(), RevisionId: first.GetRevisionId()},
	)
	require.NoError(t, err)
	assert.Equal(t, hello, old.GetSourceContents())
	assert.Equal(t, map[string]string{"k": "v"}, old.GetLabels(), "workflow-wide fields follow the latest revision")

	_, err = e.wf.GetWorkflow(
		t.Context(),
		&workflowspb.GetWorkflowRequest{Name: first.GetName(), RevisionId: "000009-abc"},
	)
	assert.Equal(t, codes.NotFound, code(err))
}

func TestUpdateErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	w := e.createWorkflow(t, "upd", hello)

	tests := []struct {
		name string
		wf   *workflowspb.Workflow
		mask []string
		want codes.Code
	}{
		{"missing workflow", &workflowspb.Workflow{Name: parent + "/workflows/ghost"}, nil, codes.NotFound},
		{"bad name", &workflowspb.Workflow{Name: "nope"}, nil, codes.InvalidArgument},
		{"unsupported mask", &workflowspb.Workflow{Name: w.GetName()}, []string{"state"}, codes.InvalidArgument},
		{
			"bad label",
			&workflowspb.Workflow{Name: w.GetName(), Labels: map[string]string{"X": "y"}},
			[]string{"labels"},
			codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.wf.UpdateWorkflow(t.Context(), &workflowspb.UpdateWorkflowRequest{
				Workflow: tt.wf, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.mask},
			})
			assert.Equal(t, tt.want, code(err), "%v", err)
		})
	}

	op, err := e.wf.UpdateWorkflow(t.Context(), &workflowspb.UpdateWorkflowRequest{
		Workflow: &workflowspb.Workflow{
			Name: w.GetName(), SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: "main: ["},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"source_contents"}},
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	assert.Equal(t, codes.InvalidArgument, code(err), "a bad source fails the update operation")

	read, err := e.wf.GetWorkflow(t.Context(), &workflowspb.GetWorkflowRequest{Name: w.GetName()})
	require.NoError(t, err)
	assert.Equal(t, w.GetRevisionId(), read.GetRevisionId(), "the failed update changed nothing")
}

func TestListWorkflows(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	for _, id := range []string{"wf_b", "wf_a", "wf_c"} {
		e.createWorkflow(t, id, hello)
	}

	list := func(req *workflowspb.ListWorkflowsRequest) ([]string, error) {
		it := e.wf.ListWorkflows(t.Context(), req)

		var names []string

		for {
			w, err := it.Next()
			if errors.Is(err, iterator.Done) {
				return names, nil
			}

			if err != nil {
				return nil, err
			}

			names = append(names, w.GetName())
		}
	}

	names, err := list(&workflowspb.ListWorkflowsRequest{Parent: parent, PageSize: 2})
	require.NoError(t, err)
	assert.Equal(t, []string{parent + "/workflows/wf_a", parent + "/workflows/wf_b", parent + "/workflows/wf_c"}, names)

	names, err = list(&workflowspb.ListWorkflowsRequest{Parent: parent, OrderBy: "name desc"})
	require.NoError(t, err)
	assert.Equal(t, parent+"/workflows/wf_c", names[0])

	names, err = list(&workflowspb.ListWorkflowsRequest{Parent: "projects/" + project + "/locations/-"})
	require.NoError(t, err)
	assert.Len(t, names, 3)

	names, err = list(&workflowspb.ListWorkflowsRequest{Parent: "projects/" + project + "/locations/europe-west1"})
	require.NoError(t, err)
	assert.Empty(t, names)

	_, err = list(&workflowspb.ListWorkflowsRequest{Parent: parent, Filter: "state=ACTIVE"})
	assert.Equal(t, codes.InvalidArgument, code(err))

	_, err = list(&workflowspb.ListWorkflowsRequest{Parent: parent, OrderBy: "bogus"})
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestOperations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	op, err := e.wf.CreateWorkflow(t.Context(), &workflowspb.CreateWorkflowRequest{
		Parent: parent, WorkflowId: "ops",
		Workflow: &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: hello}},
	})
	require.NoError(t, err)

	got, err := e.wf.LROClient.GetOperation(t.Context(), opRequest(op.Name()))
	require.NoError(t, err)
	assert.True(t, got.GetDone())
	assert.Contains(t, got.GetName(), parent+"/operations/")
	assert.Contains(t, got.GetMetadata().GetTypeUrl(), "google.cloud.workflows.v1.OperationMetadata")
}
