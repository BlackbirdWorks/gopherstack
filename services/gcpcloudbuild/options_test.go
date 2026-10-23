package gcpcloudbuild_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

func TestDynamicSubstitutions(t *testing.T) {
	t.Parallel()

	vals := map[string]string{"_V": "release-1.2.3", "_E": "", "_P": "a.b.c"}

	tests := []struct {
		name    string
		in      string
		want    string
		dynamic bool
		wantErr bool
	}{
		{name: "plain", in: "${_V}", want: "release-1.2.3", dynamic: true},
		{name: "replace first", in: "${_P/./-}", want: "a-b.c", dynamic: true},
		{name: "replace all", in: "${_P//./-}", want: "a-b-c", dynamic: true},
		{name: "delete", in: "${_P/.}", want: "ab.c", dynamic: true},
		{name: "trim prefix", in: "${_V#release-}", want: "1.2.3", dynamic: true},
		{name: "trim prefix glob longest", in: "${_P##*.}", want: "c", dynamic: true},
		{name: "trim prefix glob shortest", in: "${_P#*.}", want: "b.c", dynamic: true},
		{name: "trim suffix", in: "${_V%.3}", want: "release-1.2", dynamic: true},
		{name: "trim suffix glob longest", in: "${_P%%.*}", want: "a", dynamic: true},
		{name: "default empty", in: "${_E:-fallback}", want: "fallback", dynamic: true},
		{name: "default set", in: "${_V:-fallback}", want: "release-1.2.3", dynamic: true},
		{name: "alternate", in: "${_V:+yes}${_E:+no}", want: "yes", dynamic: true},
		{name: "upper", in: "${_V^^}", want: "RELEASE-1.2.3", dynamic: true},
		{name: "lower first", in: "${_P^}", want: "A.b.c", dynamic: true},
		{name: "substring", in: "${_V:8:3}", want: "1.2", dynamic: true},
		{name: "substring tail", in: "${_V:8}", want: "1.2.3", dynamic: true},
		{name: "negative offset", in: "${_V: -5}", want: "1.2.3", dynamic: true},
		{name: "length", in: "${#_V}", want: "13", dynamic: true},
		{name: "required empty", in: "${_E:?}", dynamic: true, wantErr: true},
		{name: "unknown op", in: "${_V@Q}", dynamic: true, wantErr: true},
		{name: "unset", in: "${_NOPE}", dynamic: true, wantErr: true},
		{name: "dollars", in: "$$HOME $_V", want: "$HOME release-1.2.3", dynamic: true},
		{name: "off by default", in: "${_V#release-}", want: "${_V#release-}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := buildOf(step("img", withArgs(tt.in)))
			b.Options = &cloudbuildpb.BuildOptions{DynamicSubstitutions: tt.dynamic}

			err := gcpcloudbuild.Substitute(b, vals)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, b.GetSteps()[0].GetArgs()[0])
		})
	}
}

func approvalTrigger(name string) *cloudbuildpb.BuildTrigger {
	b := buildOf(step("img", withID("only")))
	b.Approval = &cloudbuildpb.BuildApproval{Config: &cloudbuildpb.ApprovalConfig{ApprovalRequired: true}}

	return &cloudbuildpb.BuildTrigger{Name: name, BuildTemplate: &cloudbuildpb.BuildTrigger_Build{Build: b}}
}

func TestApprovalGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		decision     cloudbuildpb.ApprovalResult_Decision
		wantStatus   cloudbuildpb.Build_Status
		wantApproval cloudbuildpb.BuildApproval_State
		wantRan      bool
	}{
		{
			name: "approved", decision: cloudbuildpb.ApprovalResult_APPROVED,
			wantStatus: cloudbuildpb.Build_SUCCESS, wantApproval: cloudbuildpb.BuildApproval_APPROVED, wantRan: true,
		},
		{
			name: "rejected", decision: cloudbuildpb.ApprovalResult_REJECTED,
			wantStatus: cloudbuildpb.Build_CANCELLED, wantApproval: cloudbuildpb.BuildApproval_REJECTED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"img": exits(0)})

			trig, err := e.client.CreateBuildTrigger(t.Context(),
				&cloudbuildpb.CreateBuildTriggerRequest{Parent: parent, Trigger: approvalTrigger("gated")})
			require.NoError(t, err)

			op, err := e.client.RunBuildTrigger(t.Context(),
				&cloudbuildpb.RunBuildTriggerRequest{Name: trig.GetResourceName()})
			require.NoError(t, err)

			meta, err := op.Metadata()
			require.NoError(t, err)

			pending := e.get(t, meta.GetBuild().GetName())
			assert.Equal(t, cloudbuildpb.Build_PENDING, pending.GetStatus())
			assert.Equal(t, cloudbuildpb.BuildApproval_PENDING, pending.GetApproval().GetState())
			assert.True(t, pending.GetApproval().GetConfig().GetApprovalRequired())
			assert.Empty(t, e.rt.startedImages())

			aop, err := e.client.ApproveBuild(t.Context(), &cloudbuildpb.ApproveBuildRequest{
				Name:           pending.GetName(),
				ApprovalResult: &cloudbuildpb.ApprovalResult{Decision: tt.decision, Comment: "ok"},
			})
			require.NoError(t, err)

			_, _ = aop.Poll(t.Context())
			e.backend.Wait()

			got := e.get(t, pending.GetName())
			assert.Equal(t, tt.wantStatus, got.GetStatus())
			assert.Equal(t, tt.wantApproval, got.GetApproval().GetState())
			assert.Equal(t, "ok", got.GetApproval().GetResult().GetComment())
			assert.Equal(t, tt.wantRan, len(e.rt.startedImages()) == 1)

			_, err = e.client.ApproveBuild(t.Context(), &cloudbuildpb.ApproveBuildRequest{
				Name:           pending.GetName(),
				ApprovalResult: &cloudbuildpb.ApprovalResult{Decision: tt.decision},
			})
			require.Error(t, err)
		})
	}
}

func TestApprovalIgnoredOutsideTriggers(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})
	b := buildOf(step("img"))
	b.Approval = &cloudbuildpb.BuildApproval{Config: &cloudbuildpb.ApprovalConfig{ApprovalRequired: true}}

	got := e.run(t, b)
	assert.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus())
	assert.Nil(t, got.GetApproval())
}

func TestSchedulingMetadata(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})
	regional := "projects/" + project + "/locations/us-central1"

	op, err := e.client.CreateWorkerPool(t.Context(), &cloudbuildpb.CreateWorkerPoolRequest{
		Parent: regional, WorkerPoolId: "fast",
		WorkerPool: &cloudbuildpb.WorkerPool{Config: &cloudbuildpb.WorkerPool_PrivatePoolV1Config{
			PrivatePoolV1Config: &cloudbuildpb.PrivatePoolV1Config{
				WorkerConfig: &cloudbuildpb.PrivatePoolV1Config_WorkerConfig{
					MachineType: "e2-standard-8", DiskSizeGb: 200,
				},
			},
		}},
	})
	require.NoError(t, err)

	pool, err := op.Wait(t.Context())
	require.NoError(t, err)

	tests := []struct {
		opts *cloudbuildpb.BuildOptions
		name string
		want string
	}{
		{
			name: "pool",
			opts: &cloudbuildpb.BuildOptions{
				Pool: &cloudbuildpb.BuildOptions_PoolOption{Name: pool.GetName()},
			},
			want: `worker pool ` + pool.GetName() + ` (machine type "e2-standard-8", disk 200 GB)`,
		},
		{
			name: "machine", opts: &cloudbuildpb.BuildOptions{
				MachineType: cloudbuildpb.BuildOptions_E2_HIGHCPU_8, DiskSizeGb: 300,
			},
			want: "machine type E2_HIGHCPU_8, disk 300 GB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := buildOf(step("img"))
			b.Options = tt.opts

			cop, cErr := e.client.CreateBuild(t.Context(), &cloudbuildpb.CreateBuildRequest{
				Parent: regional, Build: b,
			})
			require.NoError(t, cErr)

			got, wErr := cop.Wait(t.Context())
			require.NoError(t, wErr)
			e.backend.Wait()

			assert.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus())
			assert.Equal(t, tt.opts.GetMachineType(), got.GetOptions().GetMachineType())
			assert.Equal(t, tt.opts.GetDiskSizeGb(), got.GetOptions().GetDiskSizeGb())
			assert.Contains(t, strings.Join(e.logs.lines(), "\n"), tt.want)
		})
	}
}
