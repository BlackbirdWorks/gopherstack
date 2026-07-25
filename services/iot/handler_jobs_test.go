package iot_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestBatch2_JobExecutions tests listing job executions.
func TestJobExecutions(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Create a job first
	iotOK(t, h, http.MethodPut, "/jobs/test-job", map[string]any{
		"targets":  []any{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document": `{"action":"update"}`,
	})

	// Cancel an execution (creates it). Real AWS IoT paths this under
	// /things/{thingName}/jobs/{jobId}/cancel, not
	// /jobs/{jobId}/things/{thingName}/cancel -- see PARITY.md.
	iotOK(t, h, http.MethodPut, "/things/my-thing/jobs/test-job/cancel", nil)

	// ListJobExecutionsForJob
	out := iotOK(t, h, http.MethodGet, "/jobs/test-job/things", nil)
	execs, _ := out["executionSummaries"].([]any)
	if len(execs) != 1 {
		t.Errorf("expected 1 execution for job, got %d", len(execs))
	}

	summary, ok := execs[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, summary, "thingArn", "real AWS nests thingArn, not thingName, in each summary")
	assert.NotContains(t, summary, "thingName")
	assert.Contains(t, summary, "jobExecutionSummary")

	// ListJobExecutionsForThing
	out2 := iotOK(t, h, http.MethodGet, "/things/my-thing/jobs", nil)
	execs2, _ := out2["executionSummaries"].([]any)
	if len(execs2) != 1 {
		t.Errorf("expected 1 execution for thing, got %d", len(execs2))
	}

	summary2, ok := execs2[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-job", summary2["jobId"])
	assert.Contains(t, summary2, "jobExecutionSummary")
}

// TestDescribeJob_WireShape verifies DescribeJob's response matches real AWS
// IoT: documentSource is a top-level field (not nested inside "job"), and
// the nested "job" object has no "document"/"documentSource"/"tags" fields
// at all -- aws-sdk-go-v2/service/iot/types.Job (v1.76.0) has none of the
// three. A previous version of this backend echoed all three back inside
// "job", which real AWS never does (document is only retrievable via
// GetJobDocument).
func TestDescribeJob_WireShape(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	iotOK(t, h, http.MethodPut, "/jobs/wire-shape-job", map[string]any{
		"targets":        []any{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document":       `{"action":"update"}`,
		"documentSource": "s3://bucket/key",
	})

	out := iotOK(t, h, http.MethodGet, "/jobs/wire-shape-job", nil)
	assert.Equal(t, "s3://bucket/key", out["documentSource"], "documentSource must be a top-level field")

	job, ok := out["job"].(map[string]any)
	require.True(t, ok, "expected nested job object, got %v", out["job"])
	assert.Equal(t, "wire-shape-job", job["jobId"])
	assert.NotContains(t, job, "document", "real AWS Job has no document field")
	assert.NotContains(t, job, "documentSource", "real AWS Job has no documentSource field")
	assert.NotContains(t, job, "tags", "real AWS Job has no tags field")
}

func TestBackend_DescribeManagedJobTemplate(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	tmpl, err := b.DescribeManagedJobTemplate("AWS-Reboot", "")
	require.NoError(t, err)
	assert.Equal(t, "AWS-Reboot", tmpl.TemplateName)
	assert.NotEmpty(t, tmpl.TemplateArn)
	assert.NotEmpty(t, tmpl.Document)
	assert.NotEmpty(t, tmpl.DocumentParameters)

	_, err = b.DescribeManagedJobTemplate("does-not-exist", "")
	require.ErrorIs(t, err, iot.ErrManagedJobTemplateNotFound)
}

func TestBackend_ListManagedJobTemplates(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	all := b.ListManagedJobTemplates("")
	assert.GreaterOrEqual(t, len(all), 2)

	filtered := b.ListManagedJobTemplates("AWS-Reboot")
	require.Len(t, filtered, 1)
	assert.Equal(t, "AWS-Reboot", filtered[0].TemplateName)
}

func TestHandler_ManagedJobTemplate(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	out := iotOK(t, h, http.MethodGet, "/managed-job-templates", nil)
	templates, _ := out["managedJobTemplates"].([]any)
	assert.NotEmpty(t, templates)

	out2 := iotOK(t, h, http.MethodGet, "/managed-job-templates/AWS-Reboot", nil)
	assert.Equal(t, "AWS-Reboot", out2["templateName"])

	iotExpectError(t, h, "/managed-job-templates/does-not-exist")
}

// TestNewOps_JobExecution tests job execution lifecycle.
func TestJobExecution(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// Create job first.
	iotOK(t, h, http.MethodPut, "/jobs/exec-job", map[string]any{
		"targets":  []string{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document": `{}`,
	})

	// CancelJobExecution (creates execution if not exists). Real AWS IoT
	// paths Describe/Cancel/DeleteJobExecution under
	// /things/{thingName}/jobs/{jobId}[...], not
	// /jobs/{jobId}/things/{thingName} -- see PARITY.md.
	iotOK(t, h, http.MethodPut, "/things/my-thing/jobs/exec-job/cancel", nil)

	// DescribeJobExecution
	out := iotOK(t, h, http.MethodGet, "/things/my-thing/jobs/exec-job", nil)
	exec, ok := out["execution"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution in response: %v", out)
	}
	if exec["jobId"] != "exec-job" {
		t.Errorf("exec jobId mismatch: %v", exec)
	}
	assert.Contains(t, exec, "thingArn", "real AWS's JobExecution has thingArn, not thingName")
	assert.NotContains(t, exec, "thingName")
	assert.EqualValues(t, "CANCELED", exec["status"])

	// DeleteJobExecution
	iotOK(t, h, http.MethodDelete, "/things/my-thing/jobs/exec-job/executionNumber/1", nil)
}

// TestJobExecution_RoutingAndStateGuards is a table-driven regression test
// covering two classes of previously-undiscovered bugs found while closing
// PARITY.md's job_and_jobtemplate gap:
//
//   - DescribeJobExecution/CancelJobExecution/DeleteJobExecution were routed
//     under /jobs/{jobId}/things/{thingName}[...], a path no real AWS SDK
//     client ever sends (real AWS uses /things/{thingName}/jobs/{jobId}[...],
//     confirmed against aws-sdk-go-v2/service/iot@v1.76.0's serializers.go
//     http bindings) -- all three ops were completely unreachable by a real
//     client.
//   - CancelJobExecution/DeleteJobExecution ignored force/expectedVersion/
//     statusDetails entirely; real AWS rejects canceling/deleting a
//     non-terminal execution without force=true (InvalidStateTransitionException),
//     and rejects a mismatched expectedVersion (VersionConflictException).
func TestJobExecution_RoutingAndStateGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *iot.InMemoryBackend)
		run   func(t *testing.T, h *iot.Handler)
		name  string
	}{
		{
			name: "old_jobId_things_thingName_path_no_longer_routes",
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				rec := iotRequest(t, h, http.MethodPut, "/jobs/old-job/things/my-thing/cancel", nil)
				assert.NotEqual(t, http.StatusOK, rec.Code,
					"the pre-fix path shape must not route to CancelJobExecution any more")
			},
		},
		{
			name: "describe_and_cancel_use_real_thing_scoped_path_with_correct_wire_shape",
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()

				iotOK(t, h, http.MethodPut, "/jobs/wire-job", map[string]any{
					"targets":  []any{"arn:aws:iot:us-east-1:000000000000:thing/wire-thing"},
					"document": `{}`,
				})
				iotOK(t, h, http.MethodPut, "/things/wire-thing/jobs/wire-job/cancel", nil)

				out := iotOK(t, h, http.MethodGet, "/things/wire-thing/jobs/wire-job", nil)
				exec, ok := out["execution"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, exec, "thingArn")
				assert.NotContains(t, exec, "thingName")
				assert.EqualValues(t, "CANCELED", exec["status"])
				assert.EqualValues(t, 1, exec["executionNumber"])
				// CreateJob now fans a real QUEUED JobExecution out to
				// wire-thing at version 1 (see PARITY.md's
				// job_and_jobtemplate fan-out fix); the cancel above is a
				// real QUEUED->CANCELED update to that row, so version
				// increments to 2 -- not created fresh at version 1 via
				// CancelJobExecution's create-on-miss fallback.
				assert.EqualValues(t, 2, exec["versionNumber"])
			},
		},
		{
			name: "cancel_in_progress_without_force_is_rejected",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "ip-job", ThingName: "ip-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				rec := iotRequest(t, h, http.MethodPut, "/things/ip-thing/jobs/ip-job/cancel", nil)
				assert.Equal(t, http.StatusConflict, rec.Code)

				var body map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "InvalidStateTransitionException", body["__type"])
			},
		},
		{
			name: "cancel_in_progress_with_force_succeeds",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "force-job", ThingName: "force-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				iotOK(t, h, http.MethodPut, "/things/force-thing/jobs/force-job/cancel",
					map[string]any{"force": true})

				out := iotOK(t, h, http.MethodGet, "/things/force-thing/jobs/force-job", nil)
				exec, ok := out["execution"].(map[string]any)
				require.True(t, ok)
				assert.EqualValues(t, "CANCELED", exec["status"])
				assert.Equal(t, true, exec["forceCanceled"])
				assert.EqualValues(t, 2, exec["versionNumber"])
			},
		},
		{
			name: "cancel_expectedVersion_mismatch_returns_conflict",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "ver-job", ThingName: "ver-thing",
					Status: iot.JobExecQueued, VersionNumber: 5,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				rec := iotRequest(t, h, http.MethodPut, "/things/ver-thing/jobs/ver-job/cancel",
					map[string]any{"expectedVersion": 1})
				assert.Equal(t, http.StatusConflict, rec.Code)

				var body map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "VersionConflictException", body["__type"])
			},
		},
		{
			name: "delete_non_terminal_without_force_is_rejected",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "del-job", ThingName: "del-thing",
					Status: iot.JobExecQueued, VersionNumber: 1,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				rec := iotRequest(t, h, http.MethodDelete,
					"/things/del-thing/jobs/del-job/executionNumber/1", nil)
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "delete_non_terminal_with_force_succeeds",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "del-force-job", ThingName: "del-force-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				iotOK(t, h, http.MethodDelete,
					"/things/del-force-thing/jobs/del-force-job/executionNumber/1?force=true", nil)
				iotExpectError(t, h, "/things/del-force-thing/jobs/del-force-job")
			},
		},
		{
			name: "listJobExecutions_use_nested_summary_wire_shape",
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()

				iotOK(t, h, http.MethodPut, "/jobs/list-job", map[string]any{
					"targets":  []any{"arn:aws:iot:us-east-1:000000000000:thing/list-thing"},
					"document": `{}`,
				})
				iotOK(t, h, http.MethodPut, "/things/list-thing/jobs/list-job/cancel", nil)

				forJob := iotOK(t, h, http.MethodGet, "/jobs/list-job/things", nil)
				jobSummaries, _ := forJob["executionSummaries"].([]any)
				require.Len(t, jobSummaries, 1)
				jobEntry, ok := jobSummaries[0].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, jobEntry, "thingArn")
				assert.NotContains(t, jobEntry, "thingName")
				jobExecSummary, ok := jobEntry["jobExecutionSummary"].(map[string]any)
				require.True(t, ok)
				assert.EqualValues(t, "CANCELED", jobExecSummary["status"])

				forThing := iotOK(t, h, http.MethodGet, "/things/list-thing/jobs", nil)
				thingSummaries, _ := forThing["executionSummaries"].([]any)
				require.Len(t, thingSummaries, 1)
				thingEntry, ok := thingSummaries[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "list-job", thingEntry["jobId"])
				assert.Contains(t, thingEntry, "jobExecutionSummary")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			h := iot.NewHandler(b, nil)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			tt.run(t, h)
		})
	}
}

// newIoTSDKClient stands up a real HTTP server fronting a fresh IoT handler
// and returns a real generated AWS SDK v2 IoT client pointed at it, plus the
// backing InMemoryBackend for setup that has no public HTTP surface (e.g.
// AddThingToThingGroup has no corresponding CreateJob-time semantics to
// probe). Round-tripping through a real client's own serializer/deserializer
// is what proves the wire shape is actually correct, rather than merely
// matching gopherstack's own JSON encoding -- see parity-principles.md rule 3
// and PARITY.md's elasticache note ("the previous backend-struct assertions
// could not see it").
func newIoTSDKClient(t *testing.T) (*iotsdk.Client, *iot.InMemoryBackend) {
	t.Helper()

	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)

	e := echo.New()
	registry := service.NewRegistry()
	_ = registry.Register(h)
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := iotsdk.NewFromConfig(cfg, func(o *iotsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client, b
}

// TestJob_FanOutAndAdvancedFields_SDKRoundTrip is a table-driven regression
// test, asserted through a real generated AWS SDK v2 IoT client, covering
// this pass's two job_and_jobtemplate fixes:
//
//   - CreateJob now fans a real QUEUED JobExecution out to every thing a job
//     targets (directly, or as a member of a targeted thing group), instead
//     of only ever materializing an execution lazily via
//     CancelJobExecution's create-on-miss fallback. DeleteThing cascades the
//     cleanup so a deleted thing never leaves a ghost JobExecution behind.
//   - Job's previously-unmodeled advanced fields (jobExecutionsRetryConfig,
//     presignedUrlConfig, schedulingConfig incl. maintenanceWindows,
//     destinationPackageVersions, and the computed jobProcessDetails rollup)
//     round-trip end to end: request parsing, backend state, and response
//     wire shape.
func TestJob_FanOutAndAdvancedFields_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *iot.InMemoryBackend)
		run   func(t *testing.T, ctx context.Context, client *iotsdk.Client)
		name  string
	}{
		{
			name: "advanced_fields_round_trip_through_CreateJob_and_DescribeJob",
			run: func(t *testing.T, ctx context.Context, client *iotsdk.Client) {
				t.Helper()

				_, err := client.CreateJob(ctx, &iotsdk.CreateJobInput{
					JobId:   aws.String("advanced-job"),
					Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/adv-thing"},
					JobExecutionsRetryConfig: &iottypes.JobExecutionsRetryConfig{
						CriteriaList: []iottypes.RetryCriteria{
							{FailureType: iottypes.RetryableFailureTypeFailed, NumberOfRetries: aws.Int32(3)},
						},
					},
					PresignedUrlConfig: &iottypes.PresignedUrlConfig{
						RoleArn:      aws.String("arn:aws:iam::000000000000:role/PresignRole"),
						ExpiresInSec: aws.Int64(120),
					},
					SchedulingConfig: &iottypes.SchedulingConfig{
						StartTime:   aws.String("2030-01-01T00:00"),
						EndTime:     aws.String("2030-01-01T01:00"),
						EndBehavior: iottypes.JobEndBehaviorStopRollout,
						MaintenanceWindows: []iottypes.MaintenanceWindow{
							{StartTime: aws.String("2030-01-01T00:00"), DurationInMinutes: aws.Int32(30)},
						},
					},
					DestinationPackageVersions: []string{
						"arn:aws:iot:us-east-1:000000000000:package/p/version/v1",
					},
				})
				require.NoError(t, err)

				out, err := client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: aws.String("advanced-job")})
				require.NoError(t, err)
				require.NotNil(t, out.Job)

				require.NotNil(t, out.Job.JobExecutionsRetryConfig)
				require.Len(t, out.Job.JobExecutionsRetryConfig.CriteriaList, 1)
				assert.Equal(t,
					iottypes.RetryableFailureTypeFailed,
					out.Job.JobExecutionsRetryConfig.CriteriaList[0].FailureType,
				)
				assert.EqualValues(t, 3, *out.Job.JobExecutionsRetryConfig.CriteriaList[0].NumberOfRetries)

				require.NotNil(t, out.Job.PresignedUrlConfig)
				assert.Equal(t, "arn:aws:iam::000000000000:role/PresignRole", *out.Job.PresignedUrlConfig.RoleArn)
				assert.EqualValues(t, 120, *out.Job.PresignedUrlConfig.ExpiresInSec)

				require.NotNil(t, out.Job.SchedulingConfig)
				assert.Equal(t, iottypes.JobEndBehaviorStopRollout, out.Job.SchedulingConfig.EndBehavior)
				require.Len(t, out.Job.SchedulingConfig.MaintenanceWindows, 1)
				assert.EqualValues(t, 30, *out.Job.SchedulingConfig.MaintenanceWindows[0].DurationInMinutes)

				assert.Equal(t,
					[]string{"arn:aws:iot:us-east-1:000000000000:package/p/version/v1"},
					out.Job.DestinationPackageVersions,
				)

				require.NotNil(
					t,
					out.Job.JobProcessDetails,
					"jobProcessDetails should be computed from the real fanned-out execution",
				)
				assert.EqualValues(t, 1, *out.Job.JobProcessDetails.NumberOfQueuedThings)
				assert.Equal(t, []string{"adv-thing"}, out.Job.JobProcessDetails.ProcessingTargets)
			},
		},
		{
			name: "createJob_fans_out_a_queued_execution_without_needing_cancel_first",
			run: func(t *testing.T, ctx context.Context, client *iotsdk.Client) {
				t.Helper()

				_, err := client.CreateJob(ctx, &iotsdk.CreateJobInput{
					JobId:   aws.String("fanout-job"),
					Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/fanout-thing"},
				})
				require.NoError(t, err)

				out, err := client.DescribeJobExecution(ctx, &iotsdk.DescribeJobExecutionInput{
					JobId:     aws.String("fanout-job"),
					ThingName: aws.String("fanout-thing"),
				})
				require.NoError(
					t,
					err,
					"a real JobExecution row must already exist post-CreateJob, "+
						"not only after CancelJobExecution's create-on-miss fallback",
				)
				require.NotNil(t, out.Execution)
				assert.Equal(t, iottypes.JobExecutionStatusQueued, out.Execution.Status)
				assert.EqualValues(t, 1, *out.Execution.ExecutionNumber)
				assert.EqualValues(t, 1, out.Execution.VersionNumber)
			},
		},
		{
			name: "thing_group_target_fans_out_to_direct_members_only",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "fleet-a"})
				require.NoError(t, err)
				require.NoError(t, b.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
					ThingGroupName: "fleet-a", ThingName: "fleet-thing-1",
				}))
				require.NoError(t, b.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
					ThingGroupName: "fleet-a", ThingName: "fleet-thing-2",
				}))
			},
			run: func(t *testing.T, ctx context.Context, client *iotsdk.Client) {
				t.Helper()

				_, err := client.CreateJob(ctx, &iotsdk.CreateJobInput{
					JobId:   aws.String("group-job"),
					Targets: []string{"arn:aws:iot:us-east-1:000000000000:thinggroup/fleet-a"},
				})
				require.NoError(t, err)

				out, err := client.ListJobExecutionsForJob(ctx, &iotsdk.ListJobExecutionsForJobInput{
					JobId: aws.String("group-job"),
				})
				require.NoError(t, err)
				require.Len(
					t,
					out.ExecutionSummaries,
					2,
					"both direct group members should have a fanned-out execution",
				)

				arns := make([]string, 0, len(out.ExecutionSummaries))
				for _, s := range out.ExecutionSummaries {
					arns = append(arns, *s.ThingArn)
					require.NotNil(t, s.JobExecutionSummary)
					assert.Equal(t, iottypes.JobExecutionStatusQueued, s.JobExecutionSummary.Status)
				}
				assert.ElementsMatch(t, []string{
					"arn:aws:iot:us-east-1:000000000000:thing/fleet-thing-1",
					"arn:aws:iot:us-east-1:000000000000:thing/fleet-thing-2",
				}, arns)
			},
		},
		{
			name: "deleteThing_cascades_jobExecution_cleanup",
			run: func(t *testing.T, ctx context.Context, client *iotsdk.Client) {
				t.Helper()

				_, err := client.CreateThing(ctx, &iotsdk.CreateThingInput{ThingName: aws.String("cascade-thing")})
				require.NoError(t, err)

				_, err = client.CreateJob(ctx, &iotsdk.CreateJobInput{
					JobId:   aws.String("cascade-job"),
					Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/cascade-thing"},
				})
				require.NoError(t, err)

				_, err = client.DescribeJobExecution(ctx, &iotsdk.DescribeJobExecutionInput{
					JobId: aws.String("cascade-job"), ThingName: aws.String("cascade-thing"),
				})
				require.NoError(t, err, "precondition: execution must exist before delete")

				_, err = client.DeleteThing(ctx, &iotsdk.DeleteThingInput{ThingName: aws.String("cascade-thing")})
				require.NoError(t, err)

				out, err := client.ListJobExecutionsForJob(ctx, &iotsdk.ListJobExecutionsForJobInput{
					JobId: aws.String("cascade-job"),
				})
				require.NoError(t, err)
				assert.Empty(
					t,
					out.ExecutionSummaries,
					"deleting the target thing must not leave a ghost JobExecution behind",
				)
			},
		},
		{
			name: "associateTargetsWithJob_merges_targets_and_fans_out_new_executions",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateJob(&iot.CreateJobInput{
					JobID:           "assoc-job",
					Targets:         []string{"arn:aws:iot:us-east-1:000000000000:thing/assoc-thing-1"},
					TargetSelection: "CONTINUOUS",
				})
				require.NoError(t, err)
			},
			run: func(t *testing.T, ctx context.Context, client *iotsdk.Client) {
				t.Helper()

				_, err := client.AssociateTargetsWithJob(ctx, &iotsdk.AssociateTargetsWithJobInput{
					JobId:   aws.String("assoc-job"),
					Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/assoc-thing-2"},
				})
				require.NoError(t, err)

				describeOut, err := client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: aws.String("assoc-job")})
				require.NoError(t, err)
				assert.ElementsMatch(t, []string{
					"arn:aws:iot:us-east-1:000000000000:thing/assoc-thing-1",
					"arn:aws:iot:us-east-1:000000000000:thing/assoc-thing-2",
				}, describeOut.Job.Targets, "newly associated targets must be merged into the job's own Targets list")

				execOut, err := client.DescribeJobExecution(ctx, &iotsdk.DescribeJobExecutionInput{
					JobId: aws.String("assoc-job"), ThingName: aws.String("assoc-thing-2"),
				})
				require.NoError(t, err, "a newly associated target must get a fanned-out execution immediately")
				assert.Equal(t, iottypes.JobExecutionStatusQueued, execOut.Execution.Status)
			},
		},
		{
			// ListJobs (GET /jobs, no trailing slash) and the entire
			// /job-templates family were both missing from this service's
			// RouteMatcher whitelist -- a real client's request would never
			// even reach the IoT handler's op dispatch (it matched no
			// registered service route at all), a distinct bug class from
			// the CreateJob/CreateJobTemplate method mismatches covered
			// above. Only a real SDK client driven through the actual
			// service.Router path (as this whole table does) can catch it;
			// direct h.Handler() invocation bypasses RouteMatcher entirely.
			name: "listJobs_and_jobTemplate_CRUD_reach_the_handler_through_the_real_router",
			run: func(t *testing.T, ctx context.Context, client *iotsdk.Client) {
				t.Helper()

				_, err := client.CreateJob(ctx, &iotsdk.CreateJobInput{
					JobId:   aws.String("routable-job"),
					Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/routable-thing"},
				})
				require.NoError(t, err)

				listOut, err := client.ListJobs(ctx, &iotsdk.ListJobsInput{})
				require.NoError(t, err)
				require.Len(t, listOut.Jobs, 1)
				assert.Equal(t, "routable-job", *listOut.Jobs[0].JobId)

				_, err = client.CreateJobTemplate(ctx, &iotsdk.CreateJobTemplateInput{
					JobTemplateId: aws.String("routable-template"),
					Document:      aws.String(`{}`),
					Description:   aws.String("routable template"),
				})
				require.NoError(t, err)

				descOut, err := client.DescribeJobTemplate(ctx, &iotsdk.DescribeJobTemplateInput{
					JobTemplateId: aws.String("routable-template"),
				})
				require.NoError(t, err)
				assert.Equal(t, "routable-template", *descOut.JobTemplateId)

				listTmplOut, err := client.ListJobTemplates(ctx, &iotsdk.ListJobTemplatesInput{})
				require.NoError(t, err)
				require.Len(t, listTmplOut.JobTemplates, 1)

				_, err = client.DeleteJobTemplate(ctx, &iotsdk.DeleteJobTemplateInput{
					JobTemplateId: aws.String("routable-template"),
				})
				require.NoError(t, err)
			},
		},
		{
			// JobTemplate's advanced fields were field-diffed against
			// DescribeJobTemplateOutput separately from Job's: real AWS
			// models jobExecutionsRetryConfig/presignedUrlConfig/
			// destinationPackageVersions/maintenanceWindows on the template
			// side too, but -- unlike Job -- maintenanceWindows is a
			// TOP-LEVEL field, not nested inside a schedulingConfig (real
			// JobTemplate has no schedulingConfig field at all).
			name: "jobTemplate_advanced_fields_round_trip",
			run: func(t *testing.T, ctx context.Context, client *iotsdk.Client) {
				t.Helper()

				_, err := client.CreateJobTemplate(ctx, &iotsdk.CreateJobTemplateInput{
					JobTemplateId: aws.String("advanced-template"),
					Document:      aws.String(`{}`),
					Description:   aws.String("advanced template"),
					JobExecutionsRetryConfig: &iottypes.JobExecutionsRetryConfig{
						CriteriaList: []iottypes.RetryCriteria{
							{FailureType: iottypes.RetryableFailureTypeTimedOut, NumberOfRetries: aws.Int32(1)},
						},
					},
					PresignedUrlConfig: &iottypes.PresignedUrlConfig{
						RoleArn: aws.String("arn:aws:iam::000000000000:role/TemplatePresignRole"),
					},
					DestinationPackageVersions: []string{
						"arn:aws:iot:us-east-1:000000000000:package/p/version/v2",
					},
					MaintenanceWindows: []iottypes.MaintenanceWindow{
						{StartTime: aws.String("2030-02-01T00:00"), DurationInMinutes: aws.Int32(45)},
					},
				})
				require.NoError(t, err)

				out, err := client.DescribeJobTemplate(ctx, &iotsdk.DescribeJobTemplateInput{
					JobTemplateId: aws.String("advanced-template"),
				})
				require.NoError(t, err)

				require.NotNil(t, out.JobExecutionsRetryConfig)
				require.Len(t, out.JobExecutionsRetryConfig.CriteriaList, 1)
				assert.Equal(t,
					iottypes.RetryableFailureTypeTimedOut,
					out.JobExecutionsRetryConfig.CriteriaList[0].FailureType,
				)

				require.NotNil(t, out.PresignedUrlConfig)
				assert.Equal(t, "arn:aws:iam::000000000000:role/TemplatePresignRole", *out.PresignedUrlConfig.RoleArn)

				assert.Equal(t,
					[]string{"arn:aws:iot:us-east-1:000000000000:package/p/version/v2"},
					out.DestinationPackageVersions,
				)

				require.Len(
					t,
					out.MaintenanceWindows,
					1,
					"maintenanceWindows must be top-level on JobTemplate, not nested",
				)
				assert.EqualValues(t, 45, *out.MaintenanceWindows[0].DurationInMinutes)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newIoTSDKClient(t)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			tt.run(t, t.Context(), client)
		})
	}
}

// TestNewOps_JobTemplate tests JobTemplate CRUD.
func TestJobTemplate(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateJobTemplate
	out := iotOK(t, h, http.MethodPut, "/job-templates/my-template", map[string]any{
		"description": "test template",
		"document":    `{"version":"1.0"}`,
	})
	if out["jobTemplateId"] != "my-template" {
		t.Errorf("jobTemplateId mismatch: %v", out)
	}

	// DescribeJobTemplate
	out2 := iotOK(t, h, http.MethodGet, "/job-templates/my-template", nil)
	if out2["jobTemplateId"] != "my-template" {
		t.Errorf("describe template mismatch: %v", out2)
	}

	// ListJobTemplates
	out3 := iotOK(t, h, http.MethodGet, "/job-templates", nil)
	templates, _ := out3["jobTemplates"].([]any)
	if len(templates) != 1 {
		t.Errorf("expected 1 template, got %d", len(templates))
	}

	// DeleteJobTemplate
	iotOK(t, h, http.MethodDelete, "/job-templates/my-template", nil)

	// Verify deletion
	iotExpectError(t, h, "/job-templates/my-template")
}
