package emr_test

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/emr"
)

// newTestEMRClient spins up a real HTTP server fronting h and returns a real
// aws-sdk-go-v2 emr client pointed at it, so tests exercise the actual
// generated serializer/deserializer instead of gopherstack's own request/
// response structs (which cannot detect a wrong wire key by construction).
func newTestEMRClient(t *testing.T, h *emr.Handler) *emrsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return emrsdk.NewFromConfig(cfg, func(o *emrsdk.Options) {
		o.BaseEndpoint = awssdk.String(srv.URL)
	})
}

// TestWireShape_Step_ConfigKey_RoundTrip proves DescribeStep/ListSteps emit
// the Hadoop JAR details under the real "Config" wire key
// (types.HadoopStepConfig, emr@v1.64.4 deserializers.go's "Config" case in
// awsAwsjson11_deserializeDocumentStep), not "HadoopJarStep" -- the
// request-side StepConfig convention this backend previously (and
// incorrectly) reused for the response too. A real client's typed
// Step.Config/StepSummary.Config was nil for every step regardless of
// backend state before this fix; this test cannot compile-pass, let alone
// assert-pass, against the old flat/wrong-key shape. It also proves
// Properties (real on both directions, types.HadoopJarStepConfig/
// types.HadoopStepConfig) round-trips, since it was previously unmodeled and
// silently dropped.
func TestWireShape_Step_ConfigKey_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)
	h := emr.NewHandler(backend)
	client := newTestEMRClient(t, h)
	ctx := t.Context()

	runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
		Name:      awssdk.String("step-config-cluster"),
		Instances: &emrtypes.JobFlowInstancesConfig{},
		Steps: []emrtypes.StepConfig{
			{
				Name: awssdk.String("step-one"),
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:       awssdk.String("s3://bucket/job.jar"),
					MainClass: awssdk.String("com.example.Main"),
					Args:      []string{"--verbose"},
					Properties: []emrtypes.KeyValue{
						{Key: awssdk.String("spark.executor.memory"), Value: awssdk.String("4g")},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListSteps(ctx, &emrsdk.ListStepsInput{ClusterId: runOut.JobFlowId})
	require.NoError(t, err)
	require.Len(t, listOut.Steps, 1)
	require.NotNil(t, listOut.Steps[0].Config, "StepSummary.Config must be populated, not nil")
	assert.Equal(t, "s3://bucket/job.jar", awssdk.ToString(listOut.Steps[0].Config.Jar))
	assert.Equal(t, "com.example.Main", awssdk.ToString(listOut.Steps[0].Config.MainClass))
	require.Len(t, listOut.Steps[0].Config.Properties, 1)
	assert.Equal(t, "4g", listOut.Steps[0].Config.Properties["spark.executor.memory"])

	descOut, err := client.DescribeStep(ctx, &emrsdk.DescribeStepInput{
		ClusterId: runOut.JobFlowId,
		StepId:    listOut.Steps[0].Id,
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Step)
	require.NotNil(t, descOut.Step.Config, "Step.Config must be populated, not nil")
	assert.Equal(t, "s3://bucket/job.jar", awssdk.ToString(descOut.Step.Config.Jar))
	require.Len(t, descOut.Step.Config.Properties, 1)
	assert.Equal(t, "4g", descOut.Step.Config.Properties["spark.executor.memory"])
}

// TestWireShape_RunJobFlow_StepExecutionRoleArn proves RunJobFlowInput's
// call-level StepExecutionRoleArn (emr@v1.64.4 api_op_RunJobFlow.go, applies
// to every initial step) is threaded through to DescribeStep's
// Step.ExecutionRoleArn on read-back, rather than silently discarded.
// Asserted via DescribeStep, not ListSteps: real types.StepSummary
// (deserializers.go's awsAwsjson11_deserializeDocumentStepSummary) has no
// ExecutionRoleArn member at all -- only types.Step (DescribeStep's shape)
// does.
func TestWireShape_RunJobFlow_StepExecutionRoleArn(t *testing.T) {
	t.Parallel()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)
	h := emr.NewHandler(backend)
	client := newTestEMRClient(t, h)
	ctx := t.Context()

	runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
		Name:                 awssdk.String("step-role-cluster"),
		Instances:            &emrtypes.JobFlowInstancesConfig{},
		StepExecutionRoleArn: awssdk.String("arn:aws:iam::000000000000:role/step-runtime-role"),
		Steps: []emrtypes.StepConfig{
			{
				Name:          awssdk.String("step-one"),
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{Jar: awssdk.String("s3://bucket/job.jar")},
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListSteps(ctx, &emrsdk.ListStepsInput{ClusterId: runOut.JobFlowId})
	require.NoError(t, err)
	require.Len(t, listOut.Steps, 1)

	descOut, err := client.DescribeStep(ctx, &emrsdk.DescribeStepInput{
		ClusterId: runOut.JobFlowId,
		StepId:    listOut.Steps[0].Id,
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Step)
	assert.Equal(t, "arn:aws:iam::000000000000:role/step-runtime-role",
		awssdk.ToString(descOut.Step.ExecutionRoleArn))
}

// TestWireShape_AddJobFlowSteps_ExecutionRoleArn proves
// AddJobFlowStepsInput's call-level ExecutionRoleArn (emr@v1.64.4
// api_op_AddJobFlowSteps.go) is threaded through to the new step's
// Step.ExecutionRoleArn on read-back (via DescribeStep -- see
// TestWireShape_RunJobFlow_StepExecutionRoleArn for why not ListSteps),
// rather than silently discarded.
func TestWireShape_AddJobFlowSteps_ExecutionRoleArn(t *testing.T) {
	t.Parallel()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)
	h := emr.NewHandler(backend)
	client := newTestEMRClient(t, h)
	ctx := t.Context()

	runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
		Name:      awssdk.String("add-step-role-cluster"),
		Instances: &emrtypes.JobFlowInstancesConfig{},
	})
	require.NoError(t, err)

	addOut, err := client.AddJobFlowSteps(ctx, &emrsdk.AddJobFlowStepsInput{
		JobFlowId:        runOut.JobFlowId,
		ExecutionRoleArn: awssdk.String("arn:aws:iam::000000000000:role/added-step-role"),
		Steps: []emrtypes.StepConfig{
			{
				Name:          awssdk.String("added-step"),
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{Jar: awssdk.String("s3://bucket/other.jar")},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, addOut.StepIds, 1)

	descOut, err := client.DescribeStep(ctx, &emrsdk.DescribeStepInput{
		ClusterId: runOut.JobFlowId,
		StepId:    awssdk.String(addOut.StepIds[0]),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Step)
	assert.Equal(t, "arn:aws:iam::000000000000:role/added-step-role",
		awssdk.ToString(descOut.Step.ExecutionRoleArn))
}

// TestWireShape_DescribeJobFlows_LastStateChangeReason proves the legacy
// DescribeJobFlows response keys its execution status message
// LastStateChangeReason, not StateChangeReason: real
// types.JobFlowExecutionStatusDetail (emr@v1.64.4 deserializers.go's
// awsAwsjson11_deserializeDocumentJobFlowExecutionStatusDetail case list) has
// no StateChangeReason member at all. Before this fix every real client's
// LastStateChangeReason decoded empty regardless of backend state.
func TestWireShape_DescribeJobFlows_LastStateChangeReason(t *testing.T) {
	t.Parallel()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)
	h := emr.NewHandler(backend)
	client := newTestEMRClient(t, h)
	ctx := t.Context()

	runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
		Name:      awssdk.String("job-flows-state-reason-cluster"),
		Instances: &emrtypes.JobFlowInstancesConfig{},
	})
	require.NoError(t, err)

	_, err = client.TerminateJobFlows(ctx, &emrsdk.TerminateJobFlowsInput{
		JobFlowIds: []string{awssdk.ToString(runOut.JobFlowId)},
	})
	require.NoError(t, err)

	//nolint:staticcheck // SA1019: DescribeJobFlows is deprecated but still real on the wire
	descOut, err := client.DescribeJobFlows(ctx, &emrsdk.DescribeJobFlowsInput{
		JobFlowIds: []string{awssdk.ToString(runOut.JobFlowId)},
	})
	require.NoError(t, err)
	require.Len(t, descOut.JobFlows, 1)
	require.NotNil(t, descOut.JobFlows[0].ExecutionStatusDetail)
	assert.Equal(t, "Terminated by user request",
		awssdk.ToString(descOut.JobFlows[0].ExecutionStatusDetail.LastStateChangeReason))
}

// TestWireShape_Cluster_TerminatedAt_NotOnWire proves the internal
// terminatedAt cleanup timestamp (janitor.go's TTL sweep) never reaches a
// real client: real types.Cluster has no such member (emr@v1.64.4
// deserializers.go's awsAwsjson11_deserializeDocumentCluster case list).
// Asserted against the raw response body, since a typed client has no field
// to leak into either way -- only the raw wire body can show the field was
// (or wasn't) actually sent.
func TestWireShape_Cluster_TerminatedAt_NotOnWire(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "terminate-wire-cluster"})
	require.Equal(t, 200, createRec.Code)

	var created struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	termRec := doEMRRequest(t, h, "TerminateJobFlows", map[string]any{"JobFlowIds": []string{created.JobFlowID}})
	require.Equal(t, 200, termRec.Code)

	descRec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": created.JobFlowID})
	require.Equal(t, 200, descRec.Code)

	var raw struct {
		Cluster map[string]any `json:"Cluster"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))
	_, hasTerminatedAt := raw.Cluster["TerminatedAt"]
	assert.False(t, hasTerminatedAt,
		"Cluster must not carry a TerminatedAt field on the wire -- real types.Cluster has no such member")
}

// TestWireShape_Studio_IdcUserAssignment_RoundTrip proves
// CreateStudioInput's IdcUserAssignment and TrustedIdentityPropagationEnabled
// (both real, emr@v1.64.4 api_op_CreateStudio.go) reach DescribeStudio's
// response instead of being silently discarded --
// TrustedIdentityPropagationEnabled already had a wire slot on Studio but
// nothing ever populated it from the request; IdcUserAssignment had no slot
// at all.
func TestWireShape_Studio_IdcUserAssignment_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)
	h := emr.NewHandler(backend)
	client := newTestEMRClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStudio(ctx, &emrsdk.CreateStudioInput{
		Name:                              awssdk.String("idc-studio"),
		AuthMode:                          emrtypes.AuthModeSso,
		DefaultS3Location:                 awssdk.String("s3://bucket/studio"),
		EngineSecurityGroupId:             awssdk.String("sg-eng"),
		ServiceRole:                       awssdk.String("arn:aws:iam::000000000000:role/service"),
		VpcId:                             awssdk.String("vpc-1"),
		WorkspaceSecurityGroupId:          awssdk.String("sg-workspace"),
		SubnetIds:                         []string{"subnet-1"},
		IdcUserAssignment:                 emrtypes.IdcUserAssignmentRequired,
		TrustedIdentityPropagationEnabled: awssdk.Bool(true),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStudio(ctx, &emrsdk.DescribeStudioInput{StudioId: createOut.StudioId})
	require.NoError(t, err)
	require.NotNil(t, descOut.Studio)
	assert.Equal(t, emrtypes.IdcUserAssignmentRequired, descOut.Studio.IdcUserAssignment)
	assert.True(t, awssdk.ToBool(descOut.Studio.TrustedIdentityPropagationEnabled))
}

// TestWireShape_StudioSummary_NoFabricatedFields proves ListStudios' items
// no longer carry StudioArn/DefaultS3Location -- both were invented on
// StudioSummary; the real types.StudioSummary (emr@v1.64.4 deserializers.go's
// awsAwsjson11_deserializeDocumentStudioSummary case list) has neither.
// Asserted against the raw body since a typed client has no field to
// decode either into regardless.
func TestWireShape_StudioSummary_NoFabricatedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "summary-studio",
		"AuthMode":                 "SSO",
		"DefaultS3Location":        "s3://bucket/studio",
		"EngineSecurityGroupId":    "sg-eng",
		"ServiceRole":              "arn:aws:iam::000000000000:role/service",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-workspace",
	})
	require.Equal(t, 200, createRec.Code)

	listRec := doEMRRequest(t, h, "ListStudios", map[string]any{})
	require.Equal(t, 200, listRec.Code)

	var raw struct {
		Studios []map[string]any `json:"Studios"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &raw))
	require.Len(t, raw.Studios, 1)

	_, hasArn := raw.Studios[0]["StudioArn"]
	_, hasLoc := raw.Studios[0]["DefaultS3Location"]
	assert.False(t, hasArn, "StudioSummary must not carry StudioArn -- real StudioSummary has no such member")
	assert.False(t, hasLoc,
		"StudioSummary must not carry DefaultS3Location -- real StudioSummary has no such member")
}

// TestWireShape_RunJobFlow_SessionEnabled_RoundTrip proves
// RunJobFlowInput.SessionEnabled (emr@v1.64.4 api_op_RunJobFlow.go:238-240,
// real, "Indicates whether Spark Connect sessions are enabled on the
// cluster") reaches Cluster.SessionEnabled (types.go:447-448) on read-back
// instead of being silently discarded -- gopherstack previously had no such
// field anywhere in its RunJobFlow input/Cluster output structs at all, so
// a real client's SessionEnabled was dropped by json.Unmarshal (unknown
// field, not an error) and Cluster.SessionEnabled always deserialized nil
// regardless of what was requested. It also proves the other half of real
// StartSession's documented precondition ("The cluster must be in the
// RUNNING or WAITING state and have sessions enabled") is now enforced: a
// cluster launched without SessionEnabled must reject StartSession even
// while WAITING, which it did not before this fix (nothing checked the
// field because the field did not exist).
func TestWireShape_RunJobFlow_SessionEnabled_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sessionEnabled     *bool
		name               string
		wantSessionEnabled bool
		wantStartSessionOK bool
	}{
		{
			name:               "enabled",
			sessionEnabled:     awssdk.Bool(true),
			wantSessionEnabled: true,
			wantStartSessionOK: true,
		},
		{
			name:               "disabled",
			sessionEnabled:     nil,
			wantSessionEnabled: false,
			wantStartSessionOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := emr.NewInMemoryBackend(testAccountID, testRegion)
			h := emr.NewHandler(backend)
			client := newTestEMRClient(t, h)
			ctx := t.Context()

			runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
				Name:           awssdk.String("session-" + tc.name + "-cluster"),
				Instances:      &emrtypes.JobFlowInstancesConfig{},
				SessionEnabled: tc.sessionEnabled,
			})
			require.NoError(t, err)

			descOut, err := client.DescribeCluster(ctx, &emrsdk.DescribeClusterInput{ClusterId: runOut.JobFlowId})
			require.NoError(t, err)
			require.NotNil(t, descOut.Cluster)
			assert.Equal(t, tc.wantSessionEnabled, awssdk.ToBool(descOut.Cluster.SessionEnabled),
				"Cluster.SessionEnabled must round-trip RunJobFlowInput.SessionEnabled, never fabricated")

			_, err = client.StartSession(ctx, &emrsdk.StartSessionInput{ClusterId: runOut.JobFlowId})
			if tc.wantStartSessionOK {
				assert.NoError(t, err, "StartSession must succeed on a cluster launched with SessionEnabled=true")
			} else {
				assert.Error(t, err, "StartSession must reject a cluster launched without SessionEnabled=true")
			}
		})
	}
}

// TestWireShape_DescribePersistentAppUI_RealShape proves
// DescribePersistentAppUI's response uses the real
// types.PersistentAppUI shape (PersistentAppUIId/CreationTime) instead of
// this backend's internal model, which previously leaked
// TargetResourceArn/RuntimeRoleEnabledCluster -- both real members of the
// DIFFERENT CreatePersistentAppUIOutput shape, not
// DescribePersistentAppUIOutput.PersistentAppUI (emr@v1.64.4
// deserializers.go's awsAwsjson11_deserializeDocumentPersistentAppUI case
// list has neither).
func TestWireShape_DescribePersistentAppUI_RealShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "app-ui-cluster"})
	require.Equal(t, 200, createRec.Code)

	var cluster struct {
		JobFlowID  string `json:"JobFlowId"`
		ClusterArn string `json:"ClusterArn"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cluster))

	createUIRec := doEMRRequest(t, h, "CreatePersistentAppUI", map[string]any{
		"TargetResourceArn": cluster.ClusterArn,
	})
	require.Equal(t, 200, createUIRec.Code)

	var createdUI struct {
		PersistentAppUIID string `json:"PersistentAppUIId"`
	}
	require.NoError(t, json.Unmarshal(createUIRec.Body.Bytes(), &createdUI))

	descRec := doEMRRequest(t, h, "DescribePersistentAppUI", map[string]any{
		"PersistentAppUIId": createdUI.PersistentAppUIID,
	})
	require.Equal(t, 200, descRec.Code)

	var raw struct {
		PersistentAppUI map[string]any `json:"PersistentAppUI"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))

	_, hasArn := raw.PersistentAppUI["TargetResourceArn"]
	_, hasRole := raw.PersistentAppUI["RuntimeRoleEnabledCluster"]
	assert.False(t, hasArn,
		"DescribePersistentAppUI's PersistentAppUI must not carry TargetResourceArn -- "+
			"that belongs to CreatePersistentAppUIOutput, a different shape")
	assert.False(t, hasRole,
		"DescribePersistentAppUI's PersistentAppUI must not carry RuntimeRoleEnabledCluster -- "+
			"that belongs to CreatePersistentAppUIOutput, a different shape")
	assert.Equal(t, createdUI.PersistentAppUIID, raw.PersistentAppUI["PersistentAppUIId"])
	assert.NotZero(t, raw.PersistentAppUI["CreationTime"])
}

// TestListNotebookExecutions_ExecutionEngineIdFilter proves
// ListNotebookExecutionsInput.ExecutionEngineId (real query, emr@v1.64.4
// api_op_ListNotebookExecutions.go, serializers.go's "ExecutionEngineId"
// body key) had no field for it at all in gopherstack's request struct --
// EditorId/Status were read but ExecutionEngineId/From/To were silently
// dropped, so a real client's filter on this field always no-op'd.
func TestListNotebookExecutions_ExecutionEngineIdFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestEMRClient(t, h)

	keep, err := client.StartNotebookExecution(t.Context(), &emrsdk.StartNotebookExecutionInput{
		EditorId:    awssdk.String("e-KEEP"),
		ServiceRole: awssdk.String("arn:aws:iam::000000000000:role/notebook-service-role"),
		ExecutionEngine: &emrtypes.ExecutionEngineConfig{
			Id: awssdk.String("j-ENGINE-KEEP"),
		},
	})
	require.NoError(t, err)

	_, err = client.StartNotebookExecution(t.Context(), &emrsdk.StartNotebookExecutionInput{
		EditorId:    awssdk.String("e-DROP"),
		ServiceRole: awssdk.String("arn:aws:iam::000000000000:role/notebook-service-role"),
		ExecutionEngine: &emrtypes.ExecutionEngineConfig{
			Id: awssdk.String("j-ENGINE-DROP"),
		},
	})
	require.NoError(t, err)

	out, err := client.ListNotebookExecutions(t.Context(), &emrsdk.ListNotebookExecutionsInput{
		ExecutionEngineId: awssdk.String("j-ENGINE-KEEP"),
	})
	require.NoError(t, err)
	require.Len(t, out.NotebookExecutions, 1)
	assert.Equal(
		t,
		awssdk.ToString(keep.NotebookExecutionId),
		awssdk.ToString(out.NotebookExecutions[0].NotebookExecutionId),
	)
}

// TestListNotebookExecutions_FromToFilter proves From/To (real query,
// api_op_ListNotebookExecutions.go, epoch-seconds doubles per serializers.go)
// were also silently dropped -- same missing-field defect as
// ExecutionEngineId above.
func TestListNotebookExecutions_FromToFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestEMRClient(t, h)

	started, err := client.StartNotebookExecution(t.Context(), &emrsdk.StartNotebookExecutionInput{
		EditorId:    awssdk.String("e-FROMTO"),
		ServiceRole: awssdk.String("arn:aws:iam::000000000000:role/notebook-service-role"),
		ExecutionEngine: &emrtypes.ExecutionEngineConfig{
			Id: awssdk.String("j-FROMTO"),
		},
	})
	require.NoError(t, err)

	future := time.Now().UTC().Add(time.Hour)

	excluded, err := client.ListNotebookExecutions(t.Context(), &emrsdk.ListNotebookExecutionsInput{
		From: awssdk.Time(future),
	})
	require.NoError(t, err)
	assert.Empty(t, excluded.NotebookExecutions, "From set to the future must exclude an execution started now")

	past := time.Now().UTC().Add(-time.Hour)

	included, err := client.ListNotebookExecutions(t.Context(), &emrsdk.ListNotebookExecutionsInput{
		From: awssdk.Time(past),
		To:   awssdk.Time(future),
	})
	require.NoError(t, err)
	require.Len(t, included.NotebookExecutions, 1)
	assert.Equal(
		t,
		awssdk.ToString(started.NotebookExecutionId),
		awssdk.ToString(included.NotebookExecutions[0].NotebookExecutionId),
	)
}

// TestListReleaseLabels_NextTokenPaginates proves ListReleaseLabels' request
// pagination token round-trips under its real wire key. ListReleaseLabelsInput
// serializes the token as "NextToken" (emr@v1.64.4 serializers.go's
// awsAwsjson11_serializeOpDocumentListReleaseLabelsInput -- object.Key("NextToken")),
// not "Marker" -- the sibling key ListSupportedInstanceTypesInput/Output
// genuinely does use (api_op_ListSupportedInstanceTypes.go), which this handler's
// listReleaseLabelsInput had copy-pasted. A real client's second-page NextToken was
// silently dropped by encoding/json (unknown field ignored), so the second call
// always restarted from the beginning instead of advancing. This test also proves
// MaxResults is honoured (a real, sibling bug: it was parsed but never passed to
// the backend, which paginated at a fixed size of 50 regardless of the caller's
// requested page size).
func TestListReleaseLabels_NextTokenPaginates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestEMRClient(t, h)

	first, err := client.ListReleaseLabels(t.Context(), &emrsdk.ListReleaseLabelsInput{
		MaxResults: awssdk.Int32(5),
	})
	require.NoError(t, err)
	require.Len(t, first.ReleaseLabels, 5, "MaxResults=5 must cap the first page at 5 items")
	require.NotNil(t, first.NextToken)
	require.NotEmpty(t, *first.NextToken, "a page short of the full catalog must return a NextToken")

	second, err := client.ListReleaseLabels(t.Context(), &emrsdk.ListReleaseLabelsInput{
		MaxResults: awssdk.Int32(5),
		NextToken:  first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.ReleaseLabels, 5, "MaxResults=5 must cap the second page at 5 items too")

	seen := make(map[string]bool, 10)
	for _, l := range first.ReleaseLabels {
		seen[l] = true
	}

	for _, l := range second.ReleaseLabels {
		assert.False(t, seen[l], "second page (via NextToken) repeated %q from the first page -- "+
			"NextToken was not actually applied, the listing restarted from the beginning", l)
	}
}

// TestListStudioSessionMappings_MarkerPaginates proves ListStudioSessionMappings
// honours its real Marker pagination token (api_op_ListStudioSessionMappings.go
// declares both a request Marker and a response Marker) instead of silently
// returning every mapping for the studio in one unbounded page -- previously
// neither field existed anywhere in this handler's request or response structs
// at all, unlike every sibling List op in this file (ListStudios, ListSessions),
// which already threaded Marker/NextToken through to pkgs/page.
func TestListStudioSessionMappings_MarkerPaginates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestEMRClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStudio(ctx, &emrsdk.CreateStudioInput{
		Name:                     awssdk.String("mapping-page-studio"),
		AuthMode:                 emrtypes.AuthModeSso,
		DefaultS3Location:        awssdk.String("s3://bucket/studio"),
		EngineSecurityGroupId:    awssdk.String("sg-eng"),
		ServiceRole:              awssdk.String("arn:aws:iam::000000000000:role/service"),
		VpcId:                    awssdk.String("vpc-1"),
		WorkspaceSecurityGroupId: awssdk.String("sg-workspace"),
		SubnetIds:                []string{"subnet-1"},
	})
	require.NoError(t, err)

	const total = 55 // exceeds this op's 50-item page size

	for i := range total {
		_, mapErr := client.CreateStudioSessionMapping(ctx, &emrsdk.CreateStudioSessionMappingInput{
			StudioId:         createOut.StudioId,
			IdentityType:     emrtypes.IdentityTypeUser,
			IdentityId:       awssdk.String(fmt.Sprintf("user-%03d", i)),
			IdentityName:     awssdk.String(fmt.Sprintf("user-%03d", i)),
			SessionPolicyArn: awssdk.String("arn:aws:iam::000000000000:policy/session"),
		})
		require.NoError(t, mapErr)
	}

	first, err := client.ListStudioSessionMappings(ctx, &emrsdk.ListStudioSessionMappingsInput{
		StudioId: createOut.StudioId,
	})
	require.NoError(t, err)
	assert.Less(t, len(first.SessionMappings), total,
		"a single ListStudioSessionMappings page must not return all %d mappings unbounded", total)
	require.NotNil(t, first.Marker)
	require.NotEmpty(t, *first.Marker, "a short page must return a Marker")

	second, err := client.ListStudioSessionMappings(ctx, &emrsdk.ListStudioSessionMappingsInput{
		StudioId: createOut.StudioId,
		Marker:   first.Marker,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, second.SessionMappings)

	seen := make(map[string]bool, len(first.SessionMappings))
	for _, m := range first.SessionMappings {
		seen[awssdk.ToString(m.IdentityId)] = true
	}

	for _, m := range second.SessionMappings {
		assert.False(t, seen[awssdk.ToString(m.IdentityId)],
			"second page (via Marker) repeated %q from the first page", awssdk.ToString(m.IdentityId))
	}
}

// TestListSessions_MaxResultsCapsPage proves ListSessions honours a
// caller-supplied MaxResults (real ListSessionsInput.MaxResults,
// api_op_ListSessions.go: "The maximum number of sessions to return in each
// page of results") instead of always paginating at this backend's fixed
// listSessionsPageSize of 50, the same shape ListReleaseLabels had (see
// TestListReleaseLabels_NextTokenPaginates above) until it was fixed --
// ListSessions was not caught by that same pass.
func TestListSessions_MaxResultsCapsPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestEMRClient(t, h)
	ctx := t.Context()

	clusterOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
		Name:           awssdk.String("list-sessions-page-cluster"),
		Instances:      &emrtypes.JobFlowInstancesConfig{},
		SessionEnabled: awssdk.Bool(true),
	})
	require.NoError(t, err)

	const total = 10

	for i := range total {
		_, sessErr := client.StartSession(ctx, &emrsdk.StartSessionInput{
			ClusterId: clusterOut.JobFlowId,
			Name:      awssdk.String(fmt.Sprintf("session-%03d", i)),
		})
		require.NoError(t, sessErr)
	}

	first, err := client.ListSessions(ctx, &emrsdk.ListSessionsInput{
		ClusterId:  clusterOut.JobFlowId,
		MaxResults: awssdk.Int32(4),
	})
	require.NoError(t, err)
	require.Len(t, first.Sessions, 4, "MaxResults=4 must cap the first page at 4 sessions")
	require.NotNil(t, first.NextToken)
	require.NotEmpty(t, *first.NextToken, "a page short of all %d sessions must return a NextToken", total)

	second, err := client.ListSessions(ctx, &emrsdk.ListSessionsInput{
		ClusterId:  clusterOut.JobFlowId,
		MaxResults: awssdk.Int32(4),
		NextToken:  first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.Sessions, 4, "MaxResults=4 must cap the second page at 4 sessions too")

	seen := make(map[string]bool, len(first.Sessions))
	for _, s := range first.Sessions {
		seen[awssdk.ToString(s.Id)] = true
	}

	for _, s := range second.Sessions {
		assert.False(t, seen[awssdk.ToString(s.Id)],
			"second page (via NextToken) repeated %q from the first page -- "+
				"NextToken was not actually applied, the listing restarted from the beginning", awssdk.ToString(s.Id))
	}
}
